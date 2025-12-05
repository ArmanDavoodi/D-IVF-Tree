#ifndef RDMA_MANAGER_CN_IMPL_H_
#define RDMA_MANAGER_CN_IMPL_H_

#include "utils/rdma_manager_impl.h"
namespace divftree {

RetStatus RDMA_Manager::EstablishTCPConnections() {
    RetStatus rs = RetStatus::Success();
    FatalAssert(nodes.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "nodes.size() does not match num_nodes. nodes.size()=%lu, num_nodes=%hhu",
                nodes.size(), num_nodes);
    FatalAssert(num_nodes > 1, LOG_TAG_RDMA,
                "num_nodes must be greater than 1. num_nodes=%hhu", num_nodes);

    struct sockaddr_in server_addr;
    nodes[MEMORY_NODE_ID].socket = socket(AF_INET, SOCK_STREAM, 0);
    if (nodes[MEMORY_NODE_ID].socket < 0) {
        rs = RetStatus::Fail(String("Failed to create client socket with errno %d: %s",
                                errno, strerror(errno)).ToCStr());
        goto EXIT;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = nodes[MEMORY_NODE_ID].ip_addr;
    server_addr.sin_port = htons(nodes[MEMORY_NODE_ID].port);
    if (connect(nodes[MEMORY_NODE_ID].socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        rs = RetStatus::Fail(String("Failed to connect to server with errno %d: %s",
                                errno, strerror(errno)).ToCStr());
        goto EXIT;
    }

    if (send(nodes[MEMORY_NODE_ID].socket, &self_node_id, sizeof(self_node_id), 0) != sizeof(self_node_id)) {
        rs = RetStatus::Fail(String("Failed to send self node id with errno %d: %s",
                                    errno, strerror(errno)).ToCStr());
        goto EXIT;
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Established TCP connection to memory node (node_id=%hhu, ip=%u, port=%hu)",
            MEMORY_NODE_ID, nodes[MEMORY_NODE_ID].ip_addr, nodes[MEMORY_NODE_ID].port);

EXIT:
    return rs;
}

RetStatus RDMA_Manager::Handshake() {
    RetStatus rs = RetStatus::Success();
    FatalAssert(nodes.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "nodes.size() does not match num_nodes. nodes.size()=%lu, num_nodes=%hhu",
                nodes.size(), num_nodes);
    FatalAssert(num_nodes > 1, LOG_TAG_RDMA,
                "num_nodes must be greater than 1. num_nodes=%hhu", num_nodes);
    FatalAssert(self_node_id != MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In CN build, self_node_id must not be %hhu. self_node_id=%hhu",
                MEMORY_NODE_ID, self_node_id);
    FatalAssert(self_node_id < num_nodes, LOG_TAG_RDMA,
                "self_node_id %hhu is not less than num_nodes %hhu",
                self_node_id, num_nodes);
    HandshakeInfo handshake_info;
    uint8_t i = MEMORY_NODE_ID;
    memset(&handshake_info, 0, sizeof(handshake_info));
    /* receive remote node's info */
    FatalAssert(nodes[i].socket != -1, LOG_TAG_RDMA,
                "Socket for memory node %hhu is invalid (-1) during handshake", i);
    ssize_t bytes_received = recv(nodes[i].socket, &handshake_info, sizeof(handshake_info), MSG_WAITALL);
    if (bytes_received != sizeof(handshake_info)) {
        rs = RetStatus::Fail(String("Failed to receive handshake info from memory node. "
                                    "Received %zd bytes instead of %zu bytes. errno=(%d)%s",
                                    bytes_received, sizeof(handshake_info),
                                    errno, strerror(errno)).ToCStr());
        return rs;
    }
    ProcessHandshakeInfo(i, handshake_info);

    FillHandshakeInfo(i, handshake_info);
    /* send my info to the remote node */
    ssize_t bytes_sent = send(nodes[i].socket, &handshake_info, sizeof(handshake_info), 0);
    if (bytes_sent != sizeof(handshake_info)) {
        rs = RetStatus::Fail(String("Failed to send handshake info to memory node. "
                                    "Sent %zd bytes instead of %zu bytes. errno=(%d)%s",
                                    bytes_sent, sizeof(handshake_info),
                                    errno, strerror(errno)).ToCStr());
        return rs;
    }
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Handshake completed successfully between node %hhu and memory node %hhu",
            self_node_id, MEMORY_NODE_ID);
    return rs;
}

RetStatus RDMA_Manager::EstablishRDMAConnections() {
    RetStatus rs = RetStatus::Success();
    FatalAssert(nodes.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "nodes.size() does not match num_nodes. nodes.size()=%lu, num_nodes=%hhu",
                nodes.size(), num_nodes);
    FatalAssert(num_nodes > 1, LOG_TAG_RDMA,
                "num_nodes must be greater than 1. num_nodes=%hhu", num_nodes);

    uint8_t i = MEMORY_NODE_ID;
    for (size_t j = 0; j < NUM_CONNECTIONS[CN_CLUSTER_READ]; ++j) {
        rs = ModifyQPStateToRTR(nodes[i].cn_cluster_read_connections.connection[j].qp,
                                nodes[i].cn_cluster_read_connections.connection[j].remote_qp_num,
                                nodes[i].remote_gid,
                                port_num,
                                nodes[i].cn_cluster_read_connections.connection[j].remote_psn,
                                CN_CLUSTER_READ);
        if (!rs.IsOK()) {
            DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_RDMA,
                    "Failed to modify CN_CLUSTER_READ QP to RTR for node %hhu connection %zu. Error: %s",
                    i, j, rs.Msg());
            return rs;
        }

        rs = ModifyQPStateToRTS(nodes[i].cn_cluster_read_connections.connection[j].qp,
                                nodes[i].cn_cluster_read_connections.connection[j].local_psn,
                                CN_CLUSTER_READ);
        if (!rs.IsOK()) {
            DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_RDMA,
                    "Failed to modify CN_CLUSTER_READ QP to RTS for node %hhu connection %zu. Error: %s",
                    i, j, rs.Msg());
            return rs;
        }
    }
}

RetStatus RDMA_Manager::GrabCommBuffer(uint8_t target_node_id, size_t len, BufferInfo& buffer) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in GrabCommBuffer");
    FatalAssert(target_node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid target_node_id %hhu", target_node_id);
    FatalAssert(target_node_id != self_node_id, LOG_TAG_RDMA,
                "Cannot grab communication buffer for self_node_id %hhu", self_node_id);
    FatalAssert(target_node_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In CN build, target_node_id must be MEMORY_NODE_ID %hhu. Given: %hhu",
                MEMORY_NODE_ID, target_node_id);

    memset(&buffer, 0, sizeof(BufferInfo));
    buffer.target_node_id = target_node_id;
    buffer.max_length = COMM_BUFFER_DATA_SIZE;
    size_t num_tries = 0;
    bool go_next_conn = true;
    RetStatus rs = RetStatus::Success();
    while (true) {
        if (num_tries > 0) {
            if (NUM_CONNECTIONS[CN_COMM] * NUM_COMM_BUFFERS_PER_CONNECTION % num_tries == 0) {
                DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_RDMA,
                        "could not grab communication buffer to node %hhu after %zu tries", target_node_id, num_tries);
                usleep(1); /* todo: may need to tune */
            }

            if (num_tries > NUM_CONNECTIONS[CN_COMM]) {
                go_next_conn = !go_next_conn;
            }

            if (go_next_conn) {
                uint8_t new_conn = (buffer.conn_id + 1) % NUM_CONNECTIONS[CN_COMM];
                nodes[target_node_id].cn_comm_connections.curr_conn_idx.
                    compare_exchange_strong(buffer.conn_id, new_conn);
            } else {
                uint8_t new_buff_idx = (buffer.buffer_idx + 1) % NUM_COMM_BUFFERS_PER_CONNECTION;
                nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].curr_buffer_idx.
                    compare_exchange_strong(buffer.buffer_idx, new_buff_idx);
            }
        }

        buffer.conn_id = nodes[target_node_id].cn_comm_connections.curr_conn_idx.load(std::memory_order_acquire) %
                     NUM_CONNECTIONS[CN_COMM];
        buffer.buffer_idx =
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                curr_buffer_idx.load(std::memory_order_acquire) % NUM_COMM_BUFFERS_PER_CONNECTION;
        ++num_tries;

        bool locked =
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
            buffer_lock.TryLock(SX_SHARED);
        if (!locked) {
            continue;
        }

        bool is_free = nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                       buffer_info[buffer.buffer_idx].buffer->meta.state.load(std::memory_order_acquire) ==
                       CommBufferState::BUFFER_STATE_READY;
        if (!is_free) {
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
                buffer_lock.Unlock();
            continue;
        }

        nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].num_readers.
            fetch_add(1);
        is_free = nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                    buffer_info[buffer.buffer_idx].buffer->meta.state.load(std::memory_order_acquire) ==
                    CommBufferState::BUFFER_STATE_READY;
        if (!is_free) {
            uint64_t num_holders =
                nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                buffer_info[buffer.buffer_idx].num_readers.fetch_sub(1);
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
                buffer_lock.Unlock();
            if (num_holders == 1) {
                rs = FlushCommBuffer(target_node_id, buffer.conn_id, buffer.buffer_idx);
                if (!rs.IsOK()) {
                    return rs;
                }
            }
            continue;
        }

        size_t offset = nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                            buffer_info[buffer.buffer_idx].length.fetch_add(len, std::memory_order_acquire);
        if (offset + len > buffer.max_length) {
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                buffer_info[buffer.buffer_idx].length.fetch_sub(len, std::memory_order_release);
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                buffer_info[buffer.buffer_idx].buffer->meta.state.store(CommBufferState::BUFFER_STATE_IN_USE,
                                                                 std::memory_order_release);
            uint64_t num_holders =
                nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                buffer_info[buffer.buffer_idx].num_readers.fetch_sub(1);
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
                buffer_lock.Unlock();
            if (num_holders == 1) {
                rs = FlushCommBuffer(target_node_id, buffer.conn_id, buffer.buffer_idx);
                if (!rs.IsOK()) {
                    return rs;
                }
            }
            continue;
        }

        buffer.length = len;
        buffer.buffer = nodes[target_node_id].
            cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].buffer->data + offset;
        nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
            buffer->meta.num_requests.fetch_add(1, std::memory_order_release);
        return RetStatus::Success();
    }
}

RetStatus RDMA_Manager::ReleaseCommBuffer(BufferInfo buffer, bool flush) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in ReleaseCommBuffer");
    FatalAssert(buffer.conn_id < NUM_CONNECTIONS[CN_COMM], LOG_TAG_RDMA,
                "Invalid conn_id %hhu in ReleaseCommBuffer", buffer.conn_id);
    FatalAssert(buffer.buffer_idx < NUM_COMM_BUFFERS_PER_CONNECTION, LOG_TAG_RDMA,
                "Invalid buffer_idx %hhu in ReleaseCommBuffer", buffer.buffer_idx);
    FatalAssert(buffer.target_node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid target_node_id %hhu in ReleaseCommBuffer", buffer.target_node_id);
    FatalAssert(buffer.target_node_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In CN build, target_node_id must be MEMORY_NODE_ID %hhu. Given: %hhu",
                MEMORY_NODE_ID, buffer.target_node_id);
    FatalAssert(buffer.max_length == COMM_BUFFER_DATA_SIZE, LOG_TAG_RDMA,
                "Invalid buffer length %zu in ReleaseCommBuffer", buffer.max_length);
    FatalAssert(buffer.length > 0 && buffer.length <= COMM_BUFFER_DATA_SIZE, LOG_TAG_RDMA,
                "Invalid buffer length %zu in ReleaseCommBuffer", buffer.length);
    FatalAssert(buffer.buffer != nullptr, LOG_TAG_RDMA,
                "Cannot release null buffer in ReleaseCommBuffer");
    threadSelf->SanityCheckLockHeldInModeByMe(&(nodes[buffer.target_node_id].cn_comm_connections.
        connection[buffer.conn_id].buffer_info[buffer.buffer_idx].buffer_lock), SX_SHARED);

    if (flush || reinterpret_cast<uintptr_t>(buffer.buffer) + buffer.length == reinterpret_cast<uintptr_t>(
                 nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].
                    buffer_info[buffer.buffer_idx].buffer->data) + buffer.max_length) {
        nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].
            buffer_info[buffer.buffer_idx].buffer->meta.state.store(CommBufferState::BUFFER_STATE_IN_USE,
                                                                    std::memory_order_release);
    }
    uint64_t num_holders =
        nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
            num_readers.fetch_sub(1);
    nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
        buffer_lock.Unlock();
    if ((num_holders == 1) && (nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].
                               buffer_info[buffer.buffer_idx].buffer->meta.state.load(std::memory_order_acquire) ==
                               CommBufferState::BUFFER_STATE_IN_USE)) {
        return FlushCommBuffer(buffer.target_node_id, buffer.conn_id, buffer.buffer_idx);
    }

    return RetStatus::Success();
}

RetStatus RDMA_Manager::FlushCommBuffer(uint8_t target_node_id, uint8_t conn_id, uint8_t buffer_idx) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in FlushCommBuffer");
    FatalAssert(target_node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid target_node_id %hhu in FlushCommBuffer", target_node_id);
    FatalAssert(conn_id < NUM_CONNECTIONS[CN_COMM], LOG_TAG_RDMA,
                "Invalid conn_id %hhu in FlushCommBuffer", conn_id);
    FatalAssert(buffer_idx < NUM_COMM_BUFFERS_PER_CONNECTION, LOG_TAG_RDMA,
                "Invalid buffer_idx %hhu in FlushCommBuffer", buffer_idx);
    FatalAssert(target_node_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In CN build, target_node_id must be MEMORY_NODE_ID %hhu. Given: %hhu",
                MEMORY_NODE_ID, target_node_id);
    FatalAssert(nodes[target_node_id].cn_comm_connections.connection[conn_id].
                buffer_info[buffer_idx].buffer->meta.state.load(std::memory_order_acquire) ==
                CommBufferState::BUFFER_STATE_IN_USE,
                LOG_TAG_RDMA,
                "Comm buffer to be flushed is not in IN_USE state in FlushCommBuffer");
    RetStatus rs = RetStatus::Success();

    RDMABuffer rdma_buffer;
    rdma_buffer.local_addr =
        nodes[target_node_id].cn_comm_connections.connection[conn_id].
            buffer_info[buffer_idx].buffer->data;
    rdma_buffer.remote_addr =
        nodes[target_node_id].cn_comm_connections.connection[conn_id].buffer_info[buffer_idx].remote_addr;
    rdma_buffer.length = COMM_BUFFER_SIZE;
    rdma_buffer.buffer_idx = buffer_idx;
    rdma_buffer.remote_node_id = target_node_id;

    nodes[target_node_id].cn_comm_connections.connection[conn_id].buffer_info[buffer_idx].
        buffer_lock.Lock(SX_EXCLUSIVE);
    do {
        rs = RDMAWrite(&rdma_buffer, 1, nodes[target_node_id].cn_comm_connections.connection[conn_id].ctx, 0);
        if (!rs.IsOK() && rs.stat != RetStatus::RDMA_QP_FULL) {
            FatalAssert(false, LOG_TAG_RDMA,
                        "Failed to flush communication buffer to node %hhu: %s",
                        target_node_id, rs.Msg());
            return rs;
        }
    } while (!rs.IsOK());
    nodes[target_node_id].cn_comm_connections.connection[conn_id].buffer_info[buffer_idx].buffer_lock.Unlock();
    /* the state of the buffer is set to ready using RDMAWrite at remote side! */
    return rs;
}

RetStatus RDMA_Manager::RDMAWrite(RDMABuffer* rdma_buffers, size_t num_buffers) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in RDMAWrite");
    FatalAssert(rdma_buffers != nullptr, LOG_TAG_RDMA,
                "rdma_buffers is null in RDMAWrite");
    FatalAssert(num_buffers > 0, LOG_TAG_RDMA,
                "num_buffers is zero in RDMAWrite");
    RetStatus rs = RetStatus::Success();

    /* for now we only support RDMA write to communication buffers */
    uint8_t target_node_id = rdma_buffers[0].remote_node_id;
    FatalAssert(target_node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid target_node_id %hhu in RDMAWrite", target_node_id);
    FatalAssert(target_node_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In CN build, target_node_id must be MEMORY_NODE_ID %hhu. Given: %hhu",
                MEMORY_NODE_ID, target_node_id);


    uint8_t conn_id = (nodes[target_node_id].cn_cluster_write_connections.curr_conn_idx.fetch_add(1) - 1) %
                      NUM_CONNECTIONS[CN_CLUSTER_WRITE];
    do {
        conn_id = (conn_id + 1) % NUM_CONNECTIONS[CN_CLUSTER_WRITE];
        rs = RDMAWrite(rdma_buffers, num_buffers,
                       nodes[target_node_id].cn_cluster_write_connections.connection[conn_id], 0);
        if (!rs.IsOK() && rs.stat != RetStatus::RDMA_QP_FULL) {
            FatalAssert(false, LOG_TAG_RDMA,
                        "Failed to send RDMA Write to node %hhu: %s", target_node_id, rs.Msg());
            return rs;
        }
    } while (!rs.IsOK());

    return rs;
}

RetStatus RDMA_Manager::RDMARead(RDMABuffer* rdma_buffers, size_t num_buffers, ConnTaskId& task_id) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in RDMARead");
    FatalAssert(rdma_buffers != nullptr, LOG_TAG_RDMA,
                "rdma_buffers is null in RDMARead");
    FatalAssert(num_buffers > 0, LOG_TAG_RDMA,
                "num_buffers is zero in RDMARead");
    RetStatus rs = RetStatus::Success();

    /* for now we only support RDMA write to communication buffers */
    uint8_t target_node_id = rdma_buffers[0].remote_node_id;
    FatalAssert(target_node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid target_node_id %hhu in RDMARead", target_node_id);
    FatalAssert(target_node_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In CN build, target_node_id must be MEMORY_NODE_ID %hhu. Given: %hhu",
                MEMORY_NODE_ID, target_node_id);


    uint8_t conn_id = (nodes[target_node_id].cn_cluster_read_connections.curr_conn_idx.fetch_add(1) - 1) %
                      NUM_CONNECTIONS[CN_CLUSTER_READ];
    do {
        conn_id = (conn_id + 1) % NUM_CONNECTIONS[CN_CLUSTER_READ];
        rs = RDMARead(rdma_buffers, num_buffers,
                      nodes[target_node_id].cn_cluster_read_connections.connection[conn_id], task_id);
        if (!rs.IsOK() && rs.stat != RetStatus::RDMA_QP_FULL) {
            FatalAssert(false, LOG_TAG_RDMA,
                        "Failed to send RDMA Read to node %hhu: %s", target_node_id, rs.Msg());
            return rs;
        }
    } while (!rs.IsOK());

    return rs;
}

RetStatus RDMA_Manager::PollCompletion(ConnectionType conn_type, std::vector<ConnTaskId>& completed_task_ids) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in PollCompletion");
    FatalAssert(conn_type == CN_CLUSTER_READ, LOG_TAG_RDMA,
                "Invalid ConnectionType %d in PollCompletion", static_cast<int>(conn_type));
    RetStatus rs = RetStatus::Success();

    /* todo: may need to use lower numbers */
    completed_task_ids.clear();
    completed_task_ids.reserve(MAX_SEND_WR[CN_CLUSTER_READ]);
    struct ibv_wc wc[MAX_SEND_WR[CN_CLUSTER_READ]];
    int num_comp = 0;
    constexpr uint8_t target_node_id = MEMORY_NODE_ID;
    for (size_t conn = 0; conn < NUM_CONNECTIONS[conn_type]; ++conn) {
        ConnectionContext& conn_ctx = nodes[target_node_id].
            cn_cluster_read_connections.connection[conn];
        uint16_t num_outstanding = conn_ctx.num_pending_requests.load();
        if (num_outstanding == 0) {
            continue;
        }

        num_comp = ibv_poll_cq(conn_ctx.cq, num_outstanding, wc);
        if (num_comp == 0) {
            break;
        }

        if (num_comp < 0) {
            String error_msg =
                String("Failed to poll Completion Queue for connection type %d. num_comp=(%d)%s errno=(%d)%s",
                        static_cast<int>(conn_type), num_comp, strerror(-num_comp), errno, strerror(errno));
            FatalAssert(false, LOG_TAG_RDMA, "%s", error_msg.ToCStr());
            rs = RetStatus::Fail(error_msg.ToCStr());
            return rs;
        }

        for (int i = 0; i < num_comp; ++i) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                String error_msg = String("RDMA operation failed in Completion Queue. wc_status=(%d)%s wc_wr_id=%lu",
                                            wc[i].status, ibv_wc_status_str(wc[i].status), wc[i].wr_id);
                FatalAssert(false, LOG_TAG_RDMA, "%s", error_msg.ToCStr());
                rs = RetStatus::Fail(error_msg.ToCStr());
                return rs;
            }

            completed_task_ids.emplace_back(wc[i].wr_id);
        }
        conn_ctx.num_pending_requests.fetch_sub(num_comp);
    }

    return rs;
}

RetStatus RDMA_Manager::PollUrgentMessages(std::vector<std::pair<UrgentMessageData, uint8_t>>& messages) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in PollUrgentMessages");
    RetStatus rs = RetStatus::Success();
    messages.clear();

    if (!urgent_msg_lock.TryLock(SX_EXCLUSIVE)) {
        return rs;
    }

    messages.reserve(UrgentBuffer::MAX_URGENT_MESSAGES);
    constexpr uint8_t target_node_id = MEMORY_NODE_ID;
    size_t start_idx = nodes[target_node_id].mn_urgent_connections.read_off;
    size_t end_idx = start_idx;
    std::atomic_thread_fence(std::memory_order_acquire);
    for(; nodes[target_node_id].mn_urgent_connections.buffer->messages[end_idx].IsUnseenValid();
        end_idx = (end_idx + 1) % UrgentBuffer::MAX_URGENT_MESSAGES) {
        UrgentMessage& message = nodes[target_node_id].mn_urgent_connections.buffer->messages[end_idx];
        uint8_t len = message.meta.detail.length;
        /* todo: we are doing a memcpy here -> is there a way to avoid this while letting the MN use the buffer? */
        messages.emplace_back(UrgentMessageData(), len);
        memcpy(messages.back().first.data, message.data.data, len);
        message.SetSeen();
    }

    std::atomic_thread_fence(std::memory_order_release);

    nodes[target_node_id].mn_urgent_connections.read_off = end_idx;
    uint8_t conn_id = (nodes[target_node_id].mn_urgent_connections.curr_conn_idx.fetch_add(1) - 1) %
                      NUM_CONNECTIONS[MN_URGENT];
    RDMABuffer rdma_buffer;
    rdma_buffer.local_addr = &end_idx;
    rdma_buffer.length = sizeof(end_idx);
    rdma_buffer.buffer_idx = 0;
    rdma_buffer.remote_node_id = target_node_id;
    rdma_buffer.remote_addr =
        nodes[target_node_id].mn_urgent_connections.remote_buffer;

    do {
        conn_id = (conn_id + 1) % NUM_CONNECTIONS[MN_URGENT];
        rs = RDMAWrite(&rdma_buffer, 1, nodes[target_node_id].mn_urgent_connections.connection[conn_id],
                       IBV_SEND_INLINE);
        if (!rs.IsOK() && rs.stat != RetStatus::RDMA_QP_FULL) {
            FatalAssert(false, LOG_TAG_RDMA,
                        "Failed to send RDMA Write to node %hhu: %s", target_node_id, rs.Msg());
            break;
        }
    } while (!rs.IsOK());

    urgent_msg_lock.Unlock();
    return rs;
}

RetStatus RDMA_Manager::PollCommRequests(uint8_t node_id, std::vector<BufferInfo>& request_buffers) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in PollCommRequests");
    FatalAssert(node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid node_id %hhu in PollCommRequests", node_id);
    FatalAssert(node_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In CN build, node_id must be MEMORY_NODE_ID %hhu. Given: %hhu",
                MEMORY_NODE_ID, node_id);

    request_buffers.clear();

    if (!nodes[node_id].comm_path_lock.TryLock(SX_EXCLUSIVE)) {
        return RetStatus::Success();
    }

    request_buffers.reserve(NUM_COMM_BUFFERS_PER_CONNECTION * NUM_CONNECTIONS[MN_COMM]);
    RetStatus rs = RetStatus::Success();
    for (uint8_t conn_id = 0; conn_id < NUM_CONNECTIONS[MN_COMM]; ++conn_id) {
        for (uint8_t buffer_idx = 0; buffer_idx < NUM_COMM_BUFFERS_PER_CONNECTION; ++buffer_idx) {
            CommBufferMeta& meta =
                nodes[node_id].mn_comm_connections.connection[conn_id].buffer_info[buffer_idx].buffer->meta;
            if (meta.state.load(std::memory_order_acquire) == CommBufferState::BUFFER_STATE_READY) {
                continue;
            }

            BufferInfo buffer_info;
            buffer_info.conn_id = conn_id;
            buffer_info.buffer_idx = buffer_idx;
            buffer_info.target_node_id = node_id;
            buffer_info.max_length = COMM_BUFFER_DATA_SIZE;
            buffer_info.num_requests = meta.num_requests.load(std::memory_order_acquire);
            buffer_info.buffer =
                nodes[node_id].mn_comm_connections.connection[conn_id].buffer_info[buffer_idx].buffer->data;
            request_buffers.push_back(buffer_info);
        }
    }

    return rs;
}

RetStatus RDMA_Manager::ReleaseCommReciveBuffers(uint8_t node_id, std::vector<BufferInfo>& buffers) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in PollCommRequests");
    FatalAssert(node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid node_id %hhu in PollCommRequests", node_id);
    FatalAssert(node_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In CN build, node_id must be MEMORY_NODE_ID %hhu. Given: %hhu",
                MEMORY_NODE_ID, node_id);
    threadSelf->SanityCheckLockHeldInModeByMe(&nodes[node_id].comm_path_lock, SX_EXCLUSIVE);
    RetStatus rs = RetStatus::Success();
    if (buffers.empty()) {
        nodes[node_id].comm_path_lock.Unlock();
        return rs;
    }

    RDMABuffer* rdma_buffers = new RDMABuffer[buffers.size()];
    for (size_t i = 0; i < buffers.size(); ++i) {
        BufferInfo& buffer = buffers[i];
        FatalAssert(buffer.conn_id < NUM_CONNECTIONS[MN_COMM], LOG_TAG_RDMA,
                    "Invalid conn_id %hhu in ReleaseCommReciveBuffers", buffer.conn_id);
        FatalAssert(buffer.buffer_idx < NUM_COMM_BUFFERS_PER_CONNECTION, LOG_TAG_RDMA,
                    "Invalid buffer_idx %hhu in ReleaseCommReciveBuffers", buffer.buffer_idx);
        FatalAssert(buffer.target_node_id == node_id, LOG_TAG_RDMA,
                    "Mismatched target_node_id %hhu in ReleaseCommReciveBuffers, expected %hhu",
                    buffer.target_node_id, node_id);
        FatalAssert(buffer.max_length == COMM_BUFFER_DATA_SIZE, LOG_TAG_RDMA,
                    "Invalid buffer length %zu in ReleaseCommReciveBuffers", buffer.max_length);
        FatalAssert(buffer.buffer != nullptr, LOG_TAG_RDMA,
                    "Cannot release null buffer in ReleaseCommReciveBuffers");
        nodes[node_id].mn_comm_connections.connection[buffer.conn_id].
            buffer_info[buffer.buffer_idx].buffer->meta.state.store(CommBufferState::BUFFER_STATE_READY,
                                                                    std::memory_order_release);
        rdma_buffers[i].local_addr =
            &(nodes[node_id].mn_comm_connections.connection[buffer.conn_id].
            buffer_info[buffer.buffer_idx].buffer->meta.state);
        rdma_buffers[i].length = sizeof(CommBufferState);
        rdma_buffers[i].buffer_idx = 0;
        rdma_buffers[i].remote_node_id = node_id;
        rdma_buffers[i].remote_addr =
            nodes[node_id].mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].remote_addr;
    }

    uint8_t conn_id = (nodes[node_id].mn_comm_connections.curr_conn_idx.fetch_add(1) - 1) %
                      NUM_CONNECTIONS[MN_COMM];
    do {
        conn_id = (conn_id + 1) % NUM_CONNECTIONS[MN_COMM];
        rs = RDMAWrite(rdma_buffers, buffers.size(), nodes[node_id].mn_urgent_connections.connection[conn_id],
                       IBV_SEND_INLINE);
        if (!rs.IsOK() && rs.stat != RetStatus::RDMA_QP_FULL) {
            FatalAssert(false, LOG_TAG_RDMA,
                        "Failed to send RDMA Write to node %hhu: %s", node_id, rs.Msg());
            break;
        }
    } while (!rs.IsOK());

    nodes[node_id].comm_path_lock.Unlock();
    delete[] rdma_buffers;

    return rs;
}

};
#endif