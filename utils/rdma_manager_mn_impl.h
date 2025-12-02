#ifndef RDMA_MANAGER_MN_IMPL_H_
#define RDMA_MANAGER_MN_IMPL_H_

#define MEMORY_NODE

#include "utils/rdma_manager_impl.h"
namespace divftree {

RetStatus RDMA_Manager::EstablishTCPConnections() {
    RetStatus rs = RetStatus::Success();
    FatalAssert(nodes.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "nodes.size() does not match num_nodes. nodes.size()=%lu, num_nodes=%hhu",
                nodes.size(), num_nodes);
    FatalAssert(num_nodes > 1, LOG_TAG_RDMA,
                "num_nodes must be greater than 1. num_nodes=%hhu", num_nodes);
    /* creating the server socket */
    nodes[MEMORY_NODE_ID].socket = socket(AF_INET, SOCK_STREAM, 0);
    if (nodes[MEMORY_NODE_ID].socket < 0) {
        rs = RetStatus::Fail(String("Failed to create server socket with errno %d: %s",
                                    errno, strerror(errno)).ToCStr());
        goto EXIT;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(nodes[MEMORY_NODE_ID].port);
    uint8_t remote_node_id;

    if (bind(nodes[MEMORY_NODE_ID].socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        rs = RetStatus::Fail(String("Failed to bind server socket with errno %d: %s",
                                errno, strerror(errno)).ToCStr());
        goto EXIT;
    }

    if (listen(nodes[MEMORY_NODE_ID].socket, num_nodes - 1) < 0) {
        rs = RetStatus::Fail(String("Failed to listen on server socket with errno %d: %s",
                                errno, strerror(errno)).ToCStr());
        goto EXIT;
    }

    for (uint8_t i = 1; i < num_nodes; ++i) {
        int socket = accept(nodes[MEMORY_NODE_ID].socket, NULL, NULL);
        if (socket < 0) {
            rs = RetStatus::Fail(String("Failed to accept connection on server socket with errno %d: %s",
                                    errno, strerror(errno)).ToCStr());
            goto EXIT;
        }

        remote_node_id = MEMORY_NODE_ID;
        if (recv(socket, &remote_node_id, sizeof(remote_node_id), 0) != sizeof(remote_node_id)) {
            rs = RetStatus::Fail(String("Failed to receive remote node id with errno %d: %s",
                                    errno, strerror(errno)).ToCStr());
            goto EXIT;
        }

        FatalAssert(remote_node_id < num_nodes, LOG_TAG_RDMA,
                    "Received invalid remote node id %hhu", remote_node_id);
        FatalAssert(remote_node_id != MEMORY_NODE_ID, LOG_TAG_RDMA,
                    "Received invalid remote node id %hhu", remote_node_id);
        FatalAssert(nodes[remote_node_id].socket == -1, LOG_TAG_RDMA,
                    "Received duplicate connection from remote node id %hhu", remote_node_id);
        nodes[remote_node_id].socket = socket;

        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
                "Accepted TCP connection from node (node_id=%hhu, ip=%u, port=%hu)",
                remote_node_id, nodes[remote_node_id].ip_addr, nodes[remote_node_id].port);
    }

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

    FatalAssert(self_node_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In MEMORY_NODE build, self_node_id must be %hhu. self_node_id=%hhu",
                MEMORY_NODE_ID, self_node_id);

    HandshakeInfo handshake_info;
    for (uint8_t i = 0; i < num_nodes; ++i) {
        if (i == MEMORY_NODE_ID) {
            continue;
        }

        FillHandshakeInfo(i, handshake_info);
        FatalAssert(nodes[i].socket != -1, LOG_TAG_RDMA,
                    "Socket for node %hhu is invalid (-1) during handshake", i);
        /* send my info to the remote node */
        ssize_t bytes_sent = send(nodes[i].socket, &handshake_info, sizeof(handshake_info), 0);
        if (bytes_sent != sizeof(handshake_info)) {
            rs = RetStatus::Fail(String("Failed to send handshake info to node %hhu. "
                                        "Sent %zd bytes instead of %zu bytes. errno=(%d)%s",
                                        i, bytes_sent, sizeof(handshake_info),
                                        errno, strerror(errno)).ToCStr());
            break;
        }

        /* receive remote node's info */
        ssize_t bytes_received = recv(nodes[i].socket, &handshake_info, sizeof(handshake_info), MSG_WAITALL);
        if (bytes_received != sizeof(handshake_info)) {
            rs = RetStatus::Fail(String("Failed to receive handshake info from node %hhu. "
                                        "Received %zd bytes instead of %zu bytes. errno=(%d)%s",
                                        i, bytes_received, sizeof(handshake_info),
                                        errno, strerror(errno)).ToCStr());
            break;
        }

        ProcessHandshakeInfo(i, handshake_info);

        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
                "Completed handshake with compute node %hhu", i);
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Handshake completed successfully between memory node %hhu and all compute nodes",
            MEMORY_NODE_ID);
    return rs;
}

RetStatus RDMA_Manager::EstablishRDMAConnections() {
    RetStatus rs = RetStatus::Success();
    FatalAssert(nodes.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "nodes.size() does not match num_nodes. nodes.size()=%lu, num_nodes=%hhu",
                nodes.size(), num_nodes);
    FatalAssert(num_nodes > 1, LOG_TAG_RDMA,
                "num_nodes must be greater than 1. num_nodes=%hhu", num_nodes);
    for (uint8_t i = 0; i < num_nodes; ++i) {
        if (i == MEMORY_NODE_ID) {
            continue;
        }

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
}

RetStatus RDMA_Manager::GrabCommBuffer(uint8_t target_node_id, size_t len, BufferInfo& buffer) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in GrabCommBuffer");
    FatalAssert(target_node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid target_node_id %hhu", target_node_id);
    FatalAssert(target_node_id != self_node_id, LOG_TAG_RDMA,
                "Cannot grab communication buffer for self_node_id %hhu", self_node_id);
    FatalAssert(MEMORY_NODE_ID == self_node_id, LOG_TAG_RDMA,
                "Memory node id %hhu does not match self_node_id %hhu",
                MEMORY_NODE_ID, self_node_id);

    memset(&buffer, 0, sizeof(BufferInfo));
    buffer.target_node_id = target_node_id;
    buffer.max_length = COMM_BUFFER_DATA_SIZE;
    size_t num_tries = 0;
    bool go_next_conn = true;
    RetStatus rs = RetStatus::Success();
    while (true) {
        if (num_tries > 0) {
            if (NUM_CONNECTIONS[MN_COMM] * NUM_COMM_BUFFERS_PER_CONNECTION % num_tries == 0) {
                DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_RDMA,
                        "could not grab communication buffer to node %hhu after %zu tries", target_node_id, num_tries);
                usleep(1); /* todo: may need to tune */
            }

            if (num_tries > NUM_CONNECTIONS[MN_COMM]) {
                go_next_conn = !go_next_conn;
            }

            if (go_next_conn) {
                uint8_t new_conn = (buffer.conn_id + 1) % NUM_CONNECTIONS[MN_COMM];
                nodes[target_node_id].mn_comm_connections.curr_conn_idx.
                    compare_exchange_strong(buffer.conn_id, new_conn);
            } else {
                uint8_t new_buff_idx = (buffer.buffer_idx + 1) % NUM_COMM_BUFFERS_PER_CONNECTION;
                nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].curr_buffer_idx.
                    compare_exchange_strong(buffer.buffer_idx, new_buff_idx);
            }
        }

        buffer.conn_id = nodes[target_node_id].mn_comm_connections.curr_conn_idx.load(std::memory_order_acquire) %
                     NUM_CONNECTIONS[MN_COMM];
        buffer.buffer_idx =
            nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].
                curr_buffer_idx.load(std::memory_order_acquire) % NUM_COMM_BUFFERS_PER_CONNECTION;
        ++num_tries;

        bool locked =
            nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
            buffer_lock.TryLock(SX_SHARED);
        if (!locked) {
            continue;
        }

        bool is_free = nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].
                       buffer_info[buffer.buffer_idx].buffer->meta.state.load(std::memory_order_acquire) ==
                       CommBufferState::BUFFER_STATE_READY;
        if (!is_free) {
            nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
                buffer_lock.Unlock();
            continue;
        }

        nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].num_readers.
            fetch_add(1);
        is_free = nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].
                    buffer_info[buffer.buffer_idx].buffer->meta.state.load(std::memory_order_acquire) ==
                    CommBufferState::BUFFER_STATE_READY;
        if (!is_free) {
            uint64_t num_holders =
                nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].
                buffer_info[buffer.buffer_idx].num_readers.fetch_sub(1);
            nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
                buffer_lock.Unlock();
            if (num_holders == 1) {
                rs = FlushCommBuffer(target_node_id, buffer.conn_id, buffer.buffer_idx);
                if (!rs.IsOK()) {
                    return rs;
                }
            }
            continue;
        }

        size_t offset = nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].
                            buffer_info[buffer.buffer_idx].length.fetch_add(len, std::memory_order_acquire);
        if (offset + len > buffer.max_length) {
            nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].
                buffer_info[buffer.buffer_idx].length.fetch_sub(len, std::memory_order_release);
            nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].
                buffer_info[buffer.buffer_idx].buffer->meta.state.store(CommBufferState::BUFFER_STATE_IN_USE,
                                                                        std::memory_order_release);
            uint64_t num_holders =
                nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].
                buffer_info[buffer.buffer_idx].num_readers.fetch_sub(1);
            nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
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
            mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].buffer->data + offset;
        nodes[target_node_id].mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
            buffer->meta.num_requests.fetch_add(1, std::memory_order_release);
        return RetStatus::Success();
    }
}

RetStatus RDMA_Manager::ReleaseCommBuffer(BufferInfo buffer, bool flush) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in ReleaseCommBuffer");
    FatalAssert(buffer.conn_id < NUM_CONNECTIONS[MN_COMM], LOG_TAG_RDMA,
                "Invalid conn_id %hhu in ReleaseCommBuffer", buffer.conn_id);
    FatalAssert(buffer.buffer_idx < NUM_COMM_BUFFERS_PER_CONNECTION, LOG_TAG_RDMA,
                "Invalid buffer_idx %hhu in ReleaseCommBuffer", buffer.buffer_idx);
    FatalAssert(buffer.target_node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid target_node_id %hhu in ReleaseCommBuffer", buffer.target_node_id);
    FatalAssert(buffer.target_node_id != MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In MN build, target_node_id must not be MEMORY_NODE_ID %hhu. Given: %hhu",
                MEMORY_NODE_ID, buffer.target_node_id);
    FatalAssert(buffer.max_length == COMM_BUFFER_DATA_SIZE, LOG_TAG_RDMA,
                "Invalid buffer length %zu in ReleaseCommBuffer", buffer.max_length);
    FatalAssert(buffer.length > 0 && buffer.length <= COMM_BUFFER_DATA_SIZE, LOG_TAG_RDMA,
                "Invalid buffer length %zu in ReleaseCommBuffer", buffer.length);
    FatalAssert(buffer.buffer != nullptr, LOG_TAG_RDMA,
                "Cannot release null buffer in ReleaseCommBuffer");
    threadSelf->SanityCheckLockHeldInModeByMe(&(nodes[buffer.target_node_id].mn_comm_connections.
        connection[buffer.conn_id].buffer_info[buffer.buffer_idx].buffer_lock), SX_SHARED);
    if (flush || reinterpret_cast<uintptr_t>(buffer.buffer) + buffer.length == reinterpret_cast<uintptr_t>(
                 nodes[buffer.target_node_id].mn_comm_connections.connection[buffer.conn_id].
                    buffer_info[buffer.buffer_idx].buffer->data) + buffer.max_length) {
        nodes[buffer.target_node_id].mn_comm_connections.connection[buffer.conn_id].
            buffer_info[buffer.buffer_idx].buffer->meta.state.store(CommBufferState::BUFFER_STATE_IN_USE,
                                                                    std::memory_order_release);
    }
    uint64_t num_holders =
        nodes[buffer.target_node_id].mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
            num_readers.fetch_sub(1);
    nodes[buffer.target_node_id].mn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].
        buffer_lock.Unlock();
    if ((num_holders == 1) && (nodes[buffer.target_node_id].mn_comm_connections.connection[buffer.conn_id].
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
    FatalAssert(conn_id < NUM_CONNECTIONS[MN_COMM], LOG_TAG_RDMA,
                "Invalid conn_id %hhu in FlushCommBuffer", conn_id);
    FatalAssert(buffer_idx < NUM_COMM_BUFFERS_PER_CONNECTION, LOG_TAG_RDMA,
                "Invalid buffer_idx %hhu in FlushCommBuffer", buffer_idx);
    FatalAssert(target_node_id != MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In MN build, target_node_id must not be MEMORY_NODE_ID %hhu. Given: %hhu",
                MEMORY_NODE_ID, target_node_id);
    FatalAssert(nodes[target_node_id].mn_comm_connections.connection[conn_id].
                buffer_info[buffer_idx].buffer->meta.state.load(std::memory_order_acquire) ==
                CommBufferState::BUFFER_STATE_IN_USE,
                LOG_TAG_RDMA,
                "Comm buffer to be flushed is not in IN_USE state in FlushCommBuffer");
    RetStatus rs = RetStatus::Success();

    RDMABuffer rdma_buffer;
    rdma_buffer.local_addr =
        nodes[target_node_id].mn_comm_connections.connection[conn_id].
            buffer_info[buffer_idx].buffer->data;
    rdma_buffer.remote_addr =
        nodes[target_node_id].mn_comm_connections.connection[conn_id].buffer_info[buffer_idx].remote_addr;
    rdma_buffer.length = COMM_BUFFER_SIZE;
    rdma_buffer.buffer_idx = buffer_idx;
    rdma_buffer.remote_node_id = target_node_id;

    nodes[target_node_id].mn_comm_connections.connection[conn_id].buffer_info[buffer_idx].
        buffer_lock.Lock(SX_EXCLUSIVE);
    do {
        rs = RDMAWrite(&rdma_buffer, 1, nodes[target_node_id].mn_comm_connections.connection[conn_id].ctx, 0);
        if (!rs.IsOK() && rs.stat != RetStatus::RDMA_QP_FULL) {
            FatalAssert(false, LOG_TAG_RDMA,
                        "Failed to flush communication buffer to node %hhu: %s",
                        target_node_id, rs.Msg());
            return rs;
        }
    } while (!rs.IsOK());
    nodes[target_node_id].mn_comm_connections.connection[conn_id].buffer_info[buffer_idx].buffer_lock.Unlock();
    /* the state of the buffer is set to ready using RDMAWrite at remote side! */
    return rs;
}

UrgentMessage* RDMA_Manager::BuildUrgentMessage() {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in BuildUrgentMessage");
    UrgentMessage* msg = static_cast<UrgentMessage*>(
        std::aligned_alloc(CACHE_LINE_SIZE, URGENT_MESSAGE_SIZE));
    FatalAssert(msg != nullptr, LOG_TAG_RDMA,
                "Failed to allocate urgent message of size %zu", URGENT_MESSAGE_SIZE);
    msg->Init();
    return msg;
}

void RDMA_Manager::ReleaseUrgentMessage(UrgentMessage* msg) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in BuildUrgentMessage");
    FatalAssert(msg != nullptr, LOG_TAG_RDMA,
                "Cannot release null urgent message");
    std::free(msg);
}

/* For urgent connections we can have one buffer but multiple QPS */
RetStatus RDMA_Manager::SendUrgentMessage(UrgentMessage* msg, uint8_t target_node_id) {
#ifndef MEMORY_NODE
    DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_RDMA, "CNs cannot send urgent messages!");
#endif
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in SendUrgentMessage");
    FatalAssert(msg != nullptr, LOG_TAG_RDMA,
                "Cannot send null urgent message");
    FatalAssert(ALIGNED(msg, CACHE_LINE_SIZE), LOG_TAG_RDMA,
                "Urgent message pointer %p is not 64-byte aligned", msg);
    FatalAssert(msg->meta.detail.length > 0, LOG_TAG_RDMA,
                "Cannot send urgent message with zero length");
    FatalAssert(target_node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid target_node_id %hhu in SendUrgentMessage", target_node_id);
    FatalAssert(target_node_id != self_node_id, LOG_TAG_RDMA,
                "Cannot send urgent message to self_node_id %hhu", self_node_id);
    FatalAssert(msg->meta.detail.length <= URGENT_MESSAGE_SIZE - sizeof(UrgentMessageMeta), LOG_TAG_RDMA,
                "Urgent message length %zu exceeds maximum %zu", msg->meta.detail.length, URGENT_MESSAGE_SIZE);
    RetStatus rs = RetStatus::Success();
    uint64_t read_off = URGENT_MESSAGE_SIZE;
    uint64_t write_off = 0;

    while (true) {
        read_off =
            nodes[target_node_id].mn_urgent_connections.read_off->load(std::memory_order_acquire) % URGENT_BUFFER_SIZE;
        write_off =
            nodes[target_node_id].mn_urgent_connections.write_off.load(std::memory_order_acquire) % URGENT_BUFFER_SIZE;
        while (((write_off + URGENT_MESSAGE_SIZE) % URGENT_BUFFER_SIZE) == read_off) {
            DIVFTREE_YIELD(); /* todo: may need to tune */
        }

        if (!nodes[target_node_id].mn_urgent_connections.write_off.
                compare_exchange_strong(write_off, (write_off + URGENT_MESSAGE_SIZE) % URGENT_BUFFER_SIZE)) {
            DIVFTREE_YIELD(); /* todo: may need to tune */
            continue;
        }

        break;
    }

    FatalAssert(((write_off + URGENT_MESSAGE_SIZE) % URGENT_BUFFER_SIZE) != read_off, LOG_TAG_RDMA,
                "Urgent buffer full condition after successful CAS in SendUrgentMessage to node %hhu",
                target_node_id);


    uint8_t conn_idx = (nodes[target_node_id].mn_urgent_connections.curr_conn_idx.fetch_add(1) - 1) %
                       NUM_CONNECTIONS[MN_URGENT];
    RDMABuffer rdma_buffer;
    rdma_buffer.local_addr = msg;
    rdma_buffer.length = URGENT_MESSAGE_SIZE;
    rdma_buffer.buffer_idx = 0;
    rdma_buffer.remote_node_id = target_node_id;
    rdma_buffer.remote_addr =
            nodes[target_node_id].mn_urgent_connections.remote_buffer + write_off;

    while (true) {
        conn_idx = (conn_idx + 1) % NUM_CONNECTIONS[MN_URGENT];
        rs = RDMAWrite(&rdma_buffer, 1, nodes[target_node_id].mn_urgent_connections.connection[conn_idx],
                       IBV_SEND_INLINE);
        if (rs.IsOK()) {
            break;
        }

        if (rs.stat != RetStatus::RDMA_QP_FULL) {
            FatalAssert(false, LOG_TAG_RDMA,
                        "Failed to send urgent message to node %hhu: %s", target_node_id, rs.Msg());
            return rs;
        }
    }

    FatalAssert(rs.IsOK(), LOG_TAG_RDMA,
                "Failed to send urgent message to node %hhu: %s", target_node_id, rs.Msg());
    return rs;
}

RetStatus RDMA_Manager::PollCommRequests(uint8_t node_id, std::vector<BufferInfo>& request_buffers) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in PollCommRequests");
    FatalAssert(node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid node_id %hhu in PollCommRequests", node_id);
    FatalAssert(node_id != self_node_id, LOG_TAG_RDMA,
                "Cannot poll communication requests for self_node_id %hhu", self_node_id);

    request_buffers.clear();
    request_buffers.reserve(NUM_COMM_BUFFERS_PER_CONNECTION * NUM_CONNECTIONS[CN_COMM]);
    RetStatus rs = RetStatus::Success();
    for (uint8_t conn_id = 0; conn_id < NUM_CONNECTIONS[CN_COMM]; ++conn_id) {
        for (uint8_t buffer_idx = 0; buffer_idx < NUM_COMM_BUFFERS_PER_CONNECTION; ++buffer_idx) {
            CommBufferMeta& meta =
                nodes[node_id].cn_comm_connections.connection[conn_id].buffer_info[buffer_idx].buffer->meta;
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
                nodes[node_id].cn_comm_connections.connection[conn_id].buffer_info[buffer_idx].buffer->data;
            request_buffers.push_back(buffer_info);
        }
    }
}

RetStatus RDMA_Manager::ReleaseCommReciveBuffers(uint8_t node_id, std::vector<BufferInfo>& buffers) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in PollCommRequests");
    FatalAssert(node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid node_id %hhu in PollCommRequests", node_id);
    FatalAssert(node_id != self_node_id, LOG_TAG_RDMA,
                "Cannot poll communication requests for self_node_id %hhu", self_node_id);
    RetStatus rs = RetStatus::Success();

    RDMABuffer* rdma_buffers = new RDMABuffer[buffers.size()];
    for (size_t i = 0; i < buffers.size(); ++i) {
        BufferInfo& buffer = buffers[i];
        FatalAssert(buffer.conn_id < NUM_CONNECTIONS[CN_COMM], LOG_TAG_RDMA,
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
        nodes[node_id].cn_comm_connections.connection[buffer.conn_id].
            buffer_info[buffer.buffer_idx].buffer->meta.state.store(CommBufferState::BUFFER_STATE_READY,
                                                                    std::memory_order_release);
        rdma_buffers[i].local_addr =
            &(nodes[node_id].cn_comm_connections.connection[buffer.conn_id].
            buffer_info[buffer.buffer_idx].buffer->meta.state);
        rdma_buffers[i].length = sizeof(CommBufferState);
        rdma_buffers[i].buffer_idx = 0;
        rdma_buffers[i].remote_node_id = node_id;
        rdma_buffers[i].remote_addr =
            nodes[node_id].cn_comm_connections.connection[buffer.conn_id].buffer_info[buffer.buffer_idx].remote_addr;
    }

    uint8_t conn_id = (nodes[node_id].cn_comm_connections.curr_conn_idx.fetch_add(1) - 1) %
                      NUM_CONNECTIONS[CN_COMM];
    do {
        conn_id = (conn_id + 1) % NUM_CONNECTIONS[CN_COMM];
        rs = RDMAWrite(rdma_buffers, buffers.size(), nodes[node_id].mn_urgent_connections.connection[conn_id],
                       IBV_SEND_INLINE);
        if (!rs.IsOK() && rs.stat != RetStatus::RDMA_QP_FULL) {
            FatalAssert(false, LOG_TAG_RDMA,
                        "Failed to send RDMA Write to node %hhu: %s", node_id, rs.Msg());
            return rs;
        }
    } while (!rs.IsOK());

    return RetStatus::Success();
}

};
#endif