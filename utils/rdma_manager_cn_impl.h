#ifndef RDMA_MANAGER_IMPL_H_
#define RDMA_MANAGER_IMPL_H_

#include "utils/rdma_manager.h"

namespace divftree {

NodeInfo::NodeInfo(uint8_t id, uint32_t ip, uint16_t p) :
    node_id{id}, ip_addr{ip}, port{p}, socket{-1} {
    memset(&remote_gid, 0, sizeof(remote_gid));
}

uint32_t NodeInfo::GetKey(uintptr_t addr, size_t length) const {
    if (remoteMemoryRegions.empty()) {
        DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_RDMA,
                "No memory region found for addr=0x%lx, length=%lu on node %u",
                addr, length, node_id);
    }

    auto it = remoteMemoryRegions.upper_bound(addr);
    if (it == remoteMemoryRegions.begin()) {
        DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_RDMA,
                "No memory region found for addr=0x%lx, length=%lu on node %u",
                addr, length, node_id);
    }

    --it;
    uintptr_t region_start = it->second.addr;
    uintptr_t region_end = region_start + it->second.length;
    if ((addr >= region_start) && ((addr + length) <= region_end)) {
        return it->second.rkey;
    }

    DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_RDMA,
            "No memory region found for addr=0x%lx, length=%lu on node %u",
            addr, length, node_id);
}

void NodeInfo::AddRegion(uintptr_t addr, size_t length, uint32_t rkey) {
    FatalAssert(addr != 0, LOG_TAG_RDMA,
                "Cannot add memory region with null address on node %u", node_id);
    FatalAssert(length > 0, LOG_TAG_RDMA,
                "Cannot add memory region with zero length on node %u: addr=0x%lx", node_id, addr);
    FatalAssert(addr + length > addr, LOG_TAG_RDMA,
                "Address overflow detected when adding memory region on node %u: addr=0x%lx, length=%lu",
                node_id, addr, length);
    SANITY_CHECK({
        if (!remoteMemoryRegions.empty()) {
            auto it = remoteMemoryRegions.upper_bound(addr);
            if (it != remoteMemoryRegions.end()) {
                uintptr_t region_start = it->second.addr;
                FatalAssert((addr + length) <= region_start, LOG_TAG_RDMA,
                            "Overlapping memory regions on node %u: new region (0x%lx, 0x%lx):%lu"
                            " overlaps with existing region (0x%lx, 0x%lx):%lu",
                            node_id, addr, addr + length, length, region_start, region_start + it->second.length,
                            it->second.length);
            }

            if (it != remoteMemoryRegions.begin()) {
                --it;
                uintptr_t region_end = it->second.addr + it->second.length;
                FatalAssert(region_end <= addr, LOG_TAG_RDMA,
                            "Overlapping memory regions on node %u: new region (0x%lx, 0x%lx):%lu"
                            " overlaps with existing region (0x%lx, 0x%lx):%lu",
                            node_id, addr, addr + length, length, it->second.addr, region_end,
                            it->second.length);
            }
        }
    });
    remoteMemoryRegions.emplace(addr, RemoteMemoryRegion{addr, length, rkey});
}


RetStatus RDMA_Manager::CreateQP(struct ibv_qp** qp, struct ibv_cq* cq, struct ibv_pd* pd, ConnectionType type) {
    CHECK_NOT_NULLPTR(qp, LOG_TAG_RDMA);
    CHECK_NOT_NULLPTR(cq, LOG_TAG_RDMA);
    CHECK_NOT_NULLPTR(pd, LOG_TAG_RDMA);
    struct ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.qp_type = IBV_QPT_RC; /* Reliable Connection */
    qp_init_attr.cap.max_send_wr = MAX_SEND_WR[type]; /* max outstanding send requests */
    qp_init_attr.cap.max_recv_wr = MAX_RECV_WR[type]; /* max outstanding recv requests */
    qp_init_attr.cap.max_send_sge = MAX_SEND_SGE[type];   /* max scatter/gather elements in a send request */
    qp_init_attr.cap.max_recv_sge = MAX_RECV_SGE[type];   /* max scatter/gather elements in a recv request */
    qp_init_attr.cap.max_inline_data = MAX_INLINE_DATA[type]; /* max size of inline data */
    qp_init_attr.sq_sig_all = 0; /* we should not create wc for all send requests */

    *qp = ibv_create_qp(pd, &qp_init_attr);
    if (*qp == nullptr) {
        return RetStatus::Fail(String("Failed to create Queue Pair. errno=(%d)%s",
                                        errno, strerror(errno)).ToCStr());
    }
    return RetStatus::Success();
}

RetStatus RDMA_Manager::ModifyQPStateToReset(struct ibv_qp* qp) {
    CHECK_NOT_NULLPTR(qp, LOG_TAG_RDMA);
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.qp_state = IBV_QPS_RESET;

    int flags = IBV_QP_STATE;
    int ret = ibv_modify_qp(qp, &qp_attr, flags);
    if (ret != 0) {
        return RetStatus::Fail(String("Failed to modify QP to RESET state. ret=(%d)%s errno=(%d)%s",
                                        ret, strerror(ret), errno, strerror(errno)).ToCStr());
    }
    return RetStatus::Success();
}

RetStatus RDMA_Manager::ModifyQPStateToInit(struct ibv_qp* qp, uint8_t port_num) {
    CHECK_NOT_NULLPTR(qp, LOG_TAG_RDMA);
    struct ibv_qp_attr qp_attr;
    int ret = 0;
    memset(&qp_attr, 0, sizeof(qp_attr));
    SANITY_CHECK(
        ret = ibv_query_qp(qp, &qp_attr,
                            IBV_QP_STATE,
                            nullptr);
        FatalAssert(ret == 0, LOG_TAG_RDMA,
                    "Failed to query QP state before modifying to INIT. ret=(%d)%s errno=(%d)%s",
                    ret, strerror(ret), errno, strerror(errno));
        FatalAssert(qp_attr.qp_state == IBV_QPS_RESET, LOG_TAG_RDMA,
                    "QP is not in RESET state before modifying to INIT. current_state=%d",
                    qp_attr.qp_state);
        memset(&qp_attr, 0, sizeof(qp_attr));
    );
    qp_attr.qp_state = IBV_QPS_INIT;
    qp_attr.port_num = port_num;
    qp_attr.pkey_index = 0;
    qp_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                                IBV_ACCESS_REMOTE_READ |
                                IBV_ACCESS_REMOTE_WRITE |
                                IBV_ACCESS_REMOTE_ATOMIC;

    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    ret = ibv_modify_qp(qp, &qp_attr, flags);
    if (ret != 0) {
        return RetStatus::Fail(String("Failed to modify QP to INIT state. ret=(%d)%s errno=(%d)%s",
                                        ret, strerror(ret), errno, strerror(errno)).ToCStr());
    }
    return RetStatus::Success();
}

RetStatus RDMA_Manager::ModifyQPStateToRTR(struct ibv_qp* qp, uint32_t dest_qp_num,
                                    const union ibv_gid& dest_gid,
                                    uint8_t port_num, uint32_t remote_psn, ConnectionType type) {
    CHECK_NOT_NULLPTR(qp, LOG_TAG_RDMA);
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    int ret = 0;
    SANITY_CHECK(
        ret = ibv_query_qp(qp, &qp_attr,
                            IBV_QP_STATE,
                            nullptr);
        FatalAssert(ret == 0, LOG_TAG_RDMA,
                    "Failed to query QP state before modifying to RTR. ret=(%d)%s errno=(%d)%s",
                    ret, strerror(ret), errno, strerror(errno));
        FatalAssert(qp_attr.qp_state == IBV_QPS_INIT, LOG_TAG_RDMA,
                    "QP is not in INIT state before modifying to RTR. current_state=%d",
                    qp_attr.qp_state);
        memset(&qp_attr, 0, sizeof(qp_attr));
    );

    qp_attr.qp_state = IBV_QPS_RTR;
    qp_attr.path_mtu = DEFAULT_MTU[type];
    qp_attr.dest_qp_num = dest_qp_num;
    qp_attr.rq_psn = remote_psn;
    qp_attr.max_dest_rd_atomic = MAX_RD_ATOMIC[type];
    qp_attr.min_rnr_timer = 1;
    qp_attr.ah_attr.is_global = 1;
    qp_attr.ah_attr.dlid = 0;
    qp_attr.ah_attr.sl = 0;
    qp_attr.ah_attr.src_path_bits = 0;
    qp_attr.ah_attr.port_num = port_num;
    memcpy(&qp_attr.ah_attr.grh.dgid, &dest_gid, sizeof(dest_gid));
    qp_attr.ah_attr.grh.flow_label = 0;
    qp_attr.ah_attr.grh.hop_limit = 1;
    qp_attr.ah_attr.grh.sgid_index = 0;
    qp_attr.ah_attr.grh.traffic_class = 0;

    int flags = IBV_QP_STATE |
                IBV_QP_AV |
                IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN |
                IBV_QP_MAX_DEST_RD_ATOMIC |
                IBV_QP_MIN_RNR_TIMER;
    int ret = ibv_modify_qp(qp, &qp_attr, flags);
    if (ret != 0) {
        return RetStatus::Fail(String("Failed to modify QP to RTR state. ret=(%d)%s errno=(%d)%s",
                                        ret, strerror(ret), errno, strerror(errno)).ToCStr());
    }
    return RetStatus::Success();
}

RetStatus RDMA_Manager::ModifyQPStateToRTS(struct ibv_qp* qp, uint32_t local_psn, ConnectionType type) {
    CHECK_NOT_NULLPTR(qp, LOG_TAG_RDMA);
    struct ibv_qp_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    int ret = 0;
    SANITY_CHECK(
        ret = ibv_query_qp(qp, &qp_attr,
                            IBV_QP_STATE,
                            nullptr);
        FatalAssert(ret == 0, LOG_TAG_RDMA,
                    "Failed to query QP state before modifying to RTS. ret=(%d)%s errno=(%d)%s",
                    ret, strerror(ret), errno, strerror(errno));
        FatalAssert(qp_attr.qp_state == IBV_QPS_RTR, LOG_TAG_RDMA,
                    "QP is not in RTR state before modifying to RTS. current_state=%d",
                    qp_attr.qp_state);
        memset(&qp_attr, 0, sizeof(qp_attr));
    );

    qp_attr.qp_state = IBV_QPS_RTS;
    qp_attr.sq_psn = local_psn;
    qp_attr.timeout = 14; /* todo: infinite retry -> set to something better later */
    qp_attr.retry_cnt = 7; /* todo: infinite retry -> set to something better later */
    qp_attr.rnr_retry = 7; /* todo: infinite retry -> set to something better later */
    qp_attr.max_rd_atomic = MAX_RD_ATOMIC[type];

    int flags = IBV_QP_STATE |
                IBV_QP_SQ_PSN |
                IBV_QP_TIMEOUT |
                IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY |
                IBV_QP_MAX_QP_RD_ATOMIC;
    int ret = ibv_modify_qp(qp, &qp_attr, flags);
    if (ret != 0) {
        return RetStatus::Fail(String("Failed to modify QP to RTS state. ret=(%d)%s errno=(%d)%s",
                                        ret, strerror(ret), errno, strerror(errno)).ToCStr());
    }

    SANITY_CHECK(
        struct ibv_qp_attr qp_attr_dummy;
        memset(&qp_attr_dummy, 0, sizeof(qp_attr_dummy));
        ret = ibv_query_qp(qp, &qp_attr_dummy,
                            IBV_QP_STATE,
                            nullptr);
        FatalAssert(ret == 0, LOG_TAG_RDMA,
                    "Failed to query QP state after modifying to RTS. ret=(%d)%s errno=(%d)%s",
                    ret, strerror(ret), errno, strerror(errno));
        FatalAssert(qp_attr_dummy.qp_state == IBV_QPS_RTS, LOG_TAG_RDMA,
                    "QP is not in RTS state after modifying to RTS. current_state=%d",
                    qp_attr_dummy.qp_state);
    )
    return RetStatus::Success();
}

template<typename ConnCtxType>
RetStatus RDMA_Manager::InitConnectionCtx(ConnectionType conn_type, struct ibv_cq* cq, ConnCtxType* connections,
                                          void*& next_buffer) {
    String error_msg;
    RetStatus rs = RetStatus::Success();
    struct ibv_qp* qp = nullptr;
    FatalAssert(cq != nullptr, LOG_TAG_RDMA,
                "cq cannot be nullptr");
    FatalAssert(pd != nullptr, LOG_TAG_RDMA,
                "pd cannot be nullptr");
    FatalAssert(connections != nullptr, LOG_TAG_RDMA,
                "connections cannot be nullptr");
    FatalAssert(next_buffer != nullptr, LOG_TAG_RDMA,
                "next_buffer cannot be nullptr");

    for (uint8_t conn_id = 0; conn_id < (uint8_t)(NUM_CONNECTIONS[conn_type]); ++conn_id) {
        if (conn_type == CN_COMM || conn_type == MN_COMM) {
            FatalAssert(ALIGNED(next_buffer, CACHE_LINE_SIZE), LOG_TAG_RDMA,
                "next_buffer must be aligned to CACHE_LINE_SIZE");
            CommConnectionContext* comm_ctx = reinterpret_cast<CommConnectionContext*>(&connections[conn_id]);
            comm_ctx->comm_buffers = reinterpret_cast<CommBuffer*>(next_buffer);
            next_buffer = reinterpret_cast<void*>(
                reinterpret_cast<uintptr_t>(next_buffer) + COMM_BUFFER_SIZE * NUM_COMM_BUFFERS_PER_CONNECTION);
            comm_ctx->ctx.local_psn = threadSelf->UniformRange32(0, (uint32_t)(1 << 24) - 1);
        } else if (conn_type == MN_URGENT) {
            FatalAssert(ALIGNED(next_buffer, CACHE_LINE_SIZE), LOG_TAG_RDMA,
                "next_buffer must be aligned to CACHE_LINE_SIZE");
            UrgentConnectionContext* urgent_ctx = reinterpret_cast<UrgentConnectionContext*>(&connections[conn_id]);
            urgent_ctx->urgent_buffers = reinterpret_cast<UrgentBuffer*>(next_buffer);
            next_buffer = reinterpret_cast<void*>(
                reinterpret_cast<uintptr_t>(next_buffer) + URGENT_BUFFER_SIZE);
            urgent_ctx->ctx.local_psn = threadSelf->UniformRange32(0, (uint32_t)(1 << 24) - 1);
        } else {
            connections[conn_id].local_psn = threadSelf->UniformRange32(0, (uint32_t)(1 << 24) - 1);
        }

        if (!rs.IsOK()) {
            break;
        }
        rs = CreateQP(&qp, cq, pd, conn_type);
        if (!rs.IsOK() || qp == nullptr) {
            error_msg = String("Failed to create QP for connection type %s with id %hhu. Error: %s",
                                ToString(conn_type).ToCStr(), conn_id, rs.Msg());
            rs = RetStatus::Fail(error_msg.ToCStr());
            break;
        }

        rs = ModifyQPStateToReset(qp);
        if (!rs.IsOK()) {
            error_msg = String("Failed to modify QP state to RESET for connection %hhu. Error: %s",
                                conn_id, rs.Msg());
            rs = RetStatus::Fail(error_msg.ToCStr());
            break;
        }

        rs = ModifyQPStateToInit(qp, port_num);
        if (!rs.IsOK()) {
            error_msg = String("Failed to modify QP state to INIT for connection %hhu. Error: %s",
                                conn_id, rs.Msg());
            rs = RetStatus::Fail(error_msg.ToCStr());
            break;
        }

        connections[conn_id].Init(conn_type, conn_id, qp, cq);
    }

    return rs;
}

RetStatus RDMA_Manager::EstablishTCPConnections() {
    RetStatus rs = RetStatus::Success();
    FatalAssert(nodes.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "nodes.size() does not match num_nodes. nodes.size()=%lu, num_nodes=%hhu",
                nodes.size(), num_nodes);
    FatalAssert(num_nodes > 1, LOG_TAG_RDMA,
                "num_nodes must be greater than 1. num_nodes=%hhu", num_nodes);

#ifdef MEMORY_NODE
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
#else

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
#endif

EXIT:
    return rs;
}

void RDMA_Manager::FillHandshakeInfo(uint8_t node_id, HandshakeInfo& handshake_info) {
    memset(&handshake_info, 0, sizeof(handshake_info));
    memcpy(&handshake_info.gid, &dev_gid, sizeof(dev_gid));
    auto i = localMemoryRegions.find(reinterpret_cast<uintptr_t>(buffers));
    FatalAssert(i != localMemoryRegions.end(), LOG_TAG_RDMA, "Could not find the local memory region for buffers");
    FatalAssert(i->second != nullptr, LOG_TAG_RDMA, "Local memory region pointer is nullptr for buffers");
    FatalAssert(i->second->addr == buffers, LOG_TAG_RDMA,
                "Local memory region address does not match buffers address");
    handshake_info.buffer_rkey = i->second->rkey;

    for (size_t j = 0; j < NUM_CONNECTIONS[CN_CLUSTER_READ]; ++j) {
        FatalAssert(nodes[node_id].cn_cluster_read_connections.connection[j].qp != nullptr, LOG_TAG_RDMA,
                    "CN_CLUSTER_READ QP is nullptr for node %hhu connection %lu",
                    node_id, j);
        handshake_info.cn_cluster_read_conns[j].psn = nodes[node_id].cn_cluster_read_connections.connection[j].local_psn;
        handshake_info.cn_cluster_read_conns[j].qp_num = nodes[node_id].cn_cluster_read_connections.connection[j].qp->qp_num;
    }

    for (size_t j = 0; j < NUM_CONNECTIONS[CN_CLUSTER_WRITE]; ++j) {
        FatalAssert(nodes[node_id].cn_cluster_write_connections.connection[j].qp != nullptr, LOG_TAG_RDMA,
                    "CN_CLUSTER_WRITE QP is nullptr for node %hhu connection %lu",
                    node_id, j);
        handshake_info.cn_cluster_write_conns[j].psn = nodes[node_id].cn_cluster_write_connections.connection[j].local_psn;
        handshake_info.cn_cluster_write_conns[j].qp_num = nodes[node_id].cn_cluster_write_connections.connection[j].qp->qp_num;
    }

    for (size_t j = 0; j < NUM_CONNECTIONS[CN_COMM]; ++j) {
        FatalAssert(nodes[node_id].cn_comm_connections.connection[j].ctx.qp != nullptr, LOG_TAG_RDMA,
                    "CN_COMM QP is nullptr for node %hhu connection %lu",
                    node_id, j);
        handshake_info.comm_conns[j].conn_info.psn = nodes[node_id].cn_comm_connections.connection[j].ctx.local_psn;
        handshake_info.comm_conns[j].conn_info.qp_num = nodes[node_id].cn_comm_connections.connection[j].ctx.qp->qp_num;
        handshake_info.comm_conns[j].buffer_addr =
            reinterpret_cast<uintptr_t>(&nodes[node_id].cn_comm_connections.connection[j].comm_buffers);
        FatalAssert(nodes[node_id].remoteMemoryRegions.find(handshake_info.comm_conns[j].buffer_addr) !=
                    nodes[node_id].remoteMemoryRegions.end(),
                    LOG_TAG_RDMA,
                    "Remote memory region for comm buffer not found for node %hhu connection %lu",
                    node_id, j);
    }

    for (size_t j = 0; j < NUM_CONNECTIONS[MN_COMM]; ++j) {
        FatalAssert(nodes[node_id].mn_comm_connections.connection[j].ctx.qp != nullptr, LOG_TAG_RDMA,
                    "MN_COMM QP is nullptr for node %hhu connection %lu",
                    node_id, j);
        handshake_info.comm_conns[j].conn_info.psn = nodes[node_id].mn_comm_connections.connection[j].ctx.local_psn;
        handshake_info.comm_conns[j].conn_info.qp_num = nodes[node_id].mn_comm_connections.connection[j].ctx.qp->qp_num;
        handshake_info.comm_conns[j].buffer_addr =
            reinterpret_cast<uintptr_t>(&nodes[node_id].mn_comm_connections.connection[j].comm_buffers);
        FatalAssert(nodes[node_id].remoteMemoryRegions.find(handshake_info.comm_conns[j].buffer_addr) !=
                    nodes[node_id].remoteMemoryRegions.end(),
                    LOG_TAG_RDMA,
                    "Remote memory region for comm buffer not found for node %hhu connection %lu",
                    node_id, j);
    }

    for (size_t j = 0; j < NUM_CONNECTIONS[MN_URGENT]; ++j) {
        FatalAssert(nodes[node_id].mn_urgent_connections.connection[j].ctx.qp != nullptr, LOG_TAG_RDMA,
                    "MN_URGENT QP is nullptr for node %hhu connection %lu",
                    node_id, j);
        handshake_info.urgent_conns[j].conn_info.psn = nodes[node_id].mn_urgent_connections.connection[j].ctx.local_psn;
        handshake_info.urgent_conns[j].conn_info.qp_num = nodes[node_id].mn_urgent_connections.connection[j].ctx.qp->qp_num;
        handshake_info.urgent_conns[j].buffer_addr =
            reinterpret_cast<uintptr_t>(&nodes[node_id].mn_urgent_connections.connection[j].urgent_buffers);
        FatalAssert(nodes[node_id].remoteMemoryRegions.find(handshake_info.urgent_conns[j].buffer_addr) !=
                    nodes[node_id].remoteMemoryRegions.end(),
                    LOG_TAG_RDMA,
                    "Remote memory region for urgent buffer not found for node %hhu connection %lu",
                    node_id, j);
    }
}

void RDMA_Manager::ProcessHandshakeInfo(uint8_t node_id, const HandshakeInfo& handshake_info) {
    memcpy(&nodes[node_id].remote_gid, &handshake_info.gid, sizeof(handshake_info.gid));

    for (size_t j = 0; j < NUM_CONNECTIONS[CN_CLUSTER_READ]; ++j) {
        nodes[node_id].cn_cluster_read_connections.connection[j].remote_psn = handshake_info.cn_cluster_read_conns[j].psn;
        nodes[node_id].cn_cluster_read_connections.connection[j].remote_qp_num = handshake_info.cn_cluster_read_conns[j].qp_num;
    }

    for (size_t j = 0; j < NUM_CONNECTIONS[CN_CLUSTER_WRITE]; ++j) {
        nodes[node_id].cn_cluster_write_connections.connection[j].remote_psn = handshake_info.cn_cluster_write_conns[j].psn;
        nodes[node_id].cn_cluster_write_connections.connection[j].remote_qp_num = handshake_info.cn_cluster_write_conns[j].qp_num;
    }

    for (size_t j = 0; j < NUM_CONNECTIONS[CN_COMM]; ++j) {
        nodes[node_id].cn_comm_connections.connection[j].ctx.remote_psn = handshake_info.comm_conns[j].conn_info.psn;
        nodes[node_id].cn_comm_connections.connection[j].ctx.remote_qp_num = handshake_info.comm_conns[j].conn_info.qp_num;
        RemoteMemoryRegion rmr;
        rmr.addr = handshake_info.comm_conns[j].buffer_addr;
        rmr.rkey = handshake_info.buffer_rkey;
        nodes[node_id].remoteMemoryRegions[rmr.addr] = rmr;
        nodes[node_id].cn_comm_connections.connection[j].remote_buffers = rmr.addr;
    }

    for (size_t j = 0; j < NUM_CONNECTIONS[MN_COMM]; ++j) {
        nodes[node_id].mn_comm_connections.connection[j].ctx.remote_psn = handshake_info.comm_conns[j].conn_info.psn;
        nodes[node_id].mn_comm_connections.connection[j].ctx.remote_qp_num = handshake_info.comm_conns[j].conn_info.qp_num;
        RemoteMemoryRegion rmr;
        rmr.addr = handshake_info.comm_conns[j].buffer_addr;
        rmr.rkey = handshake_info.buffer_rkey;
        nodes[node_id].remoteMemoryRegions[rmr.addr] = rmr;
        nodes[node_id].mn_comm_connections.connection[j].remote_buffers = rmr.addr;
    }

    for (size_t j = 0; j < NUM_CONNECTIONS[MN_URGENT]; ++j) {
        nodes[node_id].mn_urgent_connections.connection[j].ctx.remote_psn = handshake_info.urgent_conns[j].conn_info.psn;
        nodes[node_id].mn_urgent_connections.connection[j].ctx.remote_qp_num = handshake_info.urgent_conns[j].conn_info.qp_num;
        RemoteMemoryRegion rmr;
        rmr.addr = handshake_info.urgent_conns[j].buffer_addr;
        rmr.rkey = handshake_info.buffer_rkey;
        nodes[node_id].remoteMemoryRegions[rmr.addr] = rmr;
        nodes[node_id].mn_urgent_connections.connection[j].remote_buffers = rmr.addr;
    }
}

RetStatus RDMA_Manager::Handshake() {
    RetStatus rs = RetStatus::Success();
    FatalAssert(nodes.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "nodes.size() does not match num_nodes. nodes.size()=%lu, num_nodes=%hhu",
                nodes.size(), num_nodes);
    FatalAssert(num_nodes > 1, LOG_TAG_RDMA,
                "num_nodes must be greater than 1. num_nodes=%hhu", num_nodes);
#ifdef MEMORY_NODE
    FatalAssert(self_node_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In MEMORY_NODE build, self_node_id must be %hhu. self_node_id=%hhu",
                MEMORY_NODE_ID, self_node_id);
#else
    FatalAssert(self_node_id != MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In CN build, self_node_id must not be %hhu. self_node_id=%hhu",
                MEMORY_NODE_ID, self_node_id);
#endif
    HandshakeInfo handshake_info;
#ifdef MEMORY_NODE
    for (uint8_t i = 0; i < num_nodes; ++i) {
        if (i == MEMORY_NODE_ID) {
            continue;
        }
#else
    uint8_t i = MEMORY_NODE_ID;
#endif

#ifdef MEMORY_NODE
        FillHandshakeInfo(i, handshake_info);
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
#else
        memset(&handshake_info, 0, sizeof(handshake_info));
        /* receive remote node's info */
        ssize_t bytes_received = recv(nodes[i].socket, &handshake_info, sizeof(handshake_info), MSG_WAITALL);
        if (bytes_received != sizeof(handshake_info)) {
            rs = RetStatus::Fail(String("Failed to receive handshake info from memory node. "
                                        "Received %zd bytes instead of %zu bytes. errno=(%d)%s",
                                        bytes_received, sizeof(handshake_info),
                                        errno, strerror(errno)).ToCStr());
            break;
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
            break;
        }
#endif

#ifdef MEMORY_NODE
        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
                "Completed handshake with compute node %hhu", i);
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Handshake completed successfully between memory node %hhu and all compute nodes",
            MEMORY_NODE_ID);
#else
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Handshake completed successfully between node %hhu and memory node %hhu",
            self_node_id, MEMORY_NODE_ID);
#endif
    return rs;
}

RetStatus RDMA_Manager::EstablishRDMAConnections() {
    RetStatus rs = RetStatus::Success();
    FatalAssert(nodes.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "nodes.size() does not match num_nodes. nodes.size()=%lu, num_nodes=%hhu",
                nodes.size(), num_nodes);
    FatalAssert(num_nodes > 1, LOG_TAG_RDMA,
                "num_nodes must be greater than 1. num_nodes=%hhu", num_nodes);
#ifdef MEMORY_NODE
    for (uint8_t i = 0; i < num_nodes; ++i) {
        if (i == MEMORY_NODE_ID) {
            continue;
        }
#else
    uint8_t i = MEMORY_NODE_ID;
#endif

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
#ifdef MEMORY_NODE
    }
#endif
}

RetStatus RDMA_Manager::DisconnectTCPConnections() {
    RetStatus rs = RetStatus::Success();
    FatalAssert(nodes.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "nodes.size() does not match num_nodes. nodes.size()=%lu, num_nodes=%hhu",
                nodes.size(), num_nodes);
    for (uint8_t i = 0; i < num_nodes; ++i) {
        if (nodes[i].socket != -1) {
            close(nodes[i].socket);
            nodes[i].socket = -1;
            DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
                    "Closed TCP connection with node %hhu", i);
        }
    }
    return rs;
}

RetStatus RDMA_Manager::DisconnectAllNodes() {}
RetStatus RDMA_Manager::DeregisterAllMemoryRegions() {}

inline void RDMA_Manager::Cleanup() {
    DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED,
            "RDMA_Manager Cleanup is not implemented yet");
}

    /* todo: support multiple memory nodes */
RDMA_Manager::RDMA_Manager(uint8_t self_id, const std::vector<std::pair<const char*, uint16_t>>& node_addresses,
                           const char* target_rdma_device_name, uint8_t rdma_port, int gid_index) :
    num_nodes{(uint8_t)node_addresses.size()}, self_node_id{self_id}, port_num{rdma_port}, gid_index{gid_index} {
    if (rdmaManagerInstance != nullptr) {
        DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_RDMA,
                "RDMA_Manager is already initialized");
    }

    CHECK_NOT_NULLPTR(target_rdma_device_name, LOG_TAG_RDMA);
    FatalAssert(node_addresses.size() == (size_t)num_nodes, LOG_TAG_RDMA,
                "too many nodes provided. num_nodes=%hhu, provided=%lu", num_nodes, node_addresses.size());
    FatalAssert(self_id < num_nodes, LOG_TAG_RDMA,
                "self_node_id is invalid. self_node_id=%hhu, num_nodes=%hhu", self_id, num_nodes);
    FatalAssert(gid_index >= 0, LOG_TAG_RDMA,
                "gid_index is invalid. gid_index=%d", gid_index);
    FatalAssert(rdma_port > 0, LOG_TAG_RDMA,
                "rdma_port is invalid. rdma_port=%hhu", rdma_port);
    FatalAssert(num_nodes <= MAX_NUM_CN + MAX_NUM_MN, LOG_TAG_RDMA,
                "num_nodes exceeds the maximum supported nodes. num_nodes=%hhu, max_supported=%d",
                num_nodes, MAX_NUM_CN + MAX_NUM_MN);
    int ret = 0;
    String error_msg;
    String dev_list_str;
    int target_device_index;
    uint8_t num_valid_nodes;
    size_t buffer_size = 0;
    void* next_buffer_addr = nullptr;
    RetStatus rs = RetStatus::Success();
#ifdef MEMORY_NODE
    FatalAssert(self_id == MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In MEMORY_NODE build, self_node_id must be %hhu. self_node_id=%hhu",
                MEMORY_NODE_ID, self_id);
#else
    FatalAssert(self_id != MEMORY_NODE_ID, LOG_TAG_RDMA,
                "In non-MEMORY_NODE build, self_node_id cannot be %hhu. self_node_id=%hhu",
                MEMORY_NODE_ID, self_id);
#endif

    int num_ibv_devices = 0;
    struct ibv_device **dev_list = ibv_get_device_list(&num_ibv_devices);
    if (dev_list == nullptr || num_ibv_devices == 0) {
        error_msg = "Failed to get IB devices list";
        goto ERROR_EXIT;
    }

    dev_list_str = "Available IB devices:[";
    target_device_index = -1;
    for (int i = 0; i < num_ibv_devices; ++i) {
        dev_list_str += String("%s%s", ibv_get_device_name(dev_list[i]),
                                (i == num_ibv_devices - 1) ? "]" : ", ");
        if (strcmp(ibv_get_device_name(dev_list[i]), target_rdma_device_name) == 0) {
            target_device_index = i;
        }
    }
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA, "%s", target_rdma_device_name);

    if (target_device_index == -1) {
        ibv_free_device_list(dev_list);
        error_msg = String("Target RDMA device '%s' not found. %s",
                            target_rdma_device_name, dev_list_str.ToCStr());
        goto ERROR_EXIT;
    }
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Using RDMA device '%s'.",
            target_rdma_device_name);

    ib_ctx = ibv_open_device(dev_list[target_device_index]);
    if (ib_ctx == nullptr) {
        ibv_free_device_list(dev_list);
        error_msg = String("Failed to open RDMA device '%s'.", target_rdma_device_name);
        goto ERROR_EXIT;
    }

    ibv_free_device_list(dev_list);
    dev_list = nullptr;

    memset(&dev_attr, 0, sizeof(dev_attr));
    ret = ibv_query_device(ib_ctx, &dev_attr);
    if (ret != 0) {
        error_msg = String("Failed to query RDMA device '%s' attributes. ret=(%d)%s errno=(%d)%s",
                            target_rdma_device_name, ret, strerror(ret), errno, strerror(errno));
        goto ERROR_EXIT;
    }

    DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_RDMA,
            "RDMA device '%s' attributes: fw_ver=%s, guid=0x%016lx, sys_img_guid=0x%016lx, max_mr_size=%lu, "
            "page_size_cap=%lu, vendor_id=%u, vendor_part_id=%u, hardware_ver=%u, max_qp=%d, "
            "max_qp_wr=%d, dev_cap_flags=%016x, max_sge=%d, max_sge_rd=%d, max_cq=%d, max_cqe=%d, "
            "max_mr=%d, max_pd=%d, max_qp_rd_atom=%d, max_ee_rd_atom=%d, max_res_rd_atom=%d, max_qp_init_rd_atom=%d, "
            "max_ee_init_rd_atom=%d, atomic_cap=%s, max_ee=%d, max_rdd=%d, max_mw=%d, max_raw_ipv6_qp=%d, "
            "max_raw_ethy_qp=%d, max_mcast_grp=%d, max_mcast_qp_attach=%d, max_total_mcast_qp_attach=%d, "
            "max_ah=%d, max_fmr=%d, max_map_per_fmr=%d, max_srq=%d, max_srq_wr=%d, max_srq_sge=%d, "
            "max_pkeys=%hu, local_ca_ack_delay=%hhu, phys_port_cnt=%hhu",
            target_rdma_device_name,
            dev_attr.fw_ver, dev_attr.node_guid, dev_attr.sys_image_guid, dev_attr.max_mr_size, dev_attr.page_size_cap,
            dev_attr.vendor_id, dev_attr.vendor_part_id, dev_attr.hw_ver, dev_attr.max_qp, dev_attr.max_qp_wr,
            dev_attr.device_cap_flags,
            dev_attr.max_sge, dev_attr.max_sge_rd, dev_attr.max_cq, dev_attr.max_cqe, dev_attr.max_mr, dev_attr.max_pd,
            dev_attr.max_qp_rd_atom, dev_attr.max_ee_rd_atom, dev_attr.max_res_rd_atom, dev_attr.max_qp_init_rd_atom,
            dev_attr.max_ee_init_rd_atom, (dev_attr.atomic_cap == IBV_ATOMIC_NONE) ? "NONE" :
            (dev_attr.atomic_cap == IBV_ATOMIC_HCA) ? "HCA" : "GLOB", dev_attr.max_ee, dev_attr.max_rdd, dev_attr.max_mw,
            dev_attr.max_raw_ipv6_qp, dev_attr.max_raw_ethy_qp, dev_attr.max_mcast_grp, dev_attr.max_mcast_qp_attach,
            dev_attr.max_total_mcast_qp_attach, dev_attr.max_ah, dev_attr.max_fmr, dev_attr.max_map_per_fmr,
            dev_attr.max_srq, dev_attr.max_srq_wr, dev_attr.max_srq_sge, dev_attr.max_pkeys,
            dev_attr.local_ca_ack_delay, dev_attr.phys_port_cnt);

    FatalAssert(port_num <= dev_attr.phys_port_cnt, LOG_TAG_RDMA,
                "port_num (%hhu) exceeds the number of physical ports (%hhu) on RDMA device '%s'",
                port_num, dev_attr.phys_port_cnt, target_rdma_device_name);
    memset(&port_attr, 0, sizeof(port_attr));
    ret = ibv_query_port(ib_ctx, port_num, &port_attr);
    if (ret != 0) {
        error_msg = String("Failed to query RDMA device '%s' port %hhu attributes. ret=(%d)%s errno=(%d)%s",
                            target_rdma_device_name, port_num, ret, strerror(ret), errno, strerror(errno));
        goto ERROR_EXIT;
    }

    DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_RDMA,
        "RDMA port attributes: state=%u, max_mtu=%u, active_mtu=%u, gid_tbl_len=%u, port_cap_flags=0x%x, "
        "max_msg_sz=%u, max_vl_num=%u, sm_lid=%u, sm_sl=%u, subnet_timeout=%u, init_type_reply=%u, "
        "active_width=%u, active_speed=%u, phys_state=%u, link_layer=%u, pkey_tbl_len=%u, lid=%u, lmc=%u, "
        "qkey_viol_cntr=%u, bad_pkey_cntr=%u",
        (unsigned)port_attr.state,
        (unsigned)port_attr.max_mtu,
        (unsigned)port_attr.active_mtu,
        (unsigned)port_attr.gid_tbl_len,
        (unsigned)port_attr.port_cap_flags,
        (unsigned)port_attr.max_msg_sz,
        (unsigned)port_attr.max_vl_num,
        (unsigned)port_attr.sm_lid,
        (unsigned)port_attr.sm_sl,
        (unsigned)port_attr.subnet_timeout,
        (unsigned)port_attr.init_type_reply,
        (unsigned)port_attr.active_width,
        (unsigned)port_attr.active_speed,
        (unsigned)port_attr.phys_state,
        (unsigned)port_attr.link_layer,
        (unsigned)port_attr.pkey_tbl_len,
        (unsigned)port_attr.lid,
        (unsigned)port_attr.lmc,
        port_attr.qkey_viol_cntr,
        port_attr.bad_pkey_cntr);

    FatalAssert(gid_index < (int)port_attr.gid_tbl_len, LOG_TAG_RDMA,
                "gid_index (%d) exceeds the GID table length (%u) on RDMA device '%s' port %hhu",
                gid_index, port_attr.gid_tbl_len, target_rdma_device_name, port_num);

    ret = ibv_query_gid(ib_ctx, port_num, gid_index, &dev_gid);
    if (ret != 0) {
        error_msg = String("Failed to query RDMA device '%s' port %hhu GID at index %d. ret=(%d)%s errno=(%d)%s",
                            target_rdma_device_name, port_num, gid_index, ret, strerror(ret), errno, strerror(errno));
        goto ERROR_EXIT;
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "RDMA device '%s' port %hhu GID at index %d: "
            "%02x%02x::%02x%02x::%02x%02x::%02x%02x::%02x%02x::%02x%02x::%02x%02x::%02x%02x",
            target_rdma_device_name, port_num, gid_index,
            dev_gid.raw[0], dev_gid.raw[1], dev_gid.raw[2], dev_gid.raw[3],
            dev_gid.raw[4], dev_gid.raw[5], dev_gid.raw[6], dev_gid.raw[7],
            dev_gid.raw[8], dev_gid.raw[9], dev_gid.raw[10], dev_gid.raw[11],
            dev_gid.raw[12], dev_gid.raw[13], dev_gid.raw[14], dev_gid.raw[15]);
    pd = ibv_alloc_pd(ib_ctx);
    if (pd == nullptr) {
        error_msg = String("Failed to allocate Protection Domain.");
        goto ERROR_EXIT;
    }


#ifdef MEMORY_NODE
    buffer_size =
        (num_nodes - 1) *
        ((NUM_CONNECTIONS[CN_COMM] + NUM_CONNECTIONS[MN_COMM]) * NUM_COMM_BUFFERS_PER_CONNECTION * COMM_BUFFER_SIZE +
         URGENT_BUFFER_SIZE);
#else
    buffer_size =
        ((NUM_CONNECTIONS[CN_COMM] + NUM_CONNECTIONS[MN_COMM]) * NUM_COMM_BUFFERS_PER_CONNECTION * COMM_BUFFER_SIZE +
         URGENT_BUFFER_SIZE);
#endif
    buffers = std::aligned_alloc(CACHE_LINE_SIZE, buffer_size);

    if (buffers == nullptr) {
        error_msg = String("Failed to allocate RDMA communication buffers.");
        goto ERROR_EXIT;
    }

    rs = RegisterMemory(buffers, buffer_size);
    if (!rs.IsOK()) {
        error_msg = String("Failed to register RDMA communication buffers. Error: %s", rs.Msg());
        goto ERROR_EXIT;
    }
    buffer_lkey = localMemoryRegions[reinterpret_cast<uintptr_t>(buffers)]->lkey;

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Allocated and registered RDMA communication buffers of size %zu bytes.",
            buffer_size);

    cn_cluster_read_cq = ibv_create_cq(ib_ctx,
                                        MAX_CQE[ConnectionType::CN_CLUSTER_READ],
                                        nullptr, nullptr, 0);
    if (cn_cluster_read_cq == nullptr) {
        error_msg = String("Failed to create CN Cluster Read Completion Queue. errno=(%d)%s",
                            errno, strerror(errno));
        goto ERROR_EXIT;
    }

    cn_cluster_write_cq = ibv_create_cq(ib_ctx,
                                        MAX_CQE[ConnectionType::CN_CLUSTER_WRITE],
                                        nullptr, nullptr, 0);
    if (cn_cluster_write_cq == nullptr) {
        error_msg = String("Failed to create CN Cluster Write Completion Queue. errno=(%d)%s",
                            errno, strerror(errno));
        goto ERROR_EXIT;
    }

    cn_comm_cq = ibv_create_cq(ib_ctx,
                                MAX_CQE[ConnectionType::CN_COMM],
                                nullptr, nullptr, 0);
    if (cn_comm_cq == nullptr) {
        error_msg = String("Failed to create CN Communication Completion Queue. errno=(%d)%s",
                            errno, strerror(errno));
        goto ERROR_EXIT;
    }

    mn_comm_cq = ibv_create_cq(ib_ctx,
                                MAX_CQE[ConnectionType::MN_COMM],
                                nullptr, nullptr, 0);
    if (mn_comm_cq == nullptr) {
        error_msg = String("Failed to create MN Communication Completion Queue. errno=(%d)%s",
                            errno, strerror(errno));
        goto ERROR_EXIT;
    }

    mn_urgent_cq = ibv_create_cq(ib_ctx,
                                    MAX_CQE[ConnectionType::MN_URGENT],
                                    nullptr, nullptr, 0);
    if (mn_urgent_cq == nullptr) {
        error_msg = String("Failed to create MN Urgent Completion Queue. errno=(%d)%s",
                            errno, strerror(errno));
        goto ERROR_EXIT;
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Created Completion Queues with cqe=(cncr:%d, cncw:%d, cnc:%d, mnc:%d, mnu:%d)",
            cn_cluster_read_cq->cqe, cn_cluster_write_cq->cqe, cn_comm_cq->cqe, mn_comm_cq->cqe, mn_urgent_cq->cqe);

    num_valid_nodes = 0;
    next_buffer_addr = buffers;
    for (uint8_t i = 0; i < num_nodes; ++i) {
        if (i == self_id) {
            continue;
        }
        ++num_valid_nodes;
        nodes.emplace_back(i, inet_addr(node_addresses[i].first), node_addresses[i].second);

        rs = InitConnectionCtx(ConnectionType::CN_CLUSTER_READ, cn_cluster_read_cq,
                               nodes.back().cn_cluster_read_connections.connection, next_buffer_addr);
        if (!rs.IsOK()) {
            error_msg = String("Failed to initialize CN Read QPs to node %hhu. Error: %s",
                                i, rs.Msg());
            goto ERROR_EXIT;
        }
        FatalAssert(next_buffer_addr <= static_cast<void*>(
                        reinterpret_cast<uint8_t*>(buffers) + buffer_size),
                    LOG_TAG_RDMA,
                    "next_buffer_addr exceeded allocated buffer size after initializing CN Read QPs to node %hhu",
                    i);

        rs = InitConnectionCtx(ConnectionType::CN_CLUSTER_WRITE, cn_cluster_write_cq,
                               nodes.back().cn_cluster_write_connections.connection, next_buffer_addr);
        if (!rs.IsOK()) {
            error_msg = String("Failed to initialize CN Write QPs to node %hhu. Error: %s",
                                i, rs.Msg());
            goto ERROR_EXIT;
        }
        FatalAssert(next_buffer_addr <= static_cast<void*>(
                        reinterpret_cast<uint8_t*>(buffers) + buffer_size),
                    LOG_TAG_RDMA,
                    "next_buffer_addr exceeded allocated buffer size after initializing CN Read QPs to node %hhu",
                    i);

        rs = InitConnectionCtx(ConnectionType::CN_COMM, cn_comm_cq,
                               nodes.back().cn_comm_connections.connection, next_buffer_addr);
        if (!rs.IsOK()) {
            error_msg = String("Failed to initialize CN Comm QPs to node %hhu. Error: %s",
                                i, rs.Msg());
            goto ERROR_EXIT;
        }
        FatalAssert(next_buffer_addr <= static_cast<void*>(
                        reinterpret_cast<uint8_t*>(buffers) + buffer_size),
                    LOG_TAG_RDMA,
                    "next_buffer_addr exceeded allocated buffer size after initializing CN Read QPs to node %hhu",
                    i);

        rs = InitConnectionCtx(ConnectionType::MN_COMM, mn_comm_cq,
                               nodes.back().mn_comm_connections.connection, next_buffer_addr);
        if (!rs.IsOK()) {
            error_msg = String("Failed to initialize MN Comm QPs to node %hhu. Error: %s",
                                i, rs.Msg());
            goto ERROR_EXIT;
        }
        FatalAssert(next_buffer_addr <= static_cast<void*>(
                        reinterpret_cast<uint8_t*>(buffers) + buffer_size),
                    LOG_TAG_RDMA,
                    "next_buffer_addr exceeded allocated buffer size after initializing CN Read QPs to node %hhu",
                    i);

        rs = InitConnectionCtx(ConnectionType::MN_URGENT, mn_urgent_cq,
                               nodes.back().mn_urgent_connections.connection, next_buffer_addr);
        if (!rs.IsOK()) {
            error_msg = String("Failed to initialize MN Urgent QPs to node %hhu. Error: %s",
                                i, rs.Msg());
            goto ERROR_EXIT;
        }
        FatalAssert(next_buffer_addr <= static_cast<void*>(
                        reinterpret_cast<uint8_t*>(buffers) + buffer_size),
                    LOG_TAG_RDMA,
                    "next_buffer_addr exceeded allocated buffer size after initializing CN Read QPs to node %hhu",
                    i);
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Created QPs for each of the %hhu/%hhu nodes and set their connection buffers.",
            num_valid_nodes, num_nodes);

    return;

ERROR_EXIT:
    Cleanup();
    DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_RDMA, "%s", error_msg.ToCStr());
}

RDMA_Manager::~RDMA_Manager() {
    Cleanup();
}


RetStatus RDMA_Manager::Initialize(uint8_t self_id, const std::vector<std::pair<const char*, uint16_t>>& node_addresses,
                                   const char* target_rdma_device_name, uint8_t rdma_port, int gid_index,
                                   uint8_t cn_read_connections, uint8_t cn_write_connections,
                                   uint8_t mn_write_connections) {
    if (rdmaManagerInstance != nullptr) {
        return RetStatus::Fail("RDMA_Manager is already initialized");
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Initializing RDMA_Manager with self_id=%hhu, rdma_port=%hhu, gid_index=%d, "
            "cn_read_connections=%hhu, cn_write_connections=%hhu, mn_write_connections=%hhu",
            self_id, rdma_port, gid_index,
            cn_read_connections, cn_write_connections, mn_write_connections);
    rdmaManagerInstance = new RDMA_Manager(self_id, node_addresses, target_rdma_device_name, rdma_port, gid_index,
                                            cn_read_connections, cn_write_connections, mn_write_connections);
    if (rdmaManagerInstance == nullptr) {
        return RetStatus::Fail("Failed to initialize RDMA_Manager");
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "RDMA_Manager initialized successfully");
    return RetStatus::Success();
}

void RDMA_Manager::Destroy() {
    FatalAssert(rdmaManagerInstance != nullptr, LOG_TAG_RDMA,
                "RDMA_Manager is not initialized");
    if (rdmaManagerInstance != nullptr) {
        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
                "Destroying RDMA_Manager instance");
        delete rdmaManagerInstance;
        rdmaManagerInstance = nullptr;
    }
}

RetStatus RDMA_Manager::RegisterMemory(Address addr, size_t length) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in RegisterMemory");
    FatalAssert(addr != nullptr, LOG_TAG_RDMA,
                "Cannot register null memory region");
    FatalAssert(length > 0, LOG_TAG_RDMA,
                "Cannot register memory region with zero length");
    FatalAssert(pd != nullptr, LOG_TAG_RDMA,
                "Protection Domain is not initialized");
    struct ibv_mr* mr = ibv_reg_mr(pd, addr, length,
                                    IBV_ACCESS_LOCAL_WRITE |
                                    IBV_ACCESS_REMOTE_READ |
                                    IBV_ACCESS_REMOTE_WRITE |
                                    IBV_ACCESS_REMOTE_ATOMIC);
    if (mr == nullptr) {
        return RetStatus::Fail(String("Failed to register memory region at addr=%p with length=%lu: errno=%d: %s",
                                        addr, length, errno, strerror(errno)).ToCStr());
    }
    FatalAssert(localMemoryRegions.find(reinterpret_cast<uintptr_t>(addr)) == localMemoryRegions.end(),
                LOG_TAG_RDMA,
                "Memory region at addr=%p is already registered", addr);
    localMemoryRegions[reinterpret_cast<uintptr_t>(addr)] = mr;
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "Registered memory region at addr=%p with length=%lu, lkey=0x%x, rkey=0x%x",
            addr, length, mr->lkey, mr->rkey);
    return RetStatus::Success();
}

RetStatus RDMA_Manager::EstablishConnections() {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in EstablishConnections");
    RetStatus rs = RetStatus::Success();

    rs = EstablishTCPConnections();
    if (!rs.IsOK()) {
        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_RDMA,
                "Failed to establish TCP connections. Error: %s", rs.Msg());
        return rs;
    }

    rs = Handshake();
    if (!rs.IsOK()) {
        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_RDMA,
                "Failed to perform handshake. Error: %s", rs.Msg());
        return rs;
    }

    rs = DisconnectTCPConnections();
    if (!rs.IsOK()) {
        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_RDMA,
                "Failed to disconnect TCP connections. Error: %s", rs.Msg());
        return rs;
    }

    rs = EstablishRDMAConnections();
    if (!rs.IsOK()) {
        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_RDMA,
                "Failed to establish RDMA connections. Error: %s", rs.Msg());
        return rs;
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
            "RDMA connections established successfully");
    return rs;
}

inline RDMA_Manager* RDMA_Manager::GetInstance() {
    CHECK_NOT_NULLPTR(rdmaManagerInstance, LOG_TAG_RDMA);
    return rdmaManagerInstance;
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
                DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_RDMA, "could not grab communication buffer to node %hhu after %zu tries",
                        target_node_id, num_tries);
                usleep(1); /* todo: may need to tune */
            }

            if (num_tries > NUM_CONNECTIONS[CN_COMM]) {
                go_next_conn = !go_next_conn;
            }

            if (go_next_conn) {
                uint8_t new_conn = (buffer.conn_id + 1) % NUM_CONNECTIONS[CN_COMM];
                nodes[target_node_id].cn_comm_connections.curr_conn_idx.compare_exchange_strong(
                    buffer.conn_id, new_conn);
            } else {
                uint8_t new_buff_idx = (buffer.buffer_idx + 1) % NUM_COMM_BUFFERS_PER_CONNECTION;
                nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].curr_buffer_idx.compare_exchange_strong(
                    buffer.buffer_idx, new_buff_idx);
            }
        }

        buffer.conn_id = nodes[target_node_id].cn_comm_connections.curr_conn_idx.load(std::memory_order_acquire) %
                     NUM_CONNECTIONS[CN_COMM];
        buffer.buffer_idx =
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                curr_buffer_idx.load(std::memory_order_acquire) % NUM_COMM_BUFFERS_PER_CONNECTION;
        ++num_tries;

        bool locked = nodes[target_node_id].
            cn_comm_connections.connection[buffer.conn_id].buffer_lock[buffer.buffer_idx].TryLock(SX_SHARED);
        if (!locked) {
            continue;
        }

        bool is_free = nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                       comm_buffers[buffer.buffer_idx].meta.state.load(std::memory_order_acquire) ==
                       CommBufferState::BUFFER_STATE_READY;
        if (!is_free) {
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_lock[buffer.buffer_idx].Unlock();
            continue;
        }

        nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].num_readers[buffer.buffer_idx].
            fetch_add(1);
        is_free = nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                    comm_buffers[buffer.buffer_idx].meta.state.load(std::memory_order_acquire) ==
                    CommBufferState::BUFFER_STATE_READY;
        if (!is_free) {
            uint64_t num_holders =
                nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].num_readers[buffer.buffer_idx].
                fetch_sub(1);
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_lock[buffer.buffer_idx]
                .Unlock();
            if (num_holders == 1) {
                rs = FlushCommBuffer(target_node_id, buffer.conn_id, buffer.buffer_idx);
                if (!rs.IsOK()) {
                    return rs;
                }
            }
            continue;
        }

        size_t offset = nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                            length[buffer.buffer_idx].fetch_add(len, std::memory_order_acquire);
        if (offset + len > buffer.max_length) {
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                length[buffer.buffer_idx].fetch_sub(len, std::memory_order_release);
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].
                comm_buffers[buffer.buffer_idx].meta.state.store(CommBufferState::BUFFER_STATE_IN_USE,
                                                                 std::memory_order_release);
            uint64_t num_holders =
                nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].num_readers[buffer.buffer_idx].
                fetch_sub(1);
            nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_lock[buffer.buffer_idx].
                Unlock();
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
            cn_comm_connections.connection[buffer.conn_id].comm_buffers[buffer.buffer_idx].data + offset;
        nodes[target_node_id].cn_comm_connections.connection[buffer.conn_id].comm_buffers[buffer.buffer_idx].
            meta.num_requests.fetch_add(1, std::memory_order_release);
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
        connection[buffer.conn_id].buffer_lock[buffer.buffer_idx]), SX_SHARED);
    if (flush || reinterpret_cast<uintptr_t>(buffer.buffer) + buffer.length == reinterpret_cast<uintptr_t>(
                 nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].
                    comm_buffers[buffer.buffer_idx].data) + buffer.max_length) {
        nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].
            comm_buffers[buffer.buffer_idx].meta.state.store(CommBufferState::BUFFER_STATE_IN_USE, std::memory_order_release);
    }
    uint64_t num_holders =
        nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].
            num_readers[buffer.buffer_idx].fetch_sub(1);
    nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].buffer_lock[buffer.buffer_idx].Unlock();
    if ((num_holders == 1) && (nodes[buffer.target_node_id].cn_comm_connections.connection[buffer.conn_id].
                               comm_buffers[buffer.buffer_idx].meta.state.load(std::memory_order_acquire) ==
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
                comm_buffers[buffer_idx].meta.state.load(std::memory_order_acquire) ==
                CommBufferState::BUFFER_STATE_IN_USE,
                LOG_TAG_RDMA,
                "Comm buffer to be flushed is not in IN_USE state in FlushCommBuffer");
    RetStatus rs = RetStatus::Success();

    RDMABuffer rdma_buffer;
    rdma_buffer.local_addr =
        nodes[target_node_id].cn_comm_connections.connection[conn_id].
            comm_buffers[buffer_idx].data;
    rdma_buffer.remote_addr =
        nodes[target_node_id].cn_comm_connections.connection[conn_id].remote_buffers +
            buffer_idx * COMM_BUFFER_SIZE;
    rdma_buffer.length = COMM_BUFFER_SIZE;
    rdma_buffer.buffer_idx = buffer_idx;
    rdma_buffer.remote_node_id = target_node_id;

    nodes[target_node_id].cn_comm_connections.connection[conn_id].buffer_lock[buffer_idx].Lock(SX_EXCLUSIVE);
    do {
        rs = RDMAWrite(&rdma_buffer, 1, nodes[target_node_id].cn_comm_connections.connection[conn_id].ctx, 0);
        if (!rs.IsOK() && rs.stat != RetStatus::RDMA_QP_FULL) {
            FatalAssert(false, LOG_TAG_RDMA,
                        "Failed to flush communication buffer to node %hhu: %s",
                        target_node_id, rs.Msg());
            return rs;
        }
    } while (!rs.IsOK());
    nodes[target_node_id].cn_comm_connections.connection[conn_id].buffer_lock[buffer_idx].Unlock();
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
    msg->meta.length = 0;
    msg->meta.seen = 0;
    msg->meta.valid = 1;
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
    FatalAssert(msg->meta.length > 0, LOG_TAG_RDMA,
                "Cannot send urgent message with zero length");
    FatalAssert(target_node_id < num_nodes, LOG_TAG_RDMA,
                "Invalid target_node_id %hhu in SendUrgentMessage", target_node_id);
    FatalAssert(target_node_id != self_node_id, LOG_TAG_RDMA,
                "Cannot send urgent message to self_node_id %hhu", self_node_id);
    FatalAssert(msg->meta.length <= URGENT_MESSAGE_SIZE - sizeof(UrgentMessageMeta), LOG_TAG_RDMA,
                "Urgent message length %zu exceeds maximum %zu", msg->meta.length, URGENT_MESSAGE_SIZE);
    RetStatus rs = RetStatus::Success();
    uint64_t read_off = URGENT_MESSAGE_SIZE;
    uint64_t write_off = 0;
    uint8_t conn_idx = (nodes[target_node_id].mn_urgent_connections.curr_conn_idx.fetch_add(1) - 1) %
                       NUM_CONNECTIONS[MN_URGENT];
    RDMABuffer rdma_buffer;
    rdma_buffer.local_addr = msg;
    rdma_buffer.length = URGENT_MESSAGE_SIZE;
    rdma_buffer.buffer_idx = 0;
    rdma_buffer.remote_node_id = target_node_id;
    while (true) {
        conn_idx = (conn_idx + 1) % NUM_CONNECTIONS[MN_URGENT];
        while(true) {
            read_off =
                nodes[target_node_id].mn_urgent_connections.connection[conn_idx].
                    read_off->load(std::memory_order_acquire) % URGENT_BUFFER_SIZE;
            write_off =
                nodes[target_node_id].mn_urgent_connections.connection[conn_idx].
                write_off->load(std::memory_order_acquire) % URGENT_BUFFER_SIZE;
            if (((write_off + URGENT_MESSAGE_SIZE) % URGENT_BUFFER_SIZE) == read_off) {
                DIVFTREE_YIELD(); /* todo: may need to tune */
                break;
            }

            if (!nodes[target_node_id].mn_urgent_connections.connection[conn_idx].write_off->compare_exchange_strong(
                    write_off,
                    (write_off + URGENT_MESSAGE_SIZE) % URGENT_BUFFER_SIZE)) {
                DIVFTREE_YIELD(); /* todo: may need to tune */
                continue;
            }

            break;
        }

        if (((write_off + URGENT_MESSAGE_SIZE) % URGENT_BUFFER_SIZE) == read_off) {
            continue;
        }

        rdma_buffer.remote_addr =
            nodes[target_node_id].mn_urgent_connections.connection[conn_idx].remote_buffer + write_off;
        rs = RDMAWrite(&rdma_buffer, 1, nodes[target_node_id].mn_urgent_connections.connection[conn_idx].ctx,
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

uint32_t RDMA_Manager::GetLocalKey(Address addr, size_t length) const {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in GetLocalKey");
    FatalAssert(addr != nullptr, LOG_TAG_RDMA,
                "Cannot get local key for null address");
    FatalAssert(length > 0, LOG_TAG_RDMA,
                "Cannot get local key for zero length");
    auto it = localMemoryRegions.upper_bound(reinterpret_cast<uintptr_t>(addr));
    FatalAssert(it != localMemoryRegions.begin(), LOG_TAG_RDMA,
                "No registered memory region found for address %p", addr);
    FatalAssert(reinterpret_cast<uintptr_t>(addr) + length <= it->first, LOG_TAG_RDMA,
                "Requested memory region at addr=%p with length=%zu exceeds registered memory region at addr=%p with length=%zu",
                addr, length, reinterpret_cast<Address>(it->first), localMemoryRegions.at(it->first)->length);
    --it;
    struct ibv_mr* mr = it->second;
    FatalAssert(mr != nullptr, LOG_TAG_RDMA,
                "Memory region pointer is null for address %p", addr);
    FatalAssert(reinterpret_cast<uintptr_t>(addr) + length <=
                reinterpret_cast<uintptr_t>(it->first) + mr->length, LOG_TAG_RDMA,
                "Requested memory region at addr=%p with length=%zu exceeds registered memory region at addr=%p with length=%zu",
                addr, length, reinterpret_cast<Address>(it->first), mr->length);
    return mr->lkey;
}

/* todo: I actually need to create seperate RDMA requests for this as their dest is not the same place */
RetStatus RDMA_Manager::RDMAWrite(RDMABuffer* rdma_buffers, size_t num_buffers, ConnectionContext& conn_ctx,
                                  unsigned int send_flags) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in RDMAWrite");
    FatalAssert(rdma_buffers != nullptr, LOG_TAG_RDMA,
                "rdma_buffers is null in RDMAWrite");
    FatalAssert(num_buffers > 0, LOG_TAG_RDMA,
                "num_buffers is zero in RDMAWrite");
    FatalAssert(conn_ctx.qp != nullptr, LOG_TAG_RDMA,
                "ConnectionContext QP is null in RDMAWrite");
    FatalAssert(num_buffers <= UINT32_MAX, LOG_TAG_RDMA,
                "num_buffers exceeds UINT32_MAX in RDMAWrite");
    FatalAssert(num_buffers <= MAX_SEND_WR[conn_ctx.type], LOG_TAG_RDMA,
                "num_buffers exceeds MAX_SEND_WR (%u) in RDMAWrite", MAX_SEND_WR[conn_ctx.type]);
    RetStatus rs = RetStatus::Success();

    ConnTaskId new_task_id;
    new_task_id.connection_id = conn_ctx.connection_id;
    new_task_id.buffer_id = rdma_buffers[0].buffer_idx;
    new_task_id.task_id = conn_ctx.last_task_id.fetch_add(num_buffers) % UINT32_MAX;
    struct ibv_send_wr* wr_list = new ibv_send_wr[num_buffers];
    struct ibv_sge* sge_list = new ibv_sge[num_buffers];
    struct ibv_send_wr* bad_wr = nullptr;

    uint16_t num_outstanding = conn_ctx.num_pending_requests.fetch_add(num_buffers);
    if (num_outstanding + num_buffers > MAX_SEND_WR[conn_ctx.type]) {
        conn_ctx.num_pending_requests.fetch_sub(num_buffers);
        return RetStatus{.stat=RetStatus::RDMA_QP_FULL, .message=nullptr};
    }

    bool create_wc = (num_outstanding + num_buffers == MAX_SEND_WR[conn_ctx.type]);
    ConnTaskId last_task_id;
    for (size_t i = 0; i < num_buffers; ++i) {
        FatalAssert(rdma_buffers[i].local_addr != nullptr, LOG_TAG_RDMA,
                    "rdma_buffers[%zu] local_addr is null in RDMAWrite", i);
        FatalAssert(rdma_buffers[i].length > 0, LOG_TAG_RDMA,
                    "rdma_buffers[%zu] length is zero in RDMAWrite", i);
        FatalAssert(rdma_buffers[i].length <= UINT32_MAX, LOG_TAG_RDMA,
                    "rdma_buffers[%zu] length exceeds UINT32_MAX in RDMAWrite", i);
        FatalAssert(new_task_id.buffer_id == rdma_buffers[i].buffer_idx, LOG_TAG_RDMA,
                    "rdma_buffers[%zu] buffer_idx does not match task_id buffer_id in RDMAWrite", i);
        sge_list[i].addr = reinterpret_cast<uintptr_t>(rdma_buffers[i].local_addr);
        sge_list[i].length = static_cast<uint32_t>(rdma_buffers[i].length);
        sge_list[i].lkey = (send_flags & IBV_SEND_INLINE) != 0 ? 0 :
            GetLocalKey(rdma_buffers[i].local_addr, rdma_buffers[i].length);

        memset(&wr_list[i], 0, sizeof(wr_list[i]));
        wr_list[i].wr_id = new_task_id._raw;
        wr_list[i].sg_list = sge_list;
        wr_list[i].num_sge = 1;
        wr_list[i].opcode = IBV_WR_RDMA_WRITE;
        wr_list[i].send_flags = send_flags | ((create_wc && (i == (num_buffers - 1))) ? IBV_SEND_SIGNALED : 0);
        wr_list[i].wr.rdma.remote_addr = rdma_buffers[i].remote_addr;
        wr_list[i].wr.rdma.rkey = nodes[rdma_buffers[i].remote_node_id].GetKey(
            rdma_buffers[i].remote_addr, rdma_buffers[i].length);
        wr_list[i].next = (i == (num_buffers - 1)) ? nullptr : &wr_list[i + 1];
        if (i == (num_buffers - 1)) {
            last_task_id = new_task_id;
        }
        new_task_id.task_id = (new_task_id.task_id + 1) % UINT32_MAX;
    }

    int ret = ibv_post_send(conn_ctx.qp, &wr_list[0], &bad_wr);
    if (ret != 0) {
        String error_msg = String("Failed to post RDMA Write send work request. ret=(%d)%s errno=(%d)%s",
                                    ret, strerror(ret), errno, strerror(errno));
        FatalAssert(false, LOG_TAG_RDMA, "%s", error_msg.ToCStr());
        rs = RetStatus::Fail(error_msg.ToCStr());
        return rs;
    }

    if (create_wc) {
        struct ibv_wc wc;
        int num_comp = 0;
        while (num_comp == 0) {
            num_comp = ibv_poll_cq(conn_ctx.cq, 1, &wc);
            if (num_comp == 0) {
                /* todo: maybe use sleep instead? */
                DIVFTREE_YIELD();
            }
        }

        if (num_comp < 0) {
            String error_msg = String("Failed to poll Completion Queue for RDMA Write. num_comp=(%d)%s errno=(%d)%s",
                                        num_comp, strerror(-num_comp), errno, strerror(errno));
            FatalAssert(false, LOG_TAG_RDMA, "%s", error_msg.ToCStr());
            rs = RetStatus::Fail(error_msg.ToCStr());
            return rs;
        }

        if (wc.status != IBV_WC_SUCCESS) {
            String error_msg = String("RDMA Write failed in Completion Queue. wc_status=(%d)%s wc_wr_id=%lu",
                                        wc.status, ibv_wc_status_str(wc.status), wc.wr_id);
            FatalAssert(false, LOG_TAG_RDMA, "%s", error_msg.ToCStr());
            rs = RetStatus::Fail(error_msg.ToCStr());
            return rs;
        }

        FatalAssert(num_comp == 1, LOG_TAG_RDMA,
                    "Polled %d completions from CQ, expected 1 in RDMAWrite", num_comp);
        FatalAssert(wc.wr_id == last_task_id._raw, LOG_TAG_RDMA,
                    "wc.wr_id %lu does not match expected last_task_id %lu in RDMAWrite",
                    wc.wr_id, last_task_id._raw);
        conn_ctx.num_pending_requests.fetch_sub(MAX_SEND_WR[conn_ctx.type]);
    }
}

/* todo: for the read instead of the sg_list we have to use multiple wr to get a wc for each */
RetStatus RDMA_Manager::RDMARead(RDMABuffer* rdma_buffers, size_t num_buffers, ConnectionContext& conn_ctx,
                                 ConnTaskId& wait_id) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in RDMAWrite");
    FatalAssert(rdma_buffers != nullptr, LOG_TAG_RDMA,
                "rdma_buffers is null in RDMAWrite");
    FatalAssert(num_buffers > 0, LOG_TAG_RDMA,
                "num_buffers is zero in RDMAWrite");
    FatalAssert(conn_ctx.qp != nullptr, LOG_TAG_RDMA,
                "ConnectionContext QP is null in RDMAWrite");
    FatalAssert(num_buffers <= UINT32_MAX, LOG_TAG_RDMA,
                "num_buffers exceeds UINT32_MAX in RDMAWrite");
    FatalAssert(conn_ctx.type == CN_CLUSTER_READ, LOG_TAG_RDMA,
                "ConnectionContext type is not CN_CLUSTER_READ in RDMARead");
    FatalAssert(num_buffers <= MAX_SEND_WR[conn_ctx.type], LOG_TAG_RDMA,
                "num_buffers exceeds MAX_SEND_WR (%u) in RDMARead", MAX_SEND_WR[conn_ctx.type]);
#ifdef MEMORY_NODE
    FatalAssert(false, LOG_TAG_RDMA,
                "RDMARead should not be called on MEMORY_NODE");
#endif
    RetStatus rs = RetStatus::Success();

    uint16_t num_outstanding = conn_ctx.num_pending_requests.fetch_add(num_buffers);
    if (num_outstanding + num_buffers > MAX_SEND_WR[conn_ctx.type]) {
        conn_ctx.num_pending_requests.fetch_sub(num_buffers);
        return RetStatus{.stat=RetStatus::RDMA_QP_FULL, .message=nullptr};
    }

    ConnTaskId new_task_id;
    new_task_id.connection_id = conn_ctx.connection_id;
    new_task_id.buffer_id = 0;
    new_task_id.task_id = conn_ctx.last_task_id.fetch_add(num_buffers) % UINT32_MAX;
    struct ibv_send_wr* wr_list = new ibv_send_wr[num_buffers];
    struct ibv_sge* sge_list = new ibv_sge[num_buffers];
    struct ibv_send_wr* bad_wr = nullptr;

    FatalAssert(num_outstanding + num_buffers <= MAX_SEND_WR[conn_ctx.type], LOG_TAG_RDMA,
                "num_outstanding (%u) + num_buffers (%zu) exceeds MAX_SEND_WR (%u) in RDMAWrite",
                num_outstanding, num_buffers, MAX_SEND_WR[conn_ctx.type]);

    /* todo: what if I create a wc for all of them and eventhough sometimes things happen out of order but it
       can still help me as I will know that some clusters are ready while others are not */
    for (size_t i = 0; i < num_buffers; ++i) {
        FatalAssert(rdma_buffers[i].local_addr != nullptr, LOG_TAG_RDMA,
                "rdma_buffers[%zu] local_addr is null in RDMAWrite", i);
        FatalAssert(rdma_buffers[i].length > 0, LOG_TAG_RDMA,
                    "rdma_buffers[%zu] length is zero in RDMAWrite", i);
        FatalAssert(rdma_buffers[i].length <= UINT32_MAX, LOG_TAG_RDMA,
                    "rdma_buffers[%zu] length exceeds UINT32_MAX in RDMAWrite", i);
        FatalAssert(new_task_id.buffer_id == 0, LOG_TAG_RDMA,
                    "rdma_buffers[%zu] buffer_idx does not match task_id buffer_id in RDMAWrite", i);
        sge_list[i].addr = reinterpret_cast<uintptr_t>(rdma_buffers[i].local_addr);
        sge_list[i].length = static_cast<uint32_t>(rdma_buffers[i].length);
        sge_list[i].lkey = GetLocalKey(rdma_buffers[i].local_addr, rdma_buffers[i].length);

        memset(&wr_list[i], 0, sizeof(wr_list[i]));
        wr_list[i].wr_id = new_task_id._raw;
        wr_list[i].sg_list = sge_list;
        wr_list[i].num_sge = 1;
        wr_list[i].opcode = IBV_WR_RDMA_READ;
        wr_list[i].wr.rdma.remote_addr = rdma_buffers[i].remote_addr;
        wr_list[i].wr.rdma.rkey = nodes[rdma_buffers[i].remote_node_id].GetKey(
            rdma_buffers[i].remote_addr, rdma_buffers[i].length);
        if (i < num_buffers - 1) {
            wait_id = new_task_id;
            wr_list[i].next = &wr_list[i + 1];
            wr_list[i].send_flags = 0;
            new_task_id.task_id = (new_task_id.task_id + 1) % UINT32_MAX;
        } else {
            wr_list[i].next = nullptr;
            wr_list[i].send_flags = IBV_SEND_SIGNALED;
        }
    }

    int ret = ibv_post_send(conn_ctx.qp, &wr_list[0], &bad_wr);
    if (ret != 0) {
        String error_msg = String("Failed to post RDMA Write send work request. ret=(%d)%s errno=(%d)%s",
                                    ret, strerror(ret), errno, strerror(errno));
        FatalAssert(false, LOG_TAG_RDMA, "%s", error_msg.ToCStr());
        rs = RetStatus::Fail(error_msg.ToCStr());
        return rs;
    }

    return rs;
}

RetStatus RDMA_Manager::RDMAWrite(RDMABuffer* rdma_buffers, size_t num_buffers) {
    FatalAssert(rdmaManagerInstance == this, LOG_TAG_RDMA,
                "RDMA_Manager instance mismatch in RDMAWrite");
    FatalAssert(rdma_buffers != nullptr, LOG_TAG_RDMA,
                "rdma_buffers is null in RDMAWrite");
    FatalAssert(num_buffers > 0, LOG_TAG_RDMA,
                "num_buffers is zero in RDMAWrite");
#ifdef MEMORY_NODE
    FatalAssert(false, LOG_TAG_RDMA,
                "RDMAWrite without connection context should not be called on MEMORY_NODE");
#endif
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
#ifdef MEMORY_NODE
    FatalAssert(false, LOG_TAG_RDMA,
                "RDMARead without connection context should not be called on MEMORY_NODE");
#endif
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
#ifdef MEMORY_NODE
    FatalAssert(false, LOG_TAG_RDMA,
                "PollCompletion should not be called on MEMORY_NODE");
#endif
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

};

#endif