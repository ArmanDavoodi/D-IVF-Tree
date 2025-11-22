#ifndef RDMA_MANAGER_H_
#define RDMA_MANAGER_H_

#include "common.h"
#include "debug.h"

#include <cstring>
#include <vector>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#define MEMORY_NODE

#define MEMORY_NODE_ID ((uint8_t)0)

namespace divftree {

enum ConnectionType : uint8_t {
    CN_CLUSTER_READ = 0,
    CN_CLUSTER_WRITE,
    CN_COMM,
    MN_COMM,
    MN_URGENT,
    CONNECTION_TYPE_COUNT
};

String ToString(ConnectionType type) {
    switch (type) {
        case CN_CLUSTER_READ:
            return String("CN_CLUSTER_READ");
        case CN_CLUSTER_WRITE:
            return String("CN_CLUSTER_WRITE");
        case CN_COMM:
            return String("CN_COMM");
        case MN_COMM:
            return String("MN_COMM");
        case MN_URGENT:
            return String("MN_URGENT");
        default:
            return String("UNKNOWN_CONNECTION_TYPE");
    }
}

constexpr size_t NUM_CONNECTIONS[CONNECTION_TYPE_COUNT] = {
    4,  /* CN_CLUSTER_READ */
    2,  /* CN_CLUSTER_WRITE */
    2,  /* CN_COMM */
    2,  /* MN_COMM */
    2   /* MN_URGENT */
};


constexpr size_t NUM_COMM_BUFFERS_PER_CONNECTION = 2;
constexpr size_t NUM_URGENT_BUFFERS_PER_CONNECTION = 2;

constexpr size_t COMM_BUFFER_SIZE = 4096;
constexpr size_t URGENT_BUFFER_SIZE = 64; /* todo: needs to be set to max inline data or max size of urgent message */

union ConnTaskId {
    struct {
        uint64_t task_id : 48;
        uint64_t connection_id : 8;
        uint64_t buffer_id : 8;
    };

    uint64_t _raw;

    ConnTaskId() : task_id{0}, connection_id{0}, buffer_id{0} {}
    ConnTaskId(uint64_t raw) : _raw{raw} {}
    ConnTaskId(uint64_t t_id, uint8_t c_id, uint8_t b_id) :
        task_id{t_id}, connection_id{c_id}, buffer_id{b_id} {}
    ConnTaskId(const ConnTaskId& other) : _raw{other._raw} {}

    ConnTaskId& operator=(uint64_t raw) {
        _raw = raw;
        return *this;
    }

    ConnTaskId& operator=(const ConnTaskId& other) {
        _raw = other._raw;
        return *this;
    }

    inline bool operator==(uint64_t raw) const {
        return _raw == raw;
    }

    inline bool operator!=(uint64_t raw) const {
        return _raw != raw;
    }
};

enum CommBufferState : uint8_t {
    BUFFER_STATE_READY = 0,
    BUFFER_STATE_IN_USE
};

struct alignas(CACHE_LINE_SIZE) UrgentBuffer {
    char data[URGENT_BUFFER_SIZE - sizeof(CommBufferState)] = {0};
    std::atomic<CommBufferState> state = BUFFER_STATE_READY; /* this should be the last byte! */ /* todo alignas(64)? */
};

struct CommBufferMeta {
    uint64_t num_requests = 0;
    std::atomic<CommBufferState> state = BUFFER_STATE_READY; /* this should be the last byte! */ /* todo alignas(64)? */
};

struct alignas(CACHE_LINE_SIZE) CommBuffer {
    /*
     * For each request we first write the type and then the size(if needed) and then a flag indicating
     * whether it is an urgent request or not and then the data.
     *
     * The reciever side will first sort the requests based on urgency, type and cluster id? and then if the sort was
     * not inplace it can use RDMA write to update the viewed_id of the sender side letting it know that it can reuse
     * the buffer.
     *
     * if we use cqes for writes, we can also start using the local buffer but we cannot do an rdma send until the
     * viewed_id is updated.
     */
    char data[COMM_BUFFER_SIZE - sizeof(CommBufferMeta)] = {0};
    CommBufferMeta meta;
};

struct ConnectionContext {
    // uint8_t target_node_id; /* 0 is the memory node */
    ConnectionType type;
    uint32_t connection_id; /* todo: do I need this? */
    uint32_t local_psn;
    uint32_t remote_psn;
    uint32_t remote_qp_num;
    std::atomic<ConnTaskId> last_task_id;
    std::atomic<uint16_t> num_pending_requests;
    struct ibv_qp *qp;
    struct ibv_cq *cq; /* todo: use a single cq per node or total? */

    void Init(ConnectionType t, uint32_t id, struct ibv_qp* q, struct ibv_cq* c) {
        FatalAssert(t < CONNECTION_TYPE_COUNT, LOG_TAG_RDMA,
                    "Invalid ConnectionType %hhu for ConnectionContext", t);
        FatalAssert(q != nullptr, LOG_TAG_RDMA,
                    "qp cannot be nullptr for ConnectionContext");
        FatalAssert(c != nullptr, LOG_TAG_RDMA,
                    "cq cannot be nullptr for ConnectionContext");
        FatalAssert(type == CONNECTION_TYPE_COUNT, LOG_TAG_RDMA,
                    "ConnectionContext is already initialized");
        FatalAssert(connection_id == 0, LOG_TAG_RDMA,
                    "ConnectionContext is already initialized");
        FatalAssert(last_task_id.load(std::memory_order_relaxed) == 0, LOG_TAG_RDMA,
                    "ConnectionContext is already initialized");
        FatalAssert(num_pending_requests.load(std::memory_order_relaxed) == 0, LOG_TAG_RDMA,
                    "ConnectionContext is already initialized");
        FatalAssert(qp == nullptr, LOG_TAG_RDMA,
                    "ConnectionContext is already initialized");
        FatalAssert(cq == nullptr, LOG_TAG_RDMA,
                    "ConnectionContext is already initialized");
        type = t;
        connection_id = id;
        qp = q;
        cq = c;
    }

    ConnectionContext() : type{CONNECTION_TYPE_COUNT}, connection_id{0}, local_psn{0}, remote_psn{0}, remote_qp_num{0},
        last_task_id{0}, num_pending_requests{0}, qp{nullptr}, cq{nullptr} {}
};

struct CommConnectionContext {
    ConnectionContext ctx;
    uintptr_t remote_buffer;
    std::atomic<uint8_t> current_buffer_idx;
    SXLock buffer_lock[NUM_COMM_BUFFERS_PER_CONNECTION];
    CommBuffer comm_buffers[NUM_COMM_BUFFERS_PER_CONNECTION];

    CommConnectionContext() = default;
    void Init(ConnectionType t, uint32_t id, struct ibv_qp* q, struct ibv_cq* c) {
        ctx.Init(t, id, q, c);
        current_buffer_idx.store(0, std::memory_order_relaxed);
    }
};

struct UrgentConnectionContext {
    ConnectionContext ctx;
    uintptr_t remote_buffer;
    std::atomic<uint8_t> current_buffer_idx;
    SXLock buffer_lock[NUM_URGENT_BUFFERS_PER_CONNECTION];
    UrgentBuffer urgent_buffers[NUM_URGENT_BUFFERS_PER_CONNECTION];

    UrgentConnectionContext() = default;
    void Init(ConnectionType t, uint32_t id, struct ibv_qp* q, struct ibv_cq* c) {
        ctx.Init(t, id, q, c);
        current_buffer_idx.store(0, std::memory_order_relaxed);
    }
};

struct RemoteMemoryRegion {
    uintptr_t addr;
    size_t length;
    uint32_t rkey;

    RemoteMemoryRegion() = default;
    RemoteMemoryRegion(uintptr_t a, size_t len, uint32_t rk) :
        addr{a}, length{len}, rkey{rk} {}
};

struct RDMABuffer {
    void* local_addr;
    uintptr_t remote_addr;
    size_t length;
};

struct NodeInfo {
    const uint8_t node_id;
    const uint32_t ip_addr;
    const uint16_t port;

    union ibv_gid remote_gid;
    int socket;

    ConnectionContext cn_cluster_read_connections[NUM_CONNECTIONS[CN_CLUSTER_READ]];
    ConnectionContext cn_cluster_write_connections[NUM_CONNECTIONS[CN_CLUSTER_WRITE]];
    CommConnectionContext cn_comm_connections[NUM_CONNECTIONS[CN_COMM]];
    CommConnectionContext mn_comm_connections[NUM_CONNECTIONS[MN_COMM]];
    UrgentConnectionContext mn_urgent_connections[NUM_CONNECTIONS[MN_URGENT]];

    std::map<uintptr_t, RemoteMemoryRegion> remoteMemoryRegions;

    NodeInfo(uint8_t id, uint32_t ip, uint16_t p) :
        node_id{id}, ip_addr{ip}, port{p}, socket{-1} {
        memset(&remote_gid, 0, sizeof(remote_gid));
    }

    uint32_t GetKey(uintptr_t addr, size_t length) const {
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

    void AddRegion(uintptr_t addr, size_t length, uint32_t rkey) {
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
};

struct HandshakeConnectionInfo {
    uint32_t qp_num;
    uint32_t psn;
};
struct HandshakeBufferedConnectionInfo {
    HandshakeConnectionInfo conn_info;
    uintptr_t buffer_addr;
    uint32_t buffer_rkey;
};

struct HandshakeInfo {
    union ibv_gid gid;
    HandshakeConnectionInfo cn_cluster_read_conns[NUM_CONNECTIONS[CN_CLUSTER_READ]];
    HandshakeConnectionInfo cn_cluster_write_conns[NUM_CONNECTIONS[CN_CLUSTER_WRITE]];
    HandshakeBufferedConnectionInfo comm_conns[NUM_CONNECTIONS[CN_COMM]];
    HandshakeBufferedConnectionInfo comm_conns[NUM_CONNECTIONS[MN_COMM]];
    HandshakeBufferedConnectionInfo urgent_conns[NUM_CONNECTIONS[MN_URGENT]];
};

class RDMA_Manager {
protected:
    inline static RDMA_Manager* rdmaManagerInstance = nullptr;

#ifdef MEMORY_NODE
    /* todo: maybe I need to pass this as a runtime arg?! needs tuning */
    static constexpr uint32_t MAX_SEND_WR[CONNECTION_TYPE_COUNT] = {
        0,      /* CN_CLUSTER_READ */
        0,      /* CN_CLUSTER_WRITE */
        16,     /* CN_COMM */
        16,     /* MN_COMM */
        16      /* MN_URGENT */
    };

    static constexpr int MAX_CQE[CONNECTION_TYPE_COUNT] = {
        1,                                            /* CN_CLUSTER_READ */ /* impossible to set to 0 */
        1,                                            /* CN_CLUSTER_WRITE */ /* impossible to set to 0 */
        /* I just generate a CQE when my send counter reaches MAX_SEND_WR for sending response and wait for its
           completion */
        1 * NUM_CONNECTIONS[CN_COMM],                         /* CN_COMM */
        1 * NUM_CONNECTIONS[MN_COMM],                         /* MN_COMM */
        1 * NUM_CONNECTIONS[MN_URGENT]                        /* MN_URGENT */
    };

    /* todo: we may need to increase this later */
    static constexpr uint32_t MAX_SEND_SGE[CONNECTION_TYPE_COUNT] = {
        1,   /* CN_CLUSTER_READ */ /* todo: need to tune -> maybe should get it as conf -> equal to span */
        1,   /* CN_CLUSTER_WRITE */ /* currently we only use 2 for num clusters during split */
        1,   /* CN_COMM */
        1,   /* MN_COMM */
        1    /* MN_URGENT */
    };

    /* todo: needs tuning */
    static constexpr uint32_t MAX_INLINE_DATA[CONNECTION_TYPE_COUNT] = {
        0,                            /* CN_CLUSTER_READ */
        0,                            /* CN_CLUSTER_WRITE */
        sizeof(CommBufferState),      /* CN_COMM */
        0,                            /* MN_COMM */
        URGENT_BUFFER_SIZE            /* MN_URGENT */
    };
#else
    /* todo: maybe I need to pass this as a runtime arg?! needs tuning */
    static constexpr uint32_t MAX_SEND_WR[CONNECTION_TYPE_COUNT] = {
        1024 / NUM_CONNECTIONS[CN_CLUSTER_READ],   /* CN_CLUSTER_READ */
        64,                                        /* CN_CLUSTER_WRITE */
        16,                                        /* CN_COMM */
        16,                                        /* MN_COMM */
        16                                         /* MN_URGENT */
    };

    static constexpr int MAX_CQE[CONNECTION_TYPE_COUNT] = {
        /* one per request as I need the cqe for reads */
        MAX_SEND_WR[0] * NUM_CONNECTIONS[CN_CLUSTER_READ],              /* CN_CLUSTER_READ */
        /* I just generate a CQE when my send counter reaches MAX_SEND_WR for sending response and wait for its
           completion */
        1 * NUM_CONNECTIONS[CN_CLUSTER_WRITE],                          /* CN_CLUSTER_WRITE */
        1 * NUM_CONNECTIONS[CN_COMM],                                   /* CN_COMM */
        1 * NUM_CONNECTIONS[MN_COMM],                                   /* MN_COMM */
        1 * NUM_CONNECTIONS[MN_URGENT]                                  /* MN_URGENT */
    };

    /* todo: we may need to increase this later */
    static constexpr uint32_t MAX_SEND_SGE[CONNECTION_TYPE_COUNT] = {
        32,  /* CN_CLUSTER_READ */ /* todo: need to tune -> maybe should get it as conf -> equal to span */
        2,   /* CN_CLUSTER_WRITE */ /* currently we only use 2 for num clusters during split */
        1,   /* CN_COMM */
        1,   /* MN_COMM */
        1    /* MN_URGENT */
    };

    /* todo: needs tuning */
    static constexpr uint32_t MAX_INLINE_DATA[CONNECTION_TYPE_COUNT] = {
        0,                            /* CN_CLUSTER_READ */
        0,                            /* CN_CLUSTER_WRITE */
        0,                            /* CN_COMM */
        sizeof(CommBufferState),      /* MN_COMM */
        sizeof(CommBufferState)       /* MN_URGENT */
    };
#endif

    static constexpr uint32_t MAX_RD_ATOMIC[CONNECTION_TYPE_COUNT] = {
        MAX_SEND_WR[0],   /* CN_CLUSTER_READ */
        0,                /* CN_CLUSTER_WRITE */
        0,                /* CN_COMM */
        0,                /* MN_COMM */
        0                 /* MN_URGENT */
    };

    /* We are not using two-sided verbs */
    static constexpr uint32_t MAX_RECV_WR[CONNECTION_TYPE_COUNT] = {
        0,  /* CN_CLUSTER_READ */
        0,  /* CN_CLUSTER_WRITE */
        0,  /* CN_COMM */
        0,  /* MN_COMM */
        0   /* MN_URGENT */
    };

    /* We are not using two-sided verbs */
    static constexpr uint32_t MAX_RECV_SGE[CONNECTION_TYPE_COUNT] = {
        0,  /* CN_CLUSTER_READ */
        0,  /* CN_CLUSTER_WRITE */
        0,  /* CN_COMM */
        0,  /* MN_COMM */
        0   /* MN_URGENT */
    };

    static constexpr ibv_mtu DEFAULT_MTU[CONNECTION_TYPE_COUNT] = {
        IBV_MTU_4096, /* CN_CLUSTER_READ */
        IBV_MTU_4096, /* CN_CLUSTER_WRITE */
        IBV_MTU_4096, /* CN_COMM */
        IBV_MTU_4096, /* MN_COMM */
        IBV_MTU_256   /* MN_URGENT */
    };

    const uint8_t num_nodes;
    const uint8_t self_node_id;

    struct ibv_context *ib_ctx = nullptr;
    struct ibv_device_attr dev_attr;
    struct ibv_port_attr port_attr;
    struct ibv_pd *pd = nullptr;
    struct ibv_cq* cn_cluster_read_cq = nullptr;
    struct ibv_cq* cn_cluster_write_cq = nullptr;
    struct ibv_cq* cn_comm_cq = nullptr;
    struct ibv_cq* mn_comm_cq = nullptr;
    struct ibv_cq* mn_urgent_cq = nullptr;
    uint8_t port_num = 0;
    int gid_index = -1;
    union ibv_gid dev_gid;

    std::vector<NodeInfo> nodes;
    std::map<uintptr_t, struct ibv_mr*> localMemoryRegions;

    static RetStatus CreateQP(struct ibv_qp** qp, struct ibv_cq* cq, struct ibv_pd* pd, ConnectionType type) {
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

    static RetStatus ModifyQPStateToReset(struct ibv_qp* qp) {
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

    static RetStatus ModifyQPStateToInit(struct ibv_qp* qp, uint8_t port_num) {
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

    static RetStatus ModifyQPStateToRTR(struct ibv_qp* qp, uint32_t dest_qp_num,
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

    static RetStatus ModifyQPStateToRTS(struct ibv_qp* qp, uint32_t local_psn, ConnectionType type) {
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
    RetStatus InitConnectionCtx(ConnectionType conn_type, struct ibv_cq* cq, ConnCtxType* connections) {
        String error_msg;
        RetStatus rs = RetStatus::Success();
        struct ibv_qp* qp = nullptr;
        FatalAssert(cq != nullptr, LOG_TAG_RDMA,
                    "cq cannot be nullptr");
        FatalAssert(pd != nullptr, LOG_TAG_RDMA,
                    "pd cannot be nullptr");
        FatalAssert(connections != nullptr, LOG_TAG_RDMA,
                    "connections cannot be nullptr");

        for (uint8_t conn_id = 0; conn_id < (uint8_t)(NUM_CONNECTIONS[conn_type]); ++conn_id) {
            if (conn_type == CN_COMM || conn_type == MN_COMM) {
                CommConnectionContext* comm_ctx = reinterpret_cast<CommConnectionContext*>(&connections[conn_id]);
                rs = RegisterMemory(comm_ctx->comm_buffers,
                                    sizeof(CommBuffer) * NUM_COMM_BUFFERS_PER_CONNECTION);
                if (!rs.IsOK()) {
                    error_msg = String("Failed to register comm buffers for connection type %s with id %hhu. Error: %s",
                                        ToString(conn_type).ToCStr(), conn_id, rs.Msg());
                    rs = RetStatus::Fail(error_msg.ToCStr());
                    break;
                }

                comm_ctx->ctx.local_psn = threadSelf->UniformRange32(0, (uint32_t)(1 << 24) - 1);
            } else if (conn_type == MN_URGENT) {
                UrgentConnectionContext* urgent_ctx = reinterpret_cast<UrgentConnectionContext*>(&connections[conn_id]);
                rs = RegisterMemory(urgent_ctx->urgent_buffers,
                                    sizeof(UrgentBuffer) * NUM_URGENT_BUFFERS_PER_CONNECTION);
                if (!rs.IsOK()) {
                    error_msg = String("Failed to register urgent buffers for connection type %s with id %hhu. Error: %s",
                                        ToString(conn_type).ToCStr(), conn_id, rs.Msg());
                    rs = RetStatus::Fail(error_msg.ToCStr());
                    break;
                }
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

    RetStatus EstablishTCPConnections() {
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

    void FillHandshakeInfo(uint8_t node_id, HandshakeInfo& handshake_info) {
        memset(&handshake_info, 0, sizeof(handshake_info));
        memcpy(&handshake_info.gid, &dev_gid, sizeof(dev_gid));

        for (size_t j = 0; j < NUM_CONNECTIONS[CN_CLUSTER_READ]; ++j) {
            FatalAssert(nodes[node_id].cn_cluster_read_connections[j].qp != nullptr, LOG_TAG_RDMA,
                        "CN_CLUSTER_READ QP is nullptr for node %hhu connection %lu",
                        node_id, j);
            handshake_info.cn_cluster_read_conns[j].psn = nodes[node_id].cn_cluster_read_connections[j].local_psn;
            handshake_info.cn_cluster_read_conns[j].qp_num = nodes[node_id].cn_cluster_read_connections[j].qp->qp_num;
        }

        for (size_t j = 0; j < NUM_CONNECTIONS[CN_CLUSTER_WRITE]; ++j) {
            FatalAssert(nodes[node_id].cn_cluster_write_connections[j].qp != nullptr, LOG_TAG_RDMA,
                        "CN_CLUSTER_WRITE QP is nullptr for node %hhu connection %lu",
                        node_id, j);
            handshake_info.cn_cluster_write_conns[j].psn = nodes[node_id].cn_cluster_write_connections[j].local_psn;
            handshake_info.cn_cluster_write_conns[j].qp_num = nodes[node_id].cn_cluster_write_connections[j].qp->qp_num;
        }

        for (size_t j = 0; j < NUM_CONNECTIONS[CN_COMM]; ++j) {
            FatalAssert(nodes[node_id].cn_comm_connections[j].ctx.qp != nullptr, LOG_TAG_RDMA,
                        "CN_COMM QP is nullptr for node %hhu connection %lu",
                        node_id, j);
            handshake_info.comm_conns[j].conn_info.psn = nodes[node_id].cn_comm_connections[j].ctx.local_psn;
            handshake_info.comm_conns[j].conn_info.qp_num = nodes[node_id].cn_comm_connections[j].ctx.qp->qp_num;
            handshake_info.comm_conns[j].buffer_addr =
                reinterpret_cast<uintptr_t>(&nodes[node_id].cn_comm_connections[j].comm_buffers);
            FatalAssert(nodes[node_id].remoteMemoryRegions.find(handshake_info.comm_conns[j].buffer_addr) !=
                        nodes[node_id].remoteMemoryRegions.end(),
                        LOG_TAG_RDMA,
                        "Remote memory region for comm buffer not found for node %hhu connection %lu",
                        node_id, j);
            handshake_info.comm_conns[j].buffer_rkey =
                nodes[node_id].remoteMemoryRegions[handshake_info.comm_conns[j].buffer_addr].rkey;
        }

        for (size_t j = 0; j < NUM_CONNECTIONS[MN_COMM]; ++j) {
            FatalAssert(nodes[node_id].mn_comm_connections[j].ctx.qp != nullptr, LOG_TAG_RDMA,
                        "MN_COMM QP is nullptr for node %hhu connection %lu",
                        node_id, j);
            handshake_info.comm_conns[j].conn_info.psn = nodes[node_id].mn_comm_connections[j].ctx.local_psn;
            handshake_info.comm_conns[j].conn_info.qp_num = nodes[node_id].mn_comm_connections[j].ctx.qp->qp_num;
            handshake_info.comm_conns[j].buffer_addr =
                reinterpret_cast<uintptr_t>(&nodes[node_id].mn_comm_connections[j].comm_buffers);
            FatalAssert(nodes[node_id].remoteMemoryRegions.find(handshake_info.comm_conns[j].buffer_addr) !=
                        nodes[node_id].remoteMemoryRegions.end(),
                        LOG_TAG_RDMA,
                        "Remote memory region for comm buffer not found for node %hhu connection %lu",
                        node_id, j);
            handshake_info.comm_conns[j].buffer_rkey =
                nodes[node_id].remoteMemoryRegions[handshake_info.comm_conns[j].buffer_addr].rkey;
        }

        for (size_t j = 0; j < NUM_CONNECTIONS[MN_URGENT]; ++j) {
            FatalAssert(nodes[node_id].mn_urgent_connections[j].ctx.qp != nullptr, LOG_TAG_RDMA,
                        "MN_URGENT QP is nullptr for node %hhu connection %lu",
                        node_id, j);
            handshake_info.urgent_conns[j].conn_info.psn = nodes[node_id].mn_urgent_connections[j].ctx.local_psn;
            handshake_info.urgent_conns[j].conn_info.qp_num = nodes[node_id].mn_urgent_connections[j].ctx.qp->qp_num;
            handshake_info.urgent_conns[j].buffer_addr =
                reinterpret_cast<uintptr_t>(&nodes[node_id].mn_urgent_connections[j].urgent_buffers);
            FatalAssert(nodes[node_id].remoteMemoryRegions.find(handshake_info.urgent_conns[j].buffer_addr) !=
                        nodes[node_id].remoteMemoryRegions.end(),
                        LOG_TAG_RDMA,
                        "Remote memory region for urgent buffer not found for node %hhu connection %lu",
                        node_id, j);
            handshake_info.urgent_conns[j].buffer_rkey =
                nodes[node_id].remoteMemoryRegions[handshake_info.urgent_conns[j].buffer_addr].rkey;
        }
    }

    void ProcessHandshakeInfo(uint8_t node_id, const HandshakeInfo& handshake_info) {
        memcpy(&nodes[node_id].remote_gid, &handshake_info.gid, sizeof(handshake_info.gid));

        for (size_t j = 0; j < NUM_CONNECTIONS[CN_CLUSTER_READ]; ++j) {
            nodes[node_id].cn_cluster_read_connections[j].remote_psn = handshake_info.cn_cluster_read_conns[j].psn;
            nodes[node_id].cn_cluster_read_connections[j].remote_qp_num = handshake_info.cn_cluster_read_conns[j].qp_num;
        }

        for (size_t j = 0; j < NUM_CONNECTIONS[CN_CLUSTER_WRITE]; ++j) {
            nodes[node_id].cn_cluster_write_connections[j].remote_psn = handshake_info.cn_cluster_write_conns[j].psn;
            nodes[node_id].cn_cluster_write_connections[j].remote_qp_num = handshake_info.cn_cluster_write_conns[j].qp_num;
        }

        for (size_t j = 0; j < NUM_CONNECTIONS[CN_COMM]; ++j) {
            nodes[node_id].cn_comm_connections[j].ctx.remote_psn = handshake_info.comm_conns[j].conn_info.psn;
            nodes[node_id].cn_comm_connections[j].ctx.remote_qp_num = handshake_info.comm_conns[j].conn_info.qp_num;
            RemoteMemoryRegion rmr;
            rmr.addr = handshake_info.comm_conns[j].buffer_addr;
            rmr.rkey = handshake_info.comm_conns[j].buffer_rkey;
            nodes[node_id].remoteMemoryRegions[rmr.addr] = rmr;
            nodes[node_id].cn_comm_connections[j].remote_buffer = rmr.addr;
        }

        for (size_t j = 0; j < NUM_CONNECTIONS[MN_COMM]; ++j) {
            nodes[node_id].mn_comm_connections[j].ctx.remote_psn = handshake_info.comm_conns[j].conn_info.psn;
            nodes[node_id].mn_comm_connections[j].ctx.remote_qp_num = handshake_info.comm_conns[j].conn_info.qp_num;
            RemoteMemoryRegion rmr;
            rmr.addr = handshake_info.comm_conns[j].buffer_addr;
            rmr.rkey = handshake_info.comm_conns[j].buffer_rkey;
            nodes[node_id].remoteMemoryRegions[rmr.addr] = rmr;
            nodes[node_id].mn_comm_connections[j].remote_buffer = rmr.addr;
        }

        for (size_t j = 0; j < NUM_CONNECTIONS[MN_URGENT]; ++j) {
            nodes[node_id].mn_urgent_connections[j].ctx.remote_psn = handshake_info.urgent_conns[j].conn_info.psn;
            nodes[node_id].mn_urgent_connections[j].ctx.remote_qp_num = handshake_info.urgent_conns[j].conn_info.qp_num;
            RemoteMemoryRegion rmr;
            rmr.addr = handshake_info.urgent_conns[j].buffer_addr;
            rmr.rkey = handshake_info.urgent_conns[j].buffer_rkey;
            nodes[node_id].remoteMemoryRegions[rmr.addr] = rmr;
            nodes[node_id].mn_urgent_connections[j].remote_buffer = rmr.addr;
        }
    }

    RetStatus Handshake() {
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

    RetStatus EstablishRDMAConnections() {
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
                rs = ModifyQPStateToRTR(nodes[i].cn_cluster_read_connections[j].qp,
                                        nodes[i].cn_cluster_read_connections[j].remote_qp_num,
                                        nodes[i].remote_gid,
                                        port_num,
                                        nodes[i].cn_cluster_read_connections[j].remote_psn,
                                        CN_CLUSTER_READ);
                if (!rs.IsOK()) {
                    DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_RDMA,
                            "Failed to modify CN_CLUSTER_READ QP to RTR for node %hhu connection %zu. Error: %s",
                            i, j, rs.Msg());
                    return rs;
                }

                rs = ModifyQPStateToRTS(nodes[i].cn_cluster_read_connections[j].qp,
                                        nodes[i].cn_cluster_read_connections[j].local_psn,
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

    RetStatus DisconnectAllNodes();
    RetStatus DeregisterAllMemoryRegions();

    inline void Cleanup() {
        DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED,
                "RDMA_Manager Cleanup is not implemented yet");
    }

    /* todo: support multiple memory nodes */
    RDMA_Manager(uint8_t self_id, const std::vector<std::pair<const char*, uint16_t>>& node_addresses,
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
        int ret = 0;
        String error_msg;
        String dev_list_str;
        int target_device_index;
        uint8_t num_valid_nodes;
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
        for (uint8_t i = 0; i < num_nodes; ++i) {
            if (i == self_id) {
                continue;
            }
            ++num_valid_nodes;
            nodes.emplace_back(i, inet_addr(node_addresses[i].first), node_addresses[i].second);

            RetStatus rs = InitConnectionCtx(ConnectionType::CN_CLUSTER_READ,
                                             cn_cluster_read_cq, nodes.back().cn_cluster_read_connections);
            if (!rs.IsOK()) {
                error_msg = String("Failed to initialize CN Read QPs to node %hhu. Error: %s",
                                   i, rs.Msg());
                goto ERROR_EXIT;
            }

            rs = InitConnectionCtx(ConnectionType::CN_CLUSTER_WRITE,
                                   cn_cluster_write_cq, nodes.back().cn_cluster_write_connections);
            if (!rs.IsOK()) {
                error_msg = String("Failed to initialize CN Write QPs to node %hhu. Error: %s",
                                   i, rs.Msg());
                goto ERROR_EXIT;
            }

            rs = InitConnectionCtx(ConnectionType::CN_COMM,
                                   cn_comm_cq, nodes.back().cn_comm_connections);
            if (!rs.IsOK()) {
                error_msg = String("Failed to initialize CN Comm QPs to node %hhu. Error: %s",
                                   i, rs.Msg());
                goto ERROR_EXIT;
            }

            rs = InitConnectionCtx(ConnectionType::MN_COMM,
                                   mn_comm_cq, nodes.back().mn_comm_connections);
            if (!rs.IsOK()) {
                error_msg = String("Failed to initialize MN Comm QPs to node %hhu. Error: %s",
                                   i, rs.Msg());
                goto ERROR_EXIT;
            }

            rs = InitConnectionCtx(ConnectionType::MN_URGENT,
                                   mn_urgent_cq, nodes.back().mn_urgent_connections);
            if (!rs.IsOK()) {
                error_msg = String("Failed to initialize MN Urgent QPs to node %hhu. Error: %s",
                                   i, rs.Msg());
                goto ERROR_EXIT;
            }
        }

        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
                "Created QPs for each of the %hhu/%hhu nodes and registered their connection buffers.",
                num_valid_nodes, num_nodes);

        return;

ERROR_EXIT:
        Cleanup();
        DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_RDMA, "%s", error_msg.ToCStr());
    }

    ~RDMA_Manager() {
        Cleanup();
    }

public:
    /* --------- start of not thread-safe region --------- */
    static RetStatus Initialize(uint8_t self_id, const std::vector<std::pair<const char*, uint16_t>>& node_addresses,
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

    static void Destroy() {
        FatalAssert(rdmaManagerInstance != nullptr, LOG_TAG_RDMA,
                    "RDMA_Manager is not initialized");
        if (rdmaManagerInstance != nullptr) {
            DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_RDMA,
                    "Destroying RDMA_Manager instance");
            delete rdmaManagerInstance;
            rdmaManagerInstance = nullptr;
        }
    }
    inline static RDMA_Manager* GetInstance() {
        CHECK_NOT_NULLPTR(rdmaManagerInstance, LOG_TAG_RDMA);
        return rdmaManagerInstance;
    }

    RetStatus RegisterMemory(Address addr, size_t length) {
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

    RetStatus EstablishConnections();
    /* todo: API for creating new connections and adding new nodes to the system */

    /* --------- end of not thread-safe region --------- */

    /* for urgent buffers we CAS the state for synchronization. for commbuffers we first check the state and if valid
        */
    RetStatus GrabCommBuffer(void*& buffer, size_t len, bool is_urgent = false);
    RetStatus ReleaseCommBuffer(void* buffer, ConnTaskId& task_id, bool flush = false, bool is_urgent = false);
    RetStatus RDMAWrite(RDMABuffer* rdma_buffers, size_t num_buffers,
                        uint8_t target_node_id, uint32_t connection_id, ConnTaskId& task_id);
    RetStatus RDMARead(RDMABuffer* rdma_buffers, size_t num_buffers,
                       uint8_t target_node_id, uint32_t connection_id, ConnTaskId& task_id);

TESTABLE;
};


};

#endif