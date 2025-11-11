#ifndef RDMA_MANAGER_H_
#define RDMA_MANAGER_H_

#include "common.h"
#include "debug.h"

#include <infiniband/verbs.h>

namespace divftree {

struct ConnectionContext {
    // uint8_t target_node_id; /* 0 is the memory node */
    uint32_t connection_id;
    uint64_t last_task_id;
    struct ibv_qp *qp;
    struct ibv_cq *cq; /* todo: use a single cq per node or total? */
};

// struct MemoryRegion {
//     Address addr;
//     size_t length;
// };

struct NodeInfo {
    const uint8_t node_id;
    const uint32_t ip_addr;
    const uint16_t port;

    std::vector<ConnectionContext> connections;
    /* todo: do we need it for more than sanity_check? */
    // std::vector<MemoryRegion> remoteMemoryRegions; /* maybe sort based on address */
};

class RDMA_Manager {
protected:
    inline static RDMA_Manager* rdmaManagerInstance = nullptr;

    const uint8_t num_nodes = 0;
    const uint8_t self_node_id = 0;

    struct ibv_device **dev_list = nullptr;
    struct ibv_context *ib_ctx = nullptr;
    struct ibv_pd *pd = nullptr;

    std::vector<NodeInfo> nodes;
    std::vector<struct ibv_mr*> localMemoryRegions;

    RetStatus EstablishTCPConnections();
    RetStatus Handshake();
    RetStatus EstablishRDMAConnections();

    RetStatus DisconnectAllNodes();
    RetStatus DeregisterAllMemoryRegions();

public:
    RDMA_Manager() = default;
    ~RDMA_Manager() = default;

    /* --------- start of not thread-safe region --------- */
    static RetStatus Initialize();
    static void Cleanup();
    inline static RDMA_Manager* GetInstance();

    /* if target_node_id == self_id then it will be broadcasted */
    RetStatus RegisterMemory(Address addr, size_t length);
    RetStatus EstablishConnections();
    /* todo: API for creating new connections and adding new nodes to the system */

    /* --------- end of not thread-safe region --------- */


    RetStatus RDMAWrite(Address src, Address dest, size_t length,
                        uint8_t target_node_id, uint32_t connection_id);
    RetStatus RDMARead(Address src, Address dest, size_t length,
                       uint8_t target_node_id, uint32_t connection_id);
    // RetStatus RDMAAtomicFetchAndAdd(Address src, Address dest, uint64_t add,
    //                                 uint64_t* output, uint8_t target_node_id, uint32_t connection_id);
    // RetStatus RDMAAtomicCompareAndSwap(Address src, Address dest, uint64_t compare,
    //                                    uint64_t swap, uint64_t* output, uint8_t target_node_id,
    //                                    uint32_t connection_id);


TESTABLE;
};


};

#endif