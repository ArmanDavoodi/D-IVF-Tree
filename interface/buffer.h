#ifndef COPPER_BUFFER_INTERFACE_H_
#define COPPER_BUFFER_INTERFACE_H_

#include "common.h"
#include "vector_utils.h"

namespace copper {

class BufferManagerInterface {
// TODO: reuse deleted IDs
public:

    virtual RetStatus Init() = 0;
    virtual RetStatus Shutdown() = 0;

    virtual CopperNodeInterface* GetNode(VectorID node_id) = 0;

    /* virtual LeafNode* Get_Leaf(VectorID leaf_id) = 0; */

    virtual CopperNodeInterface* GetContainerLeaf(VectorID vec_id)= 0;

    virtual Vector GetVector(VectorID id) = 0;

    virtual bool InBuffer(VectorID id) = 0;

    virtual VectorID RecordRoot() = 0;

    virtual VectorID RecordVector(uint8_t level) = 0;

    virtual RetStatus UpdateVectorAddress(VectorID id, Address vector_address) = 0;

    virtual RetStatus UpdateClusterAddress(VectorID id, Address cluster_address) = 0;

    virtual RetStatus Update(VectorID id, Address vector_address, Address cluster_address) = 0;

    virtual void Evict(VectorID id) = 0;

    virtual void Erase(VectorID id) = 0;

    virtual uint64_t GetHeight() = 0;

    virtual String ToString() = 0;

protected:
    virtual VectorID NextID(uint8_t level) = 0;
};

};

#endif