#ifndef DIVFTREE_BUFFER_INTERFACE_H_
#define DIVFTREE_BUFFER_INTERFACE_H_

#include "common.h"
#include "vector_utils.h"

#include "utils/synchronization.h"

namespace divftree {

    /*
  * Data Structures:
  * // May have a differnet BufferEntry for non-centroid vectors as they do not need spinlock, readerPin, clusterPtr, ...
  * 1) BufferEntry:
  *     1.1) SXLock
  *     1.2) CondVar
  *     1.3) Container: VectorID -> 64bit
  *     1.4) EntryOffset: 16bit
  *     1.5) State: 16bit? -> same as the state in the cluster
  *     1.5) ContainerVersion 32bit
  *     1.6) SXSpinLock
  *     1.7) ReaderPin: 64bit
  *     1.8) Ptr: Cluster* -> 64bit
  *     1.9) Log: fixed size list:<OldVersion, NewVersion, UpdateEntry>:
  *             has a 32bit head and a 32bit tail. when a new element is added,
  *             if tail+1 == head(list is full), both head and tail are advanced by 1
  *             and oldest version is overwritten. else tail is advanced by 1.
  *
  */

class BufferVectorEntry {
public:
protected:
    /* Use atomic or volatile -> we need to use atomic store 128 bit */
    union {
        struct {
            VectorID parent_id;
            uint16_t entry_offset;
        } _data;
        uint128_t raw;
    } vector_address;
};

class BufferVertexEntry {
public:
protected:
    BufferVectorEntry centroid_address;
    SXLock cluster_lock;
    CondVar cond_var;
    SXSpinLock header_lock;
    std::atomic<uint64_t> reader_pin;
    DIVFTreeVertexInterface* cluster_ptr;
    // fixed size log
};

class BufferManagerInterface {
// TODO: reuse deleted IDs
public:
    virtual ~BufferManagerInterface() = default;

    virtual RetStatus Init() = 0;
    virtual RetStatus Shutdown() = 0;

    virtual DIVFTreeVertexInterface* GetVertex(VectorID vertex_id) = 0;
    virtual DIVFTreeVertexInterface* GetContainerLeaf(VectorID vec_id)= 0;
    virtual Vector GetVector(VectorID id) = 0;

    virtual bool InBuffer(VectorID id) = 0;

    virtual VectorID RecordRoot() = 0;
    virtual VectorID RecordVector(uint8_t level) = 0;

    virtual RetStatus UpdateVectorAddress(VectorID id, Address vector_address) = 0;
    virtual RetStatus UpdateClusterAddress(VectorID id, Address cluster_address) = 0;
    // virtual RetStatus Update(VectorID id, Address vector_address, Address cluster_address) = 0;

    // virtual void Evict(VectorID id) = 0;
    // virtual void Erase(VectorID id) = 0;

    virtual uint64_t GetHeight() = 0;

    virtual String ToString() = 0;

protected:
    virtual VectorID NextID(uint8_t level) = 0;
};

};

#endif