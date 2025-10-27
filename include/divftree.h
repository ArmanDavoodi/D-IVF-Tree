#ifndef DIVFTREE_H_
#define DIVFTREE_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"
#include "distance.h"
#include "distributed_common.h"

#include "utils/synchronization.h"
#include "utils/concurrent_datastructures.h"

#include "interface/divftree.h"

#include <algorithm>
#include <atomic>
#include <unordered_map>
#include <vector>

// Todo: better logs and asserts -> style: <Function name>(self data(a=?), input data): msg, additonal variables if needed

/*
 * Todo: There are two approaches we can take:
 * 1) Bottom-up nearest approach:
 *      This is our current approach in which we try to gurantee(lazily) that each vector is always in its closest vertex.
 *      However, this approach requires us to either have migration per insert or
 *      to check every possible leaf to see which one is closest.
 *      Imagin this example with 1 dim vectors:
 *      Layer 2:        1          2
 *      Layer 1:    0.9   1.1  1.6   2.6
 *      Layer 0:   ........................
 *      Now if we want to insert 1.4, it is closer to 1 so it will be inserted in leaf with centroid 1.1 while the closest
 *      leaf centroid to it is 1.6.
 *      probably better to have larger buckets.
 * 2) top-down nearest approach:
 *      In this approach, I gurantee that we can always take the greedy path from the root of the tree to
 *      find a vector.
 *      To do so, at the time of split of vertex k, we have to check migrataion for every vector in any layer in that subtree
 *      to other sibling vertices of vertex k.(sibling vertices are vertices with the same parent)
 *      As a result, a split in higher levels is more costly. and the cost only grows. lazy approach will cause us to have
 *      bad accuracy for a long time.
 *      probably having small buckets is better here.
 *
 */

namespace divftree {

struct MigrationCheckTask {
    VectorID first;
    VectorID second;
};

struct MergeTask {
    VectorID target;
};

struct CompactionTask {
    VectorID target;
};

struct SearchTask {
    DIVFThreadID master;
    uint64_t taskId;
    VectorID target;
    Version version;
    const VTYPE* query;
    size_t k;

    std::atomic<bool>* taken;
    std::atomic<bool>* done;
    bool done_waiting;
    SortedList<ANNVectorInfo, SimilarityComparator>* neighbours;

    SearchTask() = default;
    SearchTask(uint64_t task_id, VectorID id, Version ver, const VTYPE* q, size_t span, std::atomic<bool>* tk,
               std::atomic<bool>* dn) :
        master(threadSelf->ID()), taskId(task_id),
        target(id), version(ver), query(q), k(span),
        taken(tk), done(dn), done_waiting(false), neighbours(nullptr) {
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(q, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(tk, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(dn, LOG_TAG_DIVFTREE);
        FatalAssert(span > 0, LOG_TAG_DIVFTREE, "k cannot be 0!");
    }

    inline void CopyFrom(const SearchTask& other) {
        master = other.master;
        taskId = other.taskId;
        target = other.target;
        version = other.version;
        query = other.query;
        k = other.k;
        neighbours = other.neighbours;
    }
};

struct MigrationInfo {
    VectorID id;
    uint16_t offset;
    Version version;

    MigrationInfo() = default;
    MigrationInfo(VectorID vec_id, uint16_t vec_off, Version ver = 0) : id(vec_id), offset(vec_off), version(ver) {}

    inline bool operator<(const MigrationInfo& other) const {
        return id < other.id;
    }
};

class DIVFTreeVertex : public DIVFTreeVertexInterface {
public:
    DIVFTreeVertex() = delete;
    DIVFTreeVertex(DIVFTreeVertex&) = delete;
    DIVFTreeVertex(const DIVFTreeVertex&) = delete;
    DIVFTreeVertex(DIVFTreeVertex&&) = delete;

    DIVFTreeVertex(const DIVFTreeVertexAttributes& attributes) : attr(attributes), unpinCount{0},
                                                                 cluster(attributes.centroid_id.IsLeaf(),
                                                                         attributes.block_size,
                                                                         attributes.cap,
                                                                         attributes.index->GetAttributes().dimension,
                                                                         true) {
        CHECK_MIN_MAX_SIZE(attr.min_size, attr.cap, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_VECTORID_IS_VALID(attr.centroid_id, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_VECTORID_IS_CENTROID(attr.centroid_id, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_NOT_NULLPTR(attr.index, LOG_TAG_DIVFTREE_VERTEX);
        FatalAssert(attr.block_size > 0, LOG_TAG_DIVFTREE_VERTEX, "Block size cannot be 0!");
#ifdef MEMORY_DEBUG
        DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "%p constructed -> ID:" VECTORID_LOG_FMT ", Version:%u",
                this, VECTORID_LOG(attributes.centroid_id), attributes.version);
#endif
    }

    DIVFTreeVertex(const DIVFTreeAttributes& attributes, VectorID id, DIVFTreeInterface* index) :
        attr(id, 0,
             id.IsLeaf() ? attributes.leaf_min_size : attributes.internal_min_size,
             id.IsLeaf() ? attributes.leaf_max_size: attributes.internal_max_size,
             id.IsLeaf() ? attributes.leaf_blck_size: attributes.internal_blck_size, index),
             unpinCount{0}, cluster(id.IsLeaf(),
                                    id.IsLeaf() ? attributes.leaf_blck_size: attributes.internal_blck_size,
                                    id.IsLeaf() ? attributes.leaf_max_size: attributes.internal_max_size,
                                    attributes.dimension, true) {
        CHECK_MIN_MAX_SIZE(attr.min_size, attr.cap, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_VECTORID_IS_VALID(attr.centroid_id, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_VECTORID_IS_CENTROID(attr.centroid_id, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_NOT_NULLPTR(attr.index, LOG_TAG_DIVFTREE_VERTEX);
        FatalAssert(attr.block_size > 0, LOG_TAG_DIVFTREE_VERTEX, "Block size cannot be 0!");
        FatalAssert(attr.version == 0, LOG_TAG_DIVFTREE_VERTEX, "The first version should be 0!");
#ifdef MEMORY_DEBUG
        DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "%p constructed -> ID:" VECTORID_LOG_FMT ", Version:%u",
                this, VECTORID_LOG(id), 0);
#endif
    }

    ~DIVFTreeVertex() override {
        FatalAssert(unpinCount.load(std::memory_order_relaxed) == 0, LOG_TAG_DIVFTREE_VERTEX,
                    "the vertex is still pinned!");
#ifdef MEMORY_DEBUG
        DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "%p destroyed -> ID:" VECTORID_LOG_FMT ", Version:%u",
                this, VECTORID_LOG(attr.centroid_id), attr.version);
#endif
        if (attr.centroid_id.IsLeaf()) {
            return;
        }

        uint16_t size = cluster.header.reserved_size.load(std::memory_order_relaxed);
        FatalAssert(size == cluster.header.visible_size.load(std::memory_order_relaxed), LOG_TAG_DIVFTREE_VERTEX,
                    "mismatch between visited and reserved!");
        const uint16_t dim = attr.index->GetAttributes().dimension;
        CentroidMetaData* vmd =
            reinterpret_cast<CentroidMetaData*>(cluster.MetaData(0, attr.centroid_id.IsLeaf(),
                                                                 attr.block_size, attr.cap, dim));
        FatalAssert(cluster.NumBlocks(attr.block_size, attr.cap) == 1, LOG_TAG_NOT_IMPLEMENTED,
                    "cannot support more than one block!");
        for (uint16_t i = 0; i < size; ++i) {
            VectorState state = vmd[i].state.load(std::memory_order_relaxed);
            /*
             * if state is valid, it means that there was a compaction and we did not change the state nor did we
             * unpined it as it was simply moved to the new memory location
             */
            if (state == VECTOR_STATE_MIGRATED || state == VECTOR_STATE_OUTDATED) {
                FatalAssert(vmd[i].id != INVALID_VECTOR_ID, LOG_TAG_DIVFTREE_VERTEX, "cannot unpin invalid vector");
                BufferManager::GetInstance()->UnpinVertexVersion(vmd[i].id, vmd[i].version);
            }
        }
    }

    inline void Unpin() override {
        uint64_t curr_cnt = unpinCount.fetch_add(1) + 1;
#ifdef MEMORY_DEBUG
        DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "%p cluster unpinned -> ID:" VECTORID_LOG_FMT
                ", Version:%u, unpinCnt=%lu", this, VECTORID_LOG(attr.centroid_id), attr.version, curr_cnt);
#endif
        if (curr_cnt == 0) {
            DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Unpin and delete vertex " VECTORID_LOG_FMT
                    ", ver:%u, addr:%p", VECTORID_LOG(attr.centroid_id), attr.version, this);
            /* todo: we need to handle all the children(unpining their versions and stuff) in the destructor */
            this->~DIVFTreeVertex();
            BufferManager::GetInstance()->Recycle(this);
            return;
        }
        else {
            // DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Unpin vertex " VECTORID_LOG_FMT,
            //      VECTORID_LOG(attr.centroid_id));
        }
    }

    inline void MarkForRecycle(uint64_t pinCount) override {
        FatalAssert(pinCount >= unpinCount.load(), LOG_TAG_DIVFTREE_VERTEX,
                    "The cluster was unpinned more times than it was pinned. pinCount=%lu, unpinCount=%lu",
                    pinCount, unpinCount.load());
        uint64_t newPin = unpinCount.fetch_sub(pinCount) - pinCount;
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Mark vertex " VECTORID_LOG_FMT
            " ver:%u, addr=%p, for recycle with pin count %lu", VECTORID_LOG(attr.centroid_id),
            attr.version, this, pinCount);
#ifdef MEMORY_DEBUG
        DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE_VERTEX, "%p cluster marked for recycke -> ID:" VECTORID_LOG_FMT
                ", Version:%u, pinCount=%lu, unpinCnt=%lu", this, VECTORID_LOG(attr.centroid_id), attr.version,
                pinCount, newPin);
#endif
        if (newPin == 0) {
            DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Delete vertex " VECTORID_LOG_FMT ", ver:%u, addr:%p",
                 VECTORID_LOG(attr.centroid_id), attr.version, this);
            /* todo: we need to handle all the children(unpining their versions and stuff) in the destructor */
            this->~DIVFTreeVertex();
            BufferManager::GetInstance()->Recycle(this);
            return;
        }
    }

    /* todo: make sure the vertex is locked in shared/exclusive mode when calling this function! */
    RetStatus BatchInsert(const ConstVectorBatch& batch, uint16_t marked_for_update = INVALID_OFFSET) override {
        // CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
        FatalAssert(batch.size > 0, LOG_TAG_DIVFTREE_VERTEX,
                    "Batch size must be greater than zero.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                    "Batch data must not be null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                    "Batch id must not be null.");
        uint16_t offset = cluster.header.reserved_size.fetch_add(batch.size);
        if (offset + batch.size >= attr.cap) {
            cluster.header.reserved_size.fetch_sub(batch.size);
            return RetStatus{.stat = RetStatus::VERTEX_NOT_ENOUGH_SPACE, .message=nullptr};
        }
        FatalAssert(offset < cluster.header.reserved_size.load(), LOG_TAG_DIVFTREE_VERTEX, "Overflow detected!");
        BufferManager* bufferMgr = BufferManager::GetInstance();
        FatalAssert(bufferMgr != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                    "BufferManager is not initialized.");
        const uint16_t dim = attr.index->GetAttributes().dimension;
        VectorID marked_id = INVALID_VECTOR_ID;
        CentroidMetaData* markedMeta = nullptr;
        if (marked_for_update != INVALID_OFFSET) {
            FatalAssert(marked_for_update < offset, LOG_TAG_DIVFTREE_VERTEX,
                        "Marked for update offset %hu is not less than the start of newly inserted batch %hu.",
                        marked_for_update, offset);
            FatalAssert(attr.centroid_id.IsInternalVertex(), LOG_TAG_DIVFTREE_VERTEX,
                        "Marked for update offset can only be set for internal vertices.");
            markedMeta = reinterpret_cast<CentroidMetaData*>(
                cluster.MetaData(marked_for_update, false, attr.block_size, attr.cap, dim));
            marked_id = markedMeta->id;
        }

        VTYPE* dest = cluster.Data(offset, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        memcpy(dest, batch.data, batch.size * dim * sizeof(VTYPE));
        Address meta = cluster.MetaData(offset, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        for (uint16_t i = 0; i < batch.size; ++i) {
            if (attr.centroid_id.IsLeaf()) {
                VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(meta);
                FatalAssert(&vmd[i] ==
                            &((VectorMetaData*)(cluster.MetaData(0, true, attr.block_size, attr.cap, dim)))[i + offset],
                            LOG_TAG_DIVFTREE_VERTEX, "address mismatch!");
                vmd[i].id = batch.id[i];
                FatalAssert(vmd[i].state.load(std::memory_order_relaxed) == VECTOR_STATE_INVALID,
                            LOG_TAG_DIVFTREE_VERTEX, "Reserved space at offset %hu is not in INVALID state.",
                            offset + i);
                vmd[i].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
#ifdef EXCESS_LOGING
                DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX,
                    "Inserting vector={id= " VECTORID_LOG_FMT ", data=%s} into %s: stored vector data = %s",
                    VECTORID_LOG(batch.id[i]), VectorToString(&batch.data[i * dim], dim).ToCStr(),
                    VectorLocation(attr.centroid_id, attr.version, offset + i).ToString().ToCStr(),
                    VectorToString(&dest[i * dim], dim).ToCStr());
#endif
            } else {
                FatalAssert(batch.version != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                            "Batch version must not be null for centroid insertion.");
                CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(meta);
                FatalAssert(&vmd[i] ==
                            &((CentroidMetaData*)(cluster.MetaData(0, false, attr.block_size, attr.cap, dim)))[i + offset],
                            LOG_TAG_DIVFTREE_VERTEX, "address mismatch!");
                vmd[i].id = batch.id[i];
                /* todo: should we pin version for the marked vertex?! yeah because its version is differnet */
                bufferMgr->PinVertexVersion(batch.id[i], batch.version[i]);
                vmd[i].version = batch.version[i];
                FatalAssert(vmd[i].state.load(std::memory_order_relaxed) == VECTOR_STATE_INVALID,
                            LOG_TAG_DIVFTREE_VERTEX, "Reserved space at offset %hu is not in INVALID state.",
                            offset + i);
                vmd[i].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
#ifdef EXCESS_LOGING
                DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX,
                    "Inserting vector={id= " VECTORID_LOG_FMT ", version=%u, data=%s} into %s: stored vector data = %s",
                    VECTORID_LOG(batch.id[i]), batch.version[i], VectorToString(&batch.data[i * dim], dim).ToCStr(),
                    VectorLocation(attr.centroid_id, attr.version, offset + i).ToString().ToCStr(),
                    VectorToString(&dest[i * dim], dim).ToCStr());
#endif
            }
        }

        cluster.header.visible_size.fetch_add(batch.size);

        for (uint16_t i = 0; i < batch.size; ++i) {
            // DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "UpdateVectorLocation in batch insert:");
            bufferMgr->
                UpdateVectorLocation(batch.id[i], VectorLocation(attr.centroid_id, attr.version, offset + i),
                                     batch.id[i] != marked_id);
        }

        if (markedMeta != nullptr) {
            markedMeta->state.store(VECTOR_STATE_OUTDATED);
            cluster.header.num_deleted.fetch_add(1);
            /*
             * we cannot unpin the outdated version here because if an ANN search has read this version but
             * has not pinned it, and the time it was not outdated,
             * the version will be lost.
             */
            // bufferMgr->UnpinVertexVersion(markedMeta->id, markedMeta->version);
        }

        return RetStatus::Success();
    }

    /*
     * if id.level is vertex/cluster and targetState is Invalid then the cluster should be locked in exclusive mode
     * otherwise if id.level is vertex/cluster then target should be locked in shared mode
     */
    inline RetStatus ChangeVectorState(VectorID target, uint16_t targetOffset,
                                       VectorState& expectedState, VectorState finalState,
                                       Version* version = nullptr) override {
        CHECK_VECTORID_IS_VALID(target, LOG_TAG_DIVFTREE_VERTEX);
        FatalAssert(target._level + 1 == attr.centroid_id._level, LOG_TAG_DIVFTREE_VERTEX, "level mismatch");
        FatalAssert(expectedState == VECTOR_STATE_VALID || expectedState == VECTOR_STATE_MIGRATED,
                    LOG_TAG_DIVFTREE_VERTEX, "expected state can be either VALID or MIGRATED");
        FatalAssert(finalState == VECTOR_STATE_VALID || finalState == VECTOR_STATE_MIGRATED ||
                    finalState == VECTOR_STATE_INVALID, LOG_TAG_DIVFTREE_VERTEX,
                    "final state can only be valid, invalid or migrated!");
        FatalAssert((expectedState == VECTOR_STATE_MIGRATED) == (finalState == VECTOR_STATE_VALID),
                    LOG_TAG_DIVFTREE_VERTEX, "excpected state is migrated is migrated if and only "
                    "if final state is valid. this can only happen when a migration fails and "
                    "we want to revert our state back to valid");
        uint16_t size = cluster.header.visible_size.load(std::memory_order_acquire);
        FatalAssert(targetOffset < size, LOG_TAG_DIVFTREE_VERTEX, "offset out of range!");

        Address meta = cluster.MetaData(targetOffset, attr.centroid_id.IsLeaf(),
                                        attr.block_size, attr.cap, attr.index->GetAttributes().dimension);
        VectorID* id = nullptr;
        std::atomic<VectorState>* state = nullptr;
        if (attr.centroid_id.IsLeaf()) {
            VectorMetaData* vmd = static_cast<VectorMetaData*>(meta);
            id = &(vmd->id);
            state = &(vmd->state);
        } else {
            CentroidMetaData* vmd = static_cast<CentroidMetaData*>(meta);
            id = &(vmd->id);
            if (version != nullptr) {
                *version = vmd->version;
            }
            state = &(vmd->state);
        }

        FatalAssert(*id == target, LOG_TAG_DIVFTREE_VERTEX, "id mismatch!");
        if (expectedState == VECTOR_STATE_MIGRATED) {
            FatalAssert((*state).load(std::memory_order_acquire) == VECTOR_STATE_MIGRATED, LOG_TAG_DIVFTREE_VERTEX,
                        "mismatch state!");
            state->store(finalState, std::memory_order_release);
            return RetStatus::Success();
        }

        if (target.IsCentroid()) {
            if (finalState == VECTOR_STATE_INVALID) {
                /* deletion */
                /* todo: check target should be locked in exclusive mode! */
                FatalAssert((*state).load(std::memory_order_acquire) == VECTOR_STATE_VALID, LOG_TAG_DIVFTREE_VERTEX,
                            "mismatch state!");
                state->store(finalState, std::memory_order_release);
                return RetStatus::Success();
            } else {
                /* todo: check target should be locked in shared mode! */
                FatalAssert((*state).load(std::memory_order_acquire) != VECTOR_STATE_INVALID, LOG_TAG_DIVFTREE_VERTEX,
                            "mismatch state!");
            }
        }

        return (state->compare_exchange_strong(expectedState, finalState) ?
                RetStatus::Success() :
                RetStatus{.stat=RetStatus::FAILED_TO_CAS_VECTOR_STATE, .message=nullptr});
    }

    /*
     * if id.level is vertex/cluster and targetState is Invalid then the cluster should be locked in exclusive mode
     * otherwise if id.level is vertex/cluster then target should be locked in shared mode
     */
    inline RetStatus ChangeVectorState(Address meta, VectorState& expectedState, VectorState finalState,
                                       Version* version = nullptr) override {
        FatalAssert(expectedState == VECTOR_STATE_VALID || expectedState == VECTOR_STATE_MIGRATED,
                    LOG_TAG_DIVFTREE_VERTEX, "expected state can be either VALID or MIGRATED");
        FatalAssert(finalState == VECTOR_STATE_VALID || finalState == VECTOR_STATE_MIGRATED ||
                    finalState == VECTOR_STATE_INVALID, LOG_TAG_DIVFTREE_VERTEX,
                    "final state can only be valid, invalid or migrated!");
        FatalAssert((expectedState == VECTOR_STATE_MIGRATED) == (finalState == VECTOR_STATE_VALID),
                    LOG_TAG_DIVFTREE_VERTEX, "excpected state is migrated is migrated if and only "
                    "if final state is valid. this can only happen when a migration fails and "
                    "we want to revert our state back to valid");

        // VectorID* id = nullptr;
        std::atomic<VectorState>* state = nullptr;
        if (attr.centroid_id.IsLeaf()) {
            VectorMetaData* vmd = static_cast<VectorMetaData*>(meta);
            // id = &(vmd->id);
            state = &(vmd->state);
        } else {
            CentroidMetaData* vmd = static_cast<CentroidMetaData*>(meta);
            // id = &(vmd->id);
            if (version != nullptr) {
                *version = vmd->version;
            }
            state = &(vmd->state);
        }

        if (expectedState == VECTOR_STATE_MIGRATED) {
            FatalAssert((*state).load(std::memory_order_acquire) == VECTOR_STATE_MIGRATED, LOG_TAG_DIVFTREE_VERTEX,
                        "mismatch state!");
            state->store(finalState, std::memory_order_release);
            return RetStatus::Success();
        }

        if (attr.centroid_id.IsInternalVertex()) {
            if (finalState == VECTOR_STATE_INVALID) {
                /* deletion */
                /* todo: check target should be locked in exclusive mode! */
                FatalAssert((*state).load(std::memory_order_acquire) == VECTOR_STATE_VALID, LOG_TAG_DIVFTREE_VERTEX,
                            "mismatch state!");
                state->store(finalState, std::memory_order_release);
                return RetStatus::Success();
            } else {
                /* todo: check target should be locked in shared mode! */
                FatalAssert((*state).load(std::memory_order_acquire) != VECTOR_STATE_INVALID, LOG_TAG_DIVFTREE_VERTEX,
                            "mismatch state!");
            }
        }

        return (state->compare_exchange_strong(expectedState, finalState) ?
                RetStatus::Success() :
                RetStatus{.stat=RetStatus::FAILED_TO_CAS_VECTOR_STATE, .message=nullptr});
    }

    void Search(const VTYPE* query, size_t k, SortedList<ANNVectorInfo, SimilarityComparator>* neighbours,
                std::unordered_set<std::pair<VectorID, Version>, VectorIDVersionPairHash>& seen) override {
        /* todo: should I also ignore same vectors with different versions? e.g. if it is newer replace o.w ignore */
        CHECK_NOT_NULLPTR(query, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_NOT_NULLPTR(neighbours, LOG_TAG_DIVFTREE_VERTEX);
        FatalAssert(k > 0, LOG_TAG_DIVFTREE_VERTEX, "k must be greater than 0!");
        FatalAssert(neighbours->Size() <= k, LOG_TAG_DIVFTREE_VERTEX, "neighbour list size out of bounds!");
        const uint16_t dim = attr.index->GetAttributes().dimension;
        const DistanceType dtype = attr.index->GetAttributes().distanceAlg;
        VTYPE* data = cluster.Data(0, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        Address meta = cluster.MetaData(0, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        uint16_t old_size = 0;
        uint16_t curr_size = cluster.header.visible_size.load(std::memory_order_acquire);
        std::unordered_map<VectorID, std::pair<uint16_t, DTYPE>, VectorIDHash> in_list;
        std::unordered_set<VectorID, VectorIDHash> in_cluster;
        in_cluster.reserve(curr_size);
        in_list.reserve(curr_size);
        while(curr_size != old_size) {
            /* this will overflow and i will get greater than size when it gets to 0 */
            uint16_t last_seen = old_size - 1;
            for (uint16_t i = curr_size - 1; i != last_seen; --i) {
                ANNVectorInfo new_vector;
                if (attr.centroid_id.IsLeaf()) {
                    VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(meta);
                    if (seen.find(std::make_pair(vmd[i].id, 0)) != seen.end()) {
                        continue;
                    }

                    switch (vmd[i].state.load(std::memory_order_acquire)) {
                    case VECTOR_STATE_VALID:
                    case VECTOR_STATE_MIGRATED:
                        new_vector = ANNVectorInfo(Distance(query, &data[i * dim], dim, dtype), vmd[i].id);
                        neighbours->Insert(new_vector);
                        seen.emplace(new_vector.id, new_vector.version);
                        if (neighbours->Size() > k) {
                            neighbours->PopBack();
                        }
                        break;
                    case VECTOR_STATE_OUTDATED:
                        FatalAssert(false, LOG_TAG_DIVFTREE_VERTEX, "a pure vector cannot become outdated!");
                    default:
                        continue;
                    }
                } else {
                    CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(meta);
                    if (seen.find(std::make_pair(vmd[i].id, vmd[i].version)) != seen.end()) {
                        VectorState state = vmd[i].state.load(std::memory_order_acquire);
                        if (((state == VECTOR_STATE_VALID) || (state == VECTOR_STATE_MIGRATED)) &&
                            (in_list.find(vmd[i].id) == in_list.end())) {
                            in_list.insert({vmd[i].id, {i, Distance(query, &data[i * dim], dim, dtype)}});
                        }
                        continue;
                    }

                    std::unordered_map<VectorID, std::pair<uint16_t, DTYPE>, VectorIDHash>::iterator emplace_res;
                    std::unordered_map<VectorID, std::pair<uint16_t, DTYPE>, VectorIDHash>::iterator check;
                    switch (vmd[i].state.load(std::memory_order_acquire)) {
                    case VECTOR_STATE_VALID:
                    case VECTOR_STATE_MIGRATED:
                        check = in_list.find(vmd[i].id);
                        if (check != in_list.end()) { /* todo: we can handle this case much more efficiently! */
                            /* a vector was outdated in a previous pass */
                            FatalAssert((vmd[check->second.first].id == vmd[i].id) &&
                                        (vmd[check->second.first].state.load(std::memory_order_acquire) ==
                                        VECTOR_STATE_OUTDATED) && (vmd[check->second.first].version < vmd[i].version),
                                        LOG_TAG_DIVFTREE_VERTEX,
                                        "the one that we have seen before should be outdated!");
                            ANNVectorInfo outdated(check->second.second, vmd[check->second.first].id,
                                                   vmd[check->second.first].version);
                            auto it = neighbours->Find(outdated);
                            FatalAssert((it != neighbours->end()),
                                        LOG_TAG_DIVFTREE_VERTEX, "Could not find the outdated version!");
                            neighbours->Erase(it);
                            check->second.first = i;
                            check->second.second = Distance(query, &data[i * dim], dim, dtype);
                            emplace_res = check;
                        } else {
                            emplace_res = in_list.insert({new_vector.id,
                                                         {i, Distance(query, &data[i * dim], dim, dtype)}}).first;
                        }
                        new_vector = ANNVectorInfo(emplace_res->second.second, vmd[i].id, vmd[i].version);
                        neighbours->Insert(new_vector);
                        in_cluster.emplace(new_vector.id);
                        seen.emplace(new_vector.id, new_vector.version);
                        if (neighbours->Size() > k) {
                            auto to_be_deleted = std::prev(neighbours->end());
                            if (new_vector == *to_be_deleted) {
                                in_list.erase(emplace_res);
                            } else {
                                auto it = in_list.find(new_vector.id);
                                if (it != in_list.end()) {
                                    in_list.erase(it);
                                }
                            }
                            neighbours->Erase(to_be_deleted);
                        }
                        break;
                    case VECTOR_STATE_OUTDATED:
                        if (in_cluster.find(vmd[i].id) == in_cluster.end()) {
                            old_size = curr_size;
                        }
                        break;
                    case VECTOR_STATE_INVALID:
                        in_cluster.emplace(vmd[i].id);
                        break;
                    default:
                        DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE_VERTEX, "invalid state");
                    }
                }
            }

            if (old_size == curr_size) {
                curr_size = cluster.header.visible_size.load(std::memory_order_acquire);
                FatalAssert(curr_size > old_size, LOG_TAG_DIVFTREE_VERTEX,
                            "if we have seen an outdated vector our size should have increased!");
            } else {
                old_size = curr_size;
            }
        }
    }

    inline Cluster& GetCluster() override {
        return cluster;
    }

    inline const DIVFTreeVertexAttributes& GetAttributes() const override {
        return attr;
    }

    inline VectorID CentroidID() const override {
        return attr.centroid_id;
    }

    inline Version VertexVersion() const override {
        return attr.version;
    }

    String ToString(bool detailed = false) const override {
        uint16_t reserved = cluster.header.reserved_size.load(std::memory_order_acquire);
        uint16_t visible = cluster.header.visible_size.load(std::memory_order_acquire);
        uint16_t deleted = cluster.header.num_deleted.load(std::memory_order_acquire);
        String rs = String("{self=%p, unpinCount:%lu, reserved_size:%hu, visible_size:%hu, num_deleted:%hu, attr:%s",
                           this, unpinCount.load(std::memory_order_acquire),
                           reserved, visible, deleted, attr.ToString().ToCStr());
        if (!detailed) {
            return rs+String("}");
        }

        FatalAssert(Cluster::NumBlocks(attr.block_size, attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "Currently cannot handle more than one block!");
        rs += String(", Cluster:{num_blocks=1, blocks=[{");

        uint16_t dim = attr.index->GetAttributes().dimension;
        const VTYPE* vec = cluster.Data(0, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        const void* meta = cluster.MetaData(0, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        for (uint16_t i = 0; i < visible; ++i) {
            if (attr.centroid_id.IsLeaf()) {
                const VectorMetaData* vmd = static_cast<const VectorMetaData*>(meta);
                rs += String("{meta:{ID: " VECTORID_LOG_FMT ", State: %s}, data:{", VECTORID_LOG(vmd[i].id),
                             VectorStateToString(vmd[i].state.load(std::memory_order_acquire)).ToCStr());
                for (uint16_t j = 0; j < dim; ++j) {
                    rs += String(VTYPE_FMT, vec[i * dim + j]);
                    if (j < dim - 1) {
                        rs += String(", ");
                    }
                }
            } else {
                const CentroidMetaData* vmd = static_cast<const CentroidMetaData*>(meta);
                rs += String("{meta:{ID: " VECTORID_LOG_FMT " Version: %lu, State: %s}, data:{",
                             VECTORID_LOG(vmd[i].id), vmd[i].version,
                             VectorStateToString(vmd[i].state.load(std::memory_order_acquire)).ToCStr());
                for (uint16_t j = 0; j < dim; ++j) {
                    rs += String(VTYPE_FMT, vec[i * dim + j]);
                    if (j < dim - 1) {
                        rs += String(", ");
                    }
                }
            }
            rs += String((i == visible - 1 ? "}}" : "}}, "));
        }
        rs += String("}]}}");

        return rs;
    }


protected:
    const DIVFTreeVertexAttributes attr;
    std::atomic<uint64_t> unpinCount;
    alignas(CACHE_LINE_SIZE) Cluster cluster;

TESTABLE;
friend class DIVFTree;
};

class DIVFTree : public DIVFTreeInterface {
public:
    DIVFTree(DIVFTreeAttributes attributes) : attr(attributes), real_size(0), end_signal(false) {
        Thread* _self = new Thread(attributes.random_base_perc);
        _self->InitDIVFThread();
        attr.similarityComparator = GetDistancePairSimilarityComparator(attr.distanceAlg, false);
        attr.reverseSimilarityComparator = GetDistancePairSimilarityComparator(attr.distanceAlg, true);
        if (attr.use_block_bytes) {
            attr.leaf_blck_size = Cluster::BlockSize(attr.leaf_blck_bytes, sizeof(VectorMetaData),
                                                     attr.leaf_max_size, attr.dimension);
            attr.internal_blck_size = Cluster::BlockSize(attr.internal_blck_bytes, sizeof(CentroidMetaData),
                                                         attr.internal_max_size, attr.dimension);
        } else {
            attr.leaf_blck_bytes = Cluster::BlockBytes(attr.leaf_blck_size, sizeof(VectorMetaData),
                                                       attr.dimension);
            attr.internal_blck_bytes = Cluster::BlockBytes(attr.internal_blck_size, sizeof(CentroidMetaData),
                                                           attr.dimension);
        }
        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Create DIVFTree Index Start");
        VectorID root_id;
        BufferVertexEntry* root_entry =
            BufferManager::Init(sizeof(DIVFTreeVertex) - ALLIGNED_SIZE(sizeof(ClusterHeader)),
                                attr.leaf_blck_size, attr.internal_blck_size,
                                attr.leaf_max_size, attr.internal_max_size, attr.dimension);
        CHECK_NOT_NULLPTR(root_entry, LOG_TAG_DIVFTREE);
        (void)(new (root_entry->ReadLatestVersion(false)) DIVFTreeVertex(attr, root_entry->centroidMeta.selfId, this));
        root_entry->headerLock.Unlock();
        BufferManager::GetInstance()->
            ReleaseBufferEntryIfNotNull(root_entry, ReleaseBufferEntryFlags(true, true));

        StartBGThreads();
        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Create DIVFTree Index End");
    }

    ~DIVFTree() override {
        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Shutdown DIVFTree Index Start");
        end_signal.store(true, std::memory_order_release);

        DestroyBGThreads();

        BufferManager::GetInstance()->Shutdown();
        DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Shutdown DIVFTree Index End");

        FatalAssert(threadSelf != nullptr, LOG_TAG_DIVFTREE, "threadself not inited!");
        Thread* _self = threadSelf;
        threadSelf->DestroyDIVFThread();
        delete _self;
    }

    /* todo: for now since it is single node, the create_completion_notification here does not do anything and returning
       from this function indicates that the vector is inserted. But we need to change
       this in the multi node environment */
    RetStatus Insert(const VTYPE* vec, VectorID& vec_id, uint8_t search_span,
                     bool create_completion_notification = false) override {
        CHECK_NOT_NULLPTR(vec, LOG_TAG_DIVFTREE);
        UNUSED_VARIABLE(create_completion_notification);
        FatalAssert(search_span > 0, LOG_TAG_DIVFTREE, "Number of neighbours cannot be 0.");
        RetStatus rs = RetStatus::Success();

#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
             "Insert BEGIN: Vector=%s",
             VectorToString(vec, attr.dimension).ToCStr());
#endif

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        BufferVectorEntry* vector_entry = nullptr;
        bufferMgr->BatchCreateVectorEntry(1, &vector_entry, false);
        CHECK_NOT_NULLPTR(vector_entry, LOG_TAG_DIVFTREE);
        vec_id = vector_entry->selfId;
        CHECK_VECTORID_IS_VALID(vec_id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VECTOR(vec_id, LOG_TAG_DIVFTREE);
        const ConstVectorBatch batch(vec, &vec_id, nullptr, 1);

        std::vector<SortedList<ANNVectorInfo, SimilarityComparator>*> layers;
        VectorID target_leaf;
        Version target_version;
        BufferVertexEntry* leaf_entry = nullptr;

        do {
            while (true) {
                DIVFTreeVertex* root =
                    static_cast<DIVFTreeVertex*>(bufferMgr->GetIndexSnapshot());
                CHECK_NOT_NULLPTR(root, LOG_TAG_DIVFTREE);

                if (root->attr.centroid_id.IsLeaf()) {
                    target_leaf = root->attr.centroid_id;
                    target_version = root->attr.version;
                    bufferMgr->FreeSnapshot(root);
                    break;
                }

                layers.reserve(root->attr.centroid_id._level + 1);
                /* level 0 represents vectors and we do not need them */
                layers.emplace_back(nullptr);
                for (uint64_t i = 1; i <= root->attr.centroid_id._level; ++i) {
                    layers.emplace_back(
                        new SortedList<ANNVectorInfo, SimilarityComparator>(attr.similarityComparator));
                }

                layers[root->attr.centroid_id._level]->Insert(ANNVectorInfo(0, root->attr.centroid_id,
                                                                            root->attr.version));
                rs = ANNSearch(vec, 1, search_span, 1,
                               (uint8_t)(root->attr.centroid_id._level), (uint8_t)VectorID::LEAF_LEVEL,
                               layers, root);
                bufferMgr->FreeSnapshot(root);

                if (rs.IsOK()) {
                    FatalAssert(layers[VectorID::LEAF_LEVEL]->Size() == 1, LOG_TAG_DIVFTREE,
                                "if rs is OK we should have found the leaf!");
                    target_leaf = (*layers[VectorID::LEAF_LEVEL])[0].id;
                    target_version = (*layers[VectorID::LEAF_LEVEL])[0].version;
                }

                for (uint64_t i = 1; i < layers.size(); ++i) {
                    delete layers[i];
                }
                layers.clear();

                if (rs.IsOK()) {
                    break;
                }
                /* todo: check how many retries! */
            }

            leaf_entry = bufferMgr->ReadBufferEntry(target_leaf, SX_SHARED);
            if (leaf_entry == nullptr || leaf_entry->currentVersion != target_version) {
                /* todo: if there is a difference in version we may be able to solve it using update logs! */
                /* todo: check how many retries! */
                bufferMgr->ReleaseBufferEntryIfNotNull(leaf_entry, ReleaseBufferEntryFlags(false, false));
                rs = RetStatus::Fail(nullptr);
                continue;
            }
            /* todo: maybe we can read the parent and only go one layer up instead of rereading from root */
            rs = BatchInsertInto(leaf_entry, batch, true);
        } while(!rs.IsOK());

#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
             "Insert END: Vector=%s, vector_id=" VECTORID_LOG_FMT,
             VectorToString(vec, attr.dimension).ToCStr(), VECTORID_LOG(vec_id));
#endif

        return rs;
    }

    /* todo: for now since it is single node, the create_completion_notification here does not do anything and returning
       from this function indicates that the vector is deleted. But we need to change
       this in the multi node environment */
    RetStatus Delete(VectorID vec_id, bool create_completion_notification = false) override {
        UNUSED_VARIABLE(create_completion_notification);
        CHECK_VECTORID_IS_VALID(vec_id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VECTOR(vec_id, LOG_TAG_DIVFTREE);
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
             "DELETE START: vector_id=" VECTORID_LOG_FMT,
             VECTORID_LOG(vec_id));
#endif
        BufferVectorEntry* entry = bufferMgr->GetVectorEntry(vec_id);
        if (entry == nullptr) {
#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
             "DELETE ERR: vector_id=" VECTORID_LOG_FMT " does not exists!",
             VECTORID_LOG(vec_id));
#endif
            return RetStatus{.stat=RetStatus::VECTOR_NOT_FOUND, .message="Vector does not exist in this index"};
        }
        VectorLocation loc;
        BufferVertexEntry* parent = nullptr;
        do {
            parent = entry->ReadParentEntry(loc);
            if (parent == nullptr) {
#ifdef EXCESS_LOGING
            DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                "DELETE ERR: vector_id=" VECTORID_LOG_FMT " is either deleted or is not yet inserted!",
                VECTORID_LOG(vec_id));
#endif
                return RetStatus{.stat=RetStatus::VECTOR_NOT_FOUND,
                                .message="Vector is either deleted or is not yet inserted."};
            }
        } while(!DeleteVectorAndReleaseContainer(vec_id, parent, loc).IsOK());

#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
             "DELETE END: vector_id=" VECTORID_LOG_FMT,
             VECTORID_LOG(vec_id));
#endif
        return RetStatus::Success();
    }

    RetStatus ApproximateKNearestNeighbours(const VTYPE* query,
                                            size_t k, uint8_t internal_node_search_span, uint8_t leaf_node_search_span,
                                            SortType sort_type, std::vector<ANNVectorInfo>& neighbours) override {

        CHECK_NOT_NULLPTR(query, LOG_TAG_DIVFTREE);

        FatalAssert(k > 0, LOG_TAG_DIVFTREE, "Number of neighbours cannot be 0.");
        FatalAssert(internal_node_search_span > 0, LOG_TAG_DIVFTREE,
                    "Number of internal vertex neighbours cannot be 0.");
        FatalAssert(leaf_node_search_span > 0, LOG_TAG_DIVFTREE, "Number of leaf neighbours cannot be 0.");

#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
             "ApproximateKNearestNeighbours BEGIN: query=%s",
             VectorToString(query, attr.dimension).ToCStr());
#endif

        RetStatus rs = RetStatus::Success();

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        std::vector<SortedList<ANNVectorInfo, SimilarityComparator>*> layers;

        while (real_size.load(std::memory_order_acquire) > 0) {
            DIVFTreeVertex* root =
                static_cast<DIVFTreeVertex*>(bufferMgr->GetIndexSnapshot());
            CHECK_NOT_NULLPTR(root, LOG_TAG_DIVFTREE);

            layers.reserve(root->attr.centroid_id._level + 1);
            for (uint64_t i = 0; i <= root->attr.centroid_id._level; ++i) {
                layers.emplace_back(new SortedList<ANNVectorInfo, SimilarityComparator>(attr.similarityComparator));
            }

            layers[root->attr.centroid_id._level]->Insert(ANNVectorInfo(0, root->attr.centroid_id,
                                                                        root->attr.version));
            rs = ANNSearch(query, k, internal_node_search_span, leaf_node_search_span,
                           (uint8_t)(root->attr.centroid_id._level), (uint8_t)VectorID::VECTOR_LEVEL, layers, root);
            bufferMgr->FreeSnapshot(root);

#ifdef EXCESS_LOGING
        if (rs.IsOK()) {
            DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
                "ApproximateKNearestNeighbours END: query=%s, AKNN=%s",
                VectorToString(query, attr.dimension).ToCStr(), layers[0]->ToString<ANNVectorInfoToString>().ToCStr());
        }
#endif
            layers[0]->Extract(neighbours, (sort_type == SortType::IncreasingSimilarity));
            for (uint64_t i = 0; i < layers.size(); ++i) {
                delete layers[i];
            }
            layers.clear();

            if (rs.IsOK()) {
                break;
            }

            neighbours.clear();
        }

        return rs;
    }

    size_t Size() const override {
        return real_size.load(std::memory_order_acquire);
    }

    const DIVFTreeAttributes& GetAttributes() const override {
        return attr;
    }

    // inline String ToString(bool detailed = false) const override {
    //     String out("{Attr=<Dim=%hu, ClusteringAlg=%s, DistanceAlg=%s, "
    //                "LeafMinSize=%hu, LeafMaxSize=%hu, InternalMinSize=%hu, InternalMaxSize=%hu, "
    //                "SplitInternal=%hu, SplitLeaf=%hu, Size=%lu, Levels=%lu>, RootID=" VECTORID_LOG_FMT
    //                ", Vertexs=[", core_attr.dimension, CLUSTERING_TYPE_NAME[core_attr.clusteringAlg],
    //                DISTANCE_TYPE_NAME[core_attr.distanceAlg], leaf_min_size, leaf_max_size,
    //                internal_min_size, internal_max_size, split_internal, split_leaf,
    //                _size, _levels, VECTORID_LOG(_root));
    //     std::vector<VectorID> curr_level_stack, next_level_stack;
    //     size_t seen = 0;
    //     if (_root.IsValid()) {
    //         curr_level_stack.push_back(_root);
    //     }
    //     bool go_to_next_level = true;
    //     uint64_t curr_level = 0;
    //     while (!curr_level_stack.empty()) {
    //         VectorID curr_vertexId = curr_level_stack.back();
    //         curr_level_stack.pop_back();
    //         CHECK_VECTORID_IS_VALID(curr_vertexId, LOG_TAG_DIVFTREE);
    //         if (go_to_next_level) {
    //             curr_level = curr_vertexId._level;
    //             out += String("Level %lu:{Size=%lu, Vertexs=[", curr_level, curr_level_stack.size() + 1);
    //             go_to_next_level = false;
    //         }
    //         else {
    //             FatalAssert(curr_vertexId._level == curr_level, LOG_TAG_DIVFTREE,
    //                         "Current vertex level (%hhu) does not match expected level (%lu).",
    //                         curr_vertexId._level, curr_level);
    //         }
    //         DIVFTreeVertex* vertex = static_cast<DIVFTreeVertex*>(_bufmgr.GetVertex(curr_vertexId));
    //         CHECK_VERTEX_IS_VALID(vertex, LOG_TAG_DIVFTREE, false);
    //         out += String("<VertexID=" VECTORID_LOG_FMT ", Bucket=%s>",
    //                     VECTORID_LOG(curr_vertexId), vertex->BucketToString().ToCStr());
    //         if (!vertex->IsLeaf()) {
    //             for (uint16_t i = 0; i < vertex->Size(); ++i) {
    //                 const VectorPair& vectorPair = vertex->cluster[i];
    //                 CHECK_VECTORID_IS_VALID(vectorPair.id, LOG_TAG_DIVFTREE);
    //                 FatalAssert(vectorPair.id._level == curr_vertexId._level - 1, LOG_TAG_DIVFTREE,
    //                             "VectorID level (%hhu) does not match current vertex level (%hhu).",
    //                             vectorPair.id._level, curr_vertexId._level - 1);
    //                 FatalAssert(vectorPair.vec.IsValid(), LOG_TAG_DIVFTREE,
    //                             "Invalid vector in vertex %s at index %hu.", VECTORID_LOG(curr_vertexId), i);
    //                 next_level_stack.emplace_back(vectorPair.id);
    //             }
    //         }
    //         else {
    //             seen += vertex->Size();
    //         }
    //         if (curr_level_stack.empty()) {
    //             out += String("]}");
    //             go_to_next_level = true;
    //             next_level_stack.swap(curr_level_stack);
    //             if (!curr_level_stack.empty()) {
    //                 out += String(", ");
    //             }
    //             else {
    //                 out += String("]}");
    //             }
    //         }
    //         else {
    //             out += String(", ");
    //         }
    //     }
    //     FatalAssert(seen == _size, LOG_TAG_DIVFTREE,
    //                 "Seen vertices (%lu) does not match expected size (%lu).", seen, _size);
    //     return out;
    // }

protected:
    DIVFTreeAttributes attr;
    std::atomic<uint64_t> real_size;
    std::atomic<bool> end_signal;

    std::vector<Thread*> bg_migrators;
    BlockingQueue<MigrationCheckTask> migration_tasks;
    std::vector<Thread*> bg_mergers;
    BlockingQueue<MergeTask> merge_tasks;
    std::vector<Thread*> bg_compactors;
    BlockingQueue<CompactionTask> compaction_tasks;
    std::vector<Thread*> searchers;
    BlockingQueue<SearchTask> search_tasks;

    RetStatus CompactAndInsert(BufferVertexEntry* container_entry, const ConstVectorBatch& batch,
                               uint16_t marked_for_update = INVALID_OFFSET) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        FatalAssert(((batch.size == 0) || ((batch.data != nullptr) && (batch.id != nullptr))), LOG_TAG_DIVFTREE,
                    "Invalid batch");
        FatalAssert((batch.size != 0) || (marked_for_update == INVALID_OFFSET), LOG_TAG_DIVFTREE,
                    "If there is no batch, marked_for_update should be invalid!");
        threadSelf->SanityCheckLockHeldInModeByMe(&container_entry->clusterLock, SX_EXCLUSIVE);

        DIVFTreeVertex* current = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(current, LOG_TAG_DIVFTREE);
        FatalAssert(current->attr.index == this, LOG_TAG_DIVFTREE,
                    "input entry does not belong to this index!");
        FatalAssert(current->cluster.header.num_deleted.load(std::memory_order_relaxed) > 0, LOG_TAG_DIVFTREE,
                    "Cannot compact a cluster with no deleted elemetns!");

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        DIVFTreeVertex* compacted =
            new(bufferMgr->AllocateMemoryForVertex(container_entry->centroidMeta.selfId._level))
            DIVFTreeVertex(current->attr);
#ifndef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "compacting vertex id: " VECTORID_LOG_FMT
                "ver: %u, old addr: %p, new addr: %p", VECTORID_LOG(container_entry->centroidMeta.selfId),
                container_entry->currentVersion, current, compacted);
#endif

        uint16_t size = current->cluster.header.reserved_size.load(std::memory_order_relaxed);
        const bool is_leaf = container_entry->centroidMeta.selfId.IsLeaf();
        const uint16_t dim = attr.dimension;
        const uint16_t cap = current->attr.cap;
        const uint16_t blckSize = current->attr.cap;
        /* Todo: this will not work if we have more than one block! */
        FatalAssert(current->cluster.NumBlocks(blckSize, cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "Currently cannot handle more than one block!");

        FatalAssert(is_leaf || (batch.size == 0) || (batch.version != nullptr), LOG_TAG_DIVFTREE,
                    "Batch of versions to insert is null!");

        Address srcMetaData = current->cluster.MetaData(0, is_leaf, blckSize, cap, dim);
        VTYPE* srcData = current->cluster.Data(0, is_leaf, blckSize, cap, dim);
        Address destMetaData = compacted->cluster.MetaData(0, is_leaf, blckSize, cap, dim);
        VTYPE* destData = compacted->cluster.Data(0, is_leaf, blckSize, cap, dim);
        uint16_t new_size = 0;
        std::vector<uint16_t> offsets;
        offsets.reserve(current->cluster.header.reserved_size.load(std::memory_order_relaxed) -
                        current->cluster.header.num_deleted.load(std::memory_order_relaxed));
        VectorID marked_for_update_id = INVALID_VECTOR_ID;
        /*
         * Note: During compaction, we will update vector locations but
         * we haven't updated the container cluster ptr yet. As a result, a reader may see that offset mismatch
         * caused by this. As long as we can get the container in S mode and we have ourself in X mode, our location
         * should not change. so in cases where only offset has changed, we can simply reread the location to
         * see the new one!
         * This may be a bit funky for vectors though as they do not have any locks associated with them.
         */
        for (uint16_t i = 0; i < size; ++i) {
            if (i == marked_for_update) {
                marked_for_update_id = (is_leaf ? (reinterpret_cast<VectorMetaData*>(srcMetaData)[i].id) :
                                                  (reinterpret_cast<CentroidMetaData*>(srcMetaData)[i].id));
                continue;
            }

            if (is_leaf) {
                VectorMetaData* src_vmd = reinterpret_cast<VectorMetaData*>(srcMetaData);
                VectorMetaData* dest_vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                VectorState vstate = src_vmd[i].state.load(std::memory_order_relaxed);
                if (vstate == VECTOR_STATE_VALID) {
                    memcpy(&destData[new_size * dim], &srcData[i * dim], dim * sizeof(VTYPE));
                    dest_vmd[new_size].id = src_vmd[i].id;
                    dest_vmd[new_size].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
                    offsets.push_back(i);
                    ++new_size;
                } SANITY_CHECK( else if (vstate == VECTOR_STATE_MIGRATED) {
                    VectorLocation outdatedLoc(container_entry->centroidMeta.selfId,
                                               container_entry->currentVersion, i);
                    FatalAssert(outdatedLoc != bufferMgr->LoadCurrentVectorLocation(src_vmd[i].id), LOG_TAG_DIVFTREE,
                                "current location is migrated!");
                })
            } else {
                CentroidMetaData* src_vmd = reinterpret_cast<CentroidMetaData*>(srcMetaData);
                CentroidMetaData* dest_vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                VectorState vstate = src_vmd[i].state.load(std::memory_order_relaxed);
                if (vstate == VECTOR_STATE_VALID) {
                    memcpy(&destData[new_size * dim], &srcData[i * dim], dim * sizeof(VTYPE));
                    dest_vmd[new_size].id = src_vmd[i].id;
                    dest_vmd[new_size].version = src_vmd[i].version;
                    dest_vmd[new_size].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
                    offsets.push_back(i);
                    ++new_size;
                } SANITY_CHECK( else if (vstate == VECTOR_STATE_MIGRATED) {
                    VectorLocation outdatedLoc(container_entry->centroidMeta.selfId,
                                               container_entry->currentVersion, i);
                    FatalAssert(outdatedLoc != bufferMgr->LoadCurrentVectorLocation(src_vmd[i].id), LOG_TAG_DIVFTREE,
                                "current location is migrated!");
                })
            }
        }
        FatalAssert(current->cluster.header.visible_size.load(std::memory_order_relaxed) ==
                    current->cluster.header.reserved_size.load(std::memory_order_relaxed), LOG_TAG_DIVFTREE,
                    "not all updates went through!");
        FatalAssert(new_size == (current->cluster.header.reserved_size.load(std::memory_order_relaxed) -
                                 current->cluster.header.num_deleted.load(std::memory_order_relaxed) -
                                 (marked_for_update != INVALID_OFFSET ? 1 : 0)),
                    LOG_TAG_DIVFTREE, "not all deletes went through!");
        FatalAssert(new_size + batch.size >= new_size, LOG_TAG_DIVFTREE, "overflow detected!");
        FatalAssert(new_size + batch.size <= cap, LOG_TAG_DIVFTREE, "Cluster cannot contain all of these vectors!");
        FatalAssert(offsets.size() == new_size, LOG_TAG_DIVFTREE, "offsets size mismatch!");

        uint16_t old_reserved = new_size;

        for (uint16_t i = 0; i < batch.size; ++i) {
            memcpy(&destData[new_size * dim], &batch.data[i * dim], dim * sizeof(VTYPE));
            if (is_leaf) {
                VectorMetaData* dest_vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                dest_vmd[new_size].id = batch.id[i];
                dest_vmd[new_size].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            } else {
                CentroidMetaData* dest_vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                dest_vmd[new_size].id = batch.id[i];
                dest_vmd[new_size].version = batch.version[i];
                dest_vmd[new_size].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
                bufferMgr->PinVertexVersion(batch.id[i], batch.version[i]);
            }

            ++new_size;
        }

        compacted->cluster.header.num_deleted.store(0, std::memory_order_relaxed);
        compacted->cluster.header.reserved_size.store(new_size, std::memory_order_relaxed);
        compacted->cluster.header.visible_size.store(new_size, std::memory_order_relaxed);

#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "compacted vertex id: " VECTORID_LOG_FMT
                "ver: %u, old vertex: (%p)%s, new vertex: (%p)%s", VECTORID_LOG(container_entry->centroidMeta.selfId),
                container_entry->currentVersion, current, current->ToString(true).ToCStr(),
                compacted, compacted->ToString(true).ToCStr());
#endif

        container_entry->UpdateClusterPtr(compacted);
        current = nullptr; // this is to make sure that we no longer access the current!

        for (uint16_t i = 0; i < old_reserved; ++i) {
            if (offsets[i] == i) {
                continue;
            }
            // DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "UpdateVectorLocation in compaction:");
            if (is_leaf) {
                VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                bufferMgr->
                    UpdateVectorLocation(vmd[i].id, VectorLocation(container_entry->centroidMeta.selfId,
                                                                   container_entry->currentVersion, i), false);
                // bufferMgr->UpdateVectorLocationOffset(vmd[i].id, i);
            } else {
                CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                bufferMgr->
                    UpdateVectorLocation(vmd[i].id, VectorLocation(container_entry->centroidMeta.selfId,
                                                                   container_entry->currentVersion, i), false);
                // bufferMgr->UpdateVectorLocationOffset(vmd[i].id, i);
            }
        }

        for (uint16_t i = old_reserved; i < new_size; ++i) {
            if (is_leaf) {
                VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                if (vmd[i].id == marked_for_update_id && i == marked_for_update) {
                    continue;
                }
                // DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "UpdateVectorLocation in compaction insert:");
                bufferMgr->UpdateVectorLocation(vmd[i].id,
                    VectorLocation(container_entry->centroidMeta.selfId, container_entry->currentVersion, i),
                    vmd[i].id != marked_for_update_id);
            } else {
                CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                if (vmd[i].id == marked_for_update_id && i == marked_for_update) {
                    continue;
                }
                // DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "UpdateVectorLocation in compaction insert:");
                bufferMgr->UpdateVectorLocation(vmd[i].id,
                    VectorLocation(container_entry->centroidMeta.selfId, container_entry->currentVersion, i),
                    vmd[i].id != marked_for_update_id);
            }
        }

        return RetStatus::Success();
    }

    inline void CompactionCheck(VectorID target) {
        CHECK_VECTORID_IS_VALID(target, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(target, LOG_TAG_DIVFTREE);

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        BufferVertexEntry* entry = bufferMgr->ReadBufferEntry(target, SX_EXCLUSIVE);
        if (entry == nullptr) {
            return;
        }

        DIVFTreeVertex* cluster = static_cast<DIVFTreeVertex*>(entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(cluster, LOG_TAG_DIVFTREE);

        if (cluster->cluster.header.num_deleted.load(std::memory_order_relaxed) == 0) {
            bufferMgr->ReleaseBufferEntry(entry, ReleaseBufferEntryFlags(false, false));
            return;
        }

        entry->state.store(CLUSTER_FULL, std::memory_order_release);
        VectorBatch dummy;
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "BGCompaction triggered for " VECTORID_LOG_FMT,
                VECTORID_LOG(target));
        (void)CompactAndInsert(entry, dummy);
        bufferMgr->ReleaseBufferEntry(entry, ReleaseBufferEntryFlags(true, true));
    }

    inline void SimpleDivideClustering(const DIVFTreeVertex* base, const ConstVectorBatch& batch,
                                       BufferVertexEntry**& entries, DIVFTreeVertex**& clusters, VectorBatch& centroids,
                                       uint16_t marked_for_update = INVALID_OFFSET) {
        FatalAssert(base->attr.index == this, LOG_TAG_DIVFTREE, "index mismatch!");
        uint16_t num_vectors = base->cluster.header.reserved_size.load(std::memory_order_relaxed) -
                               base->cluster.header.num_deleted.load(std::memory_order_relaxed) + batch.size -
                               (marked_for_update != INVALID_OFFSET ? 1 : 0);
        const bool is_leaf = base->attr.centroid_id.IsLeaf();
        centroids.size = (is_leaf ? attr.split_leaf : attr.split_internal);
        if (num_vectors / centroids.size < base->attr.min_size) {
            centroids.size = num_vectors / base->attr.min_size + ((num_vectors % base->attr.min_size) > 0);
        }

        if (centroids.size < 2) {
            centroids.size = 2;
        }

        centroids.data = new VTYPE[attr.dimension * centroids.size];
        centroids.version = new Version[centroids.size];
        centroids.id = new VectorID[centroids.size];
        entries = new BufferVertexEntry*[centroids.size];
        clusters = new DIVFTreeVertex*[centroids.size];

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_CLUSTERING);
        centroids.id[0] = base->attr.centroid_id;
        centroids.version[0] = base->attr.version + 1;
        clusters[0] = new (bufferMgr->AllocateMemoryForVertex(centroids.id[0]._level))
            DIVFTreeVertex(DIVFTreeVertexAttributes(centroids.id[0], centroids.version[0], base->attr.min_size,
                                                    base->attr.cap, base->attr.block_size, base->attr.index));
        CHECK_NOT_NULLPTR(clusters[0], LOG_TAG_CLUSTERING);
        entries[0] = nullptr;
        DIVFTreeVertexInterface** clusterMemories = reinterpret_cast<DIVFTreeVertexInterface**>(&clusters[1]);
        bufferMgr->BatchCreateBufferEntry(centroids.size - 1, base->attr.centroid_id._level, &entries[1],
                                          clusterMemories, &centroids.id[1], &centroids.version[1]);

        for (uint16_t i = 1; i < centroids.size; ++i) {
            CHECK_NOT_NULLPTR(clusters[i], LOG_TAG_CLUSTERING);
            CHECK_NOT_NULLPTR(entries[i], LOG_TAG_CLUSTERING);
            new (clusters[i]) DIVFTreeVertex(
                DIVFTreeVertexAttributes(centroids.id[i], centroids.version[i], base->attr.min_size,
                                         base->attr.cap, base->attr.block_size, base->attr.index));
        }

        FatalAssert(base->cluster.NumBlocks(base->attr.block_size, base->attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "Currently cannot handle more than one block!");
        const VTYPE* vectors = base->cluster.Data(0, is_leaf, base->attr.block_size, base->attr.cap, attr.dimension);
        const void* meta = base->cluster.MetaData(0, is_leaf, base->attr.block_size, base->attr.cap, attr.dimension);
        uint16_t num_seen = 0;
        for (uint16_t i = 0; i < base->attr.cap; ++i) {
            if (i == marked_for_update) {
                continue;
            }

            const uint16_t destSize =
                clusters[num_seen % centroids.size]->cluster.header.reserved_size.load(std::memory_order_relaxed);
            void* destMeta =
                clusters[num_seen % centroids.size]->cluster.MetaData(destSize, is_leaf, base->attr.block_size,
                                                                    base->attr.cap, attr.dimension);
            VTYPE* destData =
                clusters[num_seen % centroids.size]->cluster.Data(destSize, is_leaf, base->attr.block_size,
                                                                base->attr.cap, attr.dimension);
            if (is_leaf) {
                const VectorMetaData* vmt = reinterpret_cast<const VectorMetaData*>(meta);
                VectorMetaData* destVmt = reinterpret_cast<VectorMetaData*>(destMeta);
                if (vmt[i].state.load(std::memory_order_relaxed) != VECTOR_STATE_VALID) {
                    continue;
                }
                memcpy(destData, &vectors[i * attr.dimension], sizeof(VTYPE) * attr.dimension);
                destVmt->id = vmt[i].id;
                destVmt->state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            } else {
                const CentroidMetaData* vmt = reinterpret_cast<const CentroidMetaData*>(meta);
                CentroidMetaData* destVmt = reinterpret_cast<CentroidMetaData*>(destMeta);
                if (vmt[i].state.load(std::memory_order_relaxed) != VECTOR_STATE_VALID) {
                    continue;
                }
                memcpy(destData, &vectors[i * attr.dimension], sizeof(VTYPE) * attr.dimension);
                destVmt->id = vmt[i].id;
                destVmt->version = vmt[i].version;
                destVmt->state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            }
            clusters[num_seen % centroids.size]->cluster.header.reserved_size.store(destSize + 1,
                                                                                    std::memory_order_relaxed);
            ++num_seen;
        }

        for (uint16_t i = 0; i < batch.size; ++i) {
            const uint16_t destSize =
                clusters[num_seen % centroids.size]->cluster.header.reserved_size.load(std::memory_order_relaxed);
            void* destMeta =
                clusters[num_seen % centroids.size]->cluster.MetaData(destSize, is_leaf, base->attr.block_size,
                                                                    base->attr.cap, attr.dimension);
            VTYPE* destData =
                clusters[num_seen % centroids.size]->cluster.Data(destSize, is_leaf, base->attr.block_size,
                                                                base->attr.cap, attr.dimension);
            if (is_leaf) {
                VectorMetaData* destVmt = reinterpret_cast<VectorMetaData*>(destMeta);
                memcpy(destData, &(batch.data[i * attr.dimension]), sizeof(VTYPE) * attr.dimension);
                destVmt->id = batch.id[i];
                destVmt->state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            } else {
                CentroidMetaData* destVmt = reinterpret_cast<CentroidMetaData*>(destMeta);
                memcpy(destData, &(batch.data[i * attr.dimension]), sizeof(VTYPE) * attr.dimension);
                destVmt->id = batch.id[i];
                destVmt->version = batch.version[i];
                destVmt->state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
                bufferMgr->PinVertexVersion(batch.id[i], batch.version[i]);
            }
            clusters[num_seen % centroids.size]->cluster.header.reserved_size.store(destSize + 1,
                                                                                  std::memory_order_relaxed);
            ++num_seen;
        }

        /* Now we have to compute centroids and update num visible for each cluster and do a sanityt check for their size */
        for (uint16_t i = 0; i < centroids.size; ++i) {
            uint16_t size = clusters[i]->cluster.header.reserved_size.load(std::memory_order_relaxed);
            FatalAssert((base->attr.min_size <= size) && (size <= base->attr.cap), LOG_TAG_CLUSTERING, "invalid size!");
            clusters[i]->cluster.header.num_deleted.store(0, std::memory_order_relaxed);
            clusters[i]->cluster.header.visible_size.store(size, std::memory_order_relaxed);

            bool res = ComputeCentroid(clusters[i]->cluster.Data(0, is_leaf, base->attr.block_size,
                                                                 base->attr.cap, attr.dimension),
                                       size, nullptr, 0, attr.dimension, attr.distanceAlg,
                                       &centroids.data[i * attr.dimension]);
            UNUSED_VARIABLE(res);
            FatalAssert(res, LOG_TAG_CLUSTERING, "cluster is empty!");
        }

    }

    /*
     * will only fill in the raw centroid vectors to
     * the centroids batch and allocates memory for version and ids but does not fill them
     */
    inline void Clustering(const DIVFTreeVertex* base, const ConstVectorBatch& batch,
                           BufferVertexEntry**& entries, DIVFTreeVertex**& clusters, VectorBatch& centroids,
                           uint16_t marked_for_update = INVALID_OFFSET) {
        switch (attr.clusteringAlg)
        {
        case ClusteringType::SimpleDivide:
            SimpleDivideClustering(base, batch, entries, clusters, centroids, marked_for_update);
            break;
        default:
            DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
                 "Cluster: Invalid clustering type: %hhu", (uint8_t)(attr.clusteringAlg));
        }
    }

    inline BufferVertexEntry* ExpandTree() {
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        /* todo: remove */
        VectorID oldRootId = bufferMgr->GetCurrentRootId();
        UNUSED_VARIABLE(oldRootId);
        BufferVertexEntry* new_root = bufferMgr->CreateNewRootEntry();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "Expanding old root id: " VECTORID_LOG_FMT ", new root id: "
                VECTORID_LOG_FMT, VECTORID_LOG(oldRootId), VECTORID_LOG(new_root->centroidMeta.selfId));
        (void)(new (new_root->ReadLatestVersion(false)) DIVFTreeVertex(attr, new_root->centroidMeta.selfId, this));
        new_root->headerLock.Unlock();
        new_root->DowngradeAccessToShared();
        return new_root;
    }

    RetStatus SplitAndInsert(BufferVertexEntry* container_entry, const ConstVectorBatch& batch,
                             uint16_t marked_for_update = INVALID_OFFSET) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        FatalAssert(batch.size != 0, LOG_TAG_DIVFTREE, "Batch of vectors to insert is empty.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE, "Batch of vectors to insert is null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE, "Batch of vector IDs to insert is null.");
        threadSelf->SanityCheckLockHeldInModeByMe(&container_entry->clusterLock, SX_EXCLUSIVE);

        RetStatus rs = RetStatus::Success();
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        DIVFTreeVertex* current = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(current, LOG_TAG_DIVFTREE);

#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "Splitting vertex (%p)%s",
                current, current->ToString(true).ToCStr());
#else
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "Splitting vertex with id: " VECTORID_LOG_FMT " ver:%u",
                VECTORID_LOG(container_entry->centroidMeta.selfId), container_entry->currentVersion);
#endif

        // uint16_t size = current->cluster.header.reserved_size.load(std::memory_order_relaxed);
        const bool is_leaf = container_entry->centroidMeta.selfId.IsLeaf();
        const uint16_t dim = attr.dimension;
        const uint16_t cap = current->attr.cap;
        const uint16_t blckSize = current->attr.block_size;
        // const uint8_t level = (uint8_t)(container_entry->centroidMeta.selfId._level);
        /* Todo: this will not work if we have more than one block! */
        FatalAssert(current->cluster.NumBlocks(blckSize, cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "Currently cannot handle more than one block!");

        FatalAssert(is_leaf == (batch.version == nullptr), LOG_TAG_DIVFTREE, "Batch of versions to insert is null!");
        // uint16_t total_size = current->cluster.header.reserved_size.load(std::memory_order_relaxed) -
        //                       current->cluster.header.num_deleted.load(std::memory_order_relaxed) +
        //                       batch.size;

        VectorBatch centroids;
        DIVFTreeVertex** clusters = nullptr;
        BufferVertexEntry** entries = nullptr;
        Clustering(current, batch, entries, clusters, centroids, marked_for_update);
        CHECK_NOT_NULLPTR(entries, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(clusters, LOG_TAG_DIVFTREE);
        FatalAssert(centroids.size >= 2, LOG_TAG_DIVFTREE, "numclusters should at least be 2!");
        CHECK_NOT_NULLPTR(centroids.data, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(centroids.id, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(centroids.version, LOG_TAG_DIVFTREE);

#ifdef EXCESS_LOGING
        for (uint16_t i = 0; i < centroids.size; ++i) {
            DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "new cluster: centroid=%s, data=(%p)%s",
                    VectorToString(&centroids.data[i * attr.dimension], attr.dimension).ToCStr(),
                    clusters[i], clusters[i]->ToString(true).ToCStr());
        }
#endif
        entries[0] = container_entry;

        BufferVertexEntry* parent = nullptr;
        while (true) {
            VectorLocation currentLocation = INVALID_VECTOR_LOCATION;
            parent = container_entry->ReadParentEntry(currentLocation);
            if (parent == nullptr) {
                FatalAssert(currentLocation == INVALID_VECTOR_LOCATION, LOG_TAG_DIVFTREE,
                            "null parent with valid location!");
                /* changing the root and location are not done atomically together hence this assert may fail */
                // FatalAssert(container_entry->centroidMeta.selfId == bufferMgr->GetCurrentRootId(), LOG_TAG_DIVFTREE,
                //             "null parent but we are not root!");
                parent = ExpandTree();
            } else {
                FatalAssert(currentLocation != INVALID_VECTOR_LOCATION, LOG_TAG_DIVFTREE,
                        "null parent with valid location!");
            }

            CHECK_NOT_NULLPTR(parent, LOG_TAG_DIVFTREE);
            rs = BatchInsertInto(parent, centroids, false, currentLocation.detail.entryOffset);
            if (!rs.IsOK()) {
                bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags(false, false));
                parent = nullptr;
                continue;
            }

            break;
        }

        threadSelf->SanityCheckLockHeldInModeByMe(&parent->clusterLock, SX_SHARED);

        container_entry->UpdateClusterPtr(clusters[0], centroids.version[0]);
        bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags(false, false));
        parent = nullptr;

        for (uint16_t i = 0; i < centroids.size; ++i) {
            uint16_t size = clusters[i]->cluster.header.reserved_size.load(std::memory_order_relaxed);
            FatalAssert(size == clusters[i]->cluster.header.visible_size.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "mismatch between sizes when cluster is locked in X mode");
            FatalAssert(clusters[i]->cluster.header.num_deleted.load(std::memory_order_relaxed) == 0,
                        LOG_TAG_DIVFTREE, "num deleted should be 0 at thsi stage!");
            void* meta = clusters[i]->cluster.MetaData(0, is_leaf, blckSize, cap, dim);
            CHECK_NOT_NULLPTR(meta, LOG_TAG_DIVFTREE);
            for (uint16_t offset = 0; offset < size; ++offset) {
                // DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "UpdateVectorLocation in split:");
                if (is_leaf) {
                    VectorMetaData* vmt = reinterpret_cast<VectorMetaData*>(meta);
                    bufferMgr->UpdateVectorLocation(vmt[offset].id,
                                                    VectorLocation(centroids.id[i], centroids.version[i], offset));
                } else {
                    CentroidMetaData* vmt = reinterpret_cast<CentroidMetaData*>(meta);
                    bufferMgr->UpdateVectorLocation(vmt[offset].id,
                                                    VectorLocation(centroids.id[i], centroids.version[i], offset));
                }
            }
        }

        for (uint16_t i = centroids.size; i > 1; --i) {
            bufferMgr->ReleaseBufferEntry(entries[i-1], ReleaseBufferEntryFlags(true, true));
        }
        // this is released in the caller
        // bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(true, true));

        delete[] clusters;
        delete[] entries;
        delete[] centroids.data;
        delete[] centroids.id;
        delete[] centroids.version;

        return rs;
    }

    RetStatus BatchInsertInto(BufferVertexEntry* container_entry, const ConstVectorBatch& batch, bool releaseEntry,
                              uint16_t marked_for_update = INVALID_OFFSET, bool no_major_updates = false) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        FatalAssert(batch.size != 0, LOG_TAG_DIVFTREE, "Batch of vectors to insert is empty.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE, "Batch of vectors to insert is null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE, "Batch of vector IDs to insert is null.");
        RetStatus rs = RetStatus::Success();

        threadSelf->SanityCheckLockHeldInModeByMe(&container_entry->clusterLock, SX_SHARED);
        FatalAssert(container_entry->state.load() != CLUSTER_DELETED, LOG_TAG_DIVFTREE, "Container vertex is deleted.");
        while (true) {
            /*
             * we do not need to pin the cluster here because the version pin should remain at least one as long as we
             * have a lock on it.
             */
            DIVFTreeVertex* container_vertex =
                static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
            CHECK_NOT_NULLPTR(container_vertex, LOG_TAG_DIVFTREE);
            // CHECK_VERTEX_IS_VALID(container_vertex, LOG_TAG_DIVFTREE, RetStatus::FAIL);
            rs = container_vertex->BatchInsert(batch, marked_for_update);
            if (rs.IsOK() || no_major_updates) {
                break;
            }

            FatalAssert(rs.stat == RetStatus::VERTEX_NOT_ENOUGH_SPACE,
                        LOG_TAG_DIVFTREE, "Batch insert failed with error: %s", rs.Msg());
            BufferVertexEntryState state = CLUSTER_FULL;
            rs = container_entry->UpgradeAccessToExclusive(state);
            if (!rs.IsOK()) {
                FatalAssert(rs.stat == RetStatus::FAILED_TO_CAS_ENTRY_STATE, LOG_TAG_DIVFTREE,
                        "UpgradeAccessToExclusive failed with error: %s", rs.Msg());
                switch(state) {
                case BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS: /* Can this happen? */
                case BufferVertexEntryState::CLUSTER_FULL:
                case BufferVertexEntryState::CLUSTER_DELETED: /* Can this happen? */
                    /* it is possible that the update was not applied though! */
                    rs = RetStatus{.stat=RetStatus::VERTEX_UPDATED, .message=nullptr};
                    break;
                default:
                    DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "Invalid entry state!");
                    rs = RetStatus::Fail("Invalid State!");
                    break;
                }
                break;
            }

            FatalAssert((void*)(container_vertex) ==
                        (void*)(container_entry->ReadLatestVersion(false)), LOG_TAG_DIVFTREE,
                        "ClusterPtr has changed during the atomic upgrade from shared to exclusive!");
            FatalAssert(container_vertex->cluster.header.visible_size.load(std::memory_order_relaxed) ==
                        container_vertex->cluster.header.reserved_size.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "Some updates did not go through!");
            FatalAssert(container_vertex->cluster.header.reserved_size.load(std::memory_order_relaxed) >=
                        container_vertex->cluster.header.num_deleted.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "More vectors are deleted than inserted!");
            uint16_t reserved_size =
                container_vertex->cluster.header.reserved_size.load(std::memory_order_relaxed) + batch.size;
            uint16_t real_size =
                reserved_size - container_vertex->cluster.header.num_deleted.load(std::memory_order_relaxed);
            if ((container_vertex->cluster.header.num_deleted.load(std::memory_order_relaxed) > 0) &&
                ((float)real_size * COMPACTION_FACTOR <= (float)container_vertex->attr.cap)) {
                rs = CompactAndInsert(container_entry, batch, marked_for_update);
            } else if (reserved_size < container_vertex->attr.cap) {
                container_entry->DowngradeAccessToShared();
                continue;
            } else {
                rs = SplitAndInsert(container_entry, batch, marked_for_update);
            }

            FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Major update failed!");
            if (releaseEntry) {
                BufferManager::GetInstance()->
                    ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(true, true));
            } else {
                container_entry->DowngradeAccessToShared();
            }

            return rs;
        }

        if (releaseEntry) {
            BufferManager::GetInstance()->
                ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(false, false));
        }
        return rs;
    }

    inline void PruneAtRootAndRelease(BufferVertexEntry* container_entry) {
        threadSelf->SanityCheckLockHeldInModeByMe(&container_entry->clusterLock, SX_EXCLUSIVE);
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        FatalAssert(container_entry->centroidMeta.selfId == bufferMgr->GetCurrentRootId(),
                    LOG_TAG_DIVFTREE, "we should be the root!");

        VectorID rootId = bufferMgr->GetCurrentRootId();
        CHECK_VECTORID_IS_VALID(container_entry->centroidMeta.selfId , LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_INTERNAL(container_entry->centroidMeta.selfId , LOG_TAG_DIVFTREE);

        DIVFTreeVertex* container = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(container, LOG_TAG_DIVFTREE);
        uint16_t size = container->cluster.header.reserved_size.load(std::memory_order_relaxed);
        uint16_t num_deleted = container->cluster.header.num_deleted.load(std::memory_order_relaxed);

        FatalAssert(size == container->cluster.header.visible_size.load(std::memory_order_relaxed), LOG_TAG_DIVFTREE,
                    "size should be stable!");
        FatalAssert(size >= num_deleted, LOG_TAG_DIVFTREE,
                    "we cannot delete more vectors than there are in the cluster!");
        if (size - num_deleted == 0) {
            /* all vectors in the index are deleted */
            /* todo: delete this node and then create a new root? what if an insertion comes in at this point? */
            DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "we do not handle this case for now");
        }

        if (((size - num_deleted) > 1)) {
            /* no need to prune */
            bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(true, true));
            return;
        }

        /* we should see exactly one child in this cluster and that child will become the new root */
        FatalAssert(container->cluster.NumBlocks(container->attr.block_size, container->attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "cannot handle multiple blocks");
        CentroidMetaData* vmd =
            static_cast<CentroidMetaData*>(container->
                cluster.MetaData(0, false, container->attr.block_size, container->attr.cap, attr.dimension));
        VectorID newRootId = INVALID_VECTOR_ID;
        Version newRootVersion = 0;
        for (uint16_t i = 0; i < size; ++i) {
            if (vmd[i].state.load(std::memory_order_relaxed) == VECTOR_STATE_VALID) {
                newRootId = vmd[i].id;
                newRootVersion = vmd[i].version;
                break;
            }
        }
        CHECK_VECTORID_IS_VALID(newRootId, LOG_TAG_DIVFTREE);
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "Pruned old root " VECTORID_LOG_FMT ". new root is "
                VECTORID_LOG_FMT, VECTORID_LOG(container_entry->centroidMeta.selfId), VECTORID_LOG(newRootId));
        // DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "UpdateVectorLocation prune invalidation");
        bufferMgr->UpdateVectorLocation(newRootId, INVALID_VECTOR_LOCATION);
        bufferMgr->UpdateRoot(newRootId, newRootVersion, container_entry);
        container_entry->InitiateDelete();
        bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(true, false));
    }

    inline void PruneEmptyVertexAndRelease(BufferVertexEntry* container_entry) {
        threadSelf->SanityCheckLockHeldInModeByMe(&container_entry->clusterLock, SX_EXCLUSIVE);
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        DIVFTreeVertex* container = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(container, LOG_TAG_DIVFTREE);
        uint16_t size = container->cluster.header.reserved_size.load(std::memory_order_relaxed);
        uint16_t num_deleted = container->cluster.header.num_deleted.load(std::memory_order_relaxed);

        FatalAssert(size == container->cluster.header.visible_size.load(std::memory_order_relaxed), LOG_TAG_DIVFTREE,
                    "size should be stable!");
        FatalAssert(size >= num_deleted, LOG_TAG_DIVFTREE,
                    "we cannot delete more vectors than there are in the cluster!");
        if (((size - num_deleted) >= 1)) {
            /* no need to prune */
            bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(true, true));
            return;
        }

        VectorLocation loc = bufferMgr->LoadCurrentVectorLocation(container_entry->centroidMeta.selfId);
        BufferVertexEntry* parent = nullptr;
        if ((loc == INVALID_VECTOR_LOCATION) || ((parent = container_entry->ReadParentEntry(loc)) == nullptr)) {
            FatalAssert((parent == nullptr) && (loc == INVALID_VECTOR_LOCATION), LOG_TAG_DIVFTREE,
                        "mismatch between parent ptr and location");
            PruneAtRootAndRelease(container_entry);
            return;
        }
        FatalAssert((parent != nullptr) && (loc != INVALID_VECTOR_LOCATION), LOG_TAG_DIVFTREE,
                    "mismatch between parent ptr and location");
        FatalAssert(container_entry->centroidMeta.selfId != bufferMgr->GetCurrentRootId(),
                    LOG_TAG_DIVFTREE, "we should not be the root!");
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "Pruned vertex " VECTORID_LOG_FMT,
                VECTORID_LOG(container_entry->centroidMeta.selfId));
        RetStatus rs = DeleteVectorAndReleaseContainer(container_entry->centroidMeta.selfId, parent, loc);
        UNUSED_VARIABLE(rs);
        FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "this deletion should not fail!");
        container_entry->InitiateDelete();
        bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(true, false));
    }

    inline void PruneIfNeededAndRelease(BufferVertexEntry* container_entry) {
        while (true) {
            threadSelf->SanityCheckLockHeldInModeByMe(&container_entry->clusterLock, SX_SHARED);
            CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);

            BufferManager* bufferMgr = BufferManager::GetInstance();
            CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

            VectorID rootId = bufferMgr->GetCurrentRootId();
            CHECK_VECTORID_IS_VALID(rootId, LOG_TAG_DIVFTREE);
            CHECK_VECTORID_IS_CENTROID(rootId, LOG_TAG_DIVFTREE);
            if (rootId.IsLeaf()) {
                FatalAssert(container_entry->centroidMeta.selfId == rootId, LOG_TAG_DIVFTREE, "we should be the root!");
                bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(false, false));
                return;
            }

            DIVFTreeVertex* container = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
            CHECK_NOT_NULLPTR(container, LOG_TAG_DIVFTREE);
            if (container_entry->centroidMeta.selfId == rootId) {
                /* we can be sure that we remain root as changing the current root requires exclusive lock on it */
                /* todo: can it cause problems since we are loading reserve first then deleted? -> we may see num_deleted > reserved */
                if ((container->cluster.header.reserved_size.load(std::memory_order_acquire) -
                    container->cluster.header.num_deleted.load(std::memory_order_acquire)) > 1) {
                    bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(false, false));
                    return;
                }
                BufferVertexEntryState state = BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS;
                if (container_entry->UpgradeAccessToExclusive(state).IsOK()) {
                    PruneAtRootAndRelease(container_entry);
                    return;
                } else if (state == BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS ||
                           state == BufferVertexEntryState::CLUSTER_DELETED) {
                    /* someone else has handled it */
                    bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(false, false));
                    return;
                } else if (state == BufferVertexEntryState::CLUSTER_FULL) {
                    /* recheck if pruning is needed */
                    continue;
                } else {
                    DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "we shouldn't get here! Invalid state");
                }
            }

            uint16_t deleted = container->cluster.header.num_deleted.load(std::memory_order_acquire);
            uint16_t reserved = container->cluster.header.reserved_size.load(std::memory_order_acquire);
            if ((reserved - deleted) >= 1) {
                if ((reserved - deleted) < container->attr.min_size) {
                    bool res = merge_tasks.Push(MergeTask{container_entry->centroidMeta.selfId});
                    UNUSED_VARIABLE(res);
                    FatalAssert(res, LOG_TAG_DIVFTREE, "this should not fail!");
                } else if ((deleted > 0) && (threadSelf->UniformBinary((uint32_t)((double)attr.random_base_perc *
                                             (double)deleted / (double)reserved)))) {
                    bool res = compaction_tasks.Push(CompactionTask{container_entry->centroidMeta.selfId});
                    UNUSED_VARIABLE(res);
                    FatalAssert(res, LOG_TAG_DIVFTREE, "this should not fail!");
                }
                bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(false, false));
                return;
            }
            BufferVertexEntryState state = BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS;
            if (container_entry->UpgradeAccessToExclusive(state).IsOK()) {
                if ((container->cluster.header.reserved_size.load(std::memory_order_acquire) -
                    container->cluster.header.num_deleted.load(std::memory_order_acquire)) >= 1) {
                    bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(true, true));
                    return;
                }

                rootId = bufferMgr->GetCurrentRootId();
                if (rootId.IsLeaf()) {
                    FatalAssert(container_entry->centroidMeta.selfId == rootId, LOG_TAG_DIVFTREE,
                                "we should be the root!");
                    bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(true, true));
                    return;
                }
                if (container_entry->centroidMeta.selfId == rootId) {
                    PruneAtRootAndRelease(container_entry);
                } else {
                    PruneEmptyVertexAndRelease(container_entry);
                }
                return;
            } else if (state == BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS ||
                       state == BufferVertexEntryState::CLUSTER_DELETED) {
                /* someone else has handled it */
                bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(false, false));
                return;
            } else if (state == BufferVertexEntryState::CLUSTER_FULL) {
                /* recheck if pruning is needed */
                continue;
            } else {
                DIVFLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "we shouldn't get here! Invalid state");
            }
        }
    }

    RetStatus DeleteVectorAndReleaseContainer(VectorID target, BufferVertexEntry* container_entry, VectorLocation loc) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(target, LOG_TAG_DIVFTREE);
        FatalAssert(target._level + 1 == container_entry->centroidMeta.selfId._level, LOG_TAG_DIVFTREE,
                    "target cannot be child of container");
        FatalAssert(loc.detail.containerId == container_entry->centroidMeta.selfId, LOG_TAG_DIVFTREE, "Location Id mismatch");
        FatalAssert(container_entry->currentVersion == loc.detail.containerVersion, LOG_TAG_DIVFTREE,
                    "Location version mismatch");
        threadSelf->SanityCheckLockHeldInModeByMe(&container_entry->clusterLock, SX_SHARED);
        DIVFTreeVertex* container = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(container, LOG_TAG_DIVFTREE);
        Version version = 0;
        VectorState expState = VECTOR_STATE_VALID;
        RetStatus rs = container->ChangeVectorState(target, loc.detail.entryOffset, expState, VECTOR_STATE_INVALID,
                                                    &version);
        FatalAssert(target.IsVector() || rs.IsOK(), LOG_TAG_DIVFTREE,
                    "state change will never fail if target is a vertex as it should be locked in X mode.");
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        if (rs.IsOK()) {
#ifdef EXCESS_LOGING
            DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
                    "DELETEING vector_id=" VECTORID_LOG_FMT " in Location=%s: Vector=%s",
                    VECTORID_LOG(target), loc.ToString().ToCStr(),
                    VectorToString(container->cluster.Data(loc.detail.entryOffset, container->attr.centroid_id.IsLeaf(),
                                                           container->attr.block_size, container->attr.cap,
                                                           attr.dimension), attr.dimension).ToCStr());
#endif
            // DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "UpdateVectorLocation in delete invalidation:");
            bufferMgr->UpdateVectorLocation(target, INVALID_VECTOR_LOCATION);
            if (target.IsCentroid()) {
                bufferMgr->UnpinVertexVersion(target, version);
            }
            container->cluster.header.num_deleted.fetch_add(1);
            PruneIfNeededAndRelease(container_entry);
        } else {
            bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags(false, false));
        }

        return rs;
    }

    inline RetStatus ReadAndCheckVersion(VectorID containerId, Version containerVersion,
                                         BufferVertexEntry** entries, uint16_t max_entries, uint16_t& num_entries,
                                         LockMode mode) {
        BufferVertexEntry*& containerEntry = entries[max_entries - num_entries - 1];
        BufferManager* bufferMgr = BufferManager::GetInstance();
        RetStatus rs = RetStatus::Success();

        /*
         * we do not care if we are blocked because the block does not necessarily lead to Major Updates +
         * Compaction does not change the version
         */
        containerEntry = bufferMgr->ReadBufferEntry(containerId, mode);
        if (containerEntry == nullptr) {
            rs = RetStatus{.stat=RetStatus::TARGET_DELETED, .message=nullptr};
        }

        if (rs.IsOK()) {
            ++num_entries;
            if (containerEntry->currentVersion != containerVersion) {
                rs = RetStatus{.stat=RetStatus::TARGET_UPDATED, .message=nullptr};
            }
        }

        if (!rs.IsOK()) {
            bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                               ReleaseBufferEntryFlags(false, false));
        }

        return rs;
    }

    /* Todo: recheck to see if everything works fine */
    /* todo: need to refactor */
    RetStatus Migrate(std::vector<MigrationInfo> targetBatch,
                      VectorID src_id, VectorID dest_id,
                      Version src_ver, Version dest_ver) {
        CHECK_VECTORID_IS_VALID(src_id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(src_id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(dest_id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(dest_id, LOG_TAG_DIVFTREE);
        FatalAssert(src_id != dest_id, LOG_TAG_DIVFTREE,
                    "containers should not be the same!");
        FatalAssert(src_id._level == dest_id._level, LOG_TAG_DIVFTREE,
                    "containers should be on the same level!");

        RetStatus rs = RetStatus::Success();

        if (targetBatch.empty()) {
            return rs;
        }
        FatalAssert(!targetBatch.empty(), LOG_TAG_DIVFTREE, "Empty batch!");

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        uint16_t max_entries = 2;
        if (src_id.IsInternalVertex()) {
            max_entries += targetBatch.size();
        }
        std::vector<BufferVertexEntry*> entries;
        BufferVertexEntry** src_entry;
        BufferVertexEntry** dest_entry;
        entries.resize(max_entries, nullptr);
        uint16_t num_entries = 0;

        if (src_id.IsInternalVertex()) {
            for (size_t i = 0; i < targetBatch.size(); ++i) {
                FatalAssert((i == 0) || (targetBatch[i - 1].id <= targetBatch[i].id), LOG_TAG_DIVFTREE,
                            "target batch should be sorder in the increasing order of IDs");
                FatalAssert(targetBatch[i].id._level + 1 == src_id._level, LOG_TAG_DIVFTREE, "level mismatch!");
                rs = ReadAndCheckVersion(targetBatch[i].id, targetBatch[i].version, &entries[0], max_entries,
                                         num_entries, SX_SHARED);
                if (!rs.IsOK()) {
                    return rs;
                }
            }
        }

        /* Todo: if we have version logs, maybe we can use that to handle migration when there is an update */
        /* Todo: maybe we can do a simple distance computation in case there was any updates to avoid too many fails */
        /* Todo: collect stats on the failure percentages, the reason and the target level */

        /* We should first lock the child and then the parents in order of their values to avoid deadlocks! */
        if (src_id <= dest_id) {
            rs = ReadAndCheckVersion(src_id, src_ver, &entries[0], max_entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }

            src_entry = &entries[max_entries - num_entries];

            rs = ReadAndCheckVersion(dest_id, dest_ver, &entries[0], max_entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }
            dest_entry = &entries[max_entries - num_entries];
        } else {
            rs = ReadAndCheckVersion(dest_id, dest_ver, &entries[0], max_entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }
            dest_entry = &entries[max_entries - num_entries];

            rs = ReadAndCheckVersion(src_id, src_ver, &entries[0], max_entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }

            src_entry = &entries[max_entries - num_entries];
        }

        DIVFTreeVertex* src_cluster = static_cast<DIVFTreeVertex*>((*src_entry)->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(src_cluster, LOG_TAG_DIVFTREE);
        VTYPE* data = src_cluster->cluster.Data(0, src_id.IsLeaf(), src_cluster->attr.block_size,
                                                    src_cluster->attr.cap, attr.dimension);
        void* meta = src_cluster->cluster.MetaData(0, src_id.IsLeaf(), src_cluster->attr.block_size,
                                                   src_cluster->attr.cap, attr.dimension);
        FatalAssert(src_cluster->cluster.NumBlocks(src_cluster->attr.block_size, src_cluster->attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "cannot handle more than 1 block!");

        VectorBatch batch;
        batch.size = 0;
        batch.data = new VTYPE[targetBatch.size() * attr.dimension];
        batch.id = new VectorID[targetBatch.size()];
        batch.version = nullptr;
        uint16_t* migrated_offsets = new uint16_t[targetBatch.size()];

        if (src_id.IsInternalVertex()) {
            batch.version = new Version[targetBatch.size()];
        }
        bool compacted = false;
        for (size_t i = 0; i < targetBatch.size(); ++i) {
            if (compacted) {
                VectorLocation cur_loc = bufferMgr->LoadCurrentVectorLocation(targetBatch[i].id);
                if (cur_loc.detail.containerId != src_id) {
                    continue;
                }

                FatalAssert(cur_loc.detail.containerVersion == src_ver, LOG_TAG_DIVFTREE,
                            "Eventhough the src version is the same as what was used container version has changed!");
                targetBatch[i].offset = cur_loc.detail.entryOffset;
            }

            if (src_id.IsLeaf()) {
                VectorMetaData* vmd = static_cast<VectorMetaData*>(meta);
                if (vmd[targetBatch[i].offset].id != targetBatch[i].id) {
                    /* There was a compaction */
                    FatalAssert(!compacted, LOG_TAG_DIVFTREE, "Already checked for compaction!");
                    compacted = true;
                    --i;
                    continue;
                }

                VectorState excp_state = VECTOR_STATE_VALID;
                rs = src_cluster->ChangeVectorState(&vmd[targetBatch[i].offset],
                                                    excp_state, VECTOR_STATE_MIGRATED);
                if (!rs.IsOK()) {
                    continue;
                }

                memcpy(&batch.data[(size_t)batch.size * (size_t)attr.dimension],
                       &data[(size_t)attr.dimension * (size_t)targetBatch[i].offset],
                       sizeof(VTYPE) * (size_t)attr.dimension);
                batch.id[batch.size] = vmd[targetBatch[i].offset].id;
                migrated_offsets[batch.size] = targetBatch[i].offset;
                ++batch.size;
            } else {
                CentroidMetaData* vmd = static_cast<CentroidMetaData*>(meta);
                if (vmd[targetBatch[i].offset].id != targetBatch[i].id) {
                    /* There was a compaction */
                    compacted = true;
                    --i;
                    continue;
                }

                VectorState excp_state = VECTOR_STATE_VALID;
                rs = src_cluster->ChangeVectorState(&vmd[targetBatch[i].offset],
                                                    excp_state, VECTOR_STATE_MIGRATED);
                if (!rs.IsOK()) {
                    continue;
                }

                memcpy(&batch.data[(size_t)batch.size * (size_t)attr.dimension],
                       &data[(size_t)attr.dimension * (size_t)targetBatch[i].offset],
                       sizeof(VTYPE) * (size_t)attr.dimension);
                batch.id[batch.size] = vmd[targetBatch[i].offset].id;
                batch.version[batch.size] = vmd[targetBatch[i].offset].version;
                migrated_offsets[batch.size] = targetBatch[i].offset;
                ++batch.size;
            }
        }

        /* todo: we may be able to avoid this extra copy */
        if (batch.size > 0) {
            rs = BatchInsertInto((*dest_entry), batch, true, INVALID_OFFSET, dest_id < src_id);
            *dest_entry = nullptr;
        }
        delete[] batch.data;
        batch.data = nullptr;

        if (!rs.IsOK()) {
            for (uint16_t i = 0; i < batch.size; ++i) {
                VectorState expState = VECTOR_STATE_MIGRATED;
                rs = src_cluster->ChangeVectorState(batch.id[i], migrated_offsets[i],
                                                    expState, VECTOR_STATE_VALID);
                FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "this should not fail!");
            }
            rs = RetStatus{.stat=RetStatus::NEW_CONTAINER_UPDATED, .message=nullptr};
            bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                           ReleaseBufferEntryFlags(false, false));
        } else {
            uint16_t num_deleted = src_cluster->cluster.header.num_deleted.fetch_add(batch.size) - batch.size;
            uint16_t reserved = src_cluster->cluster.header.reserved_size.load(std::memory_order_acquire);
            if ((reserved - num_deleted) < src_cluster->attr.min_size) {
                bool res = merge_tasks.Push(MergeTask{src_id});
                UNUSED_VARIABLE(res);
                FatalAssert(res, LOG_TAG_DIVFTREE, "this should not fail!");
            } else if ((num_deleted > 0) &&
                       (threadSelf->UniformBinary((uint32_t)((double)attr.random_base_perc *
                                                  ((double)num_deleted / (double)reserved))))) {
                bool res = compaction_tasks.Push(CompactionTask{src_id});
                UNUSED_VARIABLE(res);
                FatalAssert(res, LOG_TAG_DIVFTREE, "this should not fail!");
            }
            BufferVertexEntry* src_entry_cpy = *src_entry;
            *src_entry = nullptr;
            SANITY_CHECK(
                if (batch.version != nullptr) {
                    for (uint16_t i = 0; i < batch.size; ++i) {
                        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "Migrated (" VECTORID_LOG_FMT ", ver:%u) from "
                                VECTORID_LOG_FMT " to " VECTORID_LOG_FMT,
                                VECTORID_LOG(batch.id[i]), batch.version[i], VECTORID_LOG(src_id), VECTORID_LOG(dest_id));
                    }
                } else {
                    for (uint16_t i = 0; i < batch.size; ++i) {
                        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "Migrated (" VECTORID_LOG_FMT ") from "
                                VECTORID_LOG_FMT " to " VECTORID_LOG_FMT,
                                VECTORID_LOG(batch.id[i]), VECTORID_LOG(src_id), VECTORID_LOG(dest_id));
                    }
                }
            )
            bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                               ReleaseBufferEntryFlags(false, false));
            PruneIfNeededAndRelease(src_entry_cpy);
        }

        delete[] migrated_offsets;
        migrated_offsets = nullptr;
        delete[] batch.id;
        batch.id = nullptr;
        if (src_id.IsInternalVertex()) {
            delete[] batch.version;
            batch.version = nullptr;
        }

        return rs;
    }

    void MigrationCheck(VectorID first_cluster, VectorID second_cluster) {
        CHECK_VECTORID_IS_VALID(first_cluster, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(first_cluster, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(second_cluster, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(second_cluster, LOG_TAG_DIVFTREE);
        FatalAssert(first_cluster != second_cluster, LOG_TAG_DIVFTREE,
                    "clusters should be different!");
        FatalAssert(first_cluster._level == second_cluster._level, LOG_TAG_DIVFTREE,
                    "clusters should be on the same level!");

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        constexpr uint8_t max_entries = 2;
        BufferVertexEntry* entries[max_entries];
        DIVFTreeVertex* clusters[max_entries];
        DIVFTreeVertex* parents[max_entries];
        VTYPE* centroidData[max_entries];
        uint16_t visible_size[max_entries];
        Version versions[max_entries];
        VectorID ids[max_entries];
        uint8_t num_entries = 0;

        if (first_cluster > second_cluster) {
            std::swap(first_cluster, second_cluster);
        }
        ids[1] = first_cluster;
        ids[0] = second_cluster;

        entries[1] = bufferMgr->ReadBufferEntry(ids[1], SX_SHARED);
        if (entries[1] == nullptr) {
            return;
        }
        ++num_entries;
        entries[0] = bufferMgr->ReadBufferEntry(ids[0], SX_SHARED);
        if (entries[0] == nullptr) {
            bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                               ReleaseBufferEntryFlags(false, false));
            return;
        }
        ++num_entries;

        versions[1] = entries[1]->currentVersion;
        versions[0] = entries[0]->currentVersion;

        clusters[1] = static_cast<DIVFTreeVertex*>(entries[1]->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(clusters[1], LOG_TAG_DIVFTREE);
        clusters[0] = static_cast<DIVFTreeVertex*>(entries[0]->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(clusters[0], LOG_TAG_DIVFTREE);

        VectorLocation loc;
        parents[1] = static_cast<DIVFTreeVertex*>(entries[1]->centroidMeta.ReadAndPinParent(loc));
        if (parents[1] == nullptr) {
            bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                               ReleaseBufferEntryFlags(false, false));
            return;
        }
#ifdef MEMORY_DEBUG
        DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE, "MigrationCheck: cluster pinned: %p:" VECTORID_LOG_FMT, parents[1],
                VECTORID_LOG(parents[1]->attr.centroid_id));
#endif
        FatalAssert(
            (static_cast<CentroidMetaData*>(parents[1]->
                cluster.MetaData(loc.detail.entryOffset, false, parents[1]->attr.block_size,
                                 parents[1]->attr.cap, attr.dimension))->id == ids[1]) &&
            (static_cast<CentroidMetaData*>(parents[1]->
                cluster.MetaData(loc.detail.entryOffset, false, parents[1]->attr.block_size,
                                 parents[1]->attr.cap, attr.dimension))->version == versions[1]),
            LOG_TAG_DIVFTREE,
            "Invalid centroid location returned!"
        );

        // if ((static_cast<CentroidMetaData*>(parents[1]->
        //         cluster.MetaData(loc.entryOffset, false, parents[1]->attr.block_size,
        //                          parents[1]->attr.cap, attr.dimension))->id != ids[1])) {
        //     parents[1]->Unpin();
        //     bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
        //                                        ReleaseBufferEntryFlags(false, false));
        //     return;

        // }

        parents[0] = static_cast<DIVFTreeVertex*>(entries[0]->centroidMeta.ReadAndPinParent(loc));
        if (parents[0] == nullptr) {
#ifdef MEMORY_DEBUG
        DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE, "MigrationCheck: cluster unpinned: %p:" VECTORID_LOG_FMT,
                parents[1], VECTORID_LOG(parents[1]->attr.centroid_id));
#endif
            parents[1]->Unpin();
            bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                               ReleaseBufferEntryFlags(false, false));
            return;
        }

#ifdef MEMORY_DEBUG
        DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE, "MigrationCheck: cluster pinned: %p:" VECTORID_LOG_FMT, parents[0],
                VECTORID_LOG(parents[0]->attr.centroid_id));
#endif

        FatalAssert(
            static_cast<CentroidMetaData*>(parents[0]->
                cluster.MetaData(loc.detail.entryOffset, false, parents[0]->attr.block_size,
                                 parents[0]->attr.cap, attr.dimension))->id == ids[0] &&
            (static_cast<CentroidMetaData*>(parents[0]->
                cluster.MetaData(loc.detail.entryOffset, false, parents[0]->attr.block_size,
                                 parents[0]->attr.cap, attr.dimension))->version == versions[0]),
            LOG_TAG_DIVFTREE,
            "Invalid centroid location returned!"
        );

        // if ((static_cast<CentroidMetaData*>(parents[0]->
        //         cluster.MetaData(loc.entryOffset, false, parents[0]->attr.block_size,
        //                          parents[0]->attr.cap, attr.dimension))->id != ids[0])) {
        //     parents[1]->Unpin();
        //     parents[0]->Unpin();
        //     bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
        //                                        ReleaseBufferEntryFlags(false, false));
        //     return;

        // }

        std::vector<MigrationInfo> migration_batch[max_entries];
        for (uint8_t cn = 0; cn < max_entries; ++cn) {
            centroidData[cn] = parents[cn]->cluster.Data(loc.detail.entryOffset, false, parents[cn]->attr.block_size,
                                                         parents[cn]->attr.cap, attr.dimension);
            visible_size[cn] = clusters[cn]->cluster.header.visible_size.load(std::memory_order_acquire);
            FatalAssert(visible_size[cn] > 0, LOG_TAG_DIVFTREE, "size should be greater than 0!");
            migration_batch[cn].reserve(visible_size[cn]);
        }

        FatalAssert(clusters[0]->cluster.NumBlocks(clusters[0]->attr.block_size, clusters[0]->attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "cannot handle more than 1 block!");

        for (int cn = 0; cn < max_entries; ++cn) {
            VTYPE* data = clusters[cn]->cluster.Data(0, ids[1].IsLeaf(), clusters[cn]->attr.block_size,
                                                     clusters[cn]->attr.cap, attr.dimension);
            void* meta = clusters[cn]->cluster.MetaData(0, ids[1].IsLeaf(), clusters[cn]->attr.block_size,
                                                        clusters[cn]->attr.cap, attr.dimension);
            std::unordered_set<VectorID, VectorIDHash> seen;
            seen.reserve(visible_size[cn]);
            for (uint16_t i = visible_size[cn] - 1; i != UINT16_MAX; --i) {
                if (ids[1].IsLeaf()) {
                    VectorMetaData* vmd = static_cast<VectorMetaData*>(meta);
                    if ((seen.find(vmd[i].id) != seen.end()) ||
                        (vmd[i].state.load(std::memory_order_acquire) != VECTOR_STATE_VALID)) {
                        continue;
                    }
                    seen.emplace(vmd[i].id);
                    /* todo: is it worth the additional memory usage to
                    cache the distance of each vector to it's centroid? */
                    if (MoreSimilar(Distance(&data[(size_t)i * (size_t)attr.dimension], centroidData[1 - cn],
                                             attr.dimension, attr.distanceAlg),
                                    Distance(&data[(size_t)i * (size_t)attr.dimension], centroidData[cn],
                                             attr.dimension, attr.distanceAlg),
                                    attr.distanceAlg)) {
                        migration_batch[cn].emplace_back(vmd[i].id, i);
                    }
                } else {
                    CentroidMetaData* vmd = static_cast<CentroidMetaData*>(meta);
                    if ((seen.find(vmd[i].id) != seen.end()) ||
                        (vmd[i].state.load(std::memory_order_acquire) != VECTOR_STATE_VALID)) {
                        continue;
                    }
                    seen.emplace(vmd[i].id);
                    /* todo: is it worth the additional memory usage to
                    cache the distance of each vector to it's centroid? */
                    if (MoreSimilar(Distance(&data[(size_t)i * (size_t)attr.dimension], centroidData[1 - cn],
                                             attr.dimension, attr.distanceAlg),
                                    Distance(&data[(size_t)i * (size_t)attr.dimension], centroidData[cn],
                                             attr.dimension, attr.distanceAlg),
                                    attr.distanceAlg)) {
                        migration_batch[cn].emplace_back(vmd[i].id, i, vmd[i].version);
                    }
                }
            }

            if (ids[1].IsInternalVertex()) {
                std::sort(migration_batch[cn].begin(), migration_batch[cn].end());
            }
            seen.clear();
#ifdef MEMORY_DEBUG
            DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE, "MigrationCheck: cluster unpinned: %p:" VECTORID_LOG_FMT,
                    parents[cn], VECTORID_LOG(parents[cn]->attr.centroid_id));
#endif
        }

        for (uint8_t cn = 0; cn < max_entries; ++cn) {
            parents[cn]->Unpin();
        }

        bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                           ReleaseBufferEntryFlags(false, false));

        /* todo: have to change for multi ndoe setup */
        if (threadSelf->UniformBinary(attr.random_base_perc / 2)) {
            Migrate(migration_batch[0], ids[0], ids[1], versions[0], versions[1]);
            Migrate(migration_batch[1], ids[1], ids[0], versions[1], versions[0]);
        } else {
            Migrate(migration_batch[1], ids[1], ids[0], versions[1], versions[0]);
            Migrate(migration_batch[0], ids[0], ids[1], versions[0], versions[1]);
        }
    }

    /* todo: need to refactor */
    RetStatus Merge(VectorID srcId, Version srcVersion,
                    VectorID destId, Version destVersion) {
        CHECK_VECTORID_IS_VALID(srcId, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(srcId, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(destId, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(destId, LOG_TAG_DIVFTREE);
        FatalAssert(srcId != destId, LOG_TAG_DIVFTREE,
                    "clusters should be on the same level!");
        FatalAssert(srcId._level == destId._level, LOG_TAG_DIVFTREE,
                    "clusters should be on the same level!");

        RetStatus rs = RetStatus::Success();
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        constexpr uint16_t max_entries = 2;
        BufferVertexEntry* entries[max_entries];
        uint16_t num_entries = 0;
        BufferVertexEntry** srcEntry;
        BufferVertexEntry** destEntry;
        uint16_t reservedSize = 0;
        uint16_t totalSize = 0;
        DIVFTreeVertex* srcCluster = nullptr;

        /* Todo: if we have version logs, maybe we can use that to handle migration when there is an update */
        /* Todo: maybe we can do a simple distance computation in case there was any updates to avoid too many fails */
        /* Todo: collect stats on the failure percentages, the reason and the target level */

        /* We should first lock the child and then the parents in order of their values to avoid deadlocks! */
        if (srcId < destId) {
            rs = ReadAndCheckVersion(srcId, srcVersion, entries, max_entries, num_entries, SX_EXCLUSIVE);
            if (!rs.IsOK()) {
                return rs;
            }
            srcEntry = &entries[max_entries - num_entries];
            srcCluster = static_cast<DIVFTreeVertex*>((*srcEntry)->ReadLatestVersion(false));
            CHECK_NOT_NULLPTR(srcCluster, LOG_TAG_DIVFTREE);
            reservedSize = srcCluster->cluster.header.reserved_size.load(std::memory_order_relaxed);
            FatalAssert(reservedSize == srcCluster->cluster.header.visible_size.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "reserved and visible size should be the same!");
            FatalAssert(reservedSize > srcCluster->cluster.header.num_deleted.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "size should be greater than to number of deleted entries!");
            /* size should not be 0 because the one who 0ed the size will also delete the cluster! */
            totalSize = reservedSize - srcCluster->cluster.header.num_deleted.load(std::memory_order_relaxed);
            if (totalSize >= srcCluster->attr.min_size) {
                bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                                   ReleaseBufferEntryFlags(false, false));
                return RetStatus{.stat=RetStatus::SRC_HAS_TOO_MANY_VECTORS, .message=nullptr};
            }

            rs = ReadAndCheckVersion(destId, destVersion, entries, max_entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }
            destEntry = &entries[max_entries - num_entries];
        } else {
            rs = ReadAndCheckVersion(destId, destVersion, entries, max_entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }
            destEntry = &entries[max_entries - num_entries];

            rs = ReadAndCheckVersion(srcId, srcVersion, entries, max_entries, num_entries, SX_EXCLUSIVE);
            if (!rs.IsOK()) {
                return rs;
            }

            srcEntry = &entries[max_entries - num_entries];
            srcCluster = static_cast<DIVFTreeVertex*>((*srcEntry)->ReadLatestVersion(false));
            CHECK_NOT_NULLPTR(srcCluster, LOG_TAG_DIVFTREE);
            reservedSize = srcCluster->cluster.header.reserved_size.load(std::memory_order_relaxed);
            FatalAssert(reservedSize == srcCluster->cluster.header.visible_size.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "reserved and visible size should be the same!");
            FatalAssert(reservedSize > srcCluster->cluster.header.num_deleted.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "size should be greater than to number of deleted entries!");
            /* size should not be 0 because the one who 0ed the size will also delete the cluster! */
            totalSize = reservedSize - srcCluster->cluster.header.num_deleted.load(std::memory_order_relaxed);
            if (totalSize >= srcCluster->attr.min_size) {
                bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                                   ReleaseBufferEntryFlags(false, false));
                return RetStatus{.stat=RetStatus::SRC_HAS_TOO_MANY_VECTORS, .message=nullptr};
            }
        }

        /* todo: use the original pointers to write these  */
        VectorBatch batch;
        batch.size = totalSize;
        batch.id = new VectorID[totalSize];
        batch.data = new VTYPE[totalSize * attr.dimension];
        batch.version = nullptr;
        bool is_leaf = srcId.IsLeaf();
        if (is_leaf) {
            batch.version = new Version[totalSize];
        }

        VTYPE* data = srcCluster->cluster.Data(0, is_leaf, srcCluster->attr.block_size, srcCluster->attr.cap,
                                               attr.dimension);
        void* meta = srcCluster->cluster.MetaData(0, is_leaf, srcCluster->attr.block_size, srcCluster->attr.cap,
                                               attr.dimension);
        FatalAssert(srcCluster->cluster.NumBlocks(srcCluster->attr.block_size, srcCluster->attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "cannot handle more than 1 block!");
        uint16_t i = 0;
        for (uint16_t offset = 0; (offset < reservedSize) && (i < totalSize); ++offset) {
            if (is_leaf) {
                /* todo: use new API for changestate */
                VectorMetaData* vmd = static_cast<VectorMetaData*>(meta);
                if (vmd[offset].state.load(std::memory_order_relaxed) == VECTOR_STATE_VALID) {
                    vmd[offset].state.store(VECTOR_STATE_MIGRATED, std::memory_order_release);
                    memcpy(&data[offset * attr.dimension], &batch.data[i * attr.dimension],
                           sizeof(VTYPE) * attr.dimension);
                    batch.id[i] = vmd[offset].id;
                    ++i;
                }
            } else {
                CentroidMetaData* vmd = static_cast<CentroidMetaData*>(meta);
                if (vmd[offset].state.load(std::memory_order_relaxed) == VECTOR_STATE_VALID) {
                    vmd[offset].state.store(VECTOR_STATE_MIGRATED, std::memory_order_release);
                    memcpy(&data[offset * attr.dimension], &batch.data[i * attr.dimension],
                           sizeof(VTYPE) * attr.dimension);
                    batch.id[i] = vmd[offset].id;
                    batch.version[i] = vmd[offset].version;
                    ++i;
                }
            }
        }
        FatalAssert(i == totalSize, LOG_TAG_DIVFTREE, "fewer elements than excpected!");

        rs = BatchInsertInto((*destEntry), batch, true, INVALID_OFFSET, destId < srcId);
        (*destEntry) = nullptr;
        if (rs.IsOK()) {
            DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE, "Merged " VECTORID_LOG_FMT " into " VECTORID_LOG_FMT,
                    VECTORID_LOG(srcId), VECTORID_LOG(destId));
            srcCluster->cluster.header.num_deleted.store(reservedSize, std::memory_order_release);
            (*srcEntry)->state.store(CLUSTER_DELETE_IN_PROGRESS, std::memory_order_release);
            PruneEmptyVertexAndRelease(*srcEntry);
            delete[] batch.data;
            if (is_leaf) {
                delete[] batch.version;
            }
            delete[] batch.id;
            return rs;
        } else {
            /* todo: add the task of checking the src to merge check */
        }

        i = 0;
        for (uint16_t offset = 0; offset < reservedSize; ++offset) {
            FatalAssert(i < totalSize, LOG_TAG_DIVFTREE, "more elements than excpected!");
            if (is_leaf) {
                VectorMetaData* vmd = static_cast<VectorMetaData*>(meta);
                if ((vmd[offset].id == batch.id[i]) &&
                    (vmd[offset].state.load(std::memory_order_relaxed) == VECTOR_STATE_MIGRATED)) {
                    vmd[offset].state.store(VECTOR_STATE_VALID, std::memory_order_release);
                    ++i;
                }
            } else {
                CentroidMetaData* vmd = static_cast<CentroidMetaData*>(meta);
                if ((vmd[offset].id == batch.id[i]) &&
                    (vmd[offset].state.load(std::memory_order_relaxed) == VECTOR_STATE_MIGRATED)) {
                    vmd[offset].state.store(VECTOR_STATE_VALID, std::memory_order_release);
                    ++i;
                }
            }
        }

        bufferMgr->ReleaseEntriesIfNotNull(&entries[max_entries - num_entries], num_entries,
                                           ReleaseBufferEntryFlags(false, false));
        delete[] batch.data;
        if (is_leaf) {
            delete[] batch.version;
        }
        delete batch.id;

        return RetStatus{.stat=RetStatus::DEST_UPDATED, .message=nullptr};
    }

    void MergeCheck(VectorID target) {
        CHECK_VECTORID_IS_VALID(target, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(target, LOG_TAG_DIVFTREE);

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        BufferVertexEntry* target_entry = bufferMgr->ReadBufferEntry(target, SX_SHARED);
        if (target_entry == nullptr) {
            return;
        }
        Version target_ver = target_entry->currentVersion;

        VectorLocation target_loc;
        BufferVertexEntry* parent_entry = target_entry->ReadParentEntry(target_loc);
        if (parent_entry == nullptr) {
            bufferMgr->ReleaseBufferEntry(target_entry, ReleaseBufferEntryFlags(false, false));
            return;
        }

        DIVFTreeVertex* target_cluster = static_cast<DIVFTreeVertex*>(target_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(target_cluster, LOG_TAG_DIVFTREE);
        DIVFTreeVertex* parent_cluster = static_cast<DIVFTreeVertex*>(parent_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(parent_cluster, LOG_TAG_DIVFTREE);

        uint16_t target_num_deleted = target_cluster->cluster.header.num_deleted.load(std::memory_order_acquire);
        uint16_t target_size = target_cluster->cluster.header.visible_size.load(std::memory_order_acquire);
        if (target_size - target_num_deleted == 0) {
            bufferMgr->ReleaseBufferEntry(parent_entry, ReleaseBufferEntryFlags(false, false));
            bufferMgr->ReleaseBufferEntry(target_entry, ReleaseBufferEntryFlags(false, false));
            return;
        }

        VTYPE target_centroid[attr.dimension];
        bool res = ComputeCentroid(target_cluster->cluster, target_cluster->attr.block_size, target_cluster->attr.cap,
                                   target.IsLeaf(), nullptr, 0, attr.dimension, attr.distanceAlg, target_centroid);
        if (!res) {
            bufferMgr->ReleaseBufferEntry(parent_entry, ReleaseBufferEntryFlags(false, false));
            bufferMgr->ReleaseBufferEntry(target_entry, ReleaseBufferEntryFlags(false, false));
            return;
        }

        FatalAssert(parent_cluster->cluster.NumBlocks(parent_cluster->attr.block_size, parent_cluster->attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "cannot handle more than 1 block!");
        VTYPE* data = parent_cluster->cluster.Data(0, false, parent_cluster->attr.block_size, parent_cluster->attr.cap,
                                                   attr.dimension);
        CentroidMetaData* vmd =
            static_cast<CentroidMetaData*>(parent_cluster->cluster.MetaData(0, false, parent_cluster->attr.block_size,
                                           parent_cluster->attr.cap, attr.dimension));

        uint16_t parent_size = parent_cluster->cluster.header.visible_size.load(std::memory_order_acquire);
        std::unordered_set<VectorID, VectorIDHash> seen;
        seen.reserve(parent_size);
        DTYPE best_dist;
        VectorID best_id = INVALID_VECTOR_ID;
        Version best_version;
        for (uint16_t i = parent_size - 1; i != UINT16_MAX; --i) {
            if ((vmd[i].id == target) || (seen.find(vmd[i].id) != seen.end()) ||
                vmd[i].state.load(std::memory_order_acquire) != VECTOR_STATE_VALID) {
                continue;
            }
            seen.emplace(vmd[i].id);

            if (best_id == INVALID_VECTOR_ID) {
                best_id = vmd[i].id;
                best_version = vmd[i].version;
                best_dist = Distance(target_centroid, &data[i * attr.dimension], attr.dimension, attr.distanceAlg);
            } else {
                DTYPE dist = Distance(target_centroid, &data[i * attr.dimension], attr.dimension, attr.distanceAlg);
                if (MoreSimilar(dist, best_dist, attr.distanceAlg)) {
                    best_id = vmd[i].id;
                    best_version = vmd[i].version;
                    best_dist = dist;
                }
            }
        }

        bufferMgr->ReleaseBufferEntry(parent_entry, ReleaseBufferEntryFlags(false, false));
        bufferMgr->ReleaseBufferEntry(target_entry, ReleaseBufferEntryFlags(false, false));

        /* todo: change in multi node */
        if (best_id != INVALID_VECTOR_ID) {
            Merge(target, target_ver, best_id, best_version);
        }
    }

    void SearchRoot(const VTYPE* query, size_t span,
                    std::vector<SortedList<ANNVectorInfo, SimilarityComparator>*>& layers,
                    DIVFTreeVertex* pinned_root_version) {
        BufferManager* bufferMgr = BufferManager::GetInstance();
        FatalAssert(bufferMgr != nullptr, LOG_TAG_DIVFTREE, "BufferManager is not initialized.");
        CHECK_NOT_NULLPTR(pinned_root_version, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(pinned_root_version->attr.centroid_id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(pinned_root_version->attr.centroid_id, LOG_TAG_DIVFTREE);
        uint8_t level = pinned_root_version->attr.centroid_id._level;
        FatalAssert(layers[level]->Size() == 1 &&
                    (*layers[level])[0].id == pinned_root_version->attr.centroid_id &&
                    (*layers[level])[0].version == pinned_root_version->attr.version, LOG_TAG_DIVFTREE,
                    "the highest level should only contain the pinned version of the root");
        FatalAssert(span > 0, LOG_TAG_DIVFTREE, "span cannot be 0");
        std::unordered_set<std::pair<VectorID, Version>, VectorIDVersionPairHash> seen;
        seen.reserve(pinned_root_version->attr.cap);
#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
            "Searching for query=%s in root=%s, neighbours=(%p)%s", VectorToString(query, attr.dimension).ToCStr(),
            pinned_root_version->ToString(true).ToCStr(), layers[level - 1],
            layers[level - 1]->ToString<ANNVectorInfoToString>().ToCStr());
#endif
        pinned_root_version->Search(query, span, layers[level - 1], seen);
#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
            "Searching for query=%s in root with id " VECTORID_LOG_FMT ", neighbours=(%p)%s",
            VectorToString(query, attr.dimension).ToCStr(), VECTORID_LOG(pinned_root_version->attr.centroid_id),
            layers[level - 1], layers[level - 1]->ToString<ANNVectorInfoToString>().ToCStr());
#endif
    }

    void SearchVertex(VectorID id, Version version, const VTYPE* query, size_t span,
                      SortedList<ANNVectorInfo, SimilarityComparator>* neighbours,
                      std::unordered_set<std::pair<VectorID, Version>, VectorIDVersionPairHash>& seen) {
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(id, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(query, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(neighbours, LOG_TAG_DIVFTREE);
        FatalAssert(span > 0, LOG_TAG_DIVFTREE, "span should be larget than 0!");

        BufferManager* bufferMgr = BufferManager::GetInstance();
        FatalAssert(bufferMgr != nullptr, LOG_TAG_DIVFTREE, "BufferManager is not initialized.");
        bool outdated;
        DIVFTreeVertexInterface* vertex_tmp = nullptr;
        while (vertex_tmp == nullptr) {
            RetStatus rs = bufferMgr->ReadAndPinVertex(id, version, vertex_tmp, &outdated);
            /*
             * todo use stats to make sure this does not happen often! this can only happen when there was a split and
             * the new version is inserted into the parent but the clusterPtr is not yet updated.
             */
            if (vertex_tmp == nullptr) {
                if (rs.stat == RetStatus::VERSION_NOT_APPLIED) {
                    DIVFTREE_YIELD();
                    continue;
                }
                return;
            }
        }

        CHECK_NOT_NULLPTR(vertex_tmp, LOG_TAG_DIVFTREE);
        DIVFTreeVertex* vertex = static_cast<DIVFTreeVertex*>(vertex_tmp);
        if (!outdated) {
            uint16_t deleted = vertex->cluster.header.num_deleted.load(std::memory_order_acquire);
            uint16_t reserved = vertex->cluster.header.reserved_size.load(std::memory_order_acquire);
            if ((deleted > 0) && (threadSelf->UniformBinary((uint32_t)((double)attr.random_base_perc *
                                    (double)deleted / (double)reserved)))) {
                bool res = compaction_tasks.Push(CompactionTask{vertex->attr.centroid_id});
                UNUSED_VARIABLE(res);
                FatalAssert(res, LOG_TAG_DIVFTREE, "this should not fail!");
            }
        }
#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
            "Searching for query=%s in vertex=%s, neighbours=(%p)%s", VectorToString(query, attr.dimension).ToCStr(),
            vertex->ToString(true).ToCStr(), neighbours, neighbours->ToString<ANNVectorInfoToString>().ToCStr());
#endif
        vertex->Search(query, span, neighbours, seen);
#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
            "Searching for query=%s in vertex with id " VECTORID_LOG_FMT ", neighbours=(%p)%s",
            VectorToString(query, attr.dimension).ToCStr(), VECTORID_LOG(vertex->attr.centroid_id), neighbours,
            neighbours->ToString<ANNVectorInfoToString>().ToCStr());
#endif
        vertex->Unpin();
    }

    /* todo: use multiple threads for searching each layer -> what if we use a single pool for all searches?
       if there are few threads, they will do the search layer themselves but if there are free threads they
       can help each other */
    void SearchLayer(const VTYPE* query, size_t span,
                     std::vector<SortedList<ANNVectorInfo, SimilarityComparator>*>& layers, uint8_t level) {
        FatalAssert(level < layers.size() - 1, LOG_TAG_DIVFTREE, "level out of bounds!");
        FatalAssert((uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_DIVFTREE, "level out of bounds!");
        FatalAssert(layers[level - 1]->Empty(), LOG_TAG_DIVFTREE, "next level should be empty!");
        std::unordered_set<std::pair<VectorID, Version>, VectorIDVersionPairHash> seen;
        seen.reserve(layers[level]->Size() * (((uint64_t)level == VectorID::LEAF_LEVEL) ? attr.leaf_max_size :
                                                                                         attr.internal_max_size));

#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
            "SearchLayer BEGIN: query=%s, layer=%hhu, upper_layer=(%p)%s ",
            VectorToString(query, attr.dimension).ToCStr(), level-1, layers[level],
            layers[level]->ToString<ANNVectorInfoToString>().ToCStr());
#endif
        if (layers[level]->Size() == 1) {
            FatalAssert((*layers[level])[0].id._level == (uint64_t)level, LOG_TAG_DIVFTREE, "mismatch level!");
            SearchVertex((*layers[level])[0].id, (*layers[level])[0].version, query, span, layers[level - 1], seen);
#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
            "SearchLayer END single: query=%s, layer=%hhu, upper_layer=(%p)%s, lower_layer=(%p)%s",
            VectorToString(query, attr.dimension).ToCStr(),
            level-1, layers[level]->ToString<ANNVectorInfoToString>().ToCStr(),
            layers[level-1]->ToString<ANNVectorInfoToString>().ToCStr());
#endif
            return;
        }

        std::vector<SearchTask> tasks;
        uint64_t taskId = threadSelf->GetNextTaskID();
        std::atomic<bool>* sync_bools = new std::atomic<bool>[layers[level]->Size() * 2];
        tasks.reserve(layers[level]->Size());
        for (size_t i = 0; i < layers[level]->Size(); ++i) {
            FatalAssert((*layers[level])[i].id._level == (uint64_t)level, LOG_TAG_DIVFTREE, "mismatch level!");
            tasks.emplace_back(taskId, (*layers[level])[i].id, (*layers[level])[i].version, query, span,
                               &sync_bools[i], &sync_bools[i + layers[level]->Size()]);
        }
#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
            "SearchLayer: query=%s, layer=%hhu, taskId=%lu ",
            VectorToString(query, attr.dimension).ToCStr(), level-1, taskId);
#endif

        bool rs = search_tasks.BatchPush(&tasks[0], tasks.size());
        UNUSED_VARIABLE(rs);
        FatalAssert(rs, LOG_TAG_DIVFTREE, "should not fail!");

        for (size_t i = tasks.size() - 1; i != UINT64_MAX; --i) {
            bool exp = false;
            if (!tasks[i].taken->compare_exchange_strong(exp, true)) {
                continue;
            }

            SearchVertex(tasks[i].target, tasks[i].version, query, span, layers[level - 1], seen);
            tasks[i].done->store(true, std::memory_order_relaxed);
        }

        size_t num_done = 0;
        bool first_time = true;
        while(num_done != tasks.size()) {
            if (first_time) {
                first_time = false;
            } else {
                /* todo: is this too much and if so maybe we should use wait and notify instead or use YEILD */
                usleep(1);
            }
            for (size_t i = 0; i < tasks.size(); ++i) {
                if (tasks[i].done_waiting) {
                    continue;
                }
                if (tasks[i].done->load(std::memory_order_acquire)) {
                    ++num_done;
                    tasks[i].done_waiting = true;
                }
            }
        }

        /* todo: check if I need a better algorithm for this */
        std::unordered_set<uintptr_t> lists;
        for (size_t i = 0; i < tasks.size(); ++i) {
            if (tasks[i].neighbours == nullptr) {
                continue;
            }
            if (lists.find(reinterpret_cast<uintptr_t>(tasks[i].neighbours)) != lists.end()) {
                tasks[i].neighbours = nullptr;
                continue;
            }

            lists.insert(reinterpret_cast<uintptr_t>(tasks[i].neighbours));
            layers[level - 1]->MergeWith(*tasks[i].neighbours, span, false);
            delete tasks[i].neighbours;
            tasks[i].neighbours = nullptr;
        }

        delete[] sync_bools;

#ifdef EXCESS_LOGING
        DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
            "SearchLayer END: query=%s, layer=%hhu, taskId=%lu, upper_layer=(%p)%s, lower_layer=(%p)%s",
            VectorToString(query, attr.dimension).ToCStr(), level-1, taskId, layers[level],
            layers[level]->ToString<ANNVectorInfoToString>().ToCStr(), layers[level-1],
            layers[level-1]->ToString<ANNVectorInfoToString>().ToCStr());
#endif
    }

    RetStatus ANNSearch(const VTYPE* query, size_t k, uint8_t internal_node_search_span, uint8_t leaf_node_search_span,
                        uint8_t start_level, uint8_t end_level,
                        std::vector<SortedList<ANNVectorInfo, SimilarityComparator>*>& layers,
                        DIVFTreeVertex* pinned_root_version) {
        BufferManager* bufferMgr = BufferManager::GetInstance();
        FatalAssert(bufferMgr != nullptr, LOG_TAG_DIVFTREE, "BufferManager is not initialized.");
        CHECK_NOT_NULLPTR(pinned_root_version, LOG_TAG_DIVFTREE);
        /*
         * Since a version of the root is pinned, we basicaly have an MVCC snapshop of the
         * database based on that root version and it's children are not deleted unless they are unpinned or empty
         */
        FatalAssert(pinned_root_version->attr.centroid_id._level >= start_level, LOG_TAG_DIVFTREE,
                    "start_level should be lower than or equal to root level!");
        FatalAssert(end_level < start_level, LOG_TAG_DIVFTREE, "last level cannot be higher than start level!");

        uint8_t current_level = start_level;
        uint8_t span;
        while (current_level > end_level) {
            /* migration trigger check */
            if ((layers[current_level]->Size() > 1) &&
                (threadSelf->UniformBinary(attr.migration_check_triger_rate))) {
                VectorID firstId = INVALID_VECTOR_ID;
                VectorID secondId = INVALID_VECTOR_ID;
                if (threadSelf->UniformBinary(attr.migration_check_triger_single_rate)) {
                    uint64_t index = threadSelf->UniformRange64(0, layers[current_level]->Size() - 1);
                    firstId = (*layers[current_level])[index].id;
                    /* todo: this will not work when we have multi node system as it relies on creator node id to be 0*/
                    secondId = bufferMgr->GetRandomCentroidIdAtLayer(current_level, firstId);
                    if (secondId == INVALID_VECTOR_ID) {
                        firstId = INVALID_VECTOR_ID;
                    }
                } else {
                    auto indices = threadSelf->UniformRangeTwo64(0, layers[current_level]->Size() - 1);
                    firstId = (*layers[current_level])[indices.first].id;
                    secondId = (*layers[current_level])[indices.second].id;
                    uint64_t num_retry = 0;
                    while(secondId == firstId) {
                        uint64_t index = threadSelf->UniformRange64(0, layers[current_level]->Size() - 1);
                        secondId = (*layers[current_level])[index].id;
                        ++num_retry;
                        if (num_retry >= Thread::MAX_RETRY) {
                            firstId = INVALID_VECTOR_ID;
                            secondId = INVALID_VECTOR_ID;
                            break;
                        }
                    }
                }

                if ((secondId != INVALID_VECTOR_ID) && (firstId != INVALID_VECTOR_ID)) {
                    bool res = migration_tasks.Push(MigrationCheckTask{.first=firstId, .second=secondId});
                    UNUSED_VARIABLE(res);
                    FatalAssert(res, LOG_TAG_DIVFTREE, "this should not fail!");
                }
            }


            uint8_t next_level = current_level - 1;
            if (next_level == end_level) {
                span = k;
            } else if ((uint64_t)next_level > VectorID::LEAF_LEVEL) {
                span = internal_node_search_span;
            } else {
                FatalAssert((uint64_t)next_level == VectorID::LEAF_LEVEL, LOG_TAG_DIVFTREE,
                            "since current cannot be lower than end level, "
                            "if it is at vector level, the first condition will handle it.");
                span = leaf_node_search_span;
            }
            FatalAssert(!layers[current_level]->Empty(), LOG_TAG_DIVFTREE,
                        "current level cannot be empty!");
            FatalAssert(layers[next_level]->Empty(), LOG_TAG_DIVFTREE,
                        "next level should be empty!");
            FatalAssert(span > 0, LOG_TAG_DIVFTREE,
                        "span should be at least 1");
            if (pinned_root_version->attr.centroid_id._level == current_level) {
                FatalAssert(layers[current_level]->Size() == 1 &&
                            (*layers[current_level])[0].id == pinned_root_version->attr.centroid_id &&
                            (*layers[current_level])[0].version == pinned_root_version->attr.version, LOG_TAG_DIVFTREE,
                            "the highest level should only contain the pinned version of the root");
                SearchRoot(query, span, layers, pinned_root_version);
            } else {
                SearchLayer(query, span, layers, current_level);
            }

            if (layers[next_level]->Empty()) {
                layers[current_level]->Clear();
                if (current_level == pinned_root_version->attr.centroid_id._level) {
                    return RetStatus{.stat=RetStatus::FAIL, .message=nullptr};
                }
                ++current_level;
                continue;
            }
            --current_level;
        }

        return RetStatus::Success();
    }

    inline void AsyncSearch(Thread* self) {
        CHECK_NOT_NULLPTR(self, LOG_TAG_DIVFTREE);
        self->InitDIVFThread();

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        std::unordered_set<std::pair<VectorID, Version>, VectorIDVersionPairHash> seen;
        seen.reserve(std::max(attr.internal_max_size, attr.leaf_max_size));
        SearchTask oldTask;
        oldTask.target = INVALID_VECTOR_ID;

        while (!end_signal.load(std::memory_order_acquire) || !search_tasks.Empty()) {
            SearchTask nextTask;
            if (search_tasks.PopHead(nextTask)) {
                bool exp = false;
                if (!(nextTask.taken->compare_exchange_strong(exp, true))) {
                    continue;
                }
                CHECK_VECTORID_IS_VALID(nextTask.target, LOG_TAG_DIVFTREE);

                if ((nextTask.master != oldTask.master) || (nextTask.taskId != oldTask.taskId) ||
                    (oldTask.target == INVALID_VECTOR_ID)) {
                    /* todo: we can add another bool pointer to this and when we change the neighbours, we set that
                       to true so that the user knows it can proceed with that list */
                    nextTask.neighbours =
                        new SortedList<ANNVectorInfo, SimilarityComparator>(attr.similarityComparator, nextTask.k);
                    seen.clear();
                } else {
                    CHECK_NOT_NULLPTR(oldTask.neighbours, LOG_TAG_DIVFTREE);
                    nextTask.neighbours = oldTask.neighbours;
                }
#ifdef EXCESS_LOGING
            DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
                    "BGSearch BEGIN: taskId=%lu", nextTask.taskId);
#endif
                oldTask.CopyFrom(nextTask);

                SearchVertex(nextTask.target, nextTask.version, nextTask.query, nextTask.k,
                             nextTask.neighbours, seen);
#ifdef EXCESS_LOGING
            DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
                    "BGSearch END: taskId=%lu", nextTask.taskId);
#endif
                nextTask.done->store(true, std::memory_order_release);
            }
        }
        self->DestroyDIVFThread();
    }

    inline void BGMigration(Thread* self) {
        CHECK_NOT_NULLPTR(self, LOG_TAG_DIVFTREE);
        self->InitDIVFThread();

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        while (!end_signal.load(std::memory_order_acquire)) {
            MigrationCheckTask nextTask;
            if (migration_tasks.TryPopHead(nextTask)) {
#ifdef EXCESS_LOGING
            DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
                    "BGMigration existing task: 1) " VECTORID_LOG_FMT " 2) " VECTORID_LOG_FMT,
                    VECTORID_LOG(nextTask.first), VECTORID_LOG(nextTask.second));
#endif
                MigrationCheck(nextTask.first, nextTask.second);
            } else {
                auto ids = bufferMgr->GetTwoRandomCentroidIdAtNonRootLayer();
                if ((ids.first != INVALID_VECTOR_ID) && (ids.second != INVALID_VECTOR_ID)) {
#ifdef EXCESS_LOGING
                    DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
                            "BGMigration new task: 1) " VECTORID_LOG_FMT " 2) " VECTORID_LOG_FMT,
                            VECTORID_LOG(ids.first), VECTORID_LOG(ids.second));
#endif
                    MigrationCheck(ids.first, ids.second);
                } else {
                    usleep(1);
                }
            }
        }
        self->DestroyDIVFThread();
    }

    inline void BGMerge(Thread* self) {
        CHECK_NOT_NULLPTR(self, LOG_TAG_DIVFTREE);
        self->InitDIVFThread();

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        while (!end_signal.load(std::memory_order_acquire)) {
            MergeTask nextTask;
            if (merge_tasks.PopHead(nextTask)) {
#ifdef EXCESS_LOGING
                DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
                        "BGMerge task " VECTORID_LOG_FMT,
                        VECTORID_LOG(nextTask.target));
#endif
                MergeCheck(nextTask.target);
            }
        }
        self->DestroyDIVFThread();
    }

    inline void BGCompaction(Thread* self) {
        CHECK_NOT_NULLPTR(self, LOG_TAG_DIVFTREE);
        self->InitDIVFThread();

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        while (!end_signal.load(std::memory_order_acquire)) {
            CompactionTask nextTask;
            if (compaction_tasks.PopHead(nextTask)) {
#ifdef EXCESS_LOGING
                DIVFLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
                        "BGCompaction task " VECTORID_LOG_FMT,
                        VECTORID_LOG(nextTask.target));
#endif
                CompactionCheck(nextTask.target);
            }
        }
        self->DestroyDIVFThread();
    }

    inline void StartBGThreads() {
        searchers.reserve(attr.num_searchers);
        for (size_t i = 0; i < attr.num_searchers; ++i) {
            searchers.emplace_back(new Thread(attr.random_base_perc));
            searchers.back()->StartMemberFunction(&DIVFTree::AsyncSearch, this);
        }

        bg_migrators.reserve(attr.num_migrators);
        for (size_t i = 0; i < attr.num_migrators; ++i) {
            bg_migrators.emplace_back(new Thread(attr.random_base_perc));
            bg_migrators.back()->StartMemberFunction(&DIVFTree::BGMigration, this);
        }

        bg_mergers.reserve(attr.num_mergers);
        for (size_t i = 0; i < attr.num_mergers; ++i) {
            bg_mergers.emplace_back(new Thread(attr.random_base_perc));
            bg_mergers.back()->StartMemberFunction(&DIVFTree::BGMerge, this);
        }

        bg_compactors.reserve(attr.num_compactors);
        for (size_t i = 0; i < attr.num_compactors; ++i) {
            bg_compactors.emplace_back(new Thread(attr.random_base_perc));
            bg_compactors.back()->StartMemberFunction(&DIVFTree::BGCompaction, this);
        }
    }

    inline void DestroyBGThreads() {
        FatalAssert(end_signal.load(std::memory_order_acquire), LOG_TAG_DIVFTREE,
                    "We should be ending if we are here!");
        for (size_t i = 0; i < attr.num_compactors; ++i) {
            delete bg_compactors[i];
        }

        for (size_t i = 0; i < attr.num_mergers; ++i) {
            delete bg_mergers[i];
        }

        for (size_t i = 0; i < attr.num_migrators; ++i) {
            delete bg_migrators[i];
        }

        for (size_t i = 0; i < attr.num_searchers; ++i) {
            delete searchers[i];
        }
    }

TESTABLE;
};

};

#endif