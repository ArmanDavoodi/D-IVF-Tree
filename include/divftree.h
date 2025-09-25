#ifndef DIVFTREE_H_
#define DIVFTREE_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"
#include "distance.h"
#include "distributed_common.h"

#include "utils/synchronization.h"

#include "interface/divftree.h"

#include <algorithm>
#include <atomic>

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
        FatalAssert(attr.version == 0, LOG_TAG_DIVFTREE_VERTEX, "The first version should be 0!");
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
    }

    ~DIVFTreeVertex() override {
        FatalAssert(unpinCount.load(std::memory_order_relaxed) == 0, LOG_TAG_DIVFTREE_VERTEX,
                    "the vertex is still pinned!");
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
            if (state == VECTOR_STATE_VALID || state == VECTOR_STATE_MIGRATED) {
                FatalAssert(vmd[i].id != INVALID_VECTOR_ID, LOG_TAG_DIVFTREE_VERTEX, "cannot unpin invalid vector");
                BufferManager::GetInstance()->UnpinVertexVersion(vmd[i].id, vmd[i].version);
            }
        }
    }

    inline void Unpin() override {
        if (unpinCount.fetch_add(1) == UINT64_MAX) {
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Unpin and delete vertex " VECTORID_LOG_FMT,
                 VECTORID_LOG(attr.centroid_id));
            /* todo: we need to handle all the children(unpining their versions and stuff) in the destructor */
            this->~DIVFTreeVertex();
            BufferManager::GetInstance()->Recycle(this);
            return;
        }
        else {
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Unpin vertex " VECTORID_LOG_FMT,
                 VECTORID_LOG(attr.centroid_id));
        }
    }

    inline void MarkForRecycle(uint64_t pinCount) override {
        FatalAssert(pinCount >= unpinCount.load(), LOG_TAG_DIVFTREE_VERTEX,
                    "The cluster was unpinned more times than it was pinned. pinCount=%lu, unpinCount=%lu",
                    pinCount, unpinCount.load());
        uint64_t newPin = unpinCount.fetch_sub(pinCount) - pinCount;
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Mark vertex " VECTORID_LOG_FMT
            " for recycle with pin count %lu", VECTORID_LOG(attr.centroid_id), pinCount);
        if (newPin == 0) {
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Delete vertex " VECTORID_LOG_FMT,
                 VECTORID_LOG(attr.centroid_id));
            /* todo: we need to handle all the children(unpining their versions and stuff) in the destructor */
            this->~DIVFTreeVertex();
            BufferManager::GetInstance()->Recycle(this);
            return;
        }
    }

    Cluster& GetCluster() override {
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "not implemented!");
    }

    const DIVFTreeVertexAttributes& GetAttributes() const override {
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "not implemented!");
    }

    /* todo: make sure the vertex is locked in shared/exclusive mode when calling this function! */
    RetStatus BatchInsert(const VectorBatch& batch, uint16_t marked_for_update = INVALID_OFFSET) {
        // CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
        FatalAssert(batch.size > 0, LOG_TAG_DIVFTREE_VERTEX,
                    "Batch size must be greater than zero.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                    "Batch data must not be null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                    "Batch id must not be null.");
        uint16_t offset = cluster.header.reserved_size.fetch_add(batch.size);
        FatalAssert(offset < cluster.header.reserved_size.load(), LOG_TAG_DIVFTREE_VERTEX, "Overflow detected!");
        if (offset + batch.size > attr.cap) {
            cluster.header.reserved_size.fetch_sub(batch.size);
            return RetStatus{.stat = RetStatus::VERTEX_NOT_ENOUGH_SPACE, .message=nullptr};
        }
        BufferManager* bufferMgr = BufferManager::GetInstance();
        FatalAssert(bufferMgr != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                    "BufferManager is not initialized.");
        const uint16_t dim = attr.index->GetAttributes().dimension;
        VTYPE* dest = cluster.Data(offset, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        memcpy(dest, batch.data, batch.size * dim * sizeof(VTYPE));
        Address meta = cluster.MetaData(offset, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        for (uint16_t i = 0; i < batch.size; ++i) {
            if (attr.centroid_id.IsLeaf()) {
                VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(meta);
                vmd[i].id = batch.id[i];
                FatalAssert(vmd[i].state.load(std::memory_order_relaxed) == VECTOR_STATE_INVALID,
                            LOG_TAG_DIVFTREE_VERTEX, "Reserved space at offset %hu is not in INVALID state.",
                            offset + i);
                vmd[i].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            } else {
                FatalAssert(batch.version != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                            "Batch version must not be null for centroid insertion.");
                CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(meta);
                vmd[i].id = batch.id[i];
                bufferMgr->PinVertexVersion(batch.id[i], batch.version[i]);
                vmd[i].version = batch.version[i];
                FatalAssert(vmd[i].state.load(std::memory_order_relaxed) == VECTOR_STATE_INVALID,
                            LOG_TAG_DIVFTREE_VERTEX, "Reserved space at offset %hu is not in INVALID state.",
                            offset + i);
                vmd[i].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            }
        }

        cluster.header.visible_size.fetch_add(batch.size);

        for (uint16_t i = 0; i < batch.size; ++i) {
            bufferMgr->
                UpdateVectorLocation(batch.id[i], VectorLocation{.containerId = attr.centroid_id,
                                                                 .containerVersion = attr.version,
                                                                 .entryOffset = offset + i});
        }

        if (marked_for_update != INVALID_OFFSET) {
            FatalAssert(marked_for_update < offset, LOG_TAG_DIVFTREE_VERTEX,
                        "Marked for update offset %hu is not less than the start of newly inserted batch %hu.",
                        marked_for_update, offset);
            FatalAssert(attr.centroid_id.IsInternalVertex(), LOG_TAG_DIVFTREE_VERTEX,
                        "Marked for update offset can only be set for internal vertices.");
            CentroidMetaData* markedMeta = reinterpret_cast<CentroidMetaData*>(
                cluster.MetaData(marked_for_update, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim));
            markedMeta->state.store(VECTOR_STATE_OUTDATED);
            cluster.header.num_deleted.fetch_add(1);
            /* todo: can we unpin version here??? or should we do it in the destructor? yes because if we see that this is outdated we will reread cluster with the new size*/
            bufferMgr->UnpinVertexVersion(markedMeta->id, markedMeta->version);
        }

        return RetStatus::Success();
    }

    /*
     * if id.level is vertex/cluster and targetState is Invalid then the cluster should be locked in exclusive mode
     * otherwise if id.level is vertex/cluster then target should be locked in shared mode
     */
    inline RetStatus ChangeVectorState(VectorID target, uint16_t targetOffset,
                                       VectorState excpectedState, VectorState finalState, Version* version = nullptr) {
        CHECK_VECTORID_IS_VALID(target, LOG_TAG_DIVFTREE_VERTEX);
        FatalAssert(target._level + 1 == attr.centroid_id._level, LOG_TAG_DIVFTREE_VERTEX, "level mismatch");
        FatalAssert(excpectedState == VECTOR_STATE_VALID || excpectedState == VECTOR_STATE_MIGRATED,
                    LOG_TAG_DIVFTREE_VERTEX, "expected state can be either VALID or MIGRATED");
        FatalAssert(finalState == VECTOR_STATE_VALID || finalState == VECTOR_STATE_MIGRATED ||
                    finalState == VECTOR_STATE_INVALID, LOG_TAG_DIVFTREE_VERTEX,
                    "final state can only be valid, invalid or migrated!");
        FatalAssert((excpectedState == VECTOR_STATE_MIGRATED) == (finalState == VECTOR_STATE_VALID),
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
        if (excpectedState == VECTOR_STATE_MIGRATED) {
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

        return (state->compare_exchange_strong(excpectedState, finalState) ?
                RetStatus::Success() :
                RetStatus{.stat=RetStatus::FAILED_TO_CAS_VECTOR_STATE, .message=nullptr});
    }

    // RetStatus Search(const Vector& query, size_t k,
    //                  std::vector<std::pair<VectorID, DTYPE>>& neighbours) override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     FatalAssert(k > 0, LOG_TAG_DIVFTREE_VERTEX, "Number of neighbours should not be 0");
    //     FatalAssert(neighbours.size() <= k, LOG_TAG_DIVFTREE_VERTEX,
    //                 "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

    //     CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX,
    //              "Search: Cluster=%s", cluster.ToString().ToCStr());
    //     PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_DIVFTREE_VERTEX, "Search: Neighbours before search");
    //     uint16_t curr_size = cluster.FilledSize();
    //     for (uint16_t i = 0; i < curr_size; ++i) {
    //         if (!cluster[i].IsVisible()) {
    //             continue;
    //         }
    //         DTYPE distance = Distance(query, Vector(cluster[i].vector));
    //         neighbours.emplace_back(cluster[i].id, distance);
    //         std::push_heap(neighbours.begin(), neighbours.end(), _reverseSimilarityComparator);
    //         if (neighbours.size() > k) {
    //             std::pop_heap(neighbours.begin(), neighbours.end(), _reverseSimilarityComparator);
    //             neighbours.pop_back();
    //         }
    //         PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_DIVFTREE_VERTEX, "Search: Neighbours after checking vector "
    //                                 VECTORID_LOG_FMT, VECTORID_LOG(vectorPair.id));
    //     }
    //     PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_DIVFTREE_VERTEX, "Search: Neighbours after search");

    //     FatalAssert(neighbours.size() <= k, LOG_TAG_DIVFTREE_VERTEX,
    //         "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

    //     return RetStatus::Success();
    // }

    // VectorID CentroidID() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return cluster._centroid_id;
    // }

    // inline VPairComparator GetSimilarityComparator(bool reverese) const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return (reverese ? _reverseSimilarityComparator : _similarityComparator);
    // }

    // inline DTYPE Distance(const Vector& a, const Vector& b) const override {
    //     switch (_distanceAlg)
    //     {
    //     case DistanceType::L2Distance:
    //         return L2::Distance(a, b, VectorDimension());
    //     default:
    //         CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE_VERTEX,
    //              "Distance: Invalid distance type: %s", DISTANCE_TYPE_NAME[_distanceAlg]);
    //     }
    //     return 0; // Return 0 if the distance type is invalid
    // }

    // String ToString(bool detailed = false) const override {
    //     return String("{clustering algorithm=%s, distance=%s, centroid_copy=%s, LockState=%s, Cluster=",
    //                   CLUSTERING_TYPE_NAME[clusteringAlg], DISTANCE_TYPE_NAME[_distanceAlg],
    //                   _centroid_copy.ToString(cluster._dim).ToCStr(), lock.ToString().ToCStr()) +
    //            cluster.ToString(detailed) + String("}");
    // }


protected:
    const DIVFTreeVertexAttributes attr;
    std::atomic<uint64_t> unpinCount;
    Cluster cluster;

TESTABLE;
friend class DIVFTree;
};

class DIVFTree : public DIVFTreeInterface {
public:

    const DIVFTreeAttributes& GetAttributes() const override {
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "function not implemented");
    }
    // DIVFTree(DIVFTreeAttributes attr) : core_attr(attr.core), leaf_min_size(attr.leaf_min_size),
    //                                      leaf_max_size(attr.leaf_max_size),
    //                                      internal_min_size(attr.internal_min_size),
    //                                      internal_max_size(attr.internal_max_size),
    //                                      split_internal(attr.split_internal), split_leaf(attr.split_leaf),
    //                                      _similarityComparator(
    //                                         GetDistancePairSimilarityComparator(attr.core.distanceAlg, false)),
    //                                      _reverseSimilarityComparator(
    //                                         GetDistancePairSimilarityComparator(attr.core.distanceAlg, true)),
    //                                      _size(0), _root(INVALID_VECTOR_ID), _levels(2) {
    //     RetStatus rs = RetStatus::Success();
    //     rs = _bufmgr.Init();
    //     FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Failed to init buffer manager.");
    //     _root = _bufmgr.RecordRoot();
    //     FatalAssert(_root.IsValid(), LOG_TAG_DIVFTREE, "Invalid root ID: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
    //     FatalAssert(_root.IsCentroid(), LOG_TAG_DIVFTREE, "root should be a centroid: "
    //                 VECTORID_LOG_FMT, VECTORID_LOG(_root));
    //     FatalAssert(_root.IsLeaf(), LOG_TAG_DIVFTREE, "first root should be a leaf: "
    //                 VECTORID_LOG_FMT, VECTORID_LOG(_root));

    //     rs = _bufmgr.UpdateClusterAddress(_root, CreateNewVertex(_root));
    //     FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Failed to update cluster address for root: "
    //                 VECTORID_LOG_FMT, VECTORID_LOG(_root));
    //     _levels = 2;
    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE,
    //          "Init DIVFTree Index End: rootID= " VECTORID_LOG_FMT ", _levels = %hhu, attr=%s",
    //          VECTORID_LOG(_root), _levels, attr.ToString().ToCStr());
    // }

    // ~DIVFTree() override {
    //     RetStatus rs = RetStatus::Success();
    //     rs = _bufmgr.Shutdown();
    //     CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Shutdown DIVFTree Index End: rs=%s", rs.Msg());
    // }

    // RetStatus Insert(const Vector& vec, VectorID& vec_id, uint16_t vertex_per_layer) override {
    //     FatalAssert(_root.IsValid(), LOG_TAG_DIVFTREE, "Invalid root ID.");
    //     FatalAssert(_root.IsCentroid(), LOG_TAG_DIVFTREE, "Invalid root ID -> root should be a centroid.");
    //     FatalAssert(vec.IsValid(), LOG_TAG_DIVFTREE, "Invalid query vector.");

    //     FatalAssert(vertex_per_layer > 0, LOG_TAG_DIVFTREE, "vertex_per_layer cannot be 0.");
    //     FatalAssert(_levels > 1, LOG_TAG_DIVFTREE, "Height of the tree should be at least two but is %hhu.",
    //                 _levels);

    //     RetStatus rs = RetStatus::Success();

    //     CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
    //          "Insert BEGIN: Vector=%s, _size=%lu, _levels=%hhu",
    //          vec.ToString(core_attr.dimension).ToCStr(), _size, _levels);

    //     std::vector<std::pair<VectorID, DTYPE>> upper_layer, lower_layer;
    //     upper_layer.emplace_back(_root, 0);
    //     VectorID next = _root;
    //     while (next.IsInternalVertex()) {
    //         CHECK_VECTORID_IS_VALID(next, LOG_TAG_DIVFTREE);
    //         // Get the next layer of vertices
    //         rs = SearchVertexs(vec, upper_layer, lower_layer,
    //                          (next._level - 1 == VectorID::LEAF_LEVEL) ? 1 : vertex_per_layer);
    //         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Search vertices failed with error: %s", rs.Msg());
    //         FatalAssert(!lower_layer.empty(), LOG_TAG_DIVFTREE, "Lower layer should not be empty.");
    //         FatalAssert((next.IsInternalVertex() ? (lower_layer.size() <= vertex_per_layer) : (lower_layer.size() == 1)),
    //                     LOG_TAG_DIVFTREE, "Lower layer size (%lu) is larger than %hu (%s).",
    //                     lower_layer.size(), (next.IsInternalVertex() ? vertex_per_layer : 1),
    //                     (next.IsInternalVertex() ? "internal k" : "leaf case"));
    //         FatalAssert(lower_layer.front().first._level == next._level - 1, LOG_TAG_DIVFTREE,
    //                     "Lower layer first element level (%hhu) does not match expected level (%hhu).",
    //                     lower_layer.front().first._level, next._level - 1);
    //         next = lower_layer.front().first;
    //         upper_layer.swap(lower_layer);
    //     }

    //     DIVFTreeVertex* leaf = static_cast<DIVFTreeVertex*>(_bufmgr.GetVertex(upper_layer.front().first));
    //     CHECK_VERTEX_IS_VALID(leaf, LOG_TAG_DIVFTREE, false);
    //     FatalAssert(leaf->IsLeaf(), LOG_TAG_DIVFTREE, "Next vertex should be a leaf but is " VECTORID_LOG_FMT,
    //                 VECTORID_LOG(next));

    //     if (leaf->IsFull()) {
    //         CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "Leaf is full.");
    //     }

    //     vec_id = RecordInto(vec, leaf);
    //     if (vec_id.IsValid()) {
    //         ++_size;
    //         if (leaf->IsFull()) {
    //             Split(leaf); // todo background job?
    //             // todo assert success
    //         }
    //     }
    //     else {
    //         rs = RetStatus::Fail(""); //todo
    //     }

    //     FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Insert failed with error: %s", rs.Msg());
    //     FatalAssert(_levels == _bufmgr.GetHeight(), LOG_TAG_DIVFTREE,
    //                 "Levels mismatch: _levels=%hhu, _bufmgr.directory.size()=%lu", _levels, _bufmgr.GetHeight());

    //     CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
    //          "Insert END: Vector=%s, vec_id=" VECTORID_LOG_FMT ", _size=%lu, _levels=%hhu",
    //          vec.ToString(core_attr.dimension).ToCStr(), VECTORID_LOG(vec_id), _size, _levels);

    //     return rs;
    // }

    // RetStatus Delete(VectorID vec_id) override {
    //     FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Delete not implemented");
    //     UNUSED_VARIABLE(vec_id);
    //     RetStatus rs = RetStatus::Success();
    //     return rs;
    // }

    // RetStatus ApproximateKNearestNeighbours(const Vector& query, size_t k,
    //                                         uint16_t _internal_k, uint16_t _leaf_k,
    //                                         std::vector<std::pair<VectorID, DTYPE>>& neighbours,
    //                                         bool sort = true, bool sort_from_more_similar_to_less = true) override {

    //     FatalAssert(_root.IsValid(), LOG_TAG_DIVFTREE, "Invalid root ID.");
    //     FatalAssert(_root.IsCentroid(), LOG_TAG_DIVFTREE, "Invalid root ID -> root should be a centroid.");
    //     FatalAssert(query.IsValid(), LOG_TAG_DIVFTREE, "Invalid query vector.");
    //     FatalAssert(k > 0, LOG_TAG_DIVFTREE, "Number of neighbours cannot be 0.");
    //     FatalAssert(_levels > 1, LOG_TAG_DIVFTREE, "Height of the tree should be at least two but is %hhu.",
    //                 _levels);

    //     FatalAssert(_internal_k > 0, LOG_TAG_DIVFTREE, "Number of internal vertex neighbours cannot be 0.");
    //     FatalAssert(_leaf_k > 0, LOG_TAG_DIVFTREE, "Number of leaf neighbours cannot be 0.");

    //     CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
    //          "ApproximateKNearestNeighbours BEGIN: query=%s, k=%lu, _internal_k=%hu, _leaf_k=%hu, index_size=%lu, "
    //          "num_levels=%hhu", query.ToString(core_attr.dimension).ToCStr(), k, _internal_k,
    //          _leaf_k, _size, _levels);

    //     RetStatus rs = RetStatus::Success();
    //     neighbours.clear();

    //     std::vector<std::pair<VectorID, DTYPE>> upper_layer, lower_layer;
    //     upper_layer.emplace_back(_root, 0);
    //     VectorID next = _root;
    //     while (next.IsCentroid()) {
    //         CHECK_VECTORID_IS_VALID(next, LOG_TAG_DIVFTREE);
    //         // Get the next layer of vertices
    //         size_t search_n = (next.IsLeaf() ? k :
    //                                            (next._level - 1 == VectorID::LEAF_LEVEL ? _leaf_k : _internal_k));
    //         rs = SearchVertexs(query, upper_layer, lower_layer, search_n);
    //         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Search vertices failed with error: %s", rs.Msg());
    //         FatalAssert(!lower_layer.empty(), LOG_TAG_DIVFTREE, "Lower layer should not be empty.");
    //         FatalAssert(lower_layer.size() <= search_n,
    //                     LOG_TAG_DIVFTREE, "Lower layer size (%lu) is larger than %hu (%s).",
    //                     lower_layer.size(), search_n,
    //                     (next.IsLeaf() ? "vector case" : (next._level - 1 == VectorID::LEAF_LEVEL ?
    //                                                                     "leaf case" : "internal case")));
    //         FatalAssert(lower_layer.front().first._level == next._level - 1, LOG_TAG_DIVFTREE,
    //                     "Lower layer first element level (%hhu) does not match expected level (%hhu).",
    //                     lower_layer.front().first._level, next._level - 1);
    //         next = lower_layer.front().first;
    //         upper_layer.swap(lower_layer);
    //     }

    //     neighbours.swap(upper_layer);
    //     if (sort) {
    //         std::sort_heap(neighbours.begin(), neighbours.end(), _reverseSimilarityComparator);
    //         if (!sort_from_more_similar_to_less) {
    //             std::reverse(neighbours.begin(), neighbours.end());
    //         }
    //     }

    //     PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_DIVFTREE, "ApproximateKNearestNeighbours End: neighbours=");

    //     return rs;
    // }

    // size_t Size() const override {
    //     return _size;
    // }

    // inline DTYPE Distance(const Vector& a, const Vector& b) const override {
    //     switch (core_attr.distanceAlg)
    //     {
    //     case DistanceType::L2Distance:
    //         return L2::Distance(a, b, core_attr.dimension);
    //     default:
    //         CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE_VERTEX,
    //              "Distance: Invalid distance type: %s", DISTANCE_TYPE_NAME[core_attr.distanceAlg]);
    //     }
    //     return 0; // Return 0 if the distance type is invalid
    // }

    // inline size_t Bytes(bool is_internal_vertex) const override {
    //     return DIVFTreeVertex::Bytes(core_attr.dimension, is_internal_vertex ? internal_max_size : leaf_max_size);
    // }

    // inline String ToString() override {
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
    const DIVFTreeAttributes attr;

    // inline bool MoreSimilar(const DTYPE& a, const DTYPE& b) const {
    //     switch (core_attr.distanceAlg) {
    //     case DistanceType::L2Distance:
    //         return L2::MoreSimilar(a, b);
    //     default:
    //         CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
    //              "MoreSimilar: Invalid distance type: %s", DISTANCE_TYPE_NAME[core_attr.distanceAlg]);
    //     }
    //     return false; // Return false if the distance type is invalid
    // }

    RetStatus CompactAndInsert(BufferVertexEntry* container_entry, const VectorBatch& batch,
                               uint16_t marked_for_update = INVALID_OFFSET) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        FatalAssert(batch.size != 0, LOG_TAG_DIVFTREE, "Batch of vectors to insert is empty.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE, "Batch of vectors to insert is null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE, "Batch of vector IDs to insert is null.");
        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_EXCLUSIVE);

        DIVFTreeVertex* current = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(current, LOG_TAG_DIVFTREE);
        FatalAssert(current->GetAttributes().index == this, LOG_TAG_DIVFTREE,
                    "input entry does not belong to this index!");

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        DIVFTreeVertex* compacted =
            new(bufferMgr->AllocateMemoryForVertex(container_entry->centroidMeta.selfId._level))
            DIVFTreeVertex(current->GetAttributes());

        uint16_t size = current->cluster.header.reserved_size.load(std::memory_order_relaxed);
        const bool is_leaf = container_entry->centroidMeta.selfId.IsLeaf();
        const uint16_t dim = attr.dimension;
        const uint16_t cap = current->attr.cap;
        const uint16_t blckSize = current->attr.cap;
        /* Todo: this will not work if we have more than one block! */
        FatalAssert(current->cluster.NumBlocks(blckSize, cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "Currently cannot handle more than one block!");

        FatalAssert(!is_leaf || (batch.version != nullptr), LOG_TAG_DIVFTREE, "Batch of versions to insert is null!");

        Address srcMetaData = current->cluster.MetaData(0, is_leaf, blckSize, cap, dim);
        VTYPE* srcData = current->cluster.Data(0, is_leaf, blckSize, cap, dim);
        Address destMetaData = compacted->cluster.MetaData(0, is_leaf, blckSize, cap, dim);
        VTYPE* destData = compacted->cluster.Data(0, is_leaf, blckSize, cap, dim);
        uint16_t new_size = 0;
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
        FatalAssert(new_size == current->cluster.header.reserved_size.load(std::memory_order_relaxed) -
                                current->cluster.header.num_deleted.load(std::memory_order_relaxed),
                    LOG_TAG_DIVFTREE, "not all deletes went through!");
        FatalAssert(new_size + batch.size > new_size, LOG_TAG_DIVFTREE, "overflow detected!");
        FatalAssert(new_size + batch.size < cap, LOG_TAG_DIVFTREE, "Cluster cannot contain all of these vectors!");

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

        container_entry->UpdateClusterPtr(compacted);
        current = nullptr; // this is to make sure that we no longer access the current!

        for (uint16_t i = 0; i < old_reserved; ++i) {
            if (is_leaf) {
                VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                bufferMgr->UpdateVectorLocationOffset(vmd[i].id, i);
            } else {
                CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                bufferMgr->UpdateVectorLocationOffset(vmd[i].id, i);
            }
        }

        for (uint16_t i = old_reserved; i < new_size; ++i) {
            if (is_leaf) {
                VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                bufferMgr->UpdateVectorLocation(vmd[i].id,
                    VectorLocation(container_entry->centroidMeta.selfId, container_entry->currentVersion, new_size));
            } else {
                CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                bufferMgr->UpdateVectorLocation(vmd[i].id,
                    VectorLocation(container_entry->centroidMeta.selfId, container_entry->currentVersion, new_size));
            }
        }

        return RetStatus::Success();
    }

    inline void SimpleDivideClustering(const DIVFTreeVertex* base, const VectorBatch& batch,
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

            ComputeCentroid(clusters[i]->cluster.Data(0, is_leaf, base->attr.block_size, base->attr.cap, attr.dimension),
                            size, nullptr, 0, attr.dimension, attr.distanceAlg, &centroids.data[i * attr.dimension]);
        }

    }

    /*
     * will only fill in the raw centroid vectors to
     * the centroids batch and allocates memory for version and ids but does not fill them
     */
    inline void Clustering(const DIVFTreeVertex* base, const VectorBatch& batch,
                           BufferVertexEntry**& entries, DIVFTreeVertex**& clusters, VectorBatch& centroids,
                           uint16_t marked_for_update = INVALID_OFFSET) {
        switch (attr.clusteringAlg)
        {
        case ClusteringType::SimpleDivide:
            SimpleDivideClustering(base, batch, entries, clusters, centroids, marked_for_update);
        default:
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
                 "Cluster: Invalid clustering type: %hhu", (uint8_t)(attr.clusteringAlg));
        }
    }

    inline BufferVertexEntry* ExpandTree() {
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        BufferVertexEntry* new_root = bufferMgr->CreateNewRootEntry();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        (void)(new (new_root->ReadLatestVersion(false)) DIVFTreeVertex(attr, new_root->centroidMeta.selfId, this));
        new_root->DowngradeAccessToShared();
        return new_root;
    }

    RetStatus SplitAndInsert(BufferVertexEntry* container_entry, const VectorBatch& batch,
                             uint16_t marked_for_update = INVALID_OFFSET) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        FatalAssert(batch.size != 0, LOG_TAG_DIVFTREE, "Batch of vectors to insert is empty.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE, "Batch of vectors to insert is null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE, "Batch of vector IDs to insert is null.");
        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_EXCLUSIVE);

        RetStatus rs = RetStatus::Success();
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        DIVFTreeVertex* current = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(current, LOG_TAG_DIVFTREE);

        uint16_t size = current->cluster.header.reserved_size.load(std::memory_order_relaxed);
        const bool is_leaf = container_entry->centroidMeta.selfId.IsLeaf();
        const uint16_t dim = attr.dimension;
        const uint16_t cap = current->attr.cap;
        const uint16_t blckSize = current->attr.block_size;
        const uint8_t level = (uint8_t)(container_entry->centroidMeta.selfId._level);
        /* Todo: this will not work if we have more than one block! */
        FatalAssert(current->cluster.NumBlocks(blckSize, cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "Currently cannot handle more than one block!");

        FatalAssert(!is_leaf || (batch.version != nullptr), LOG_TAG_DIVFTREE, "Batch of versions to insert is null!");
        uint16_t total_size = current->cluster.header.reserved_size.load(std::memory_order_relaxed) -
                              current->cluster.header.num_deleted.load(std::memory_order_relaxed) +
                              batch.size;

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
        entries[0] = container_entry;

        BufferVertexEntry* parent = nullptr;
        while (true) {
            VectorLocation currentLocation = INVALID_VECTOR_LOCATION;
            parent = container_entry->ReadParent(currentLocation);
            if (parent == nullptr) {
                FatalAssert(currentLocation == INVALID_VECTOR_LOCATION, LOG_TAG_DIVFTREE,
                            "null parent with valid location!");
                FatalAssert(container_entry->centroidMeta.selfId == bufferMgr->GetCurrentRootId(), LOG_TAG_DIVFTREE,
                            "null parent but we are not root!");
                parent = ExpandTree();
            }

            CHECK_NOT_NULLPTR(parent, LOG_TAG_DIVFTREE);
            FatalAssert(currentLocation != INVALID_VECTOR_LOCATION, LOG_TAG_DIVFTREE,
                        "null parent with valid location!");
            rs = BatchInsertInto(parent, centroids, false, currentLocation.entryOffset);
            if (!rs.IsOK()) {
                bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                parent = nullptr;
                continue;
            }

            break;
        }

        threadSelf->SanityCheckLockHeldInModeByMe(&parent->clusterLock, SX_SHARED);

        container_entry->UpdateClusterPtr(clusters[0], centroids.version[0]);
        bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
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

        for (uint16_t i = centroids.size - 1; i != 0; --i) {
            bufferMgr->ReleaseBufferEntry(entries[i-1], ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});
        }
        bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});

        delete[] clusters;
        delete[] entries;
        delete[] centroids.data;
        delete[] centroids.id;
        delete[] centroids.version;

        return rs;
    }

    RetStatus BatchInsertInto(BufferVertexEntry* container_entry, const VectorBatch& batch, bool releaseEntry,
                              uint16_t marked_for_update = INVALID_OFFSET) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        FatalAssert(batch.size != 0, LOG_TAG_DIVFTREE, "Batch of vectors to insert is empty.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE, "Batch of vectors to insert is null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE, "Batch of vector IDs to insert is null.");
        RetStatus rs = RetStatus::Success();

        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_SHARED);
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
            rs = container_vertex->BatchInsert(batch);
            if (rs.IsOK()) {
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
                    CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "Invalid entry state!");
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
            FatalAssert(container_vertex->cluster.header.reserved_size.load(std::memory_order_relaxed) >
                        container_vertex->cluster.header.num_deleted.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "More vectors are deleted than inserted!");
            uint16_t reserved_size =
                container_vertex->cluster.header.reserved_size.load(std::memory_order_relaxed) + batch.size;
            uint16_t real_size =
                reserved_size - container_vertex->cluster.header.num_deleted.load(std::memory_order_relaxed);
            if ((float)real_size * COMPACTION_FACTOR <= (float)container_vertex->attr.cap) {
                rs = CompactAndInsert(container_entry, batch);
            } else if (reserved_size < container_vertex->attr.cap) {
                container_entry->DowngradeAccessToShared();
                continue;
            } else {
                rs = SplitAndInsert(container_entry, batch);
            }

            FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Major update failed!");
            if (releaseEntry) {
                BufferManager::GetInstance()->
                    ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});
            } else {
                container_entry->DowngradeAccessToShared();
            }

            return rs;
        }

        if (releaseEntry) {
            BufferManager::GetInstance()->
                ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
        }
        return rs;
    }

    inline void PruneIfNeededAndRelease(BufferVertexEntry* container_entry) {
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "not implemeted");
    }

    RetStatus DeleteVectorAndReleaseContainer(VectorID target, BufferVertexEntry* container_entry, VectorLocation loc) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(target, LOG_TAG_DIVFTREE);
        FatalAssert(target._level + 1 == container_entry->centroidMeta.selfId._level, LOG_TAG_DIVFTREE,
                    "target cannot be child of container");
        FatalAssert(loc.containerId == container_entry->centroidMeta.selfId, LOG_TAG_DIVFTREE, "Location Id mismatch");
        FatalAssert(container_entry->currentVersion == loc.containerVersion, LOG_TAG_DIVFTREE,
                    "Location version mismatch");
        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_SHARED);
        DIVFTreeVertex* container = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(container, LOG_TAG_DIVFTREE);
        Version version = 0;
        RetStatus rs = container->ChangeVectorState(target, loc.entryOffset, VECTOR_STATE_VALID, VECTOR_STATE_INVALID,
                                                    &version);
        FatalAssert(target.IsVector() || rs.IsOK(), LOG_TAG_DIVFTREE,
                    "state change will never fail if target is a vertex as it should be locked in X mode.");
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        if (rs.IsOK()) {
            bufferMgr->UpdateVectorLocation(target, INVALID_VECTOR_LOCATION);
            if (target.IsCentroid()) {
                bufferMgr->UnpinVertexVersion(target, version);
            }
            container->cluster.header.num_deleted.fetch_add(1);
            PruneIfNeededAndRelease(container_entry);
        } else {
            bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
        }

        return rs;
    }


    // RetStatus SearchVertexs(const Vector& query,
    //                       const std::vector<std::pair<VectorID, DTYPE>>& upper_layer,
    //                       std::vector<std::pair<VectorID, DTYPE>>& lower_layer, size_t n) override {
    //     FatalAssert(n > 0, LOG_TAG_DIVFTREE, "Number of vertices to search for should be greater than 0.");
    //     FatalAssert(!upper_layer.empty(), LOG_TAG_DIVFTREE, "Upper layer should not be empty.");
    //     FatalAssert(query.IsValid(), LOG_TAG_DIVFTREE, "Query vector is invalid.");

    //     CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
    //          "Search_Vertexs BEGIN: query=%s, n=%lu, upper_layer_size=%lu, searching_level=%hhu",
    //          query.ToString(core_attr.dimension).ToCStr(), n, upper_layer.size(), upper_layer.front().first._level);

    //     RetStatus rs = RetStatus::Success();
    //     lower_layer.clear();
    //     lower_layer.reserve(n);
    //     uint16_t level = upper_layer.front().first._level;
    //     for (const std::pair<VectorID, DTYPE>& vertex_data : upper_layer) {
    //         VectorID vertex_id = vertex_data.first;

    //         CHECK_VECTORID_IS_VALID(vertex_id, LOG_TAG_DIVFTREE);
    //         CHECK_VECTORID_IS_CENTROID(vertex_id, LOG_TAG_DIVFTREE);
    //         FatalAssert(vertex_id._level == level, LOG_TAG_DIVFTREE, "Vertex level mismatch: expected %hhu, got %hhu.",
    //                     level, vertex_id._level);

    //         DIVFTreeVertex* vertex = static_cast<DIVFTreeVertex*>(_bufmgr.GetVertex(vertex_id));
    //         CHECK_VERTEX_IS_VALID(vertex, LOG_TAG_DIVFTREE, false);

    //         if (vertex_id != _root) {
    //             CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
    //                 "Search_Vertexs: vertex_id=" VECTORID_LOG_FMT ", vertex_centroid=%s, vertex_type=%s",
    //                 VECTORID_LOG(vertex_id), _bufmgr.GetVector(vertex_id).ToString(core_attr.dimension).ToCStr(),
    //                 ((vertex->IsLeaf()) ? "Leaf" : "Internal"));
    //         }
    //         else {
    //             CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
    //                 "Search_Vertexs(root): vertex_id=" VECTORID_LOG_FMT ", vertex_type=%s",
    //                 VECTORID_LOG(vertex_id), ((vertex->IsLeaf()) ? "Leaf" : "Internal"));
    //         }

    //         rs = vertex->Search(query, n, lower_layer);
    //         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Search failed at vertex " VECTORID_LOG_FMT " with err(%s).",
    //                     VECTORID_LOG(vertex_id), rs.Msg());
    //     }

    //     CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
    //          "Search_Vertexs END: query=%s, n=%lu, upper_layer_size=%lu, searching_level=%hhu, "
    //          "lower_layer_size=%lu, lower_level=%hhu",
    //          query.ToString(core_attr.dimension).ToCStr(), n, upper_layer.size(),
    //          upper_layer.front().first._level, lower_layer.size(),
    //          lower_layer.front().first._level);

    //     return rs;
    // }

TESTABLE;
};

};

#endif