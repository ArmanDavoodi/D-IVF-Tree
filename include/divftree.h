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
    static inline size_t AlignedBytes(uint16_t dim, uint16_t max_size) {
        return ALLIGNED_SIZE(sizeof(DIVFTreeVertex)) + Cluster::AllignedDataBytes(dim, max_size);
    }

    /* adds padding to the end of cluster and packs vertex header with  */
    static inline DIVFTreeVertex* CreateNewVertex(DIVFTreeVertexAttributes attr) {
        DIVFTreeVertex* vertex = static_cast<DIVFTreeVertex*>(
                                    std::aligned_alloc(CACHE_LINE_SIZE,
                                                       AlignedBytes(attr.core.dimension, attr.max_size)));
        FatalAssert(vertex != nullptr, LOG_TAG_TEST, "Failed to allocate memory for DIVFTreeVertex.");
        new (vertex) DIVFTreeVertex(attr);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_TEST, "Created Vertex: %s",
             vertex->ToString().ToCStr());
        return vertex;
    }

    ~DIVFTreeVertex() override = default;

    /* should probably hold the vertex lock in shared mode */
    RetStatus AssignParent(VectorID parent_id) override {
        CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
        FatalAssert(parent_id._level == _cluster._centroid_id._level + 1, LOG_TAG_DIVFTREE_VERTEX,
                    "Level mismatch between parent and self. parent=" VECTORID_LOG_FMT
                    " self=%s.", VECTORID_LOG(parent_id), ToString().ToCStr());

        _cluster._parent_id = parent_id;
        return RetStatus::Success();
    }

    RetStatus BatchUpdate(BatchVertexUpdate& updates) override {
        CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
        FatalAssert(updates.target_cluster == _cluster._centroid_id, LOG_TAG_DIVFTREE_VERTEX,
                    "Target cluster ID does not match vertex centroid ID. target=%s, centroid=%s",
                    updates.target_cluster.ToString().ToCStr(), _cluster._centroid_id.ToString().ToCStr());
        RetStatus rs = RetStatus::Success();
        /* At this point this vertex should already pinned by the called/buffer */
        if (_lock.LockWithBlockCheck(SX_SHARED)) {
            _lock.Unlock();
            return RetStatus{.stat = RetStatus::VERTEX_UPDATED};
        }
        /* todo: handle urgent flag */
        /* first apply all moves and deletes */
        uint16_t filled_size = _cluster.FilledSize();
        ClusterEntry* entry = _cluster._entry;
        VectorState state;
        /* Todo add some asserts */
        for (uint16_t i = 0; i < filled_size; ++i) {
            if (!entry[i].IsValid()) {
                continue;
            }
            std::map<divftree::VectorID, divftree::BatchVertexUpdateEntry>::iterator it;
            it = updates.updates[VERTEX_DELETE].find(entry[i].id);
            if (it != updates.updates[VERTEX_DELETE].end()) {
                /* this might happen due to lock-free synchronization between minor updates */
                if (!entry[i].Delete(state)) {
                    CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                         "Failed to delete vector with ID " VECTORID_LOG_FMT " and state %s in vertex %s.",
                         VECTORID_LOG(entry[i].id), state.ToString().ToCStr(), ToString().ToCStr());
                    it->second.result->stat = RetStatus::VECTOR_IS_INVALID;
                }
                else {
                    it->second.result->stat = RetStatus::SUCCESS;
                }
                updates.updates[VERTEX_DELETE].erase(it);
                continue;
            }

            it = updates.updates[VERTEX_MIGRATE].find(entry[i].id);
            if (it != updates.updates[VERTEX_MIGRATE].end()) {
                /* this might happen due to lock-free synchronization between minor updates */
                if(!entry[i].Move(state)) {
                    if (!state.IsValid()) {
                        CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                            "Failed to migrate invalid vector with ID " VECTORID_LOG_FMT " in vertex %s.",
                            VECTORID_LOG(entry[i].id), ToString().ToCStr());
                        it->second.result->stat = RetStatus::VECTOR_IS_INVALID;
                    }
                    else {
                        FatalAssert(state.IsMoving(), LOG_TAG_DIVFTREE,
                                    "If migration fails for valid vector, it has to be already migrated!");
                        CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                            "Failed to migrate moved vector with ID " VECTORID_LOG_FMT " in vertex %s.",
                            VECTORID_LOG(entry[i].id), ToString().ToCStr());
                        it->second.result->stat = RetStatus::VECTOR_IS_MIGRATED;
                    }
                }
                else {
                    it->second.result->stat = RetStatus::SUCCESS;
                }
                updates.updates[VERTEX_MIGRATE].erase(it);
                continue;
            }

            it = updates.updates[VERTEX_INPLACE_UPDATE].find(entry[i].id);
            /* Todo this should not happen when we do not have an exclusive lock */
            if (it != updates.updates[VERTEX_INPLACE_UPDATE].end()) {
                FatalAssert(*it->second.inplace_update_args.vector_data != nullptr, LOG_TAG_DIVFTREE,
                            "Inplace update vector data cannot be null.");
                memcpy(entry[i].vector, it->second.inplace_update_args.vector_data,
                       _cluster._dimension * sizeof(VTYPE));
                if (!entry[i].IsVisible()) {
                    CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                         "Inplace update for vector with ID " VECTORID_LOG_FMT " in vertex %s is not visible.",
                         VECTORID_LOG(entry[i].id), ToString().ToCStr());
                    it->second.result->stat = RetStatus::VECTOR_IS_INVISIBLE;
                }
                else {
                    it->second.result->stat = RetStatus::SUCCESS;
                }
            }
        }

        /* check if there are any non insertion updates missing */
        for (auto& update_pair : updates.updates[VERTEX_DELETE]) {
            CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                 "Batch update contains delete operation for vector with ID " VECTORID_LOG_FMT
                 " but it was not found in the vertex %s.",
                 VECTORID_LOG(update_pair.first), ToString().ToCStr());
            update_pair.second.result->stat = RetStatus::VECTOR_NOT_FOUND;
        }
        for (auto& update_pair : updates.updates[VERTEX_MIGRATE]) {
            CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                 "Batch update contains migrate operation for vector with ID " VECTORID_LOG_FMT
                 " but it was not found in the vertex %s.",
                 VECTORID_LOG(update_pair.first), ToString().ToCStr());
            update_pair.second.result->stat = RetStatus::VECTOR_NOT_FOUND;
        }
        for (auto& update_pair : updates.updates[VERTEX_INPLACE_UPDATE]) {
            CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                 "Batch update contains inplace update operation for vector with ID " VECTORID_LOG_FMT
                 " but it was not found in the vertex %s.",
                 VECTORID_LOG(update_pair.first), ToString().ToCStr());
            update_pair.second.result->stat = RetStatus::VECTOR_NOT_FOUND;
        }

        updates.updates[VERTEX_DELETE].clear();
        updates.updates[VERTEX_MIGRATE].clear();
        updates.updates[VERTEX_INPLACE_UPDATE].clear();

        /* now apply all inserts */
        rs = _cluster.BatchInsert(updates.updates[VERTEX_INSERT]);
        if (rs.stat == RetStatus::VERTEX_NOT_ENOUGH_SPACE) {
            /* compete for a flag or something and if you get it you have to split */
        }
        else if (rs.stat != RetStatus::SUCCESS) {
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
                 "Batch update failed with status: %s in vertex %s.",
                 rs.Msg(), ToString().ToCStr());
        }
        /* todo return success? */
    }

    // uint16_t BatchInsert(const void** vector_data, const VectorID* vec_id,
    //                      uint16_t num_vectors, bool& need_split, Address& offset) override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     CHECK_VECTORID_IS_VALID(vec_id, LOG_TAG_DIVFTREE_VERTEX);
    //     FatalAssert(vec_id._level == _cluster._centroid_id._level - 1, LOG_TAG_DIVFTREE_VERTEX, "Level mismatch! "
    //         "input vector: (id: %lu, level: %lu), centroid vector: (id: %lu, level: %lu)"
    //         , vec_id._id, vec_id._level, _cluster._centroid_id._id, _cluster._centroid_id._level);
    //     FatalAssert(vector_data != nullptr, LOG_TAG_DIVFTREE_VERTEX, "Cannot insert null vector into the bucket with id %lu.",
    //                 vec_id._id, _cluster._centroid_id._id);

    //     uint16_t num_reserved = _cluster.BatchInsert(vector_data, &vec_id, num_vectors,
    //                                                  need_split, (ClusterEntry*)offset);

    //     CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX,
    //          "BatchInsert: NumInserted=%hhu, NumRemaining=%hhu, NeedSplit=%s, InsertedOffset=%hhu, Cluster=%s",
    //          num_reserved, num_vectors - num_reserved, (need_split ? "T" : "F"), _cluster._max_size - num_reserved,
    //          _cluster.ToString().ToCStr());
    //     return num_reserved;
    // }

    // VectorUpdate MigrateLastVectorTo(DIVFTreeVertexInterface* _dest) override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, true);
    //     CHECK_VERTEX_IS_VALID(_dest, LOG_TAG_DIVFTREE_VERTEX, false);
    //     FatalAssert(_dest->Level() == _centroid_id._level, LOG_TAG_DIVFTREE_VERTEX, "Level mismatch! "
    //                "_dest = (id: %lu, level: %lu), self = (id: %lu, level: %lu)",
    //                _dest->CentroidID()._id, _dest->CentroidID()._level, _centroid_id._id, _centroid_id._level);

    //     VectorUpdate update;
    //     update.vector_id = _cluster.GetLastVectorID();
    //     update.vector_data = _dest->Insert(_cluster.GetLastVector(), update.vector_id);
    //     FatalAssert(update.IsValid(), LOG_TAG_DIVFTREE_VERTEX, "Invalid Update. ID="
    //                 VECTORID_LOG_FMT, VECTORID_LOG(_centroid_id));
    //     _cluster.DeleteLast();
    //     return update;
    // }

    // RetStatus Search(const Vector& query, size_t k,
    //                  std::vector<std::pair<VectorID, DTYPE>>& neighbours) override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     FatalAssert(k > 0, LOG_TAG_DIVFTREE_VERTEX, "Number of neighbours should not be 0");
    //     FatalAssert(neighbours.size() <= k, LOG_TAG_DIVFTREE_VERTEX,
    //                 "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

    //     CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX,
    //              "Search: Cluster=%s", _cluster.ToString().ToCStr());
    //     PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_DIVFTREE_VERTEX, "Search: Neighbours before search");

    //     for (uint16_t i = 0; i < _cluster.Size(); ++i) {
    //         const VectorPair& vectorPair = _cluster[i];
    //         DTYPE distance = Distance(query, vectorPair.vec);
    //         neighbours.emplace_back(vectorPair.id, distance);
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
    //     return _centroid_id;
    // }

    // VectorID ParentID() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _parent_id;
    // }

    // uint16_t Size() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _cluster.Size();
    // }

    // bool IsFull() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _cluster.Size() == _cluster.Capacity();
    // }

    // bool IsAlmostEmpty() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _cluster.Size() == _min_size;
    // }

    // bool Contains(VectorID id) const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _cluster.Contains(id);
    // }

    // bool IsLeaf() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _centroid_id.IsLeaf();
    // }

    // uint8_t Level() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _centroid_id._level;
    // }

    // Vector ComputeCurrentCentroid() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, true);
    //     return ComputeCentroid(static_cast<const VTYPE*>(_cluster.GetVectors()),
    //                                    _cluster.Size(), _cluster.Dimension(), _distanceAlg);
    // }

    // inline uint16_t MinSize() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _min_size;
    // }
    // inline uint16_t MaxSize() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _cluster.Capacity();
    // }
    // inline uint16_t VectorDimension() const override {
    //     CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
    //     return _cluster.Dimension();
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

    String ToString(bool detailed = false) const override {
        return String("{clustering algorithm=%s, distance=%s, centroid_copy=%s, LockState=%s, Cluster=",
                      CLUSTERING_TYPE_NAME[_clusteringAlg], DISTANCE_TYPE_NAME[_distanceAlg],
                      _centroid_copy.ToString(_cluster._dim).ToCStr(), lock.ToString().ToCStr()) +
               _cluster.ToString(detailed) + String("}");
    }

    // static inline size_t Bytes(uint16_t dim, uint16_t capacity) {
    //     return sizeof(DIVFTreeVertex) + Cluster::DataBytes(dim, capacity);
    // }

protected:

    DIVFTreeVertex(DIVFTreeVertexAttributes attr) : _clusteringAlg(attr.core.clusteringAlg),
                                                    _distanceAlg(attr.core.distanceAlg),
                                                    _similarityComparator(attr.similarityComparator),
                                                    _reverseSimilarityComparator(attr.reverseSimilarityComparator),
                                                    _centroid_copy(attr.centroid_copy, attr.core.dimension) {
        CHECK_VERTEX_ATTRIBUTES(attr, LOG_TAG_DIVFTREE_VERTEX);

        new (&_cluster) Cluster(sizeof(DIVFTreeVertex), attr.version, attr.centroid_id, attr.min_size,
                                attr.max_size, attr.core.dimension, attr.cluster_owner,
                                NUM_COMPUTE_NODES);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_TEST, "Created Vertex: this=%p, cluster=%s",
             this, _cluster.ToString().ToCStr());
    }


    const ClusteringType _clusteringAlg;
    const DistanceType _distanceAlg;
    const VPairComparator _similarityComparator;
    const VPairComparator _reverseSimilarityComparator;
    const Vector _centroid_copy;
    SXLock _lock;
    CondVar _cond_var;
    Cluster _cluster;

TESTABLE;
friend class DIVFTree;
};

// class DIVFTree : public DIVFTreeInterface {
// public:
//     DIVFTree(DIVFTreeAttributes attr) : core_attr(attr.core), leaf_min_size(attr.leaf_min_size),
//                                          leaf_max_size(attr.leaf_max_size),
//                                          internal_min_size(attr.internal_min_size),
//                                          internal_max_size(attr.internal_max_size),
//                                          split_internal(attr.split_internal), split_leaf(attr.split_leaf),
//                                          _similarityComparator(
//                                             GetDistancePairSimilarityComparator(attr.core.distanceAlg, false)),
//                                          _reverseSimilarityComparator(
//                                             GetDistancePairSimilarityComparator(attr.core.distanceAlg, true)),
//                                          _size(0), _root(INVALID_VECTOR_ID), _levels(2) {
//         RetStatus rs = RetStatus::Success();
//         rs = _bufmgr.Init();
//         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Failed to init buffer manager.");
//         _root = _bufmgr.RecordRoot();
//         FatalAssert(_root.IsValid(), LOG_TAG_DIVFTREE, "Invalid root ID: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
//         FatalAssert(_root.IsCentroid(), LOG_TAG_DIVFTREE, "root should be a centroid: "
//                     VECTORID_LOG_FMT, VECTORID_LOG(_root));
//         FatalAssert(_root.IsLeaf(), LOG_TAG_DIVFTREE, "first root should be a leaf: "
//                     VECTORID_LOG_FMT, VECTORID_LOG(_root));

//         rs = _bufmgr.UpdateClusterAddress(_root, CreateNewVertex(_root));
//         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Failed to update cluster address for root: "
//                     VECTORID_LOG_FMT, VECTORID_LOG(_root));
//         _levels = 2;
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE,
//              "Init DIVFTree Index End: rootID= " VECTORID_LOG_FMT ", _levels = %hhu, attr=%s",
//              VECTORID_LOG(_root), _levels, attr.ToString().ToCStr());
//     }

//     ~DIVFTree() override {
//         RetStatus rs = RetStatus::Success();
//         rs = _bufmgr.Shutdown();
//         CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Shutdown DIVFTree Index End: rs=%s", rs.Msg());
//     }

//     RetStatus Insert(const Vector& vec, VectorID& vec_id, uint16_t vertex_per_layer) override {
//         FatalAssert(_root.IsValid(), LOG_TAG_DIVFTREE, "Invalid root ID.");
//         FatalAssert(_root.IsCentroid(), LOG_TAG_DIVFTREE, "Invalid root ID -> root should be a centroid.");
//         FatalAssert(vec.IsValid(), LOG_TAG_DIVFTREE, "Invalid query vector.");

//         FatalAssert(vertex_per_layer > 0, LOG_TAG_DIVFTREE, "vertex_per_layer cannot be 0.");
//         FatalAssert(_levels > 1, LOG_TAG_DIVFTREE, "Height of the tree should be at least two but is %hhu.",
//                     _levels);

//         RetStatus rs = RetStatus::Success();

//         CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//              "Insert BEGIN: Vector=%s, _size=%lu, _levels=%hhu",
//              vec.ToString(core_attr.dimension).ToCStr(), _size, _levels);

//         std::vector<std::pair<VectorID, DTYPE>> upper_layer, lower_layer;
//         upper_layer.emplace_back(_root, 0);
//         VectorID next = _root;
//         while (next.IsInternalVertex()) {
//             CHECK_VECTORID_IS_VALID(next, LOG_TAG_DIVFTREE);
//             // Get the next layer of vertices
//             rs = SearchVertexs(vec, upper_layer, lower_layer,
//                              (next._level - 1 == VectorID::LEAF_LEVEL) ? 1 : vertex_per_layer);
//             FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Search vertices failed with error: %s", rs.Msg());
//             FatalAssert(!lower_layer.empty(), LOG_TAG_DIVFTREE, "Lower layer should not be empty.");
//             FatalAssert((next.IsInternalVertex() ? (lower_layer.size() <= vertex_per_layer) : (lower_layer.size() == 1)),
//                         LOG_TAG_DIVFTREE, "Lower layer size (%lu) is larger than %hu (%s).",
//                         lower_layer.size(), (next.IsInternalVertex() ? vertex_per_layer : 1),
//                         (next.IsInternalVertex() ? "internal k" : "leaf case"));
//             FatalAssert(lower_layer.front().first._level == next._level - 1, LOG_TAG_DIVFTREE,
//                         "Lower layer first element level (%hhu) does not match expected level (%hhu).",
//                         lower_layer.front().first._level, next._level - 1);
//             next = lower_layer.front().first;
//             upper_layer.swap(lower_layer);
//         }

//         DIVFTreeVertex* leaf = static_cast<DIVFTreeVertex*>(_bufmgr.GetVertex(upper_layer.front().first));
//         CHECK_VERTEX_IS_VALID(leaf, LOG_TAG_DIVFTREE, false);
//         FatalAssert(leaf->IsLeaf(), LOG_TAG_DIVFTREE, "Next vertex should be a leaf but is " VECTORID_LOG_FMT,
//                     VECTORID_LOG(next));

//         if (leaf->IsFull()) {
//             CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "Leaf is full.");
//         }

//         vec_id = RecordInto(vec, leaf);
//         if (vec_id.IsValid()) {
//             ++_size;
//             if (leaf->IsFull()) {
//                 Split(leaf); // todo background job?
//                 // todo assert success
//             }
//         }
//         else {
//             rs = RetStatus::Fail(""); //todo
//         }

//         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Insert failed with error: %s", rs.Msg());
//         FatalAssert(_levels == _bufmgr.GetHeight(), LOG_TAG_DIVFTREE,
//                     "Levels mismatch: _levels=%hhu, _bufmgr.directory.size()=%lu", _levels, _bufmgr.GetHeight());

//         CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//              "Insert END: Vector=%s, vec_id=" VECTORID_LOG_FMT ", _size=%lu, _levels=%hhu",
//              vec.ToString(core_attr.dimension).ToCStr(), VECTORID_LOG(vec_id), _size, _levels);

//         return rs;
//     }

//     RetStatus Delete(VectorID vec_id) override {
//         FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Delete not implemented");
//         UNUSED_VARIABLE(vec_id);
//         RetStatus rs = RetStatus::Success();
//         return rs;
//     }

//     RetStatus ApproximateKNearestNeighbours(const Vector& query, size_t k,
//                                             uint16_t _internal_k, uint16_t _leaf_k,
//                                             std::vector<std::pair<VectorID, DTYPE>>& neighbours,
//                                             bool sort = true, bool sort_from_more_similar_to_less = true) override {

//         FatalAssert(_root.IsValid(), LOG_TAG_DIVFTREE, "Invalid root ID.");
//         FatalAssert(_root.IsCentroid(), LOG_TAG_DIVFTREE, "Invalid root ID -> root should be a centroid.");
//         FatalAssert(query.IsValid(), LOG_TAG_DIVFTREE, "Invalid query vector.");
//         FatalAssert(k > 0, LOG_TAG_DIVFTREE, "Number of neighbours cannot be 0.");
//         FatalAssert(_levels > 1, LOG_TAG_DIVFTREE, "Height of the tree should be at least two but is %hhu.",
//                     _levels);

//         FatalAssert(_internal_k > 0, LOG_TAG_DIVFTREE, "Number of internal vertex neighbours cannot be 0.");
//         FatalAssert(_leaf_k > 0, LOG_TAG_DIVFTREE, "Number of leaf neighbours cannot be 0.");

//         CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//              "ApproximateKNearestNeighbours BEGIN: query=%s, k=%lu, _internal_k=%hu, _leaf_k=%hu, index_size=%lu, "
//              "num_levels=%hhu", query.ToString(core_attr.dimension).ToCStr(), k, _internal_k,
//              _leaf_k, _size, _levels);

//         RetStatus rs = RetStatus::Success();
//         neighbours.clear();

//         std::vector<std::pair<VectorID, DTYPE>> upper_layer, lower_layer;
//         upper_layer.emplace_back(_root, 0);
//         VectorID next = _root;
//         while (next.IsCentroid()) {
//             CHECK_VECTORID_IS_VALID(next, LOG_TAG_DIVFTREE);
//             // Get the next layer of vertices
//             size_t search_n = (next.IsLeaf() ? k :
//                                                (next._level - 1 == VectorID::LEAF_LEVEL ? _leaf_k : _internal_k));
//             rs = SearchVertexs(query, upper_layer, lower_layer, search_n);
//             FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Search vertices failed with error: %s", rs.Msg());
//             FatalAssert(!lower_layer.empty(), LOG_TAG_DIVFTREE, "Lower layer should not be empty.");
//             FatalAssert(lower_layer.size() <= search_n,
//                         LOG_TAG_DIVFTREE, "Lower layer size (%lu) is larger than %hu (%s).",
//                         lower_layer.size(), search_n,
//                         (next.IsLeaf() ? "vector case" : (next._level - 1 == VectorID::LEAF_LEVEL ?
//                                                                         "leaf case" : "internal case")));
//             FatalAssert(lower_layer.front().first._level == next._level - 1, LOG_TAG_DIVFTREE,
//                         "Lower layer first element level (%hhu) does not match expected level (%hhu).",
//                         lower_layer.front().first._level, next._level - 1);
//             next = lower_layer.front().first;
//             upper_layer.swap(lower_layer);
//         }

//         neighbours.swap(upper_layer);
//         if (sort) {
//             std::sort_heap(neighbours.begin(), neighbours.end(), _reverseSimilarityComparator);
//             if (!sort_from_more_similar_to_less) {
//                 std::reverse(neighbours.begin(), neighbours.end());
//             }
//         }

//         PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_DIVFTREE, "ApproximateKNearestNeighbours End: neighbours=");

//         return rs;
//     }

//     size_t Size() const override {
//         return _size;
//     }

//     inline DTYPE Distance(const Vector& a, const Vector& b) const override {
//         switch (core_attr.distanceAlg)
//         {
//         case DistanceType::L2Distance:
//             return L2::Distance(a, b, core_attr.dimension);
//         default:
//             CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE_VERTEX,
//                  "Distance: Invalid distance type: %s", DISTANCE_TYPE_NAME[core_attr.distanceAlg]);
//         }
//         return 0; // Return 0 if the distance type is invalid
//     }

//     inline size_t Bytes(bool is_internal_vertex) const override {
//         return DIVFTreeVertex::Bytes(core_attr.dimension, is_internal_vertex ? internal_max_size : leaf_max_size);
//     }

//     inline String ToString() override {
//         String out("{Attr=<Dim=%hu, ClusteringAlg=%s, DistanceAlg=%s, "
//                    "LeafMinSize=%hu, LeafMaxSize=%hu, InternalMinSize=%hu, InternalMaxSize=%hu, "
//                    "SplitInternal=%hu, SplitLeaf=%hu, Size=%lu, Levels=%lu>, RootID=" VECTORID_LOG_FMT
//                    ", Vertexs=[", core_attr.dimension, CLUSTERING_TYPE_NAME[core_attr.clusteringAlg],
//                    DISTANCE_TYPE_NAME[core_attr.distanceAlg], leaf_min_size, leaf_max_size,
//                    internal_min_size, internal_max_size, split_internal, split_leaf,
//                    _size, _levels, VECTORID_LOG(_root));
//         std::vector<VectorID> curr_level_stack, next_level_stack;
//         size_t seen = 0;
//         if (_root.IsValid()) {
//             curr_level_stack.push_back(_root);
//         }
//         bool go_to_next_level = true;
//         uint64_t curr_level = 0;
//         while (!curr_level_stack.empty()) {
//             VectorID curr_vertexId = curr_level_stack.back();
//             curr_level_stack.pop_back();
//             CHECK_VECTORID_IS_VALID(curr_vertexId, LOG_TAG_DIVFTREE);
//             if (go_to_next_level) {
//                 curr_level = curr_vertexId._level;
//                 out += String("Level %lu:{Size=%lu, Vertexs=[", curr_level, curr_level_stack.size() + 1);
//                 go_to_next_level = false;
//             }
//             else {
//                 FatalAssert(curr_vertexId._level == curr_level, LOG_TAG_DIVFTREE,
//                             "Current vertex level (%hhu) does not match expected level (%lu).",
//                             curr_vertexId._level, curr_level);
//             }
//             DIVFTreeVertex* vertex = static_cast<DIVFTreeVertex*>(_bufmgr.GetVertex(curr_vertexId));
//             CHECK_VERTEX_IS_VALID(vertex, LOG_TAG_DIVFTREE, false);
//             out += String("<VertexID=" VECTORID_LOG_FMT ", Bucket=%s>",
//                         VECTORID_LOG(curr_vertexId), vertex->BucketToString().ToCStr());
//             if (!vertex->IsLeaf()) {
//                 for (uint16_t i = 0; i < vertex->Size(); ++i) {
//                     const VectorPair& vectorPair = vertex->_cluster[i];
//                     CHECK_VECTORID_IS_VALID(vectorPair.id, LOG_TAG_DIVFTREE);
//                     FatalAssert(vectorPair.id._level == curr_vertexId._level - 1, LOG_TAG_DIVFTREE,
//                                 "VectorID level (%hhu) does not match current vertex level (%hhu).",
//                                 vectorPair.id._level, curr_vertexId._level - 1);
//                     FatalAssert(vectorPair.vec.IsValid(), LOG_TAG_DIVFTREE,
//                                 "Invalid vector in vertex %s at index %hu.", VECTORID_LOG(curr_vertexId), i);
//                     next_level_stack.emplace_back(vectorPair.id);
//                 }
//             }
//             else {
//                 seen += vertex->Size();
//             }
//             if (curr_level_stack.empty()) {
//                 out += String("]}");
//                 go_to_next_level = true;
//                 next_level_stack.swap(curr_level_stack);
//                 if (!curr_level_stack.empty()) {
//                     out += String(", ");
//                 }
//                 else {
//                     out += String("]}");
//                 }
//             }
//             else {
//                 out += String(", ");
//             }
//         }
//         FatalAssert(seen == _size, LOG_TAG_DIVFTREE,
//                     "Seen vertices (%lu) does not match expected size (%lu).", seen, _size);
//         return out;
//     }

// protected:
//     const DIVFTreeCoreAttributes core_attr;
//     const uint16_t leaf_min_size;
//     const uint16_t leaf_max_size;
//     const uint16_t internal_min_size;
//     const uint16_t internal_max_size;
//     const uint16_t split_internal;
//     const uint16_t split_leaf;
//     const VPairComparator _similarityComparator;
//     const VPairComparator _reverseSimilarityComparator;
//     size_t _size;
//     BufferManager _bufmgr;
//     VectorID _root;
//     uint64_t _levels;

//     inline bool MoreSimilar(const DTYPE& a, const DTYPE& b) const {
//         switch (core_attr.distanceAlg) {
//         case DistanceType::L2Distance:
//             return L2::MoreSimilar(a, b);
//         default:
//             CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
//                  "MoreSimilar: Invalid distance type: %s", DISTANCE_TYPE_NAME[core_attr.distanceAlg]);
//         }
//         return false; // Return false if the distance type is invalid
//     }

//     RetStatus SearchVertexs(const Vector& query,
//                           const std::vector<std::pair<VectorID, DTYPE>>& upper_layer,
//                           std::vector<std::pair<VectorID, DTYPE>>& lower_layer, size_t n) override {
//         FatalAssert(n > 0, LOG_TAG_DIVFTREE, "Number of vertices to search for should be greater than 0.");
//         FatalAssert(!upper_layer.empty(), LOG_TAG_DIVFTREE, "Upper layer should not be empty.");
//         FatalAssert(query.IsValid(), LOG_TAG_DIVFTREE, "Query vector is invalid.");

//         CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//              "Search_Vertexs BEGIN: query=%s, n=%lu, upper_layer_size=%lu, searching_level=%hhu",
//              query.ToString(core_attr.dimension).ToCStr(), n, upper_layer.size(), upper_layer.front().first._level);

//         RetStatus rs = RetStatus::Success();
//         lower_layer.clear();
//         lower_layer.reserve(n);
//         uint16_t level = upper_layer.front().first._level;
//         for (const std::pair<VectorID, DTYPE>& vertex_data : upper_layer) {
//             VectorID vertex_id = vertex_data.first;

//             CHECK_VECTORID_IS_VALID(vertex_id, LOG_TAG_DIVFTREE);
//             CHECK_VECTORID_IS_CENTROID(vertex_id, LOG_TAG_DIVFTREE);
//             FatalAssert(vertex_id._level == level, LOG_TAG_DIVFTREE, "Vertex level mismatch: expected %hhu, got %hhu.",
//                         level, vertex_id._level);

//             DIVFTreeVertex* vertex = static_cast<DIVFTreeVertex*>(_bufmgr.GetVertex(vertex_id));
//             CHECK_VERTEX_IS_VALID(vertex, LOG_TAG_DIVFTREE, false);

//             if (vertex_id != _root) {
//                 CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//                     "Search_Vertexs: vertex_id=" VECTORID_LOG_FMT ", vertex_centroid=%s, vertex_type=%s",
//                     VECTORID_LOG(vertex_id), _bufmgr.GetVector(vertex_id).ToString(core_attr.dimension).ToCStr(),
//                     ((vertex->IsLeaf()) ? "Leaf" : "Internal"));
//             }
//             else {
//                 CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//                     "Search_Vertexs(root): vertex_id=" VECTORID_LOG_FMT ", vertex_type=%s",
//                     VECTORID_LOG(vertex_id), ((vertex->IsLeaf()) ? "Leaf" : "Internal"));
//             }

//             rs = vertex->Search(query, n, lower_layer);
//             FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Search failed at vertex " VECTORID_LOG_FMT " with err(%s).",
//                         VECTORID_LOG(vertex_id), rs.Msg());
//         }

//         CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//              "Search_Vertexs END: query=%s, n=%lu, upper_layer_size=%lu, searching_level=%hhu, "
//              "lower_layer_size=%lu, lower_level=%hhu",
//              query.ToString(core_attr.dimension).ToCStr(), n, upper_layer.size(),
//              upper_layer.front().first._level, lower_layer.size(),
//              lower_layer.front().first._level);

//         return rs;
//     }

//     VectorID RecordInto(const Vector& vec, DIVFTreeVertexInterface* container_vertex,
//                         DIVFTreeVertexInterface* vertex = nullptr) override {
//         CHECK_VERTEX_IS_VALID(container_vertex, LOG_TAG_DIVFTREE, false);
//         FatalAssert(vertex == nullptr ||
//             (vertex->CentroidID().IsValid() && vertex->ParentID() == INVALID_VECTOR_ID),
//             LOG_TAG_DIVFTREE, "Input vertex should not have a parent assigned to it.");
//         FatalAssert(vec.IsValid(), LOG_TAG_DIVFTREE, "Invalid vector.");

//         RetStatus rs = RetStatus::Success();

//         VectorID vector_id = INVALID_VECTOR_ID;
//         if (vertex != nullptr) {
//             vector_id = vertex->CentroidID();
//             rs = vertex->AssignParent(container_vertex->CentroidID());
//             FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Failed tp assign parent_id");
//         }
//         else {
//             vector_id = _bufmgr.RecordVector(container_vertex->Level() - 1);
//             CHECK_VECTORID_IS_VALID(vector_id, LOG_TAG_DIVFTREE);
//         }

//         Address vec_add = container_vertex->Insert(vec, vector_id);
//         FatalAssert(vec_add != INVALID_ADDRESS, LOG_TAG_DIVFTREE, "Failed to insert vector into parent.");
//         rs = _bufmgr.UpdateVectorAddress(vector_id, vec_add);
//         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Failed to update vector address for vector_id: "
//                     VECTORID_LOG_FMT, VECTORID_LOG(vector_id));
//         return vector_id;
//     }

//     RetStatus ExpandTree(DIVFTreeVertexInterface* root, const Vector& centroid) override {
//         CHECK_VERTEX_IS_VALID(root, LOG_TAG_DIVFTREE, false);
//         FatalAssert(root->CentroidID() == _root, LOG_TAG_DIVFTREE, "root should be the current root: input_root_id="
//                     VECTORID_LOG_FMT "current root id=" VECTORID_LOG_FMT,
//                     VECTORID_LOG(root->CentroidID()), VECTORID_LOG(_root));
//         RetStatus rs = RetStatus::Success();
//         VectorID new_root_id = _bufmgr.RecordRoot();
//         DIVFTreeVertex* new_root = static_cast<DIVFTreeVertex*>(CreateNewVertex(new_root_id));
//         rs = _bufmgr.UpdateClusterAddress(new_root_id, new_root);
//         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Failed to update cluster address for new_root_id: "
//                     VECTORID_LOG_FMT, VECTORID_LOG(new_root_id));
//         rs = _bufmgr.UpdateVectorAddress(_root, new_root->Insert(centroid, _root));
//         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Failed to update vector address for new_root_id: "
//                     VECTORID_LOG_FMT, VECTORID_LOG(new_root_id));
//         _root = new_root_id;
//         rs = root->AssignParent(new_root_id);
//         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Failed to AssignParent for new_root. new_root_id="
//                     VECTORID_LOG_FMT ", old_root_id=" VECTORID_LOG_FMT,
//                     VECTORID_LOG(new_root_id), VECTORID_LOG(root->CentroidID()));
//         ++_levels;
//         CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//              "Expand_Tree: New root created with id=" VECTORID_LOG_FMT ", previous root id= " VECTORID_LOG_FMT,
//              VECTORID_LOG(new_root_id), VECTORID_LOG(root->CentroidID()));
//         // todo assert success of every operation
//         return rs;
//     }

//     size_t FindClosestCluster(const std::vector<DIVFTreeVertexInterface*>& candidates, const Vector& vec) override {
//         FatalAssert(!candidates.empty(), LOG_TAG_DIVFTREE, "Candidates should not be empty.");
//         FatalAssert(vec.IsValid(), LOG_TAG_DIVFTREE, "Invalid vector.");
//         CHECK_VECTORID_IS_VALID(candidates[0]->CentroidID(), LOG_TAG_DIVFTREE);

//         size_t best_idx = 0;
//         if (candidates.size() == 1) {
//             return best_idx; // only one candidate
//         }

//         Vector target = _bufmgr.GetVector(candidates[0]->CentroidID());
//         FatalAssert(target.IsValid(), LOG_TAG_DIVFTREE, "Target vector is invalid for candidate 0: "
//                     VECTORID_LOG_FMT, VECTORID_LOG(candidates[0]->CentroidID()));
//         DTYPE best_dist = Distance(vec, target);

//         for (size_t i = 1; i < candidates.size(); ++i) { // todo check for memory leaks and stuff
//             target.Unlink();
//             CHECK_VECTORID_IS_VALID(candidates[i]->CentroidID(), LOG_TAG_DIVFTREE);
//             target = _bufmgr.GetVector(candidates[i]->CentroidID());
//             FatalAssert(target.IsValid(), LOG_TAG_DIVFTREE, "Target vector is invalid for candidate %lu: "
//                         VECTORID_LOG_FMT, i, VECTORID_LOG(candidates[i]->CentroidID()));
//             DTYPE tmp_dist = Distance(vec, target);
//             if (MoreSimilar(tmp_dist, best_dist)) {
//                 best_idx = i;
//                 best_dist = tmp_dist;
//             }
//         }

//         return best_idx;
//     }

//     RetStatus Split(std::vector<DIVFTreeVertexInterface*>& candidates, size_t vertex_idx) override {
//         FatalAssert(candidates.size() > vertex_idx, LOG_TAG_DIVFTREE, "candidates should contain vertex.");
//         DIVFTreeVertex* vertex = static_cast<DIVFTreeVertex*>(candidates[vertex_idx]);
//         CHECK_VERTEX_IS_VALID(vertex, LOG_TAG_DIVFTREE, true);
//         FatalAssert(vertex->IsFull(), LOG_TAG_DIVFTREE, "vertex should be full.");

//         RetStatus rs = RetStatus::Success();
//         size_t last_size = candidates.size();
//         std::vector<Vector> centroids;

//         CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//              "Split BEGIN: " VERTEX_LOG_FMT, VERTEX_PTR_LOG(vertex));

//         rs = Cluster(candidates, vertex_idx, centroids, split_leaf);
//         FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Clustering failed");
//         FatalAssert(centroids[0].IsValid(), LOG_TAG_DIVFTREE, "new centroid vector of base vertex is invalid");
//         FatalAssert(candidates.size() > last_size, LOG_TAG_DIVFTREE, "No new vertices were created.");
//         FatalAssert(candidates.size() - last_size == centroids.size() - 1, LOG_TAG_DIVFTREE, "Missmatch between number of created vertices and number of centroids. Number of created vertices:%lu, number of centroids:%lu",
//             candidates.size() - last_size, centroids.size());

//         if (vertex->CentroidID() == _root) {
//             ExpandTree(vertex, centroids[0]);
//         }
//         else {
//             _bufmgr.GetVector(vertex->CentroidID()).CopyFrom(centroids[0], core_attr.dimension);
//         }

//         std::vector<DIVFTreeVertexInterface*> parents;
//         parents.push_back(_bufmgr.GetVertex(vertex->ParentID()));

//         // we can skip vertex as at this point we only have one parent and we place it there for now
//         for (size_t vertex_it = 1; vertex_it < centroids.size(); ++vertex_it) {
//             FatalAssert(centroids[vertex_it].IsValid(), LOG_TAG_DIVFTREE, "Centroid vector is invalid.");
//             // find the best parent
//             size_t closest = FindClosestCluster(parents, centroids[vertex_it]);

//             if (parents[closest]->IsFull()) {
//                 CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "Vertex is Full. Vertex=" VERTEX_LOG_FMT,
//                      VERTEX_PTR_LOG(parents[closest]));
//             }

//             RecordInto(centroids[vertex_it], parents[closest], candidates[vertex_it + last_size - 1]);

//             if (parents[closest]->CentroidID() != _root) {
//                 CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//                     "Split: Put Vertex " VERTEX_LOG_FMT " Centroid:%s, into Parent " VERTEX_LOG_FMT
//                     " Parent_Centroid:%s", VERTEX_PTR_LOG(candidates[vertex_it + last_size - 1]),
//                     centroids[vertex_it].ToString(core_attr.dimension).ToCStr(), VERTEX_PTR_LOG(parents[closest]),
//                     _bufmgr.GetVector(parents[closest]->CentroidID()).ToString(core_attr.dimension).ToCStr());
//             }
//             else {
//                 CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//                     "Split: Put Vertex " VERTEX_LOG_FMT " Centroid:%s, into Parent(root) " VERTEX_LOG_FMT,
//                     VERTEX_PTR_LOG(candidates[vertex_it + last_size - 1]),
//                     centroids[vertex_it].ToString(core_attr.dimension).ToCStr(), VERTEX_PTR_LOG(parents[closest]));
//             }

//             // todo assert ok
//             if (parents[closest]->IsFull()) {
//                 rs = Split(parents, closest); // todo background job?
//                 FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "");
//             }
//         }

//         CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
//              "Split END: " VERTEX_LOG_FMT "Centroid:%s",
//              VERTEX_PTR_LOG(vertex),  _bufmgr.GetVector(vertex->CentroidID()).ToString(core_attr.dimension).ToCStr());
//         return rs;
//     }

//     RetStatus Split(DIVFTreeVertexInterface* leaf) override {
//         std::vector<DIVFTreeVertexInterface*> candids;
//         candids.push_back(leaf);
//         return Split(candids, 0);
//     }

//     inline DIVFTreeVertexInterface* CreateNewVertex(VectorID id) override {
//         CHECK_VECTORID_IS_VALID(id, LOG_TAG_DIVFTREE_VERTEX);
//         CHECK_VECTORID_IS_CENTROID(id, LOG_TAG_DIVFTREE_VERTEX);

//         const bool is_internal_vertex = id.IsInternalVertex();
//         DIVFTreeVertex* new_vertex = static_cast<DIVFTreeVertex*>(malloc(Bytes(is_internal_vertex)));
//         CHECK_NOT_NULLPTR(new_vertex, LOG_TAG_DIVFTREE_VERTEX);

//         DIVFTreeVertexAttributes attr;
//         attr.core = core_attr;
//         attr.similarityComparator = _similarityComparator;
//         attr.reverseSimilarityComparator = _reverseSimilarityComparator;
//         if (is_internal_vertex) {
//             attr.max_size = internal_max_size;
//             attr.min_size = internal_min_size;
//         }
//         else {
//             attr.max_size = leaf_max_size;
//             attr.min_size = leaf_min_size;
//         }

//         new (new_vertex) DIVFTreeVertex(id, attr);
//         CHECK_VERTEX_IS_VALID(new_vertex, LOG_TAG_DIVFTREE_VERTEX, false);
//         return new_vertex;
//     }

//     inline RetStatus SimpleDivideClustering(std::vector<DIVFTreeVertexInterface*>& vertices, size_t target_vertex_index,
//                                            std::vector<Vector>& centroids, uint16_t split_into) {
//         FatalAssert(vertices.size() > target_vertex_index, LOG_TAG_CLUSTERING, "vertices should contain vertex.");
//         DIVFTreeVertexInterface* target = vertices[target_vertex_index];
//         CHECK_VERTEX_IS_VALID(target, LOG_TAG_CLUSTERING, true);
//         FatalAssert(split_into > 0, LOG_TAG_CLUSTERING, "split_into should be greater than 0.");

//         RetStatus rs = RetStatus::Success();

//         split_into = (target->MaxSize() / split_into < target->MinSize() ? target->MaxSize() / target->MinSize() :
//                                                                         split_into);
//         if (split_into < 2) {
//             split_into = 2;
//         }

//         uint16_t num_vec_per_vertex = target->MaxSize() / split_into;
//         uint16_t num_vec_rem = target->MaxSize() % split_into;
//         vertices.reserve(vertices.size() + split_into - 1);
//         centroids.reserve(split_into);
//         centroids.emplace_back(nullptr);
//         for (uint16_t i = num_vec_per_vertex + num_vec_rem; i < target->MaxSize(); ++i) {
//             if ((i - num_vec_rem) % num_vec_per_vertex == 0) {
//                 VectorID vector_id = _bufmgr.RecordVector(target->Level());
//                 FatalAssert(vector_id.IsValid(), LOG_TAG_CLUSTERING, "Failed to record vector.");
//                 DIVFTreeVertexInterface* new_vertex = CreateNewVertex(vector_id);
//                 FatalAssert(new_vertex != nullptr, LOG_TAG_CLUSTERING, "Failed to create sibling vertex.");
//                 vertices.emplace_back(new_vertex);
//                 _bufmgr.UpdateClusterAddress(vector_id, vertices.back());
//             }
//             VectorUpdate update = target->MigrateLastVectorTo(vertices.back());
//             // todo check update is ok and everything is successfull

//             _bufmgr.UpdateVectorAddress(update.vector_id, update.vector_data);
//             if (update.vector_id.IsCentroid()) {
//                 _bufmgr.GetVertex(update.vector_id)->AssignParent(vertices.back()->CentroidID());
//             }

//             if ((i + 1 - num_vec_rem) % num_vec_per_vertex == 0) {
//                 centroids.emplace_back(vertices.back()->ComputeCurrentCentroid());
//                 CLOG(LOG_LEVEL_DEBUG, LOG_TAG_CLUSTERING,
//                     "Simple Cluster: Created Vertex " VERTEX_LOG_FMT " Centroid:%s",
//                     VERTEX_PTR_LOG(vertices.back()), centroids.back().ToString(target->VectorDimension()).ToCStr());
//             }
//         }
//         centroids[0] = target->ComputeCurrentCentroid();

//         return rs;
//     }

//     inline RetStatus Cluster(std::vector<DIVFTreeVertexInterface*>& vertices, size_t target_vertex_index,
//                               std::vector<Vector>& centroids, uint16_t split_into) override {
//         switch (core_attr.clusteringAlg)
//         {
//         case ClusteringType::SimpleDivide:
//             return SimpleDivideClustering(vertices, target_vertex_index, centroids, split_into);
//         default:
//             CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
//                  "Cluster: Invalid clustering type: %s", CLUSTERING_TYPE_NAME[core_attr.clusteringAlg]);
//         }
//         return RetStatus::Fail("Invalid clustering type");
//     }

// TESTABLE;
// };

};

#endif