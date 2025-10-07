#ifndef DIVFTREE_INTERFACE_H_
#define DIVFTREE_INTERFACE_H_

#include "common.h"
#include "vector_utils.h"
#include "distance.h"

#include "utils/thread.h"
#include "utils/sorted_list.h"

#include <unordered_set>

namespace divftree {

struct DIVFTreeVertexAttributes {
    VectorID centroid_id;
    Version version;
    uint16_t min_size; // todo: do we need this at compute nodes?
    uint16_t cap; // todo: do we need this at compute nodes?
    uint16_t block_size;

    DIVFTreeInterface *index;

    DIVFTreeVertexAttributes() : centroid_id{INVALID_VECTOR_ID}, version{0}, min_size{0}, cap{0}, block_size{0}, index{nullptr}; /* todo: to be remvoed */
    DIVFTreeVertexAttributes(VectorID id, Version ver, uint16_t min,
                             uint16_t max, uint16_t blck_size, DIVFTreeInterface* idx) :
                                centroid_id{id}, version{ver}, min_size{min}, cap{max}, block_size{blck_size},
                                index{idx};
};

struct DIVFTreeAttributes {
    ClusteringType clusteringAlg;
    DistanceType distanceAlg;
    SimilarityComparator similarityComparator;
    SimilarityComparator reverseSimilarityComparator;

    uint16_t leaf_min_size;
    uint16_t leaf_max_size;
    uint16_t internal_min_size;
    uint16_t internal_max_size;

    uint16_t leaf_blck_size;
    uint16_t internal_blck_size;

    uint16_t split_internal;
    uint16_t split_leaf;

    uint16_t dimension;

    size_t num_migrators;
    size_t num_mergers;
    size_t num_compactors;

    uint32_t migration_check_triger_rate;
    uint32_t migration_check_triger_single_rate;

    uint32_t random_base_perc;

    String ToString() const {
        return String("{dimension=%hu, clusteringAlg=%s, distanceAlg=%s, "
                      "leaf_min_size=%hu, leaf_max_size=%hu, "
                      "internal_min_size=%hu, internal_max_size=%hu, "
                      "split_internal=%hu, split_leaf=%hu}",
                      core.dimension, CLUSTERING_TYPE_NAME[core.clusteringAlg], DISTANCE_TYPE_NAME[core.distanceAlg],
                      leaf_min_size, leaf_max_size, internal_min_size, internal_max_size, split_internal, split_leaf);
    }
};

// #define CHECK_CORE_ATTRIBUTES(attr, tag) \
//     FatalAssert((attr).core.dimension > 0, (tag), "Dimension must be greater than 0."); \
//     FatalAssert(IsValid((attr).core.clusteringAlg), (tag), "Clustering algorithm is invalid."); \
//     FatalAssert(IsValid((attr).core.distanceAlg), (tag), "Distance algorithm is invalid.")

// #define CHECK_VERTEX_ATTRIBUTES(attr, tag) \
//     CHECK_CORE_ATTRIBUTES(attr, tag); \
//     CHECK_MIN_MAX_SIZE(attr.min_size, attr.max_size, tag); \
//     CHECK_VECTORID_IS_VALID(attr.centroid_id, tag); \
//     CHECK_VECTORID_IS_CENTROID(attr.centroid_id, tag); \
//     FatalAssert(attr.cluster_owner < MAX_COMPUTE_NODE_ID, tag, \
//                 "Cluster owner is invalid. cluster_owner=%hu", attr.cluster_owner); \
//     FatalAssert(IsComputeNode(attr.cluster_owner), tag, \
//                 "Cluster owner is not a compute node. cluster_owner=%hu", attr.cluster_owner);

// #define CHECK_DIVFTREE_ATTRIBUTES(attr, tag) \
//     CHECK_CORE_ATTRIBUTES(attr.core, tag); \
//     CHECK_MIN_MAX_SIZE(attr.leaf_min_size, attr.leaf_max_size, tag); \
//     CHECK_MIN_MAX_SIZE(attr.internal_min_size, attr.internal_max_size, tag)

class DIVFTreeVertexInterface {
public:
    DIVFTreeVertexInterface() = default;
    DIVFTreeVertexInterface(DIVFTreeVertexInterface&) = delete;
    DIVFTreeVertexInterface(const DIVFTreeVertexInterface&) = delete;
    DIVFTreeVertexInterface(DIVFTreeVertexInterface&&) = delete;
    virtual ~DIVFTreeVertexInterface() = default;

    virtual void Unpin() = 0;
    virtual void MarkForRecycle(uint64_t pinCount) = 0;

    virtual RetStatus BatchInsert(const ConstVectorBatch& batch, uint16_t marked_for_update = INVALID_OFFSET) = 0;
    virtual RetStatus ChangeVectorState(VectorID target, uint16_t targetOffset,
                                        VectorState& expectedState, VectorState finalState,
                                        Version* version = nullptr) = 0;
    virtual RetStatus ChangeVectorState(Address meta, VectorState& expectedState, VectorState finalState,
                                        Version* version = nullptr) = 0;
    virtual void Search(const VTYPE* query, size_t k,
                        SortedList<ANNVectorInfo, SimilarityComparator>& neighbours,
                        std::unordered_set<std::pair<VectorID, Version>, VectorIDVersionPairHash>& seen) = 0;
    // virtual String ToString(bool detailed = false) const = 0;
};

class DIVFTreeInterface {
public:
    DIVFTreeInterface() = default;
    virtual ~DIVFTreeInterface() = default;

    virtual const DIVFTreeAttributes& GetAttributes() const = 0;

    virtual RetStatus Insert(const VTYPE* vec, VectorID& vec_id, uint8_t search_span,
                             bool create_completion_notification = false) = 0;
    virtual RetStatus Delete(VectorID vec_id, bool create_completion_notification = false) = 0;

    virtual RetStatus ApproximateKNearestNeighbours(const VTYPE* query, size_t k,
                                                    uint8_t internal_node_search_span, uint8_t leaf_node_search_span,
                                                    SortType sort_type, std::vector<ANNVectorInfo>& neighbours) = 0;

    virtual size_t Size() const = 0;
    virtual const DIVFTreeAttributes& GetAttributes() const = 0;
    // virtual String ToString() = 0;
};

};

#endif