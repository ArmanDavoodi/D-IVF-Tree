#ifndef DIVFTREE_INTERFACE_H_
#define DIVFTREE_INTERFACE_H_

#include "common.h"
#include "vector_utils.h"
#include "distance.h"

#include <map>

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
                                centroid_id{id}, version{ver}, min_size{min}, cap{max}, block_size{blck_size},\
                                index{idx};
};

struct DIVFTreeAttributes {
    ClusteringType clusteringAlg;
    DistanceType distanceAlg;
    VPairComparator similarityComparator;
    VPairComparator reverseSimilarityComparator;

    uint16_t leaf_min_size;
    uint16_t leaf_max_size;
    uint16_t internal_min_size;
    uint16_t internal_max_size;

    uint16_t leaf_blck_size;
    uint16_t internal_blck_size;

    uint16_t split_internal;
    uint16_t split_leaf;

    uint16_t dimension;


    String ToString() const {
        return String("{dimension=%hu, clusteringAlg=%s, distanceAlg=%s, "
                      "leaf_min_size=%hu, leaf_max_size=%hu, "
                      "internal_min_size=%hu, internal_max_size=%hu, "
                      "split_internal=%hu, split_leaf=%hu}",
                      core.dimension, CLUSTERING_TYPE_NAME[core.clusteringAlg], DISTANCE_TYPE_NAME[core.distanceAlg],
                      leaf_min_size, leaf_max_size, internal_min_size, internal_max_size, split_internal, split_leaf);
    }
};

// struct DIVFTreeAttributes {

    // DIVFTreeCoreAttributes core;
    // uint16_t leaf_min_size;
    // uint16_t leaf_max_size;
    // uint16_t internal_min_size;
    // uint16_t internal_max_size;
    // uint16_t split_internal;
    // uint16_t split_leaf;

    // String ToString() const {
    //     return String("{dimension=%hu, clusteringAlg=%s, distanceAlg=%s, "
    //                   "leaf_min_size=%hu, leaf_max_size=%hu, "
    //                   "internal_min_size=%hu, internal_max_size=%hu, "
    //                   "split_internal=%hu, split_leaf=%hu}",
    //                   core.dimension, CLUSTERING_TYPE_NAME[core.clusteringAlg], DISTANCE_TYPE_NAME[core.distanceAlg],
    //                   leaf_min_size, leaf_max_size, internal_min_size, internal_max_size, split_internal, split_leaf);
    // }
// };

#define CHECK_CORE_ATTRIBUTES(attr, tag) \
    FatalAssert((attr).core.dimension > 0, (tag), "Dimension must be greater than 0."); \
    FatalAssert(IsValid((attr).core.clusteringAlg), (tag), "Clustering algorithm is invalid."); \
    FatalAssert(IsValid((attr).core.distanceAlg), (tag), "Distance algorithm is invalid.")

#define CHECK_VERTEX_ATTRIBUTES(attr, tag) \
    CHECK_CORE_ATTRIBUTES(attr, tag); \
    CHECK_MIN_MAX_SIZE(attr.min_size, attr.max_size, tag); \
    CHECK_VECTORID_IS_VALID(attr.centroid_id, tag); \
    CHECK_VECTORID_IS_CENTROID(attr.centroid_id, tag); \
    FatalAssert(attr.cluster_owner < MAX_COMPUTE_NODE_ID, tag, \
                "Cluster owner is invalid. cluster_owner=%hu", attr.cluster_owner); \
    FatalAssert(IsComputeNode(attr.cluster_owner), tag, \
                "Cluster owner is not a compute node. cluster_owner=%hu", attr.cluster_owner);

#define CHECK_DIVFTREE_ATTRIBUTES(attr, tag) \
    CHECK_CORE_ATTRIBUTES(attr.core, tag); \
    CHECK_MIN_MAX_SIZE(attr.leaf_min_size, attr.leaf_max_size, tag); \
    CHECK_MIN_MAX_SIZE(attr.internal_min_size, attr.internal_max_size, tag)

class DIVFTreeVertexInterface {
public:
    // virtual ~DIVFTreeVertexInterface() = default;
    // virtual RetStatus AssignParent(VectorID parent_id) = 0;

    virtual void MarkForRecycle(uint64_t pinCount) = 0;
    virtual void Unpin() = 0;
    virtual Cluster& GetCluster() = 0;

    virtual const DIVFTreeVertexAttributes& GetAttributes() const = 0;

    // virtual RetStatus BatchUpdate(BatchVertexUpdate& updates) = 0;

    // virtual RetStatus Search(const Vector& query, size_t k,
    //                          std::vector<std::pair<VectorID, DTYPE>>& neighbours) = 0;

    // virtual VectorID CentroidID() const = 0;
    // virtual VectorID ParentID() const = 0;
    // virtual uint16_t Size() const = 0;

    // virtual bool IsFull() const = 0;
    // virtual bool IsAlmostEmpty() const = 0;
    // virtual bool Contains(VectorID id) const = 0;

    // virtual bool IsLeaf() const = 0;
    // virtual uint8_t Level() const = 0;

    // virtual Vector ComputeCurrentCentroid() const = 0;

    // virtual uint16_t MinSize() const = 0;
    // virtual uint16_t MaxSize() const = 0;
    // virtual uint16_t VectorDimension() const = 0;

    // /* todo: A better method(compared to polymorphism) to allow inlining for optimization */
    // virtual VPairComparator GetSimilarityComparator(bool reverese) const = 0;
    // virtual DTYPE Distance(const Vector& a, const Vector& b) const = 0;

    // virtual String ToString(bool detailed = false) const = 0;
};

class DIVFTreeInterface {
public:
    DIVFTreeInterface() = default;
    virtual ~DIVFTreeInterface() = default;

    virtual const DIVFTreeAttributes& GetAttributes() const = 0;

    // virtual RetStatus Insert(const Vector& vec, VectorID& vec_id, uint16_t vertex_per_layer) = 0;
    // virtual RetStatus Delete(VectorID vec_id) = 0;

    // virtual RetStatus ApproximateKNearestNeighbours(const Vector& query, size_t k,
    //                                                 uint16_t _internal_k, uint16_t _leaf_k,
    //                                                 std::vector<std::pair<VectorID, DTYPE>>& neighbours,
    //                                                 bool sort = true, bool sort_from_more_similar_to_less = true) = 0;

    // virtual size_t Size() const = 0;

    // virtual DTYPE Distance(const Vector& a, const Vector& b) const = 0;
    // virtual size_t Bytes(bool is_internal_vertex) const = 0;

    // virtual String ToString() = 0;

    // inline static RetStatus KNearestNeighbours(const Vector& query, size_t k, uint16_t dim,
    //                                            const std::vector<std::pair<VectorID, Vector>>& _data,
    //                                            std::vector<std::pair<VectorID, DTYPE>>& neighbours,
    //                                            const DistanceType distanceAlg,
    //                                            bool sort = true, bool sort_from_more_similar_to_less = true) {
    //     FatalAssert(k > 0, LOG_TAG_BASIC, "k should be greater than 0.");
    //     FatalAssert(dim > 0, LOG_TAG_BASIC, "Vector dimension should be greater than 0.");
    //     FatalAssert(_data.size() > 0, LOG_TAG_BASIC, "Data should contain at least one vector.");
    //     FatalAssert(query.IsValid(), LOG_TAG_BASIC, "Query vector is invalid.");
    //     FatalAssert(neighbours.empty(), LOG_TAG_BASIC, "Neighbours vector should be empty.");
    //     CLOG_IF_TRUE(_data.size() <= k, LOG_LEVEL_WARNING, LOG_TAG_BASIC,
    //                  "Data size (%lu) is less than or equal to k (%lu).", _data.size(), k);

    //     for (const auto& pair : _data) {
    //         DTYPE distance = divftree::Distance(query, pair.second, dim, distanceAlg);
    //         neighbours.emplace_back(pair.first, distance);
    //         std::push_heap(neighbours.begin(), neighbours.end(),
    //                       GetDistancePairSimilarityComparator(distanceAlg, true));
    //         if (neighbours.size() > k) {
    //             std::pop_heap(neighbours.begin(), neighbours.end(),
    //                           GetDistancePairSimilarityComparator(distanceAlg, true));
    //             neighbours.pop_back();
    //         }
    //     }
    //     if (sort) {
    //         std::sort_heap(neighbours.begin(), neighbours.end(),
    //                        GetDistancePairSimilarityComparator(distanceAlg, true));
    //         if (!sort_from_more_similar_to_less) {
    //             std::reverse(neighbours.begin(), neighbours.end());
    //         }
    //     }

    //     return RetStatus::Success();
    // }

protected:
    // virtual RetStatus SearchVertexs(const Vector& query,
    //                               const std::vector<std::pair<VectorID, DTYPE>>& upper_layer,
    //                               std::vector<std::pair<VectorID, DTYPE>>& lower_layer, size_t n) = 0;

    // virtual VectorID RecordInto(const Vector& vec, DIVFTreeVertexInterface* container_vertex,
    //                             DIVFTreeVertexInterface* vertex = nullptr) = 0;

    // virtual RetStatus ExpandTree(DIVFTreeVertexInterface* root, const Vector& centroid) = 0;

    // virtual size_t FindClosestCluster(const std::vector<DIVFTreeVertexInterface*>& candidates,
    //                                    const Vector& vec) = 0;

    // virtual RetStatus Split(std::vector<DIVFTreeVertexInterface*>& candidates, size_t vertex_idx) = 0;
    // virtual RetStatus Split(DIVFTreeVertexInterface* leaf) = 0;

    // virtual DIVFTreeVertexInterface* CreateNewVertex(VectorID id) = 0;

    // virtual RetStatus Cluster(std::vector<DIVFTreeVertexInterface*>& vertices, size_t target_vertex_index,
    //                           std::vector<Vector>& centroids, uint16_t split_into) = 0;
};

};

#endif