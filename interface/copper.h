#ifndef COPPER_INTERFACE_H_
#define COPPER_INTERFACE_H_

#include "common.h"
#include "vector_utils.h"
#include "distance.h"

namespace copper {

struct CopperCoreAttributes {
    // DataType vtype;
    uint16_t dimention;
    // DataType dtype;
    ClusteringType clusteringAlg;
    DistanceType distanceAlg;
};

struct CopperNodeAttributes {
    CopperCoreAttributes core;
    uint16_t min_size;
    uint16_t max_size;
    VPairComparator similarityComparator;
    VPairComparator reverseSimilarityComparator;
};

struct CopperAttributes {
    CopperCoreAttributes core;
    uint16_t leaf_min_size;
    uint16_t leaf_max_size;
    uint16_t internal_min_size;
    uint16_t internal_max_size;
    uint16_t split_internal;
    uint16_t split_leaf;

    String ToString() const {
        return String("{dimention=%hu, clusteringAlg=%s, distanceAlg=%s, "
                      "leaf_min_size=%hu, leaf_max_size=%hu, "
                      "internal_min_size=%hu, internal_max_size=%hu, "
                      "split_internal=%hu, split_leaf=%hu}",
                      core.dimention, CLUSTERING_TYPE_NAME[core.clusteringAlg], DISTANCE_TYPE_NAME[core.distanceAlg],
                      leaf_min_size, leaf_max_size, internal_min_size, internal_max_size, split_internal, split_leaf);
    }
};

#define CHECK_CORE_ATTRIBUTES(attr, tag) \
    FatalAssert((attr).core.dimention > 0, (tag), "Dimention must be greater than 0."); \
    FatalAssert(IsValid((attr).core.clusteringAlg), (tag), "Clustering algorithm is invalid."); \
    FatalAssert(IsValid((attr).core.distanceAlg), (tag), "Distance algorithm is invalid.")

#define CHECK_NODE_ATTRIBUTES(attr, tag) \
    CHECK_CORE_ATTRIBUTES(attr, tag); \
    CHECK_MIN_MAX_SIZE(attr.min_size, attr.max_size, tag)

#define CHECK_COPPER_ATTRIBUTES(attr, tag) \
    CHECK_CORE_ATTRIBUTES(attr.core, tag); \
    CHECK_MIN_MAX_SIZE(attr.leaf_min_size, attr.leaf_max_size, tag); \
    CHECK_MIN_MAX_SIZE(attr.internal_min_size, attr.internal_max_size, tag)

class CopperNodeInterface {
public:
    CopperNodeInterface() = default;
    virtual ~CopperNodeInterface() = default;
    virtual RetStatus AssignParent(VectorID parent_id) = 0;

    virtual Address Insert(const Vector& vec, VectorID vec_id) = 0;
    virtual VectorUpdate MigrateLastVectorTo(CopperNodeInterface* _dest) = 0;

    virtual RetStatus Search(const Vector& query, size_t k,
                             std::vector<std::pair<VectorID, DTYPE>>& neighbours) = 0;

    virtual VectorID CentroidID() const = 0;
    virtual VectorID ParentID() const = 0;
    virtual uint16_t Size() const = 0;

    virtual bool IsFull() const = 0;
    virtual bool IsAlmostEmpty() const = 0;
    virtual bool Contains(VectorID id) const = 0;

    virtual bool IsLeaf() const = 0;
    virtual uint8_t Level() const = 0;

    virtual Vector ComputeCurrentCentroid() const = 0;

    virtual uint16_t MinSize() const = 0;
    virtual uint16_t MaxSize() const = 0;
    virtual uint16_t VectorDimention() const = 0;

    /* todo: A better method(compared to polymorphism) to allow inlining for optimization */
    virtual VPairComparator GetSimilarityComparator(bool reverese) const = 0;
    virtual DTYPE Distance(const Vector& a, const Vector& b) const = 0;

    virtual String BucketToString() const = 0;
};

class VectorIndexInterface {
public:
    VectorIndexInterface() = default;
    virtual ~VectorIndexInterface() = default;

    virtual RetStatus Insert(const Vector& vec, VectorID& vec_id, uint16_t node_per_layer) = 0;
    virtual RetStatus Delete(VectorID vec_id) = 0;

    virtual RetStatus ApproximateKNearestNeighbours(const Vector& query, size_t k,
                                                    uint16_t _internal_k, uint16_t _leaf_k,
                                                    std::vector<std::pair<VectorID, DTYPE>>& neighbours,
                                                    bool sort = true, bool sort_from_more_similar_to_less = true) = 0;

    virtual size_t Size() const = 0;

    virtual DTYPE Distance(const Vector& a, const Vector& b) const = 0;
    virtual size_t Bytes(bool is_internal_node) const = 0;

    inline static RetStatus KNearestNeighbours(const Vector& query, size_t k, uint16_t dim,
                                               const std::vector<std::pair<VectorID, Vector>>& _data,
                                               std::vector<std::pair<VectorID, DTYPE>>& neighbours,
                                               const DistanceType distanceAlg,
                                               bool sort = true, bool sort_from_more_similar_to_less = true) {
        FatalAssert(k > 0, LOG_TAG_BASIC, "k should be greater than 0.");
        FatalAssert(dim > 0, LOG_TAG_BASIC, "Vector dimension should be greater than 0.");
        FatalAssert(_data.size() > 0, LOG_TAG_BASIC, "Data should contain at least one vector.");
        FatalAssert(query.IsValid(), LOG_TAG_BASIC, "Query vector is invalid.");
        FatalAssert(neighbours.empty(), LOG_TAG_BASIC, "Neighbours vector should be empty.");
        CLOG_IF_TRUE(_data.size() <= k, LOG_LEVEL_WARNING, LOG_TAG_BASIC,
                     "Data size (%lu) is less than or equal to k (%lu).", _data.size(), k);

        for (const auto& pair : _data) {
            DTYPE distance = Distance(query, pair.second, dim, distanceAlg);
            neighbours.emplace_back(pair.first, distance);
            std::push_heap(neighbours.begin(), neighbours.end(),
                          GetDistancePairSimilarityComparator(distanceAlg, true));
            if (neighbours.size() > k) {
                std::pop_heap(neighbours.begin(), neighbours.end(),
                              GetDistancePairSimilarityComparator(distanceAlg, true));
                neighbours.pop_back();
            }
        }
        if (sort) {
            std::sort_heap(neighbours.begin(), neighbours.end(),
                           GetDistancePairSimilarityComparator(distanceAlg, true));
            if (!sort_from_more_similar_to_less) {
                std::reverse(neighbours.begin(), neighbours.end());
            }
        }
    }

protected:
    virtual RetStatus SearchNodes(const Vector& query,
                                  const std::vector<std::pair<VectorID, DTYPE>>& upper_layer,
                                  std::vector<std::pair<VectorID, DTYPE>>& lower_layer, size_t n) = 0;

    virtual VectorID RecordInto(const Vector& vec, CopperNodeInterface* container_node,
                                CopperNodeInterface* node = nullptr) = 0;

    virtual RetStatus ExpandTree(CopperNodeInterface* root, const Vector& centroid) = 0;

    virtual size_t FindClosestCluster(const std::vector<CopperNodeInterface*>& candidates,
                                       const Vector& vec) = 0;

    virtual RetStatus Split(std::vector<CopperNodeInterface*>& candidates, size_t node_idx) = 0;
    virtual RetStatus Split(CopperNodeInterface* leaf) = 0;

    virtual CopperNodeInterface* CreateNewNode(VectorID id) = 0;

    virtual RetStatus Cluster(std::vector<CopperNodeInterface*>& nodes, size_t target_node_index,
                              std::vector<Vector>& centroids, uint16_t split_into) = 0;
};

};

#endif