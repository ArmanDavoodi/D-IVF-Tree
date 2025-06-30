#ifndef COPPER_INTERFACE_H_
#define COPPER_INTERFACE_H_

#include "common.h"
#include "vector_utils.h"

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
};

struct CopperAttributes {
    CopperCoreAttributes core;
    uint16_t leaf_min_size;
    uint16_t leaf_max_size;
    uint16_t internal_min_size;
    uint16_t internal_max_size;
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
    virtual const DIST_ID_PAIR_SIMILARITY_INTERFACE& GetSimilarityComparator(bool reverese = false) const = 0;
    virtual DTYPE Distance(const Vector& a, const Vector& b) const = 0;

    virtual size_t Bytes() const = 0; /* return sizeof(Node) - sizeof(char[1]) + sizeof(VTYPE) * cap * dim -> todo: what about allignment?*/
    // virtual CopperNodeInterface* CreateSibling(VectorID id) const = 0;

    virtual String BucketToString() const = 0;
};

class VectorIndexInterface {
public:
    virtual RetStatus Init(CopperCoreAttributes attr) = 0;
    virtual RetStatus Shutdown() = 0;

    virtual RetStatus Insert(const Vector& vec, VectorID& vec_id, uint16_t node_per_layer) = 0;
    virtual RetStatus Delete(VectorID vec_id) = 0;

    virtual RetStatus ApproximateKNearestNeighbours(const Vector& query, size_t k,
                                                    uint16_t _internal_k, uint16_t _leaf_k,
                                                    std::vector<std::pair<VectorID, DTYPE>>& neighbours,
                                                    bool sort = true, bool sort_from_more_similar_to_less = true) = 0;

    virtual size_t Size() const = 0;
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