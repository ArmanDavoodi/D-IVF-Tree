#ifndef COPPER_INTERFACE_H_
#define COPPER_INTERFACE_H_

#include "common.h"
#include "vector_utils.h"

namespace copper {

struct CopperCoreAttributes {
    DataType vtype;
    uint16_t dimention;
    DataType dtype;
    ClusteringType clusteringAlg;
    DistanceType distanceAlg;
};

struct CopperNodeAttributes {
    CopperCoreAttributes& core;
    uint16_t min_size;
    uint16_t max_size;
};

class CopperNodeInterface {
public:

    virtual RetStatus Init(VectorID id, CopperNodeAttributes attr) = 0;
    virtual RetStatus Destroy() = 0;

    virtual RetStatus AssignParent(VectorID parent_id) = 0;

    virtual Address Insert(const Vector& vec, VectorID vec_id) = 0;

    // virtual RetStatus Delete(VectorID vec_id, VectorID& swapped_vec_id, Vector& swapped_vec) = 0;

    virtual VectorUpdate MigrateLastVectorTo(CopperNodeInterface* _dest) = 0;

    virtual RetStatus Search(const Vector& query, size_t k,
                             std::vector<std::pair<VectorID, void>>& neighbours) = 0;

    // virtual VectorID Find_Nearest(const Vector& query) = 0;

    virtual uint16_t Size() const = 0;

    virtual VectorID CentroidID() const = 0;

    virtual VectorID ParentID() const = 0;

    virtual bool IsLeaf() const = 0;

    virtual bool IsFull() const = 0;

    virtual bool IsAlmostEmpty() const = 0;

    virtual uint8_t Level() const = 0;

    virtual bool Contains(VectorID id) const = 0;

    virtual Vector ComputeCurrentCentroid() const = 0;

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

    // virtual Leaf_Node* Find_Leaf(const Vector& query) = 0;

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
};

};

#endif