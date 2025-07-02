#ifndef COPPER_H_
#define COPPER_H_

#include "common.h"
#include "vector_utils.h"

#include "interface/copper.h"

/* Needs distance.h and buffer.h */

namespace copper {

class CopperNode : public CopperNodeInterface {
public:
    CopperNode(VectorID id, CopperNodeAttributes attr) : _data(id, attr) {}
    ~CopperNode() = default;
    RetStatus AssignParent(VectorID parent_id) override {
        _data._parent_id = parent_id;
    }

    Address Insert(const Vector& vec, VectorID vec_id) override {
        return _data._bucket.Insert(vec, vec_id);
    }

    VectorUpdate MigrateLastVectorTo(CopperNodeInterface* _dest) override {
        return VectorUpdate{INVALID_VECTOR_ID, INVALID_ADDRESS}; // Placeholder for actual implementation
    }

    RetStatus Search(const Vector& query, size_t k,
                     std::vector<std::pair<VectorID, DTYPE>>& neighbours) override {
        return RetStatus::Success(); // Placeholder for actual implementation
    }

    VectorID CentroidID() const override {
        return _data._centroid_id;
    }

    VectorID ParentID() const override {
        return _data._parent_id;
    }

    uint16_t Size() const override {
        return _data._bucket.Size();
    }

    bool IsFull() const override {
        return _data._bucket.Size() >= _data._bucket.Capacity();
    }

    bool IsAlmostEmpty() const override {
        return _data._bucket.Size() <= _data._min_size;
    }

    bool Contains(VectorID id) const override {
        return _data._bucket.Contains(id);
    }

    bool IsLeaf() const override {
        return _data._centroid_id.IsLeaf();
    }

    uint8_t Level() const override {
        return _data._centroid_id._level;
    }

    Vector ComputeCurrentCentroid() const override {
        return Vector(); // Placeholder for actual centroid computation
    }

    uint16_t MinSize() const override {
        return _data._min_size;
    }

    uint16_t MaxSize() const override {
        return _data._bucket.Capacity();
    }

    uint16_t VectorDimention() const override {
        return _data._bucket.Dimension();
    }

    /* todo: A better method(compared to polymorphism) to allow inlining for optimization */
    const DIST_ID_PAIR_SIMILARITY_INTERFACE& GetSimilarityComparator(bool reverese = false) const override {
        if (reverese) {
            return *_data._reverseSimilarityComparator;
        }
        return *_data._similarityComparator;
    }

    DTYPE Distance(const Vector& a, const Vector& b) const override {
        return 0.0; // Placeholder for actual distance computation
    }

    size_t Bytes() const override {
        _data.Bytes(); // Return the size of the node in bytes
    }

    String BucketToString() const override {
        return _data._bucket.ToString(); // Return a string representation of the bucket
    }

protected:
    CopperNodeData _data;

TESTABLE;
};

class VectorIndex : public VectorIndexInterface {
public:

    VectorIndex(CopperAttributes attr) : _attr(attr),
                                         _similarityComparator(
                                            GetDistancePairSimilarityComparator(attr.core.distanceAlg, false)),
                                         _reverseSimilarityComparator(
                                            GetDistancePairSimilarityComparator(attr.core.distanceAlg, true)),
                                         _size(0), _root(INVALID_VECTOR_ID), _levels(2) {
        RetStatus rs = RetStatus::Success();
        rs = _bufmgr.Init();
        FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to init buffer manager.");
        _root = _bufmgr.RecordRoot();
        FatalAssert(_root.IsValid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
        FatalAssert(_root.IsCentroid(), LOG_TAG_VECTOR_INDEX, "root should be a centroid: "
                    VECTORID_LOG_FMT, VECTORID_LOG(_root));
        FatalAssert(_root.IsLeaf(), LOG_TAG_VECTOR_INDEX, "first root should be a leaf: "
                    VECTORID_LOG_FMT, VECTORID_LOG(_root));

        rs = _bufmgr.UpdateClusterAddress(_root, CreateNewNode(_root));
        FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update cluster address for root: "
                    VECTORID_LOG_FMT, VECTORID_LOG(_root));
        _levels = 2;
        CLOG(LOG_LEVEL_LOG, LOG_TAG_VECTOR_INDEX,
             "Init Copper Index End: rootID= " VECTORID_LOG_FMT ", _levels = %hhu, attr=%s",
             VECTORID_LOG(_root), _levels, _attr.ToString().ToCStr());
    }

    ~VectorIndex() override {
        RetStatus rs = RetStatus::Success();
        rs = _bufmgr.Shutdown();
        delete _similarityComparator;
        delete _reverseSimilarityComparator;
        CLOG(LOG_LEVEL_LOG, LOG_TAG_VECTOR_INDEX, "Shutdown Copper Index End: rs=%s", rs.Msg());
    }

    RetStatus Insert(const Vector& vec, VectorID& vec_id, uint16_t node_per_layer) override {
        return RetStatus::Success(); // Placeholder for actual insertion logic
    }

    RetStatus Delete(VectorID vec_id) override {
        return RetStatus::Success(); // Placeholder for actual deletion logic
    }

    RetStatus ApproximateKNearestNeighbours(const Vector& query, size_t k,
                                                    uint16_t _internal_k, uint16_t _leaf_k,
                                                    std::vector<std::pair<VectorID, DTYPE>>& neighbours,
                                                    bool sort = true, bool sort_from_more_similar_to_less = true) override {
        return RetStatus::Success(); // Placeholder for actual search logic
    }

    size_t Size() const override {
        return _size; // Return the size of the index
    }

    DTYPE Distance(const Vector& a, const Vector& b) const override {
        return 0.0;
    }

    size_t Bytes(bool is_internal_node) const override {
        if (is_internal_node) {
            return sizeof(CopperNodeHeaderData) + sizeof(VTYPE) * _attr.core.dimention * _attr.internal_max_size;
        } else {
            return sizeof(CopperNodeHeaderData) + sizeof(VTYPE) * _attr.core.dimention * _attr.leaf_max_size;
        }
    }
protected:
    const CopperAttributes _attr;
    const DIST_ID_PAIR_SIMILARITY_INTERFACE* const _similarityComparator;
    const DIST_ID_PAIR_SIMILARITY_INTERFACE* const _reverseSimilarityComparator;
    size_t _size;
    BufferManager _bufmgr;
    VectorID _root;
    uint64_t _levels;

    RetStatus SearchNodes(const Vector& query,
                          const std::vector<std::pair<VectorID, DTYPE>>& upper_layer,
                          std::vector<std::pair<VectorID, DTYPE>>& lower_layer, size_t n) override {
        return RetStatus::Success(); // Placeholder for actual search logic
    }

    VectorID RecordInto(const Vector& vec, CopperNodeInterface* container_node,
                        CopperNodeInterface* node = nullptr) override {
        return INVALID_VECTOR_ID; // Placeholder for actual record logic
    }

    RetStatus ExpandTree(CopperNodeInterface* root, const Vector& centroid) override {
        return RetStatus::Success(); // Placeholder for actual tree expansion logic
    }

    size_t FindClosestCluster(const std::vector<CopperNodeInterface*>& candidates,
                              const Vector& vec) override {
        return 0; // Placeholder for actual cluster finding logic
    }

    RetStatus Split(std::vector<CopperNodeInterface*>& candidates, size_t node_idx) override {
        return RetStatus::Success(); // Placeholder for actual split logic
    }

    RetStatus Split(CopperNodeInterface* leaf) override {
        return RetStatus::Success(); // Placeholder for actual split logic
    }

    CopperNodeInterface* CreateNewNode(VectorID id) override {
        const bool is_internal_node = id.IsInternalNode();
        CopperNode* new_node = static_cast<CopperNode*>(malloc(Bytes(is_internal_node)));
        CHECK_NOT_NULLPTR(new_node, LOG_TAG_COPPER_NODE);

        CopperNodeAttributes attr;
        attr.core = _attr.core;
        attr.similarityComparator = _similarityComparator;
        attr.reverseSimilarityComparator = _reverseSimilarityComparator;
        if (is_internal_node) {
            attr.max_size = _attr.internal_max_size;
            attr.min_size = _attr.internal_min_size;
        }
        else {
            attr.max_size = _attr.leaf_max_size;
            attr.min_size = _attr.leaf_min_size;
        }

        new (new_node) CopperNode(id, attr);
        CHECK_NODE_IS_VALID(new_node, LOG_TAG_COPPER_NODE, false);
        return new_node;
    }

    RetStatus Cluster(std::vector<CopperNodeInterface*>& nodes, size_t target_node_index,
                      std::vector<Vector>& centroids, uint16_t split_into) override {
        return RetStatus::Success(); // Placeholder for actual clustering logic
    }

TESTABLE;
};

};

#endif