#ifndef COPPER_H_
#define COPPER_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"
#include "distance.h"

#include "interface/copper.h"

#include <algorithm>

// Todo: better logs and asserts -> style: <Function name>(self data(a=?), input data): msg, additonal variables if needed

/*
 * Todo: There are two approaches we can take:
 * 1) Bottom-up nearest approach:
 *      This is our current approach in which we try to gurantee(lazily) that each vector is always in its closest node.
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
 *      To do so, at the time of split of node k, we have to check migrataion for every vector in any layer in that subtree
 *      to other sibling nodes of node k.(sibling nodes are nodes with the same parent)
 *      As a result, a split in higher levels is more costly. and the cost only grows. lazy approach will cause us to have
 *      bad accuracy for a long time.
 *      probably having small buckets is better here.
 *
 */

namespace copper {

class CopperNode : public CopperNodeInterface {
public:
    CopperNode(VectorID id, CopperNodeAttributes attr) : _centroid_id(id), _parent_id(INVALID_VECTOR_ID),
        _clusteringAlg(attr.core.clusteringAlg), _distanceAlg(attr.core.distanceAlg), _min_size(attr.min_size),
        _bucket(attr.core.dimention, attr.max_size),
        _similarityComparator(attr.similarityComparator),
        _reverseSimilarityComparator(attr.reverseSimilarityComparator) {

        CHECK_VECTORID_IS_VALID(id, LOG_TAG_COPPER_NODE);
        CHECK_VECTORID_IS_CENTROID(id, LOG_TAG_COPPER_NODE);
        CHECK_NODE_ATTRIBUTES(attr, LOG_TAG_COPPER_NODE);
    }

    ~CopperNode() override = default; /* No dynamic allocation inside of the Node data -> should be freed by buffermgr */

    RetStatus AssignParent(VectorID parent_id) override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        FatalAssert(parent_id._level == _centroid_id._level + 1, LOG_TAG_COPPER_NODE,
                    "Assign_Parent(self = " NODE_LOG_FMT ", parent_id = " VECTORID_LOG_FMT
                    "): Level mismatch between parent and self.", NODE_PTR_LOG(this), VECTORID_LOG(parent_id));

        _parent_id = parent_id;
        return RetStatus::Success();
    }

    Address Insert(const Vector& vec, VectorID vec_id) override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        CHECK_VECTORID_IS_VALID(vec_id, LOG_TAG_COPPER_NODE);
        FatalAssert(vec_id._level == _centroid_id._level - 1, LOG_TAG_COPPER_NODE, "Level mismatch! "
            "input vector: (id: %lu, level: %lu), centroid vector: (id: %lu, level: %lu)"
            , vec_id._id, vec_id._level, _centroid_id._id, _centroid_id._level);
        FatalAssert(vec.IsValid(), LOG_TAG_COPPER_NODE, "Cannot insert invalid vector %lu into the bucket with id %lu.",
                    vec_id._id, _centroid_id._id);
        Address addr = _bucket.Insert(vec, vec_id);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_COPPER_NODE,
                 "Insert: Node-Bucket=%s, Inserted VectorID=" VECTORID_LOG_FMT ", Vector=%s",
                 _bucket.ToString().ToCStr(), VECTORID_LOG(vec_id), vec.ToString(_bucket.Dimension()).ToCStr());
        return addr;
    }

    VectorUpdate MigrateLastVectorTo(CopperNodeInterface* _dest) override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, true);
        CHECK_NODE_IS_VALID(_dest, LOG_TAG_COPPER_NODE, false);
        FatalAssert(_dest->Level() == _centroid_id._level, LOG_TAG_COPPER_NODE, "Level mismatch! "
                   "_dest = (id: %lu, level: %lu), self = (id: %lu, level: %lu)",
                   _dest->CentroidID()._id, _dest->CentroidID()._level, _centroid_id._id, _centroid_id._level);

        VectorUpdate update;
        update.vector_id = _bucket.GetLastVectorID();
        update.vector_data = _dest->Insert(_bucket.GetLastVector(), update.vector_id);
        FatalAssert(update.IsValid(), LOG_TAG_COPPER_NODE, "Invalid Update. ID="
                    VECTORID_LOG_FMT, VECTORID_LOG(_centroid_id));
        _bucket.DeleteLast();
        return update;
    }

    RetStatus Search(const Vector& query, size_t k,
                     std::vector<std::pair<VectorID, DTYPE>>& neighbours) override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        FatalAssert(k > 0, LOG_TAG_COPPER_NODE, "Number of neighbours should not be 0");
        FatalAssert(neighbours.size() <= k, LOG_TAG_COPPER_NODE,
                    "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_COPPER_NODE,
                 "Search: Node-Bucket=%s", _bucket.ToString().ToCStr());
        PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_COPPER_NODE, "Search: Neighbours before search");

        for (uint16_t i = 0; i < _bucket.Size(); ++i) {
            const VectorPair& vectorPair = _bucket[i];
            DTYPE distance = Distance(query, vectorPair.vec);
            neighbours.emplace_back(vectorPair.id, distance);
            std::push_heap<std::vector<std::pair<VectorID, DTYPE>>::iterator,
                           const DIST_ID_PAIR_SIMILARITY_INTERFACE&>(neighbours.begin(), neighbours.end(),
                                                                     *(_similarityComparator));
            if (neighbours.size() > k) {
                std::pop_heap<std::vector<std::pair<VectorID, DTYPE>>::iterator,
                              const DIST_ID_PAIR_SIMILARITY_INTERFACE&>(neighbours.begin(), neighbours.end(),
                                                                        *(_similarityComparator));
                neighbours.pop_back();
            }
        }
        PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_COPPER_NODE, "Search: Neighbours after search");

        FatalAssert(neighbours.size() <= k, LOG_TAG_COPPER_NODE,
            "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

        return RetStatus::Success();
    }

    VectorID CentroidID() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _centroid_id;
    }

    VectorID ParentID() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _parent_id;
    }

    uint16_t Size() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _bucket.Size();
    }

    bool IsFull() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _bucket.Size() == _bucket.Capacity();
    }

    bool IsAlmostEmpty() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _bucket.Size() == _min_size;
    }

    bool Contains(VectorID id) const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _bucket.Contains(id);
    }

    bool IsLeaf() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _centroid_id.IsLeaf();
    }

    uint8_t Level() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _centroid_id._level;
    }

    Vector ComputeCurrentCentroid() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, true);

        switch (_distanceAlg)
        {
        case DistanceType::L2_Distance:
            return L2::ComputeCentroid(static_cast<const VTYPE*>(_bucket.GetVectors()),
                                       _bucket.Size(), _bucket.Dimension());
        }
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_COPPER_NODE,
             "ComputeCurrentCentroid: Invalid distance type: %s", DISTANCE_TYPE_NAME[_distanceAlg]);
        return Vector(); // Return an empty vector if the distance type is invalid
    }

    inline uint16_t MinSize() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _min_size;
    }
    inline uint16_t MaxSize() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _bucket.Capacity();
    }
    inline uint16_t VectorDimention() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _bucket.Dimension();
    }

    inline const DIST_ID_PAIR_SIMILARITY_INTERFACE& GetSimilarityComparator(bool reverese = false) const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        if (reverese) {
            return *(_reverseSimilarityComparator);
        } else {
            return *(_similarityComparator);
        }
    }

    inline DTYPE Distance(const Vector& a, const Vector& b) const override {
        switch (_distanceAlg)
        {
        case DistanceType::L2_Distance:
            return L2::Distance(a, b, VectorDimention());
        }
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_COPPER_NODE,
             "Distance: Invalid distance type: %s", DISTANCE_TYPE_NAME[_distanceAlg]);
        return 0; // Return 0 if the distance type is invalid
    }

    String BucketToString() const override {
        return _bucket.ToString();
    }

    static inline size_t Bytes(uint16_t dim, uint16_t capacity) {
        return sizeof(CopperNode) + VectorSet::DataBytes(dim, capacity);
    }

protected:
    const VectorID _centroid_id;
    VectorID _parent_id;
    const ClusteringType _clusteringAlg;
    const DistanceType _distanceAlg;
    const uint16_t _min_size;
    const DIST_ID_PAIR_SIMILARITY_INTERFACE* const _similarityComparator;
    const DIST_ID_PAIR_SIMILARITY_INTERFACE* const _reverseSimilarityComparator;
    VectorSet _bucket;

TESTABLE;
};

class VectorIndex : public VectorIndexInterface {
public:
    VectorIndex(CopperAttributes attr) : core_attr(attr.core), leaf_min_size(attr.leaf_min_size),
                                         leaf_max_size(attr.leaf_max_size),
                                         internal_min_size(attr.internal_min_size),
                                         internal_max_size(attr.internal_max_size),
                                         split_internal(attr.split_internal), split_leaf(attr.split_leaf),
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
             VECTORID_LOG(_root), _levels, attr.ToString().ToCStr());
    }

    ~VectorIndex() override {
        RetStatus rs = RetStatus::Success();
        rs = _bufmgr.Shutdown();
        delete _similarityComparator;
        delete _reverseSimilarityComparator;
        CLOG(LOG_LEVEL_LOG, LOG_TAG_VECTOR_INDEX, "Shutdown Copper Index End: rs=%s", rs.Msg());
    }

    RetStatus Insert(const Vector& vec, VectorID& vec_id, uint16_t node_per_layer) override {
        FatalAssert(_root.IsValid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID.");
        FatalAssert(_root.IsCentroid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID -> root should be a centroid.");
        FatalAssert(vec.IsValid(), LOG_TAG_VECTOR_INDEX, "Invalid query vector.");

        FatalAssert(node_per_layer > 0, LOG_TAG_VECTOR_INDEX, "node_per_layer cannot be 0.");
        FatalAssert(_levels > 1, LOG_TAG_VECTOR_INDEX, "Height of the tree should be at least two but is %hhu.",
                    _levels);

        RetStatus rs = RetStatus::Success();

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
             "Insert BEGIN: Vector=%s, _size=%lu, _levels=%hhu",
             vec.ToString(core_attr.dimention).ToCStr(), _size, _levels);

        std::vector<std::pair<VectorID, DTYPE>> upper_layer, lower_layer;
        upper_layer.emplace_back(_root, 0);
        VectorID next = _root;
        while (next.IsCentroid()) {
            CHECK_VECTORID_IS_VALID(next, LOG_TAG_VECTOR_INDEX);
            // Get the next layer of nodes
            rs = SearchNodes(vec, upper_layer, lower_layer, next.IsInternalNode() ? node_per_layer : 1);
            FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Search nodes failed with error: %s", rs.Msg());
            FatalAssert(!lower_layer.empty(), LOG_TAG_VECTOR_INDEX, "Lower layer should not be empty.");
            FatalAssert((next.IsInternalNode() ? (lower_layer.size() <= node_per_layer) : (lower_layer.size() == 1)),
                        LOG_TAG_VECTOR_INDEX, "Lower layer size (%lu) is larger than %hu (%s).",
                        lower_layer.size(), (next.IsInternalNode() ? node_per_layer : 1),
                        (next.IsInternalNode() ? "internal k" : "leaf case"));
            FatalAssert(lower_layer.front().first._level == next._level - 1, LOG_TAG_VECTOR_INDEX,
                        "Lower layer first element level (%hhu) does not match expected level (%hhu).",
                        lower_layer.front().first._level, next._level - 1);
            next = lower_layer.front().first;
            upper_layer.swap(lower_layer);
        }

        CopperNode* leaf = static_cast<CopperNode*>(_bufmgr.GetNode(upper_layer.front().first));
        CHECK_NODE_IS_VALID(leaf, LOG_TAG_VECTOR_INDEX, false);
        FatalAssert(leaf->IsLeaf(), LOG_TAG_VECTOR_INDEX, "Next node should be a leaf but is " VECTORID_LOG_FMT,
                    VECTORID_LOG(next));

        if (leaf->IsFull()) {
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_VECTOR_INDEX, "Leaf is full.");
        }

        vec_id = RecordInto(vec, leaf);
        if (vec_id.IsValid()) {
            ++_size;
            if (leaf->IsFull()) {
                Split(leaf); // todo background job?
                // todo assert success
            }
        }
        else {
            rs = RetStatus::Fail(""); //todo
        }

        FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Insert failed with error: %s", rs.Msg());
        FatalAssert(_levels == _bufmgr.GetHeight(), LOG_TAG_VECTOR_INDEX,
                    "Levels mismatch: _levels=%hhu, _bufmgr.directory.size()=%lu", _levels, _bufmgr.GetHeight());

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
             "Insert END: Vector=%s, vec_id=" VECTORID_LOG_FMT ", _size=%lu, _levels=%hhu",
             vec.ToString(core_attr.dimention).ToCStr(), VECTORID_LOG(vec_id), _size, _levels);

        return rs;
    }

    RetStatus Delete(VectorID vec_id) override {
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Delete not implemented");

        RetStatus rs = RetStatus::Success();
        return rs;
    }

    RetStatus ApproximateKNearestNeighbours(const Vector& query, size_t k,
                                            uint16_t _internal_k, uint16_t _leaf_k,
                                            std::vector<std::pair<VectorID, DTYPE>>& neighbours,
                                            bool sort = true, bool sort_from_more_similar_to_less = true) override {

        FatalAssert(_root.IsValid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID.");
        FatalAssert(_root.IsCentroid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID -> root should be a centroid.");
        FatalAssert(query.IsValid(), LOG_TAG_VECTOR_INDEX, "Invalid query vector.");
        FatalAssert(k > 0, LOG_TAG_VECTOR_INDEX, "Number of neighbours cannot be 0.");
        FatalAssert(_levels > 1, LOG_TAG_VECTOR_INDEX, "Height of the tree should be at least two but is %hhu.",
                    _levels);

        FatalAssert(_internal_k > 0, LOG_TAG_VECTOR_INDEX, "Number of internal node neighbours cannot be 0.");
        FatalAssert(_leaf_k > 0, LOG_TAG_VECTOR_INDEX, "Number of leaf neighbours cannot be 0.");

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
             "ApproximateKNearestNeighbours BEGIN: query=%s, k=%lu, _internal_k=%hu, _leaf_k=%hu, index_size=%lu, "
             "num_levels=%hhu", query.ToString(core_attr.dimention).ToCStr(), k, _internal_k,
             _leaf_k, _size, _levels);

        RetStatus rs = RetStatus::Success();
        neighbours.clear();

        std::vector<std::pair<VectorID, DTYPE>> upper_layer, lower_layer;
        upper_layer.emplace_back(_root, 0);
        VectorID next = _root;
        while (next.IsCentroid()) {
            CHECK_VECTORID_IS_VALID(next, LOG_TAG_VECTOR_INDEX);
            // Get the next layer of nodes
            rs = SearchNodes(query, upper_layer, lower_layer, next.IsInternalNode() ? _internal_k : _leaf_k);
            FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Search nodes failed with error: %s", rs.Msg());
            FatalAssert(!lower_layer.empty(), LOG_TAG_VECTOR_INDEX, "Lower layer should not be empty.");
            FatalAssert((next.IsInternalNode() ? (lower_layer.size() <= _internal_k) : (lower_layer.size() <= _leaf_k)),
                        LOG_TAG_VECTOR_INDEX, "Lower layer size (%lu) is larger than %hu (%s).",
                        lower_layer.size(), (next.IsInternalNode() ? _internal_k : _leaf_k),
                        (next.IsInternalNode() ? "internal k" : "leaf case"));
            FatalAssert(lower_layer.front().first._level == next._level - 1, LOG_TAG_VECTOR_INDEX,
                        "Lower layer first element level (%hhu) does not match expected level (%hhu).",
                        lower_layer.front().first._level, next._level - 1);
            next = lower_layer.front().first;
            upper_layer.swap(lower_layer);
        }

        neighbours.swap(upper_layer);
        if (sort) {
            std::sort_heap<std::vector<std::pair<VectorID, DTYPE>>::iterator,
                           const DIST_ID_PAIR_SIMILARITY_INTERFACE&>(
                           neighbours.begin(), neighbours.end(),
                           sort_from_more_similar_to_less ? *(_similarityComparator) :
                                                            *(_reverseSimilarityComparator));
        }

        PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_VECTOR_INDEX, "ApproximateKNearestNeighbours End: neighbours=");

        return rs;
    }

    size_t Size() const override {
        return _size;
    }

    inline DTYPE Distance(const Vector& a, const Vector& b) const override {
        switch (core_attr.distanceAlg)
        {
        case DistanceType::L2_Distance:
            return L2::Distance(a, b, core_attr.dimention);
        }
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_COPPER_NODE,
             "Distance: Invalid distance type: %s", DISTANCE_TYPE_NAME[core_attr.distanceAlg]);
        return 0; // Return 0 if the distance type is invalid
    }

    inline size_t Bytes(bool is_internal_node) const override {
        return CopperNode::Bytes(core_attr.dimention, is_internal_node ? internal_max_size : leaf_max_size);
    }

protected:
    const CopperCoreAttributes core_attr;
    const uint16_t leaf_min_size;
    const uint16_t leaf_max_size;
    const uint16_t internal_min_size;
    const uint16_t internal_max_size;
    const uint16_t split_internal;
    const uint16_t split_leaf;
    const DIST_ID_PAIR_SIMILARITY_INTERFACE* const _similarityComparator;
    const DIST_ID_PAIR_SIMILARITY_INTERFACE* const _reverseSimilarityComparator;
    size_t _size;
    BufferManager _bufmgr;
    VectorID _root;
    uint64_t _levels;

    inline bool MoreSimilar(const DTYPE& a, const DTYPE& b) const {
        switch (core_attr.distanceAlg) {
        case DistanceType::L2_Distance:
            return L2::MoreSimilar(a, b);
        }
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_VECTOR_INDEX,
             "MoreSimilar: Invalid distance type: %s", DISTANCE_TYPE_NAME[core_attr.distanceAlg]);
        return false; // Return false if the distance type is invalid
    }

    RetStatus SearchNodes(const Vector& query,
                          const std::vector<std::pair<VectorID, DTYPE>>& upper_layer,
                          std::vector<std::pair<VectorID, DTYPE>>& lower_layer, size_t n) override {
        FatalAssert(n > 0, LOG_TAG_VECTOR_INDEX, "Number of nodes to search for should be greater than 0.");
        FatalAssert(!upper_layer.empty(), LOG_TAG_VECTOR_INDEX, "Upper layer should not be empty.");
        FatalAssert(query.IsValid(), LOG_TAG_VECTOR_INDEX, "Query vector is invalid.");

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
             "Search_Nodes BEGIN: query=%s, n=%lu, upper_layer_size=%lu, searching_level=%hhu",
             query.ToString(core_attr.dimention).ToCStr(), n, upper_layer.size(), upper_layer.front().first._level);

        RetStatus rs = RetStatus::Success();
        lower_layer.clear();
        lower_layer.reserve(n);
        uint16_t level = upper_layer.front().first._level;
        for (const std::pair<VectorID, DTYPE>& node_data : upper_layer) {
            VectorID node_id = node_data.first;

            CHECK_VECTORID_IS_VALID(node_id, LOG_TAG_VECTOR_INDEX);
            CHECK_VECTORID_IS_CENTROID(node_id, LOG_TAG_VECTOR_INDEX);
            FatalAssert(node_id._level == level, LOG_TAG_VECTOR_INDEX, "Node level mismatch: expected %hhu, got %hhu.",
                        level, node_id._level);

            CopperNode* node = static_cast<CopperNode*>(_bufmgr.GetNode(node_id));
            CHECK_NODE_IS_VALID(node, LOG_TAG_VECTOR_INDEX, false);

            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
                 "Search_Nodes: node_id=" VECTORID_LOG_FMT ", node_centroid=%s, node_type=%s",
                 VECTORID_LOG(node_id), _bufmgr.GetVector(node_id).ToString(core_attr.dimention).ToCStr(),
                 ((node->IsLeaf()) ? "Leaf" : "Internal"));

            rs = node->Search(query, n, lower_layer);
            FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Search failed at node " VECTORID_LOG_FMT " with err(%s).",
                        VECTORID_LOG(node_id), rs.Msg());
        }

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
             "Search_Nodes END: query=%s, n=%lu, upper_layer_size=%lu, searching_level=%hhu, "
             "lower_layer_size=%lu, lower_level=%hhu",
             query.ToString(core_attr.dimention).ToCStr(), n, upper_layer.size(),
             upper_layer.front().first._level, lower_layer.size(),
             lower_layer.front().first._level);

        return rs;
    }

    VectorID RecordInto(const Vector& vec, CopperNodeInterface* container_node,
                        CopperNodeInterface* node = nullptr) override {
        CHECK_NODE_IS_VALID(container_node, LOG_TAG_VECTOR_INDEX, false);
        FatalAssert(node == nullptr ||
            (node->CentroidID().IsValid() && node->ParentID() == INVALID_VECTOR_ID),
            LOG_TAG_VECTOR_INDEX, "Input node should not have a parent assigned to it.");
        FatalAssert(vec.IsValid(), LOG_TAG_VECTOR_INDEX, "Invalid vector.");

        RetStatus rs = RetStatus::Success();

        VectorID vector_id = INVALID_VECTOR_ID;
        if (node != nullptr) {
            vector_id = node->CentroidID();
            rs = node->AssignParent(container_node->CentroidID());
            FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed tp assign parent_id");
        }
        else {
            vector_id = _bufmgr.RecordVector(container_node->Level() - 1);
            CHECK_VECTORID_IS_VALID(vector_id, LOG_TAG_VECTOR_INDEX);
        }

        Address vec_add = container_node->Insert(vec, vector_id);
        FatalAssert(vec_add != INVALID_ADDRESS, LOG_TAG_VECTOR_INDEX, "Failed to insert vector into parent.");
        rs = _bufmgr.UpdateVectorAddress(vector_id, vec_add);
        FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update vector address for vector_id: "
                    VECTORID_LOG_FMT, VECTORID_LOG(vector_id));
        return vector_id;
    }

    RetStatus ExpandTree(CopperNodeInterface* root, const Vector& centroid) override {
        CHECK_NODE_IS_VALID(root, LOG_TAG_VECTOR_INDEX, false);
        FatalAssert(root->CentroidID() == _root, LOG_TAG_VECTOR_INDEX, "root should be the current root: input_root_id="
                    VECTORID_LOG_FMT "current root id=" VECTORID_LOG_FMT,
                    VECTORID_LOG(root->CentroidID()), VECTORID_LOG(_root));
        RetStatus rs = RetStatus::Success();
        VectorID new_root_id = _bufmgr.RecordRoot();
        CopperNode* new_root = static_cast<CopperNode*>(CreateNewNode(new_root_id));
        rs = _bufmgr.UpdateClusterAddress(new_root_id, new_root);
        FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update cluster address for new_root_id: "
                    VECTORID_LOG_FMT, VECTORID_LOG(new_root_id));
        rs = _bufmgr.UpdateVectorAddress(_root, new_root->Insert(centroid, _root));
        FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to update vector address for new_root_id: "
                    VECTORID_LOG_FMT, VECTORID_LOG(new_root_id));
        _root = new_root_id;
        rs = root->AssignParent(new_root_id);
        FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Failed to AssignParent for new_root. new_root_id="
                    VECTORID_LOG_FMT ", old_root_id=" VECTORID_LOG_FMT,
                    VECTORID_LOG(new_root_id), VECTORID_LOG(root->CentroidID()));
        ++_levels;
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
             "Expand_Tree: New root created with id=" VECTORID_LOG_FMT ", previous root id= " VECTORID_LOG_FMT,
             VECTORID_LOG(new_root_id), VECTORID_LOG(root->CentroidID()));
        // todo assert success of every operation
        return rs;
    }

    size_t FindClosestCluster(const std::vector<CopperNodeInterface*>& candidates, const Vector& vec) override {
        FatalAssert(!candidates.empty(), LOG_TAG_VECTOR_INDEX, "Candidates should not be empty.");
        FatalAssert(vec.IsValid(), LOG_TAG_VECTOR_INDEX, "Invalid vector.");
        CHECK_VECTORID_IS_VALID(candidates[0]->CentroidID(), LOG_TAG_VECTOR_INDEX);

        size_t best_idx = 0;
        Vector target = _bufmgr.GetVector(candidates[0]->CentroidID());
        FatalAssert(target.IsValid(), LOG_TAG_VECTOR_INDEX, "Target vector is invalid for candidate 0: "
                    VECTORID_LOG_FMT, VECTORID_LOG(candidates[0]->CentroidID()));
        DTYPE best_dist = Distance(vec, target);

        for (size_t i = 1; i < candidates.size(); ++i) { // todo check for memory leaks and stuff
            target.Unlink();
            CHECK_VECTORID_IS_VALID(candidates[i]->CentroidID(), LOG_TAG_VECTOR_INDEX);
            target = _bufmgr.GetVector(candidates[i]->CentroidID());
            FatalAssert(target.IsValid(), LOG_TAG_VECTOR_INDEX, "Target vector is invalid for candidate %lu: "
                        VECTORID_LOG_FMT, i, VECTORID_LOG(candidates[i]->CentroidID()));
            DTYPE tmp_dist = Distance(vec, target);
            if (MoreSimilar(tmp_dist, best_dist)) {
                best_idx = i;
                best_dist = tmp_dist;
            }
        }

        return best_idx;
    }

    RetStatus Split(std::vector<CopperNodeInterface*>& candidates, size_t node_idx) override {
        FatalAssert(candidates.size() > node_idx, LOG_TAG_VECTOR_INDEX, "candidates should contain node.");
        CopperNode* node = static_cast<CopperNode*>(candidates[node_idx]);
        CHECK_NODE_IS_VALID(node, LOG_TAG_VECTOR_INDEX, true);
        FatalAssert(node->IsFull(), LOG_TAG_VECTOR_INDEX, "node should be full.");

        RetStatus rs = RetStatus::Success();
        size_t last_size = candidates.size();
        std::vector<Vector> centroids;

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
             "Split BEGIN: " NODE_LOG_FMT, "Centroid:%s",
             NODE_PTR_LOG(node), centroids[0].ToString(core_attr.dimention).ToCStr());

        rs = Cluster(candidates, node_idx, centroids, split_leaf);
        FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "Clustering failed");
        FatalAssert(centroids[0].IsValid(), LOG_TAG_VECTOR_INDEX, "new centroid vector of base node is invalid");
        FatalAssert(candidates.size() > last_size, LOG_TAG_VECTOR_INDEX, "No new nodes were created.");
        FatalAssert(candidates.size() - last_size == centroids.size() - 1, LOG_TAG_VECTOR_INDEX, "Missmatch between number of created nodes and number of centroids. Number of created nodes:%lu, number of centroids:%lu",
            candidates.size() - last_size, centroids.size());

        if (node->CentroidID() == _root) {
            ExpandTree(node, centroids[0]);
        }
        else {
            _bufmgr.GetVector(node->CentroidID()).CopyFrom(centroids[0], core_attr.dimention);
        }

        std::vector<CopperNodeInterface*> parents;
        parents.push_back(_bufmgr.GetNode(node->CentroidID()));

        // we can skip node as at this point we only have one parent and we place it there for now
        for (size_t node_it = 1; node_it < centroids.size(); ++node_it) {
            FatalAssert(centroids[node_it].IsValid(), LOG_TAG_VECTOR_INDEX, "Centroid vector is invalid.");
            // find the best parent
            size_t closest = FindClosestCluster(parents, centroids[node_it]);

            if (parents[closest]->IsFull()) {
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_VECTOR_INDEX, "Node is Full. Node=" NODE_LOG_FMT,
                     NODE_PTR_LOG(parents[closest]));
            }

            RecordInto(centroids[node_it], parents[closest], candidates[node_it + last_size - 1]);

            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
                 "Split: Put Node " NODE_LOG_FMT, " Centroid:%s, into Parent " NODE_LOG_FMT,
                 " Parent_Centroid:%s", NODE_PTR_LOG(candidates[node_it + last_size - 1]),
                 centroids[node_it].ToString(core_attr.dimention).ToCStr(), NODE_PTR_LOG(parents[closest]),
                 _bufmgr.GetVector(parents[closest]->CentroidID()).ToString(core_attr.dimention).ToCStr());

            // todo assert ok
            if (parents[closest]->IsFull()) {
                rs = Split(parents, closest); // todo background job?
                FatalAssert(rs.IsOK(), LOG_TAG_VECTOR_INDEX, "");
            }
        }

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR_INDEX,
             "Split END: " NODE_LOG_FMT, "Centroid:%s",
             NODE_PTR_LOG(node),  _bufmgr.GetVector(node->CentroidID()).ToString(core_attr.dimention).ToCStr());
        return rs;
    }

    RetStatus Split(CopperNodeInterface* leaf) override {
        std::vector<CopperNodeInterface*> candids;
        candids.push_back(leaf);
        return Split(candids, 0);
    }

    inline CopperNodeInterface* CreateNewNode(VectorID id) override {
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_COPPER_NODE);
        CHECK_VECTORID_IS_CENTROID(id, LOG_TAG_COPPER_NODE);

        const bool is_internal_node = id.IsInternalNode();
        CopperNode* new_node = static_cast<CopperNode*>(malloc(Bytes(is_internal_node)));
        CHECK_NOT_NULLPTR(new_node, LOG_TAG_COPPER_NODE);

        CopperNodeAttributes attr;
        attr.core = core_attr;
        attr.similarityComparator = _similarityComparator;
        attr.reverseSimilarityComparator = _reverseSimilarityComparator;
        if (is_internal_node) {
            attr.max_size = internal_max_size;
            attr.min_size = internal_min_size;
        }
        else {
            attr.max_size = leaf_max_size;
            attr.min_size = leaf_min_size;
        }

        new (new_node) CopperNode(id, attr);
        CHECK_NODE_IS_VALID(new_node, LOG_TAG_COPPER_NODE, false);
        return new_node;
    }

    inline RetStatus SimpleDivideClustering(std::vector<CopperNodeInterface*>& nodes, size_t target_node_index,
                                           std::vector<Vector>& centroids, uint16_t split_into) {
        FatalAssert(nodes.size() > target_node_index, LOG_TAG_CLUSTERING, "nodes should contain node.");
        CopperNodeInterface* target = nodes[target_node_index];
        CHECK_NODE_IS_VALID(target, LOG_TAG_CLUSTERING, true);
        FatalAssert(split_into > 0, LOG_TAG_CLUSTERING, "split_into should be greater than 0.");

        RetStatus rs = RetStatus::Success();

        split_into = (target->MaxSize() / split_into < target->MinSize() ? target->MaxSize() / target->MinSize() :
                                                                        split_into);
        if (split_into < 2) {
            split_into = 2;
        }

        uint16_t num_vec_per_node = target->MaxSize() / split_into;
        uint16_t num_vec_rem = target->MaxSize() % split_into;
        nodes.reserve(nodes.size() + split_into - 1);
        centroids.reserve(split_into);
        centroids.emplace_back(nullptr);
        for (uint16_t i = num_vec_per_node + num_vec_rem; i < target->MaxSize(); ++i) {
            if ((i - num_vec_rem) % num_vec_per_node == 0) {
                VectorID vector_id = _bufmgr.RecordVector(target->Level());
                FatalAssert(vector_id.IsValid(), LOG_TAG_CLUSTERING, "Failed to record vector.");
                CopperNodeInterface* new_node = CreateNewNode(vector_id);
                FatalAssert(new_node != nullptr, LOG_TAG_CLUSTERING, "Failed to create sibling node.");
                nodes.emplace_back(new_node);
                _bufmgr.UpdateClusterAddress(vector_id, nodes.back());
            }
            VectorUpdate update = target->MigrateLastVectorTo(nodes.back());
            // todo check update is ok and everything is successfull

            _bufmgr.UpdateVectorAddress(update.vector_id, update.vector_data);
            _bufmgr.GetNode(update.vector_id)->AssignParent(nodes.back()->CentroidID());

            if ((i + 1 - num_vec_rem) % num_vec_per_node == 0) {
                centroids.emplace_back(nodes.back()->ComputeCurrentCentroid());
                CLOG(LOG_LEVEL_DEBUG, LOG_TAG_CLUSTERING,
                    "Simple Cluster: Created Node " NODE_LOG_FMT " Centroid:%s",
                    NODE_PTR_LOG(nodes.back()), centroids.back().ToString(target->VectorDimention()).ToCStr());
            }
        }
        centroids[0] = target->ComputeCurrentCentroid();

        return rs;
    }

    inline RetStatus Cluster(std::vector<CopperNodeInterface*>& nodes, size_t target_node_index,
                              std::vector<Vector>& centroids, uint16_t split_into) override {
        switch (core_attr.clusteringAlg)
        {
        case ClusteringType::SimpleDivide:
            return SimpleDivideClustering(nodes, target_node_index, centroids, split_into);
        }
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_VECTOR_INDEX,
             "Cluster: Invalid clustering type: %s", CLUSTERING_TYPE_NAME[core_attr.clusteringAlg]);
        return RetStatus::Fail("Invalid clustering type");
    }

TESTABLE;
};

};

#endif