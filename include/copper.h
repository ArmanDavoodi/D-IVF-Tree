#ifndef COPPER_H_
#define COPPER_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"
#include "core.h"

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

struct CopperNodeHeaderData {
    VectorID _centroid_id;
    VectorID _parent_id;
    ClusteringType _clusteringAlg;
    DistanceType _distanceAlg;
    uint16_t _min_size;
    DIST_ID_PAIR_SIMILARITY_INTERFACE* _similarityComparator;
    DIST_ID_PAIR_SIMILARITY_INTERFACE* _reverseSimilarityComparator;
    VectorSetHeader _bucket;
};

struct CopperNodeData {
    static DIST_ID_PAIR_SIMILARITY_INTERFACE* GetDistancePairSimilarityComparator(DistanceType distanceAlg,
                                                                                  bool reverse) {
        switch (distanceAlg) {
            case DistanceType::L2:
                return (reverse ?
                        static_cast<DIST_ID_PAIR_SIMILARITY_INTERFACE*>(new L2::DIST_ID_PAIR_REVERSE_SIMILARITY()) :
                        static_cast<DIST_ID_PAIR_SIMILARITY_INTERFACE*>(new L2::DIST_ID_PAIR_SIMILARITY()));
            default:
                FatalAssert(false, LOG_TAG_COPPER_NODE, "Invalid distance algorithm: %d", (int)distanceAlg);
        }
    }

    CopperNodeData(VectorID id, CopperNodeAttributes attr) : _centroid_id(id), _parent_id(INVALID_VECTOR_ID),
        _clusteringAlg(attr.core.clusteringAlg), _distanceAlg(attr.core.distanceAlg), _min_size(attr.min_size),
        _bucket(attr.core.dimention, attr.max_size),
        _similarityComparator(GetDistancePairSimilarityComparator(attr.core.distanceAlg, false)),
        _reverseSimilarityComparator(GetDistancePairSimilarityComparator(attr.core.distanceAlg, true)) {
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_COPPER_NODE);
        CHECK_VECTORID_IS_CENTROID(id, LOG_TAG_COPPER_NODE);
        CHECK_NODE_ATTRIBUTES(attr, LOG_TAG_COPPER_NODE);
    }

    void Destroy() {
        if (_similarityComparator != nullptr) {
            delete _similarityComparator;
        }
        if (_reverseSimilarityComparator != nullptr) {
            delete _reverseSimilarityComparator;
        }
    }

    const VectorID _centroid_id;
    VectorID _parent_id;
    const ClusteringType _clusteringAlg;
    const DistanceType _distanceAlg;
    const uint16_t _min_size;
    const DIST_ID_PAIR_SIMILARITY_INTERFACE* const _similarityComparator;
    const DIST_ID_PAIR_SIMILARITY_INTERFACE* const _reverseSimilarityComparator;
    VectorSet _bucket;
};

static_assert((sizeof(CopperNodeHeaderData) == sizeof(CopperNodeData)) ||
              ((sizeof(CopperNodeHeaderData) + sizeof(char[1])) == sizeof(CopperNodeData)));

class CopperNode : public CopperNodeInterface {
public:
    CopperNode(VectorID id, CopperNodeAttributes attr) : _data(id, attr) {}
    ~CopperNode() override {
        _data.Destroy();
    };

    RetStatus AssignParent(VectorID parent_id) override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        FatalAssert(parent_id._level == _data._centroid_id._level + 1, LOG_TAG_COPPER_NODE,
                    "Assign_Parent(self = " NODE_LOG_FMT ", parent_id = " VECTORID_LOG_FMT
                    "): Level mismatch between parent and self.", NODE_PTR_LOG(this), VECTORID_LOG(parent_id));

        _data._parent_id = parent_id;
        return RetStatus::Success();
    }

    Address Insert(const Vector& vec, VectorID vec_id) override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        CHECK_VECTORID_IS_VALID(vec_id, LOG_TAG_COPPER_NODE);
        FatalAssert(vec_id._level == _data._centroid_id._level - 1, LOG_TAG_COPPER_NODE, "Level mismatch! "
            "input vector: (id: %lu, level: %lu), centroid vector: (id: %lu, level: %lu)"
            , vec_id._id, vec_id._level, _data._centroid_id._id, _data._centroid_id._level);
        FatalAssert(vec.IsValid(), LOG_TAG_COPPER_NODE, "Cannot insert invalid vector %lu into the bucket with id %lu.",
                    vec_id._id, _data._centroid_id._id);
        Address addr = _data._bucket.Insert(vec, vec_id);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_COPPER_NODE,
                 "Insert: Node-Bucket=%s, Inserted VectorID=" VECTORID_LOG_FMT ", Vector=%s",
                 _data._bucket.ToString().ToCStr(), VECTORID_LOG(vec_id), vec.ToString(_data._bucket.Dimension()).ToCStr());
        return addr;
    }

    VectorUpdate MigrateLastVectorTo(CopperNodeInterface* _dest) override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, true);
        CHECK_NODE_IS_VALID(_dest, LOG_TAG_COPPER_NODE, false);
        FatalAssert(_dest->Level() == _data._centroid_id._level, LOG_TAG_COPPER_NODE, "Level mismatch! "
                   "_dest = (id: %lu, level: %lu), self = (id: %lu, level: %lu)",
                   _dest->CentroidID()._id, _dest->CentroidID()._level, _data._centroid_id._id, _data._centroid_id._level);

        VectorUpdate update;
        update.vector_id = _data._bucket.GetLastVectorID();
        update.vector_data = _dest->Insert(_data._bucket.GetLastVector(), update.vector_id);
        FatalAssert(update.IsValid(), LOG_TAG_COPPER_NODE, "Invalid Update. ID="
                    VECTORID_LOG_FMT, VECTORID_LOG(_data._centroid_id));
        _data._bucket.DeleteLast();
        return update;
    }

    RetStatus Search(const Vector& query, size_t k,
                     std::vector<std::pair<VectorID, DTYPE>>& neighbours) override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        FatalAssert(k > 0, LOG_TAG_COPPER_NODE, "Number of neighbours should not be 0");
        FatalAssert(neighbours.size() <= k, LOG_TAG_COPPER_NODE,
                    "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_COPPER_NODE,
                 "Search: Node-Bucket=%s", _data._bucket.ToString().ToCStr());
        PRINT_VECTOR_PAIR_BATCH(neighbours, LOG_TAG_COPPER_NODE, "Search: Neighbours before search");

        for (uint16_t i = 0; i < _data._bucket.Size(); ++i) {
            const VectorPair& vectorPair = _data._bucket[i];
            DTYPE distance = Distance(query, vectorPair.vec);
            neighbours.emplace_back(vectorPair.id, distance);
            std::push_heap<std::vector<std::pair<VectorID, DTYPE>>::iterator,
                           const DIST_ID_PAIR_SIMILARITY_INTERFACE&>(neighbours.begin(), neighbours.end(),
                                                                     *(_data._similarityComparator));
            if (neighbours.size() > k) {
                std::pop_heap<std::vector<std::pair<VectorID, DTYPE>>::iterator,
                              const DIST_ID_PAIR_SIMILARITY_INTERFACE&>(neighbours.begin(), neighbours.end(),
                                                                        *(_data._similarityComparator));
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
        return _data._centroid_id;
    }

    VectorID ParentID() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._parent_id;
    }

    uint16_t Size() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._bucket.Size();
    }

    bool IsFull() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._bucket.Size() == _data._bucket.Capacity();
    }

    bool IsAlmostEmpty() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._bucket.Size() == _data._min_size;
    }

    bool Contains(VectorID id) const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._bucket.Contains(id);
    }

    bool IsLeaf() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._centroid_id.IsLeaf();
    }

    uint8_t Level() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._centroid_id._level;
    }

    Vector ComputeCurrentCentroid() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, true);

        switch (_data._distanceAlg)
        {
        case DistanceType::L2:
            return L2::ComputeCentroid(static_cast<const VTYPE*>(_data._bucket.GetAddress()),
                                       _data._bucket.Size(), _data._bucket.Dimension());
        }
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_COPPER_NODE,
             "ComputeCurrentCentroid: Invalid distance type: %s", DISTANCE_TYPE_NAME[_data._distanceAlg]);
        return Vector(); // Return an empty vector if the distance type is invalid
    }

    inline uint16_t MinSize() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._min_size;
    }
    inline uint16_t MaxSize() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._bucket.Capacity();
    }
    inline uint16_t VectorDimention() const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        return _data._bucket.Dimension();
    }

    inline const DIST_ID_PAIR_SIMILARITY_INTERFACE& GetSimilarityComparator(bool reverese = false) const override {
        CHECK_NODE_SELF_IS_VALID(LOG_TAG_COPPER_NODE, false);
        if (reverese) {
            return *(_data._reverseSimilarityComparator);
        } else {
            return *(_data._similarityComparator);
        }
    }

    inline DTYPE Distance(const Vector& a, const Vector& b) const override {
        switch (_data._distanceAlg)
        {
        case DistanceType::L2:
            return L2::Distance(a, b, VectorDimention());
        }
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_COPPER_NODE,
             "Distance: Invalid distance type: %s", DISTANCE_TYPE_NAME[_data._distanceAlg]);
        return 0; // Return 0 if the distance type is invalid
    }

    inline size_t Bytes() const override {
        return sizeof(CopperNodeHeaderData) + sizeof(VTYPE) * _data._bucket.Dimension() * _data._bucket.Capacity();
    }

    String BucketToString() const override {
        return _data._bucket.ToString();
    }


    /* todo: instead have a createNode function in the vectorindex itself so we can also create a root node if needed */
    // inline CopperNodeInterface* CreateSibling(VectorID id) const override {
    //     CHECK_VECTORID_IS_VALID(id, LOG_TAG_COPPER_NODE);
    //     FatalAssert(id._level == _data._data._centroid_id._level, LOG_TAG_COPPER_NODE, "Mismatch level. selfID="
    //                 VECTORID_LOG_FMT ", newSiblingID=" VECTORID_LOG_FMT, VECTORID_LOG(_data._data._centroid_id),
    //                 VECTORID_LOG(id));

    //     CopperNode* sibling = static_cast<CopperNode*>(malloc(Bytes()));
    //     CHECK_NOT_NULLPTR(sibling, LOG_TAG_COPPER_NODE);

    //     CopperNodeAttributes attr;
    //     attr.core.clusteringAlg = _data._clusteringAlg;
    //     attr.core.distanceAlg = _data._distanceAlg;
    //     attr.core.dimention = _data._bucket.Dimension();
    //     attr.max_size = _data._bucket.Capacity();
    //     attr.min_size = _data._min_size;

    //     new (sibling) CopperNode(id, attr);
    //     return sibling;
    // }

protected:
    CopperNodeData _data;
// friend class VectorIndex;
TESTABLE;
};

class VectorIndex : public VectorIndexInterface {
public:

    RetStatus Init() {
        RetStatus rs = RetStatus::Success();
        _bufmgr.Init();
        _root = _bufmgr.Record_Root();
        FatalAssert(_root.Is_Valid(), LOG_TAG_VectorIndex, "Invalid root ID: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
        FatalAssert(_root.Is_Centroid(), LOG_TAG_VectorIndex, "root should be a centroid: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
        FatalAssert(_root.Is_Leaf(), LOG_TAG_VectorIndex, "first root should be a leaf: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
        rs = _bufmgr.UpdateClusterAddress(_root, new Leaf_Node(_root));
        FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "Failed to update cluster address for root: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
        _levels = 2;
        CLOG(LOG_LEVEL_LOG, LOG_TAG_VectorIndex,
             "Init Copper Index End: rootID= " VECTORID_LOG_FMT ", _levels = %hhu",
             VECTORID_LOG(_root), _levels);
        return rs;
    }

    RetStatus Shutdown() {
        RetStatus rs = RetStatus::Success();
        rs = _bufmgr.Shutdown();
        CLOG(LOG_LEVEL_LOG, LOG_TAG_VectorIndex, "Shutdown Copper Index End: rs=%s", rs.Msg());
        return rs;
    }

    RetStatus Insert(const Vector<T, _DIM>& vec, VectorID& vec_id, uint16_t node_per_layer) {
        FatalAssert(_root.Is_Valid(), LOG_TAG_VectorIndex, "Invalid root ID.");
        FatalAssert(_root.Is_Centroid(), LOG_TAG_VectorIndex, "Invalid root ID -> root should be a centroid.");
        FatalAssert(vec.Is_Valid(), LOG_TAG_VectorIndex, "Invalid query vector.");

        FatalAssert(node_per_layer > 0, LOG_TAG_VectorIndex, "node_per_layer cannot be 0.");
        FatalAssert(_levels > 1, LOG_TAG_VectorIndex, "Height of the tree should be at least two but is %hhu.", _levels);

        RetStatus rs = RetStatus::Success();

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
             "Insert BEGIN: Vector=%s, _size=%lu, _levels=%hhu",
             vec.to_string().c_str(), _size, _levels);

        std::vector<std::pair<VectorID, DTYPE>> upper_layer, lower_layer;
        upper_layer.emplace_back(_root, 0);
        VectorID next = _root;
        while (next.Is_Internal_Node()) {
            // Get the next layer of nodes
            rs = Search_Nodes<Internal_Node>(vec, upper_layer, lower_layer, node_per_layer);
            FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "Search nodes failed with error: %s", rs.Msg());
            FatalAssert(!lower_layer.empty(), LOG_TAG_VectorIndex, "Lower layer should not be empty.");
            FatalAssert(lower_layer.size() <= node_per_layer, LOG_TAG_VectorIndex,
                        "Lower layer size (%lu) is larger than internal k (%hu).", lower_layer.size(), node_per_layer);
            FatalAssert(lower_layer.fron().first._level == next._level - 1, LOG_TAG_VectorIndex,
                        "Lower layer first element level (%hhu) does not match expected level (%hhu).",
                        lower_layer.front().first._level, next._level - 1);
            next = lower_layer.front().first;
            upper_layer.swap(lower_layer);
        }

        FatalAssert(next.Is_Leaf(), LOG_TAG_VectorIndex, "Next node should be a leaf but is" VECTORID_LOG_FMT,
                    VECTORID_LOG(next));

        rs = Search_Nodes<Leaf_Node>(vec, upper_layer, lower_layer, 1);
        FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "Search nodes failed with error: %s", rs.Msg());
        FatalAssert(lower_layer.size() == 1, LOG_TAG_VectorIndex,
                    "Lower layer size (%lu) is not 1", lower_layer.size());

        Leaf_Node* leaf = _bufmgr.template Get_Node<Leaf_Node>(lower_layer.front().first);
        FatalAssert(leaf != nullptr, LOG_TAG_VectorIndex, "Leaf not found.");

        if (leaf->Is_Full()) {
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_VectorIndex, "Leaf is full.");
        }

        vec_id = Record_Into<KL_MIN, KL_MAX>(vec, leaf);
        if (vec_id.Is_Valid()) {
            ++_size;
            if (leaf->Is_Full()) {
                Split(leaf); // todo background job?
                // todo assert success
            }
        }
        else {
            rs = RetStatus::Fail(""); //todo
        }
        FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "Insert failed with error: %s", rs.Msg());
        FatalAssert(_levels == _bufmgr.Get_Height(), LOG_TAG_VectorIndex,
                    "Levels mismatch: _levels=%hhu, _bufmgr.directory.size()=%lu", _levels, _bufmgr.Get_Height());

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
             "Insert END: Vector=%s, vec_id=" VECTORID_LOG_FMT ", _size=%lu, _levels=%hhu",
             vec.to_string().c_str(), VECTORID_LOG(vec_id), _size, _levels);

        return rs;
    }

    RetStatus Delete(VectorID vec_id) {
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Delete not implemented");

        RetStatus rs = RetStatus::Success();
        return rs;
    }

    RetStatus ApproximateKNearestNeighbours(const Vector<T, _DIM>& query, size_t k,
                                        uint16_t _internal_k, uint16_t _leaf_k,
                                        std::vector<std::pair<VectorID, DTYPE>>& neighbours,
                                        bool sort = true, bool sort_from_more_similar_to_less = true) {

        FatalAssert(_root.Is_Valid(), LOG_TAG_VectorIndex, "Invalid root ID.");
        FatalAssert(_root.Is_Centroid(), LOG_TAG_VectorIndex, "Invalid root ID -> root should be a centroid.");
        FatalAssert(query.Is_Valid(), LOG_TAG_VectorIndex, "Invalid query vector.");
        FatalAssert(k > 0, LOG_TAG_VectorIndex, "Number of neighbours cannot be 0.");
        FatalAssert(_levels > 1, LOG_TAG_VectorIndex, "Height of the tree should be at least two but is %hhu.", _levels);

        FatalAssert(_internal_k > 0, LOG_TAG_VectorIndex, "Number of internal node neighbours cannot be 0.");
        FatalAssert(_leaf_k > 0, LOG_TAG_VectorIndex, "Number of leaf neighbours cannot be 0.");

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
             "ApproximateKNearestNeighbours BEGIN: query=%s, k=%lu, _internal_k=%hu, _leaf_k=%hu, index_size=%lu, num_levels=%hhu",
             query.to_string().c_str(), k, _internal_k, _leaf_k, _size, _levels);

        RetStatus rs = RetStatus::Success();
        neighbours.clear();

        std::vector<std::pair<VectorID, DTYPE>> upper_layer, lower_layer;
        upper_layer.emplace_back(_root, 0);
        VectorID next = _root;
        while (next.Is_Internal_Node()) {
            // Get the next layer of nodes
            rs = Search_Nodes<Internal_Node>(query, upper_layer, lower_layer, _internal_k);
            FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "Search nodes failed with error: %s", rs.Msg());
            FatalAssert(!lower_layer.empty(), LOG_TAG_VectorIndex, "Lower layer should not be empty.");
            FatalAssert(lower_layer.size() <= _internal_k, LOG_TAG_VectorIndex,
                        "Lower layer size (%lu) is larger than internal k (%hu).", lower_layer.size(), _internal_k);
            FatalAssert(lower_layer.fron().first._level == next._level - 1, LOG_TAG_VectorIndex,
                        "Lower layer first element level (%hhu) does not match expected level (%hhu).",
                        lower_layer.front().first._level, next._level - 1);
            next = lower_layer.front().first;
            upper_layer.swap(lower_layer);
        }

        FatalAssert(next.Is_Leaf(), LOG_TAG_VectorIndex, "Next node should be a leaf but is" VECTORID_LOG_FMT,
                    VECTORID_LOG(next));

        rs = Search_Nodes<Leaf_Node>(query, upper_layer, lower_layer, _leaf_k);
        FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "Search nodes failed with error: %s", rs.Msg());
        FatalAssert(!lower_layer.empty(), LOG_TAG_VectorIndex, "Lower layer should not be empty.");
        FatalAssert(lower_layer.size() <= _leaf_k, LOG_TAG_VectorIndex,
                    "Lower layer size (%lu) is larger than leaf k (%hu).", lower_layer.size(), _leaf_k);
        neighbours.swap(lower_layer);
        if (sort) {
            std::sort_heap(neighbours.begin(), neighbours.end(),
                            sort_from_more_similar_to_less ? *(_data._similarityComparator) : _core.Less_Similar_Comp());
        }

        PRINT_VECTOR_PAIR_BATCH(neighbours, "ApproximateKNearestNeighbours End: neighbours=");

        return rs;
    }

    size_t Size() const {
        return _size;
    }

protected:

    typedef CopperNode<T, _DIM, KI_MIN, KI_MAX, DTYPE, _CORE> Internal_Node;
    typedef CopperNode<T, _DIM, KL_MIN, KL_MAX, DTYPE, _CORE> Leaf_Node;
    template<uint16_t _K_MIN, uint16_t _K_MAX>
        requires((_K_MIN == KI_MIN && _K_MAX == KI_MAX) || (_K_MIN == KL_MIN && _K_MAX == KL_MAX))
    using Node = CopperNode<T, _DIM, _K_MIN, _K_MAX, DTYPE, _CORE>;

    _CORE<T, _DIM, DTYPE> _core; /* Todo: avoid copying _core */

    size_t _size;
    BufferManager<T, _DIM, KI_MIN, KI_MAX, KL_MIN, KL_MAX, DTYPE, _CORE> _bufmgr;
    VectorID _root;
    uint16_t _split_internal;
    uint16_t _split_leaf;
    uint64_t _levels;

    // Leaf_Node* Find_Leaf(const Vector<T, _DIM>& query) {
    //     FatalAssert(_root.Is_Valid(), LOG_TAG_VectorIndex, "Invalid root ID.");
    //     FatalAssert(_root.Is_Centroid(), LOG_TAG_VectorIndex, "Invalid root ID -> root should be a centroid.");

    //     if (_root.Is_Leaf()) {
    //         return _bufmgr.template Get_Node<Leaf_Node>(_root);
    //     }
    //     VectorID next = _root;

    //     while (!next.Is_Leaf()) {
    //         FatalAssert(next.Is_Valid(), LOG_TAG_VectorIndex, "Invalid vector id:%lu", next._id);
    //         Internal_Node* node = _bufmgr.template Get_Node<Internal_Node>(next);
    //         FatalAssert(node != nullptr, LOG_TAG_VectorIndex, "nullptr node with id %lu.", next._id);
    //         next = node->Find_Nearest(query);
    //     }

    //     FatalAssert(next.Is_Valid(), LOG_TAG_VectorIndex, "Invalid vector id:%lu", next._id);
    //     FatalAssert(next.Is_Leaf(), LOG_TAG_VectorIndex, "Invalid leaf vector id:%lu", next._id);
    //     return _bufmgr.template Get_Node<Leaf_Node>(next);
    // }

    template<typename NodeType>
    RetStatus Search_Nodes(const Vector<T, _DIM>& query,
                                  const std::vector<std::pair<VectorID, DTYPE>>& upper_layer,
                                  std::vector<std::pair<VectorID, DTYPE>>& lower_layer, size_t n) {
        static_assert(std::is_same<NodeType, Internal_Node>::value || std::is_same<NodeType, Leaf_Node>::value,
                  "NodeType must be either Internal_Node or Leaf_Node");
        FatalAssert(n > 0, LOG_TAG_VectorIndex, "Number of nodes to search for should be greater than 0.");
        FatalAssert(!upper_layer.empty(), LOG_TAG_VectorIndex, "Upper layer should not be empty.");
        FatalAssert(query.Is_Valid(), LOG_TAG_VectorIndex, "Query vector is invalid.");

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
             "Search_Nodes BEGIN: query=%s, n=%lu, upper_layer_size=%lu, searching_level=%hhu",
             query.to_string().c_str(), n, upper_layer.size(), upper_layer.front().first._level);

        RetStatus rs = RetStatus::Success();
        lower_layer.clear();
        lower_layer.reserve(n);
        uint16_t level = upper_layer.front().first._level;
        for (const std::pair<VectorID, DTYPE>& node_data : upper_layer) {
            VectorID node_id = node_data.first;

            FatalAssert(node_id.Is_Valid(), LOG_TAG_VectorIndex, "Invalid vector id:" VECTORID_LOG_FMT,
                        VECTORID_LOG(node_id));
            FatalAssert(node_id.Is_Centroid(), LOG_TAG_VectorIndex, "Node id should be a centroid: " VECTORID_LOG_FMT,
                        VECTORID_LOG(node_id));
            FatalAssert(node_id._level == level, LOG_TAG_VectorIndex, "Node level mismatch: expected %hhu, got %hhu.",
                        level, node_id._level);

            NodeType* node = _bufmgr.template Get_Node<NodeType>(node_id);
            FatalAssert(node != nullptr, LOG_TAG_VectorIndex, "nullptr node with id " VECTORID_LOG_FMT ".",
                        VECTORID_LOG(node_id));

            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
                 "Search_Nodes: node_id=" VECTORID_LOG_FMT ", node_centroid=%s, node_type=%s",
                 VECTORID_LOG(node_id), _bufmgr.Get_Vector_By_ID(node_id).to_string().c_str(),
                 ((node->Is_Leaf()) ? "Leaf" : "Internal"));

            rs = node->Search(query, n, lower_layer);
            FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "Search failed at node " VECTORID_LOG_FMT " with err(%s).",
                        VECTORID_LOG(node_id), rs.Msg());
        }

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
             "Search_Nodes END: query=%s, n=%lu, upper_layer_size=%lu, searching_level=%hhu, "
             "lower_layer_size=%lu, lower_level=%hhu",
             query.to_string().c_str(), n, upper_layer.size(), upper_layer.front().first._level, lower_layer.size(),
             lower_layer.front().first._level);

        return rs;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    VectorID Record_Into(const Vector<T, _DIM>& vec, Node<_K_MIN, _K_MAX>* container_node,
                                Node<_K_MIN, _K_MAX>* node = nullptr) {
        FatalAssert(container_node != nullptr, LOG_TAG_VectorIndex, "No container node provided.");
        FatalAssert(container_node->CentroidID().Is_Valid(), LOG_TAG_VectorIndex, "container does not have a valid id.");
        FatalAssert(container_node->Level() >= VectorID::LEAF_LEVEL, LOG_TAG_VectorIndex, "Invalid container level.");
        FatalAssert(node == nullptr ||
            (node->CentroidID().Is_Valid() && node->ParentID() == INVALID_VECTOR_ID),
            LOG_TAG_VectorIndex, "Input node should not have a parent assigned to it.");
        FatalAssert(vec.Is_Valid(), LOG_TAG_VectorIndex, "Invalid vector.");

        RetStatus rs = RetStatus::Success();


        VectorID vector_id = INVALID_VECTOR_ID;
        if (node != nullptr) {
            vector_id = node->CentroidID();
            rs = node->Assign_Parent(container_node->CentroidID());
            FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "Failed tp assign parent_id");
        }
        else {
            vector_id = _bufmgr.Record_Vector(container_node->Level() - 1);
            FatalAssert(vector_id.Is_Valid(), LOG_TAG_VectorIndex, "Failed to record vector.");
        }

        Address vec_add = container_node->Insert(vec, vector_id);
        FatalAssert(vec_add != INVALID_ADDRESS, LOG_TAG_VectorIndex, "Failed to insert vector into parent.");
        _bufmgr.UpdateVectorAddress(vector_id, vec_add);
        return vector_id;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    RetStatus Expand_Tree(Node<_K_MIN, _K_MAX>* root, const Vector<T, _DIM>& centroid) {
        // todo assert root is not nul and is indeed root and centroid is valid
        RetStatus rs = RetStatus::Success();
        VectorID new_root_id = _bufmgr.Record_Root();
        Internal_Node* new_root = new Internal_Node(new_root_id);
        _bufmgr.UpdateClusterAddress(new_root_id, new_root);
        _bufmgr.UpdateVectorAddress(_root, new_root->Insert(centroid, _root));
        _root = new_root_id;
        root->Assign_Parent(new_root_id);
        ++_levels;
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
             "Expand_Tree: New root created with id=" VECTORID_LOG_FMT ", previous root id= " VECTORID_LOG_FMT,
             VECTORID_LOG(new_root_id), VECTORID_LOG(root->CentroidID()));
        // todo assert success of every operation
        return rs;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    size_t Find_Closest_Cluster(const std::vector<Node<_K_MIN, _K_MAX>*>& candidates,
                                       const Vector<T, _DIM>& vec) {
        size_t best_idx = 0;
        DTYPE best_dist = _core.Distance(vec, _bufmgr.Get_Vector_By_ID(candidates[0]->_data._centroid_id));
        // todo assert candidates[0]->_data._centroid_id and its vector

        for (size_t i = 1; i < candidates.size(); ++i) { // todo check for memory leaks and stuff
            DTYPE tmp_dist = _core.Distance(vec, _bufmgr.Get_Vector_By_ID(candidates[i]->_data._centroid_id));
            // todo assert candidates[0]->_data._centroid_id and its vector
            if (_core.More_Similar(tmp_dist, best_dist)) {
                best_idx = i;
                best_dist = tmp_dist;
            }
        }

        return best_idx;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    RetStatus Split(std::vector<Node<_K_MIN, _K_MAX>*>& candidates, size_t node_idx) {
        FatalAssert(candidates.size() > node_idx, LOG_TAG_VectorIndex, "candidates should contain node.");
        Node<_K_MIN, _K_MAX>* node = candidates[node_idx];
        FatalAssert(node != nullptr, LOG_TAG_VectorIndex, "node should not be nullptr.");
        FatalAssert(node->Is_Full(), LOG_TAG_VectorIndex, "node should be full.");

        RetStatus rs = RetStatus::Success();
        size_t last_size = candidates.size();
        std::vector<Vector<T, _DIM>> centroids;
        // if node is root, node_centroid vector will be invalid and a new vector will be created for node centroid
        // otherwise, node_centroid will only be updated
        Vector<T, _DIM> node_centroid = _bufmgr.Get_Vector_By_ID(node->CentroidID());

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
             "Split BEGIN: " NODE_LOG_FMT, "Centroid:%s"
             NODE_PTR_LOG(node, true), node_centroid.to_string().c_str());

        rs = _core.Cluster<_K_MIN, _K_MAX>(candidates, node_idx, _split_leaf, centroids, _bufmgr);
        node_centroid = centroids[0];
        FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "Clustering failed");
        FatalAssert(node_centroid.Is_Valid(), LOG_TAG_VectorIndex, "new centroid vector of base node is invalid");
        FatalAssert(candidates.size() > last_size, LOG_TAG_VectorIndex, "No new nodes were created.");
        FatalAssert(candidates.size() - last_size == centroids.size() - 1, LOG_TAG_VectorIndex, "Missmatch between number of created nodes and number of centroids. Number of created nodes:%lu, number of centroids:%lu",
            candidates.size() - last_size, centroids.size());

        if (node->CentroidID() == _root) {
            Expand_Tree<_K_MIN, _K_MAX>(node, node_centroid);
        }

        std::vector<Internal_Node*> parents;
        parents.push_back(_bufmgr.template Get_Node<Internal_Node>(node->_data._parent_id));

        // we can skip node as at this point we only have one parent and we place it there for now
        for (size_t node_it = 1; node_it < centroids.size(); ++node_it) {
            FatalAssert(centroids[node_it].Is_Valid(), LOG_TAG_VectorIndex, "Centroid vector is invalid.");
            // find the best parent
            size_t closest = Find_Closest_Cluster<_K_MIN, _K_MAX>(parents, centroids[node_it]);

            if (parents[closest]->Is_Full()) {
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_VectorIndex, "Node %lu is Full.", parents[closest]->_data._centroid_id);
            }

            Record_Into<_K_MIN, _K_MAX>(centroids[node_it], parents[closest], candidates[node_it + last_size - 1]);

            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
                 "Split: Put Node " NODE_LOG_FMT, " Centroid:%s, into Parent " NODE_LOG_FMT,
                 " Parent_Centroid:%s", NODE_PTR_LOG(candidates[node_it + last_size - 1], true),
                 centroids[node_it].to_string().c_str(), Node_PTR_LOG(parents[closest], true),
                 _bufmgr.Get_Vector_By_ID(parents[closest]->CentroidID()).to_string().c_str());

            // todo assert ok
            if (parents[closest]->Is_Full()) {
                rs = Split(parents, closest); // todo background job?
                FatalAssert(rs.Is_OK(), LOG_TAG_VectorIndex, "");
            }
        }

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
             "Split END: " NODE_LOG_FMT, "Centroid:%s"
             NODE_PTR_LOG(node, true), node_centroid.to_string().c_str());
        return rs;
    }

    RetStatus Split(Leaf_Node* leaf) {
        std::vector<Leaf_Node*> candids;
        candids.push_back(leaf);
        return Split<KL_MIN, KL_MAX>(candids, 0);
    }

TESTABLE;
};

};

#endif