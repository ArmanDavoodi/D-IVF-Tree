#ifndef COPPER_H_
#define COPPER_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"
#include "core.h"

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

template <typename T, uint16_t _DIM, uint16_t _MIN_SIZE, uint16_t _MAX_SIZE,
          typename DIST_TYPE, template<typename, uint16_t, typename> class _CORE>
class CopperNode {
static_assert(_MIN_SIZE > 0);
static_assert(_MAX_SIZE > _MIN_SIZE);
static_assert(_MAX_SIZE / 2 >= _MIN_SIZE);
static_assert(_DIM > 0);
static_assert(
    requires(const _CORE<T, _DIM, DIST_TYPE>& core, const std::pair<VectorID, DIST_TYPE>& a,
             const std::pair<VectorID, DIST_TYPE>& b) {
        { core.More_Similar_Comp()(a, b) } -> std::same_as<bool>;
        { core.Less_Similar_Comp()(a, b) } -> std::same_as<bool>;
    },
    "_CORE must have a More_Similar_Comp() and Less_Similar_Comp() method returning a callable object accepting"
        "(const std::pair<VectorID, DIST_TYPE>&, const std::pair<VectorID, DIST_TYPE>&) and returning bool"
);
static_assert(
    requires(const _CORE<T, _DIM, DIST_TYPE>& core, const Vector<T, _DIM>& a, const Vector<T, _DIM>& b) {
        { core.Distance(a, b) } -> std::same_as<DIST_TYPE>;
    },
    "_CORE must have a Distance(const Vector<T, _DIM>&, const Vector<T, _DIM>&) method returning DIST_TYPE"
);
static_assert(
    requires(const _CORE<T, _DIM, DIST_TYPE>& core, const DIST_TYPE& a, const DIST_TYPE& b) {
        { core.More_Similar(a, b) } -> std::same_as<bool>;
    },
    "_CORE must have a More_Similar(const DIST_TYPE&, const DIST_TYPE&) method returning true if the first distance"
        " is less than the second one."
);
static_assert(
    requires(const _CORE<T, _DIM, DIST_TYPE>& core, const T* vectors, size_t size) {
        { core.Compute_Centroid(vectors, size) } -> std::same_as<Vector<T, _DIM>>;
    },
    "_CORE must have a More_Similar(const DIST_TYPE&, const DIST_TYPE&) method returning true if the first distance"
        " is less than the second one."
);

public:
    static constexpr uint16_t _DIM_ = _DIM;
    static constexpr uint16_t _MIN_SIZE_ = _MIN_SIZE;
    static constexpr uint16_t _MAX_SIZE_ = _MAX_SIZE;

    CopperNode(VectorID id) : _centroid_id(id), _parent_id(INVALID_VECTOR_ID) {
        FatalAssert(id.Is_Valid(), LOG_TAG_CopperNode, "CopperNode(id = " VECTORID_LOG_FMT "): "
                    "cannot create node with invalid id", VECTORID_LOG(id));
        FatalAssert(id.Is_Centroid(), LOG_TAG_CopperNode, "CopperNode(" VECTORID_LOG_FMT "): non-centroid input id",
                    VECTORID_LOG(id));
    }

    ~CopperNode() {
    }

    inline RetStatus Assign_Parent(VectorID parent_id) {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "Assign_Parent(self = " NODE_LOG_FMT
                    ", parent_id = " VECTORID_LOG_FMT "): Node does not have a valid centroid.", NODE_PTR_LOG(this, false),
                    VECTORID_LOG(parent_id));
        FatalAssert(_centroid_id.Is_Centroid(), LOG_TAG_CopperNode, "Assign_Parent(self = " NODE_LOG_FMT
                    ", parent_id = " VECTORID_LOG_FMT "): Node is not a centroid.", NODE_PTR_LOG(this, false),
                    VECTORID_LOG(parent_id));
        FatalAssert(parent_id.Is_Valid(), LOG_TAG_CopperNode, "Assign_Parent(self = " NODE_LOG_FMT
                    ", parent_id = " VECTORID_LOG_FMT "): Cannot assign an invalid id to parent.", NODE_PTR_LOG(this, false),
                    VECTORID_LOG(parent_id));
        FatalAssert(parent_id._level == _centroid_id._level + 1, LOG_TAG_CopperNode, "Assign_Parent(self = " NODE_LOG_FMT
            ", parent_id = " VECTORID_LOG_FMT "): Level mismatch between parent and self.", NODE_PTR_LOG(this, false),
            VECTORID_LOG(parent_id));

        _parent_id = parent_id;
        return RetStatus::Success();
    }

    inline Address Insert(const Vector<T, _DIM>& vec, VectorID vec_id) {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "Node does not have a valid centroid.");
        FatalAssert(_centroid_id.Is_Centroid(), LOG_TAG_CopperNode, "Node with id %lu is not a centroid.", _centroid_id._id);
        FatalAssert(_bucket.Size() < _MAX_SIZE, LOG_TAG_CopperNode,
                    "Node is full: size=%hu, _MAX_SIZE=%hu", _bucket.Size(), _MAX_SIZE);
        FatalAssert(vec_id.Is_Valid(), LOG_TAG_CopperNode, "Cannot insert vector with invalid id into "
                    "the bucket with id %lu.", _centroid_id._id);
        FatalAssert(vec_id._level == _centroid_id._level - 1, LOG_TAG_CopperNode, "Level mismatch! "
            "input vector: (id: %lu, level: %lu), centroid vector: (id: %lu, level: %lu)"
            , vec_id._id, vec_id._level, _centroid_id._id, _centroid_id._level);
        FatalAssert(vec.Is_Valid(), LOG_TAG_CopperNode, "Cannot insert invalid vector %lu into the bucket with id %lu.",
                    vec_id._id, _centroid_id._id);
        Address addr = _bucket.Insert(vec, vec_id);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_CopperNode,
                 "Insert: Node-Bucket=%s, Inserted VectorID=" VECTORID_LOG_FMT ", Vector=%s",
                 _bucket.to_string().c_str(), VECTORID_LOG(vec_id), vec.to_string().c_str());
        return addr;
    }

    // inline RetStatus Delete(VectorID vec_id, VectorID& swapped_vec_id, Vector<T, _DIM>& swapped_vec) {
    //     FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "Node does not have a valid centroid.");
    //     FatalAssert(_bucket.Size() > _MIN_SIZE, LOG_TAG_CopperNode,
    //                 "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);

    //     _bucket.Delete(vec_id, swapped_vec_id, swapped_vec); // delete now returns an update instead
    //     return RetStatus::Success();
    // }

    inline VectorUpdate MigrateLastVectorTo(CopperNode<T, _DIM, _MIN_SIZE, _MAX_SIZE, DIST_TYPE, _CORE>* _dest) {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "Node does not have a valid centroid.");
        FatalAssert(_bucket.Size() >= _MIN_SIZE, LOG_TAG_CopperNode,
            "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        FatalAssert(_dest != nullptr, LOG_TAG_CopperNode, "destination node cannot be null");
        FatalAssert(_dest->_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "destination node cannot have invalid id");
        FatalAssert(_dest->Level() == _centroid_id._level, LOG_TAG_CopperNode, "Level mismatch! "
                   "_dest = (id: %lu, level: %lu), self = (id: %lu, level: %lu)",
                   _dest->_centroid_id._id, _dest->_centroid_id._level, _centroid_id._id, _centroid_id._level);

        VectorUpdate update;
        update.vector_id = _bucket.Get_Last_VectorID();
        FatalAssert(update.vector_id.Is_Valid(), LOG_TAG_CopperNode, "invalid vector id in bucket with id %lu.",
                    _centroid_id._id);
        const Vector<T, _DIM> &v = _bucket.Get_Last_Vector();
        FatalAssert(v.Is_Valid(), LOG_TAG_CopperNode, "bucket's last vector is invalid. id=%lu", _centroid_id._id);
        update.vector_data = _dest->Insert(std::move(v), update.vector_id);
        FatalAssert(update.vector_data != INVALID_ADDRESS, LOG_TAG_CopperNode,
                    "bucket's last vector is invalid. id=%lu", _centroid_id._id);
        _bucket.Delete_Last();
        return update;
    }

    /* Do not make the function const or we have to use copy constructor each time we use operator[] on bucket */
    inline RetStatus Search(const Vector<T, _DIM>& query, size_t k,
                            std::vector<std::pair<VectorID, DIST_TYPE>>& neighbours) {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "Node does not have a valid centroid.");
        FatalAssert(k > 0, LOG_TAG_CopperNode, "Number of neighbours should not be 0");
        FatalAssert(_bucket.Size() >= _MIN_SIZE, LOG_TAG_CopperNode,
            "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        FatalAssert(_bucket.Size() <= _MAX_SIZE, LOG_TAG_CopperNode,
            "Node has too many elements: size=%hu, _MAX_SIZE=%hu.", _bucket.Size(), _MAX_SIZE);

        FatalAssert(neighbours.size() <= k, LOG_TAG_CopperNode,
            "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_CopperNode,
                 "Search: Node-Bucket=%s", _bucket.to_string().c_str());
        PRINT_VECTOR_PAIR_BATCH(neighbours, "Search: Neighbours before search");

        for (uint16_t i = 0; i < _bucket.Size(); ++i) {
            const VectorPair<T, _DIM> vec = _bucket[i];
            DIST_TYPE distance = _core.Distance(query, vec.vector);
            neighbours.emplace_back(vec.id, distance);
            std::push_heap(neighbours.begin(), neighbours.end(), _core.More_Similar_Comp());
            if (neighbours.size() > k) {
                std::pop_heap(neighbours.begin(), neighbours.end(), _core.More_Similar_Comp());
                neighbours.pop_back();
            }
        }
        PRINT_VECTOR_PAIR_BATCH(neighbours, "Search: Neighbours after search");

        FatalAssert(neighbours.size() <= k, LOG_TAG_CopperNode,
            "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

        return RetStatus::Success();
    }

    // inline VectorID Find_Nearest(const Vector<T, _DIM>& query) {
    //     FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "Node does not have a valid centroid.");
    //     FatalAssert(_bucket.Size() >= _MIN_SIZE, LOG_TAG_CopperNode,
    //         "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
    //     FatalAssert(_bucket.Size() <= _MAX_SIZE, LOG_TAG_CopperNode,
    //         "Node has too many elements: size=%hu, _MAX_SIZE=%hu.", _bucket.Size(), _MAX_SIZE);

    //     VectorPair<T, _DIM> best_vec = _bucket[0];
    //     DIST_TYPE best_dist = _core.Distance(query, best_vec.vector);

    //     for (uint16_t i = 1; i < _bucket.Size(); ++i) {
    //         VectorPair<T, _DIM> next_vec = _bucket[i];
    //         DIST_TYPE new_dist = _core.Distance(query, next_vec.vector);
    //         if (_core.More_Similar(new_dist, best_dist)) {
    //             best_dist = new_dist;
    //             best_vec = std::move(next_vec);
    //         }
    //     }

    //     return best_vec.id;
    // }

    inline uint16_t Size() const {
        return _bucket.Size();
    }

    inline VectorID CentroidID() const {
        return _centroid_id;
    }

    inline VectorID ParentID() const {
        return _parent_id;
    }

    inline bool Is_Leaf() const {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "Node does not have a valid centroid.");
        return _centroid_id.Is_Leaf();
    }

    inline bool Is_Full() const {
        return _bucket.Size() >= _MAX_SIZE;
    }

    inline bool Is_Almost_Empty() const {
        return _bucket.Size() <= _MIN_SIZE;
    }

    inline uint8_t Level() const {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "Node does not have a valid centroid.");
        return _centroid_id._level;
    }

    inline bool Contains(VectorID id) const {
        return _bucket.Contains(id);
    }

    inline Vector<T, _DIM> Compute_Current_Centroid() const {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_CopperNode, "Node does not have a valid centroid.");
        FatalAssert(_bucket.Size() >= _MIN_SIZE, LOG_TAG_CopperNode, "Node does not have enough elements. "
                    "size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        return _core.Compute_Centroid(_bucket.Get_Typed_Address(), _bucket.Size());
    }

    inline String bucket_to_string() const {
        return _bucket.to_string();
    }

protected:
    _CORE<T, _DIM, DIST_TYPE> _core;

    const VectorID _centroid_id;
    VectorID _parent_id;
    VectorSet<T, _DIM, _MAX_SIZE> _bucket;

// friend class VectorIndex;
TESTABLE;
};

template <typename T, uint16_t _DIM, uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX,
          typename DIST_TYPE, template<typename, uint16_t, typename> class _CORE>
class VectorIndex {
static_assert(KI_MIN > 0);
static_assert(KI_MAX > KI_MIN);
static_assert(KI_MAX / 2 >= KI_MIN);
static_assert(KL_MIN > 0);
static_assert(KL_MAX > KL_MIN);
static_assert(KL_MAX / 2 >= KL_MIN);
static_assert(
    requires(const _CORE<T, _DIM, DIST_TYPE>& core, const std::pair<VectorID, DIST_TYPE>& a,
             const std::pair<VectorID, DIST_TYPE>& b) {
        { core.More_Similar_Comp()(a, b) } -> std::same_as<bool>;
        { core.Less_Similar_Comp()(a, b) } -> std::same_as<bool>;
    },
    "_CORE must have a More_Similar_Comp() and Less_Similar_Comp() method returning a callable object accepting"
        "(const std::pair<VectorID, DIST_TYPE>&, const std::pair<VectorID, DIST_TYPE>&) and returning bool"
);
static_assert(
    requires(const _CORE<T, _DIM, DIST_TYPE>& core, const Vector<T, _DIM>& a, const Vector<T, _DIM>& b) {
        { core.Distance(a, b) } -> std::same_as<DIST_TYPE>;
    },
    "_CORE must have a Distance(const Vector<T, _DIM>&, const Vector<T, _DIM>&) method returning DIST_TYPE"
);
static_assert(
    requires(const _CORE<T, _DIM, DIST_TYPE>& core, const DIST_TYPE& a, const DIST_TYPE& b) {
        { core.More_Similar(a, b) } -> std::same_as<bool>;
    },
    "_CORE must have a More_Similar(const DIST_TYPE&, const DIST_TYPE&) method returning true if the first distance"
        " is less than the second one."
);
static_assert(
    requires(const _CORE<T, _DIM, DIST_TYPE>& core, const T* vectors, size_t size) {
        { core.Compute_Centroid(vectors, size) } -> std::same_as<Vector<T, _DIM>>;
    },
    "_CORE must have a More_Similar(const DIST_TYPE&, const DIST_TYPE&) method returning true if the first distance"
        " is less than the second one."
);
static_assert(
    requires(
        const _CORE<T, _DIM, DIST_TYPE>& core,
        std::vector<CopperNode<T, _DIM, KI_MIN, KI_MAX, DIST_TYPE, _CORE>*>& inodes,
        std::vector<CopperNode<T, _DIM, KL_MIN, KL_MAX, DIST_TYPE, _CORE>*>& lnodes,
        size_t node_idx, uint16_t _split_leaf,
        std::vector<Vector<T, _DIM>>& centroids,
        BufferManager<T, _DIM, KI_MIN, KI_MAX, KL_MIN, KL_MAX, DIST_TYPE, _CORE>& bufmgr
    ) {
        { core.Cluster<KI_MIN, KI_MAX>(inodes, node_idx, _split_leaf, centroids, bufmgr) } -> std::same_as<RetStatus>;
        { core.Cluster<KL_MIN, KL_MAX>(lnodes, node_idx, _split_leaf, centroids, bufmgr) } -> std::same_as<RetStatus>;
    },
    "_CORE must have a template function Cluster with two uint16_t template arguments (_K_MIN, _K_MAX) and signature: "
    "RetStatus Cluster(std::vector<Node<_K_MIN, _K_MAX>*>&, size_t, std::vector<Vector<T, _DIM>>&, BufferManager<T, _DIM, _K_MIN, _K_MAX, _K_MIN, _K_MAX, _CORE<T, _DIM, DIST_TYPE>>&) const"
);

public:

    inline RetStatus Init() {
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

    inline RetStatus Shutdown() {
        RetStatus rs = RetStatus::Success();
        rs = _bufmgr.Shutdown();
        CLOG(LOG_LEVEL_LOG, LOG_TAG_VectorIndex, "Shutdown Copper Index End: rs=%s", rs.Msg());
        return rs;
    }

    inline RetStatus Insert(const Vector<T, _DIM>& vec, VectorID& vec_id, uint16_t node_per_layer) {
        FatalAssert(_root.Is_Valid(), LOG_TAG_VectorIndex, "Invalid root ID.");
        FatalAssert(_root.Is_Centroid(), LOG_TAG_VectorIndex, "Invalid root ID -> root should be a centroid.");
        FatalAssert(vec.Is_Valid(), LOG_TAG_VectorIndex, "Invalid query vector.");

        FatalAssert(node_per_layer > 0, LOG_TAG_VectorIndex, "node_per_layer cannot be 0.");
        FatalAssert(_levels > 1, LOG_TAG_VectorIndex, "Height of the tree should be at least two but is %hhu.", _levels);

        RetStatus rs = RetStatus::Success();

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VectorIndex,
             "Insert BEGIN: Vector=%s, _size=%lu, _levels=%hhu",
             vec.to_string().c_str(), _size, _levels);

        std::vector<std::pair<VectorID, DIST_TYPE>> upper_layer, lower_layer;
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

    inline RetStatus Delete(VectorID vec_id) {
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Delete not implemented");

        RetStatus rs = RetStatus::Success();
        return rs;
    }

    inline RetStatus ApproximateKNearestNeighbours(const Vector<T, _DIM>& query, size_t k,
                                        uint16_t _internal_k, uint16_t _leaf_k,
                                        std::vector<std::pair<VectorID, DIST_TYPE>>& neighbours,
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

        std::vector<std::pair<VectorID, DIST_TYPE>> upper_layer, lower_layer;
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
                            sort_from_more_similar_to_less ? _core.More_Similar_Comp() : _core.Less_Similar_Comp());
        }

        PRINT_VECTOR_PAIR_BATCH(neighbours, "ApproximateKNearestNeighbours End: neighbours=");

        return rs;
    }

    inline size_t Size() const {
        return _size;
    }

protected:

    typedef CopperNode<T, _DIM, KI_MIN, KI_MAX, DIST_TYPE, _CORE> Internal_Node;
    typedef CopperNode<T, _DIM, KL_MIN, KL_MAX, DIST_TYPE, _CORE> Leaf_Node;
    template<uint16_t _K_MIN, uint16_t _K_MAX>
        requires((_K_MIN == KI_MIN && _K_MAX == KI_MAX) || (_K_MIN == KL_MIN && _K_MAX == KL_MAX))
    using Node = CopperNode<T, _DIM, _K_MIN, _K_MAX, DIST_TYPE, _CORE>;

    _CORE<T, _DIM, DIST_TYPE> _core; /* Todo: avoid copying _core */

    size_t _size;
    BufferManager<T, _DIM, KI_MIN, KI_MAX, KL_MIN, KL_MAX, DIST_TYPE, _CORE> _bufmgr;
    VectorID _root;
    uint16_t _split_internal;
    uint16_t _split_leaf;
    uint64_t _levels;

    // inline Leaf_Node* Find_Leaf(const Vector<T, _DIM>& query) {
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
    inline RetStatus Search_Nodes(const Vector<T, _DIM>& query,
                                  const std::vector<std::pair<VectorID, DIST_TYPE>>& upper_layer,
                                  std::vector<std::pair<VectorID, DIST_TYPE>>& lower_layer, size_t n) {
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
        for (const std::pair<VectorID, DIST_TYPE>& node_data : upper_layer) {
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
    inline VectorID Record_Into(const Vector<T, _DIM>& vec, Node<_K_MIN, _K_MAX>* container_node,
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
    inline RetStatus Expand_Tree(Node<_K_MIN, _K_MAX>* root, const Vector<T, _DIM>& centroid) {
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
    inline size_t Find_Closest_Cluster(const std::vector<Node<_K_MIN, _K_MAX>*>& candidates,
                                       const Vector<T, _DIM>& vec) {
        size_t best_idx = 0;
        DIST_TYPE best_dist = _core.Distance(vec, _bufmgr.Get_Vector_By_ID(candidates[0]->_centroid_id));
        // todo assert candidates[0]->_centroid_id and its vector

        for (size_t i = 1; i < candidates.size(); ++i) { // todo check for memory leaks and stuff
            DIST_TYPE tmp_dist = _core.Distance(vec, _bufmgr.Get_Vector_By_ID(candidates[i]->_centroid_id));
            // todo assert candidates[0]->_centroid_id and its vector
            if (_core.More_Similar(tmp_dist, best_dist)) {
                best_idx = i;
                best_dist = tmp_dist;
            }
        }

        return best_idx;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline RetStatus Split(std::vector<Node<_K_MIN, _K_MAX>*>& candidates, size_t node_idx) {
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
        parents.push_back(_bufmgr.template Get_Node<Internal_Node>(node->_parent_id));

        // we can skip node as at this point we only have one parent and we place it there for now
        for (size_t node_it = 1; node_it < centroids.size(); ++node_it) {
            FatalAssert(centroids[node_it].Is_Valid(), LOG_TAG_VectorIndex, "Centroid vector is invalid.");
            // find the best parent
            size_t closest = Find_Closest_Cluster<_K_MIN, _K_MAX>(parents, centroids[node_it]);

            if (parents[closest]->Is_Full()) {
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_VectorIndex, "Node %lu is Full.", parents[closest]->_centroid_id);
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

    inline RetStatus Split(Leaf_Node* leaf) {
        std::vector<Leaf_Node*> candids;
        candids.push_back(leaf);
        return Split<KL_MIN, KL_MAX>(candids, 0);
    }

TESTABLE;
};

};

#endif