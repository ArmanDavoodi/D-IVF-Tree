#ifndef COPPER_H_
#define COPPER_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"

#include <algorithm>

#ifdef TESTING
namespace UT {
class Test;
};
#endif

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
            typename DIST_TYPE, typename _DIST>
class Copper_Node {
static_assert(_MIN_SIZE > 0);
static_assert(_MAX_SIZE > _MIN_SIZE);
static_assert(_MAX_SIZE / 2 >= _MIN_SIZE);
static_assert(_DIM > 0);
public:
    static constexpr uint16_t _DIM_ = _DIM;
    static constexpr uint16_t _MIN_SIZE_ = _MIN_SIZE;
    static constexpr uint16_t _MAX_SIZE_ = _MAX_SIZE;

    struct _DIST_ID_PAIR_SIMILARITY {
        _DIST _cmp;

        _DIST_ID_PAIR_SIMILARITY(_DIST _d) : _cmp(_d) {}
        inline bool operator()(const std::pair<VectorID, DIST_TYPE>& a, const std::pair<VectorID, DIST_TYPE>& b) const {
            return _cmp(a.second, b.second);
        }
    };

    Copper_Node(VectorID id) : _centroid_id(id), _parent_id(INVALID_VECTOR_ID) {
        FatalAssert(id.Is_Valid(), LOG_TAG_COPPER_NODE, "Copper_Node(id = " VECTORID_LOG_FMT "): "
                    "cannot create node with invalid id", VECTORID_LOG(id));
        FatalAssert(id.Is_Centroid(), LOG_TAG_COPPER_NODE, "Copper_Node(" VECTORID_LOG_FMT "): non-centroid input id",
                    VECTORID_LOG(id));
    }

    inline RetStatus Assign_Parent(VectorID parent_id) {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Assign_Parent(self = " NODE_LOG_FMT
                    ", parent_id = " VECTORID_LOG_FMT "): Node does not have a valid centroid.", NODE_PTR_LOG(this),
                    VECTORID_LOG(parent_id));
        FatalAssert(_centroid_id.Is_Centroid(), LOG_TAG_COPPER_NODE, "Assign_Parent(self = " NODE_LOG_FMT
                    ", parent_id = " VECTORID_LOG_FMT "): Node is not a centroid.", NODE_PTR_LOG(this),
                    VECTORID_LOG(parent_id));
        FatalAssert(parent_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Assign_Parent(self = " NODE_LOG_FMT
                    ", parent_id = " VECTORID_LOG_FMT "): Cannot assign an invalid id to parent.", NODE_PTR_LOG(this),
                    VECTORID_LOG(parent_id));
        FatalAssert(parent_id._level == _centroid_id._level + 1, LOG_TAG_COPPER_NODE, "Assign_Parent(self = " NODE_LOG_FMT
            ", parent_id = " VECTORID_LOG_FMT "): Level mismatch between parent and self.", NODE_PTR_LOG(this),
            VECTORID_LOG(parent_id));

        _parent_id = parent_id;
        return RetStatus::Success();
    }

    inline Address Insert(const Vector<T, _DIM>& vec, VectorID vec_id) {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Node does not have a valid centroid.");
        FatalAssert(_centroid_id.Is_Centroid(), LOG_TAG_COPPER_NODE, "Node with id %lu is not a centroid.", _centroid_id._id);
        FatalAssert(_bucket.Size() < _MAX_SIZE, LOG_TAG_COPPER_NODE,
                    "Node is full: size=%hu, _MAX_SIZE=%hu", _bucket.Size(), _MAX_SIZE);
        FatalAssert(vec_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Cannot insert vector with invalid id into "
                    "the bucket with id %lu.", _centroid_id._id);
        FatalAssert(vec_id._level == _centroid_id._level - 1, LOG_TAG_COPPER_NODE, "Level mismatch! "
            "input vector: (id: %lu, level: %lu), centroid vector: (id: %lu, level: %lu)"
            , vec_id._id, vec_id._level, _centroid_id._id, _centroid_id._level);
        FatalAssert(vec.Is_Valid(), LOG_TAG_COPPER_NODE, "Cannot insert invalid vector %lu into the bucket with id %lu.",
                    vec_id._id, _centroid_id._id)

        return _bucket.Insert(vec, vec_id);
    }

    // inline RetStatus Delete(VectorID vec_id, VectorID& swapped_vec_id, Vector<T, _DIM>& swapped_vec) {
    //     FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Node does not have a valid centroid.");
    //     FatalAssert(_bucket.Size() > _MIN_SIZE, LOG_TAG_COPPER_NODE,
    //                 "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);

    //     _bucket.Delete(vec_id, swapped_vec_id, swapped_vec); // delete now returns an update instead
    //     return RetStatus::Success();
    // }

    inline VectorUpdate MigrateLastVectorTo(Copper_Node<T, _DIM, _MIN_SIZE, _MAX_SIZE, DIST_TYPE, _DIST>* _dest) {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Node does not have a valid centroid.");
        FatalAssert(_bucket.Size() >= _MIN_SIZE, LOG_TAG_COPPER_NODE,
            "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        FatalAssert(_dest != nullptr, LOG_TAG_COPPER_NODE, "destination node cannot be null");
        FatalAssert(_dest->_centroid_id.Is_Valid(), LOG_TAG_COPPER_NODE, "destination node cannot have invalid id");
        FatalAssert(_dest->Level() == _centroid_id._level, LOG_TAG_COPPER_NODE, "Level mismatch! "
                   "_dest = (id: %lu, level: %lu), self = (id: %lu, level: %lu)",
                   _dest->_centroid_id._id, _dest->_centroid_id._level, _centroid_id._id, _centroid_id._level);

        VectorUpdate update;
        update.vector_id = _bucket.Get_Last_VectorID();
        FatalAssert(update.vector_id.Is_Valid(), LOG_TAG_COPPER_NODE, "invalid vector id in bucket with id %lu.",
                    _centroid_id._id);
        const Vector<T, _DIM> &v = _bucket.Get_Last_Vector();
        FatalAssert(v.Is_Valid(), LOG_TAG_COPPER_NODE, "bucket's last vector is invalid. id=%lu", _centroid_id._id);
        update.vector_data = _dest->Insert(std::move(v), update.vector_id);
        FatalAssert(update.vector_data != INVALID_ADDRESS, LOG_TAG_COPPER_NODE,
                    "bucket's last vector is invalid. id=%lu", _centroid_id._id);
        _bucket.Delete_Last();
        return update;
    }

    inline RetStatus ApproximateKNearestNeighbours(const Vector<T, _DIM>& query, size_t k, uint16_t sample_size,
            std::vector<std::pair<VectorID, DIST_TYPE>>& neighbours) const {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Node does not have a valid centroid.");
        FatalAssert(k > 0, LOG_TAG_COPPER_NODE, "Number of neighbours should not be 0");
        FatalAssert(_bucket.Size() >= _MIN_SIZE, LOG_TAG_COPPER_NODE,
            "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        FatalAssert(_bucket.Size() <= _MAX_SIZE, LOG_TAG_COPPER_NODE,
            "Node has too many elements: size=%hu, _MAX_SIZE=%hu.", _bucket.Size(), _MAX_SIZE);

        FatalAssert(sample_size > 0, LOG_TAG_COPPER_NODE,
                "sample_size cannot be 0");

        FatalAssert(neighbours.size() <= k, LOG_TAG_COPPER_NODE,
            "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

        if (neighbours.size() < k && neighbours.size() - k > sample_size) {
            sample_size = neighbours.size() - k;
        }

        if (sample_size > _bucket.Size()) {
            sample_size = _bucket.Size()
        }

        // TODO a more efficient and accurate sampling method should be implemented rather than only checking the n first elements
        // TODO after using a more efficient sampling method, we should also handle the case where there is no sampling
        for (uint16_t i = 0; i < sample_size; ++i) {
            const VectorPair<T, _DIM> vec = _bucket[i];
            DIST_TYPE distance = _dist(query, vec.vector);
            neighbours.emplace_back(vec.id, distance);
            std::push_heap(neighbours.begin(), neighbours.end(), _more_similar);
            if (neighbours.size() > k) {
                std::pop_heap(neighbours.begin(), neighbours.end(), _more_similar)
                neighbours.pop_back();
            }
        }

        FatalAssert(neighbours.size() <= k, LOG_TAG_COPPER_NODE,
            "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

        return RetStatus::Success();
    }

    inline VectorID Find_Nearest(const Vector<T, _DIM>& query) {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Node does not have a valid centroid.");
        FatalAssert(_bucket.Size() >= _MIN_SIZE, LOG_TAG_COPPER_NODE,
            "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        FatalAssert(_bucket.Size() <= _MAX_SIZE, LOG_TAG_COPPER_NODE,
            "Node has too many elements: size=%hu, _MAX_SIZE=%hu.", _bucket.Size(), _MAX_SIZE);

        VectorPair<T, _DIM> best_vec = _bucket[0];
        DIST_TYPE best_dist = _dist(query, best_vec.vector);

        for (uint16_t i = 1; i < _bucket.Size(); ++i) {
            VectorPair<T, _DIM> next_vec = _bucket[i];
            DIST_TYPE new_dist = _dist(query, next_vec.vector);
            if (_dist(new_dist, best_dist)) {
                best_dist = new_dist;
                best_vec = std::move(next_vec);
            }
        }

        return best_vec.id;
    }

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
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Node does not have a valid centroid.");
        return _centroid_id.Is_Leaf();
    }

    inline bool Is_Full() const {
        return _bucket.Size() >= _MAX_SIZE;
    }

    inline bool Is_Almost_Empty() const {
        return _bucket.Size() <= _MIN_SIZE;
    }

    inline uint8_t Level() const {
        FatalAssert(_centroid_id.Is_Valid(), LOG_TAG_COPPER_NODE, "Node does not have a valid centroid.");
        return _centroid_id._level;
    }

    inline bool Contains(VectorID id) const {
        return _bucket.Contains(id)
    }

    inline Vector<T, _DIM> Compute_Current_Centroid() const {
        return _dist.Compute_Centroid(_bucket.Get_Typed_Address(), _bucket.Size())
    }

protected:
    _DIST _dist;
    _DIST_ID_PAIR_SIMILARITY _more_similar{_dist};

    VectorID _centroid_id;
    VectorID _parent_id;
    VectorSet<T, _DIM, _MAX_SIZE> _bucket;

// friend class VectorIndex;
friend class UT::Test;
};

template <typename T, uint16_t _DIM,
            uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX,
            typename DIST_TYPE, typename _DIST>
class VectorIndex {
static_assert(KI_MIN > 0);
static_assert(KI_MAX > KI_MIN);
static_assert(KI_MAX / 2 >= KI_MIN);
static_assert(KL_MIN > 0);
static_assert(KL_MAX > KL_MIN);
static_assert(KL_MAX / 2 >= KL_MIN);
static_assert(std::is_invocable_r_v<DIST_TYPE, _DIST(), const Vector<T, _DIM>&, const Vector<T, _DIM>&>,
              "_DIST must be callable on two vectors and return DIST_TYPE");
static_assert(std::is_invocable_r_v<bool(), _DIST(), const DIST_TYPE&, const DIST_TYPE&>,
              "_DIST must be callable on two distances and return bool");

public:

    inline RetStatus Init() {
        RetStatus rs = RetStatus::Success();
        _bufmgr.Init();
        _root = _bufmgr.Record_Root();
        FatalAssert(_root.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
        FatalAssert(_root.Is_Centroid(), LOG_TAG_VECTOR_INDEX, "root should be a centroid: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
        FatalAssert(_root.Is_Leaf(), LOG_TAG_VECTOR_INDEX, "first root should be a leaf: " VECTORID_LOG_FMT, VECTORID_LOG(_root));
        rs = _bufmgr.UpdateClusterAddress(_root, new Leaf_Node(_root));
        return rs;
    }

    inline RetStatus Shutdown() {
        RetStatus rs = RetStatus::Success();
        rs = _bufmgr.Shutdown();
        return rs;
    }

    inline RetStatus Insert(const Vector<T, _DIM>& vec, VectorID& vec_id) {
        Leaf_Node* leaf = Find_Leaf(vec);
        RetStatus rc = RetStatus::Success();
        FatalAssert(leaf != nullptr, LOG_TAG_VECTOR_INDEX, "Leaf not found.");

        if (leaf->Is_Full()) {
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_VECTOR_INDEX, "Leaf is full.");
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
            rc = RetStatus::Fail(""); //todo
        }

        return rc;
    }

    inline RetStatus Delete(VectorID vec_id) {
        // FatalAssert(_root.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID.");
        // FatalAssert(_root.Is_Centroid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID -> root should be a centroid.");
        // FatalAssert(vec_id.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Invalid input vector id.");
        // FatalAssert(!vec_id.Is_Centroid(), LOG_TAG_VECTOR_INDEX, "Invalid input vector id -> vector should not be a centroid.");
        // FatalAssert(_levels > 1, LOG_TAG_VECTOR_INDEX, "Height of the tree should be at least two but is %hhu.", _levels);

        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Delete not implemented");

        RetStatus rc = RetStatus::Success();
        // Leaf_Node* leaf = _bufmgr->Get_Container_Leaf(vec_id);
        // FatalAssert(leaf != nullptr, LOG_TAG_VECTOR_INDEX, "Container leaf not found.");

        // if (leaf->Is_Almost_Empty()) {
        //     // todo handle merge
        //     CLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "Merge is not implemented.");
        // }

        // Vector<T, _DIM> swapped_vector = Vector<T, _DIM>::NEW_INVALID();
        // VectorID swapped_vector_id = INVALID_VECTOR_ID;

        // rc = leaf->Delete(vec_id, swapped_vector_id, swapped_vector);
        // if (!rc.Is_OK()) {
        //     CLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "Recovery in case of delete failure not implemented.");
        // }

        // // todo handle the swapped vector
        // CLOG(LOG_LEVEL_ERROR, LOG_TAG_NOT_IMPLEMENTED, "Handling swapped vectors is not implemented.");
        // rc = RetStatus::Fail("Not implemented");
        return rc;
    }

    inline RetStatus ApproximateKNearestNeighbours(const Vector<T, _DIM>& query, size_t k,
                                        uint16_t _internal_k, uint16_t _leaf_k,
                                        std::vector<std::pair<VectorID, DIST_TYPE>>& neighbours,
                                        uint16_t _internal_search = KI_MAX,
                                        uint16_t _leaf_search = KI_MAX, uint16_t _vector_search = KL_MAX,
                                        bool sort = true, bool sort_from_more_similar_to_less = true) {

        FatalAssert(_root.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID.");
        FatalAssert(_root.Is_Centroid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID -> root should be a centroid.");
        FatalAssert(query.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Invalid query vector.");
        FatalAssert(k > 0, LOG_TAG_VECTOR_INDEX, "Number of neighbours cannot be 0.");
        FatalAssert(_levels > 1, LOG_TAG_VECTOR_INDEX, "Height of the tree should be at least two but is %hhu.", _levels);

        FatalAssert(_internal_k > 0, LOG_TAG_VECTOR_INDEX, "Number of internal node neighbours cannot be 0.");
        FatalAssert(_leaf_k > 0, LOG_TAG_VECTOR_INDEX, "Number of leaf neighbours cannot be 0.");

        FatalAssert(_internal_search > 0, LOG_TAG_VECTOR_INDEX, "Internal search sample size cannot be 0.");
        FatalAssert(_leaf_search > 0, LOG_TAG_VECTOR_INDEX, "Leaf search sample size cannot be 0.");
        FatalAssert(_vector_search > 0, LOG_TAG_VECTOR_INDEX, "Vector search sample size cannot be 0.");

        std::vector<std::pair<VectorID, DIST_TYPE>> heap_stack;
        heap_stack.reserve(k + 1);
        neighbours.clear();
        neighbours.reserve(k + 1);

        RetStatus rc = RetStatus::Success();

        VectorID node_id = _root;
        uint8_t level = (uint8_t)node_id._level;
        Vector<T, _DIM> node_centroid_vector = _bufmgr->Get_Vector(node_id);
        FatalAssert(node_centroid_vector.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Could not get vector with id %lu.", node_id._id);

        heap_stack.push_back({node_id, _dist(query, node_centroid_vector)});
        std::push_heap(heap_stack.begin(), heap_stack.end(), _more_similar);

        if (node_id.Is_Leaf()) {
            goto NEIGHBOUR_EXTRACTION;
        }

        while (level > 1) {
            uint16_t search_k = _internal_k;
            uint16_t search_sampling = _internal_search;

            if (level == 2) { // todo unlikely
                search_k = _leaf_k;
                search_sampling = _leaf_search;
            }

            while (!heap_stack.empty()) {
                std::pop_heap(heap_stack.begin(), heap_stack.end(), _more_similar);
                std::pair<VectorID, DIST_TYPE> dist_id_pair = heap_stack.back();
                heap_stack.pop_back();

                FatalAssert(dist_id_pair.first.Is_Valid(), LOG_TAG_VECTOR_INDEX,
                            "Invalid vector id at top of the stack.");
                FatalAssert(level == (uint8_t)(dist_id_pair.first._level), LOG_TAG_VECTOR_INDEX,
                    "Level mismatch detected: level(%hhu) != dist_id_pair.level(%lu).",
                    level, dist_id_pair.first._level);

                // TODO pin and unpin for eviction in disaggregated setup
                Internal_Node* node = _bufmgr.Get_Node(dist_id_pair.first);
                FatalAssert(node != nullptr, LOG_TAG_VECTOR_INDEX, "Failed to get node with id %lu.",
                            dist_id_pair.first);
                rc = node->ApproximateKNearestNeighbours(query, search_k, search_sampling, neighbours, _more_similar);
                FatalAssert(rc.Is_OK(), LOG_TAG_VECTOR_INDEX, "ANN search failed at node(%lu) with err(%s).",
                            dist_id_pair.first, rc.Msg());
            }

            std::swap(heap_stack, neighbours);
            --level;
        }

NEIGHBOUR_EXTRACTION:

        FatalAssert(!heap_stack.empty(), LOG_TAG_VECTOR_INDEX, "Heap stack should not be empty.");

        FatalAssert(heap_stack.top().first._level == 1ul, LOG_TAG_VECTOR_INDEX,
                    "node level of the current stack top(%lu) is not 1", heap_stack.top().first._level);
        FatalAssert(level == 1hhu, LOG_TAG_VECTOR_INDEX,
            "level(%hhu) is not 1", level);

        while (!heap_stack.empty()) {
            std::pop_heap(heap_stack.begin(), heap_stack.end(), _more_similar);
            std::pair<VectorID, DIST_TYPE> dist_id_pair = heap_stack.back();
            heap_stack.pop_back();

            FatalAssert(dist_id_pair.first.Is_Valid(), LOG_TAG_VECTOR_INDEX,
                "Invalid vector id at top of the stack.");
            FatalAssert(level == (uint8_t)(dist_id_pair.first._level), LOG_TAG_VECTOR_INDEX,
                    "Level mismatch detected: level(%hhu) != dist_id_pair.level(%lu).",
                    level, dist_id_pair.first._level);


            Leaf_Node* leaf = _bufmgr.Get_Leaf(dist_id_pair.first);
            FatalAssert(leaf != nullptr, LOG_TAG_VECTOR_INDEX, "Failed to get leaf with id %lu.",
                            dist_id_pair.first);
            rc = leaf->ApproximateKNearestNeighbours(query, k, _vector_search, neighbours, _more_similar);
            FatalAssert(rc.Is_OK(), LOG_TAG_VECTOR_INDEX, "ANN search failed at leaf(%lu) with err(%s).",
                            dist_id_pair.first, rc.Msg());
        }

        if (sort) {
            std::sort_heap(neighbours.begin(), neighbours.end(),
                            sort_from_more_similar_to_less ? _more_similar : _less_similar)
        }
        return rc;
    }

    inline size_t Size() const {
        return _size;
    }

protected:

    typedef Copper_Node<T, _DIM, KI_MIN, KI_MAX, DIST_TYPE, _DIST> Internal_Node;
    typedef Copper_Node<T, _DIM, KL_MIN, KL_MAX, DIST_TYPE, _DIST> Leaf_Node;
    template<uint16_t _K_MIN, uint16_t _K_MAX>
        requires((_K_MIN == KI_MIN && _K_MAX == KI_MAX) || (_K_MIN == KL_MIN && _K_MAX == KL_MAX))
    using Node = Copper_Node<T, _DIM, _K_MIN, _K_MAX, DIST_TYPE, _DIST>;

    struct _DIST_ID_PAIR_SIMILARITY {
        _DIST _cmp;

        _DIST_ID_PAIR_SIMILARITY(_DIST _d) : _cmp(_d) {}
        inline bool operator()(const std::pair<VectorID, DIST_TYPE>& a, const std::pair<VectorID, DIST_TYPE>& b) const {
            return _cmp(a.second, b.second);
        }
    };

    struct _DIST_ID_PAIR_REVERSE_SIMILARITY {
        _DIST _cmp;

        _DIST_ID_PAIR_REVERSE_SIMILARITY(_DIST _d) : _cmp(_d) {}
        inline bool operator()(const std::pair<VectorID, DIST_TYPE>& a, const std::pair<VectorID, DIST_TYPE>& b) const {
            return !_cmp(a.second, b.second);
        }
    };

    _DIST _dist;
    _DIST_ID_PAIR_SIMILARITY _more_similar{_dist};
    _DIST_ID_PAIR_REVERSE_SIMILARITY _less_similar{_dist};

    size_t _size;
    Buffer_Manager<T, _DIM, KI_MIN, KI_MAX, KL_MIN, KL_MAX, DIST_TYPE, _DIST> _bufmgr;
    VectorID _root;
    uint16_t _split_internal;
    uint16_t _split_leaf;

    inline Leaf_Node* Find_Leaf(const Vector<T, _DIM>& query) {
        FatalAssert(_root.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID.");
        FatalAssert(_root.Is_Centroid(), LOG_TAG_VECTOR_INDEX, "Invalid root ID -> root should be a centroid.");

        if (_root.Is_Leaf()) {
            return _bufmgr.Get_Leaf(_root);
        }
        VectorID next = _root;

        while (!next.Is_Leaf()) {
            FatalAssert(next.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Invalid vector id:%lu", next._id);
            Internal_Node* node = _bufmgr.Get_Node(next);
            FatalAssert(node != nullptr, LOG_TAG_VECTOR_INDEX, "Invisible node with id %lu.", next._id);
            next = node->Find_Nearest(query);
        }

        FatalAssert(next.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Invalid vector id:%lu", next._id);
        FatalAssert(next.Is_Leaf(), LOG_TAG_VECTOR_INDEX, "Invalid leaf vector id:%lu", next._id);
        return _bufmgr.Get_Leaf(next);
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline VectorID Record_Into(const Vector<T, _DIM>& vec, Node<_K_MIN, _K_MAX>* container_node,
                                Node<_K_MIN, _K_MAX>* node = nullptr) {
        FatalAssert(container_node != nullptr, LOG_TAG_VECTOR_INDEX, "No container node provided.");
        FatalAssert(container_node->CentroidID().Is_Valid(), LOG_TAG_VECTOR_INDEX, "container does not have a valid id.");
        FatalAssert(container_node->Level() >= VectorID::LEAF_LEVEL, LOG_TAG_VECTOR_INDEX, "Invalid container level.");
        FatalAssert(node == nullptr ||
            (node->CentroidID().Is_Valid() && node->ParentID() == INVALID_VECTOR_ID),
            LOG_TAG_VECTOR_INDEX, "Input node should not have a parent assigned to it.");
        FatalAssert(vec.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Invalid vector.");

        RetStatus rs = RetStatus::Success();


        VectorID vector_id = INVALID_VECTOR_ID;
        if (node != nullptr) {
            vector_id = node->CentroidID();
            rs = node->Assign_Parent(container_node->CentroidID());
            FatalAssert(rs.Is_OK(), LOG_TAG_VECTOR_INDEX, "Failed tp assign parent_id");
        }
        else {
            vector_id = _bufmgr.Record_Vector(container_node->Level() - 1huu);
            FatalAssert(vector_id.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Failed to record vector.");
        }

        Address vec_add = container_node->Insert(vec, vector_id);
        FatalAssert(vec_add != INVALID_ADDRESS, LOG_TAG_VECTOR_INDEX, "Failed to insert vector into parent.");
        _bufmgr.UpdateVectorAddress(vector_id, vec_add);
        return vector_id;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline RetStatus Clustering(std::vector<Node<_K_MIN, _K_MAX>*>& nodes, size_t node_idx,
                                std::vector<Vector<T, _DIM>>& centroids) {
        FatalAssert(nodes.size() > node_idx, LOG_TAG_VECTOR_INDEX, "nodes should contain node.");
        Node<_K_MIN, _K_MAX>* node = nodes[node_idx];
        FatalAssert(node != nullptr, LOG_TAG_VECTOR_INDEX, "node should not be nullptr.");
        FatalAssert(node->Is_Full(), LOG_TAG_VECTOR_INDEX, "node should be full.");

        RetStatus rs = RetStatus::Success();

        uint16_t split_into = (_K_MAX / _split_leaf < _K_MIN ? _K_MAX / _K_MIN : _split_leaf);
        if (split_into < 2) {
            split_into = 2;
        }

        uint16_t num_vec_per_node = _K_MAX / split_into;
        uint16_t num_vec_rem = _K_MAX % split_into;
        nodes.reserve(nodes.size() + split_into - 1);
        centroids.reserve(split_into);
        centroids.emplace_back(nullptr);
        for (uint16_t i = num_vec_per_node + num_vec_rem; i < _K_MAX; ++i) {
            if ((i - num_vec_rem) % num_vec_per_node == 0) {
                VectorID vector_id = _bufmgr.Record_Vector(node->Level());
                FatalAssert(vector_id.Is_Valid(), LOG_TAG_VECTOR_INDEX, "Failed to record vector.");
                nodes.emplace_back(new Node<_K_MIN, _K_MAX>(vector_id));
                _bufmgr.UpdateClusterAddress(vector_id, nodes.back());
            }
            VectorUpdate update = node->MigrateLastVectorTo(nodes.back());
            // todo check update is ok and everything is successfull

            _bufmgr.UpdateVectorAddress(update.vector_id, update.vector_data);
            if (update.vector_id.Is_Centroid()) {
                _bufmgr.Get_Node(update.vector_id)->Assign_Parent(nodes.back()->CentroidID());
                // todo check  successfull
            }

            if ((i + 1 - num_vec_rem) % num_vec_per_node == 0) {
                centroids.emplace_back(nodes.back()->Compute_Current_Centroid())
            }
        }
        centroids[0] = node->Compute_Current_Centroid();

        return rs;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline RetStatus Expand_Tree(Node<_K_MIN, _K_MAX>* root, const Vector<T, _DIM>& centroid) {
        // todo assert root is not nul and is indeed root and centroid is valid
        VectorID new_root_id = _bufmgr.Record_Root();
        Internal_Node* new_root = new Internal_Node(new_root_id);
        _bufmgr.UpdateClusterAddress(new_root_id, new_root);
        _bufmgr.UpdateVectorAddress(_root, new_root->Insert(centroid, _root));
        _root = new_root_id;
        root->Assign_Parent(new_root_id);
        // todo assert success of every operation
        // todo return
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline size_t Find_Closest_Cluster(const std::vector<Node<_K_MIN, _K_MAX>*>& candidates,
                                       const Vector<T, _DIM>& vec) {
        size_t best_idx = 0;
        DIST_TYPE best_dist = _dist(vec, _bufmgr.Get_Vector(candidates[0]->_centroid_id));
        // todo assert candidates[0]->_centroid_id and its vector

        for (size_t i = 1; i < candidates.size(); ++i) { // todo check for memory leaks and stuff
            DIST_TYPE tmp_dist = _dist(vec, _bufmgr.Get_Vector(candidates[i]->_centroid_id));
            // todo assert candidates[0]->_centroid_id and its vector
            if (_dist(tmp_dist, best_dist)) {
                best_idx = i;
                best_dist = tmp_dist;
            }
        }

        return best_idx;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline RetStatus Split(std::vector<Node<_K_MIN, _K_MAX>*>& candidates, size_t node_idx) {
        FatalAssert(candidates.size() > node_idx, LOG_TAG_VECTOR_INDEX, "candidates should contain node.");
        Node<_K_MIN, _K_MAX>* node = candidates[node_idx];
        FatalAssert(node != nullptr, LOG_TAG_VECTOR_INDEX, "node should not be nullptr.");
        FatalAssert(node->Is_Full(), LOG_TAG_VECTOR_INDEX, "node should be full.");

        RetStatus rs = RetStatus::Success();
        size_t last_size = candidates.size();
        std::vector<Vector<T, _DIM>> centroids;
        // if node is root, node_centroid vector will be invalid and a new vector will be created for node centroid
        // otherwise, node_centroid will only be updated
        Vector<T, _DIM> node_centroid = _bufmgr.Get_Vector(node->CentroidID());
        rs = Clustering<_K_MIN, _K_MAX>(candidates, node_idx, centroids);
        node_centroid = centroids[0];
        FatalAssert(rs.Is_OK(), LOG_TAG_VECTOR_INDEX, "Clustering failed");
        FatalAssert(node_centroid.Is_Valid(), LOG_TAG_VECTOR_INDEX, "new centroid vector of base node is invalid")
        FatalAssert(candidates.size() > last_size, LOG_TAG_VECTOR_INDEX, "No new nodes were created.");
        FatalAssert(candidates.size() - last_size == centroids.size() - 1, LOG_TAG_VECTOR_INDEX, "Missmatch between number of created nodes and number of centroids. Number of created nodes:%lu, number of centroids:%lu",
            candidates.size() - last_size, centroids.size());

        if (node->CentroidID() == _root) {
            Expand_Tree<_K_MIN, _K_MAX>(node, node_centroid);
        }

        std::vector<Internal_Node*> parents;
        parents.push_back(_bufmgr.Get_Node(node->_parent_id));

        // we can skip node as at this point we only have one parent and we place it there for now
        for (size_t node_it = 1; node_it < centroids.size(); ++node_it) {
            FatalAssert(centroids[node_it].Is_Valid(), LOG_TAG_VECTOR_INDEX, "Centroid vector is invalid.");
            // find the best parent
            size_t closest = Find_Closest_Cluster<_K_MIN, _K_MAX>(parents, centroids[node_it]);

            if (parents[closest]->Is_Full()) {
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_VECTOR_INDEX, "Node %lu is Full.", parents[closest]->_centroid_id)
            }

            Record_Into<_K_MIN, _K_MAX>(centroids[node_it], parents[closest], candidates[node_it + last_size - 1]);

            // todo assert ok
            if (parents[closest]->Is_Full()) {
                rs = Split(parents, closest); // todo background job?
                FatalAssert(rs.Is_OK(), LOG_TAG_VECTOR_INDEX, "");
            }
        }
        return rs;
    }

    inline RetStatus Split(Leaf_Node* leaf) {
        std::vector<Leaf_Node*> candids;
        candids.push_back(leaf);
        return Split<KL_MIN, KL_MAX>(candids, 0);
    }

friend class UT::Test;
};

};

#endif