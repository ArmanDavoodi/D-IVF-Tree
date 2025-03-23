#ifndef COPPER_H_
#define COPPER_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"

#include <algorithm>

namespace copper {

template <typename T, uint16_t _DIM, uint16_t _MIN_SIZE, uint16_t _MAX_SIZE, 
            typename DIST_TYPE, typename _DIST>
class Copper_Node {
static_assert(_MIN_SIZE > 0);
static_assert(_MAX_SIZE > _MIN_SIZE);
static_assert(_MAX_SIZE / 2 >= _MIN_SIZE);
static_assert(_DIM > 0);
public:

    struct _DIST_ID_PAIR_SIMILARITY {
        _DIST _cmp;
        
        _DIST_ID_PAIR_SIMILARITY(_DIST _d) : _cmp(_d) {}
        inline bool operator()(const std::pair<VectorID, DIST_TYPE>& a, const std::pair<VectorID, DIST_TYPE>& b) const {
            return _cmp(a.second, b.second);
        }
    };
    /* todo to be removed: buffer manager should be the creator of nodes*/
    // Copper_Node() {

    // }

    inline RetStatus Insert(const Vector<T, _DIM>& vec, VectorID vec_id) {
        AssertFatal(_bucket.Size() < _MAX_SIZE, LOG_TAG_DEFAULT, 
                    "Node is full: size=%hu, _MAX_SIZE=%hu", _bucket.Size(), _MAX_SIZE);

        _bucket.Insert(vec, vec_id);
        return RetStatus::Success();
    }

    inline RetStatus Delete(VectorID vec_id, VectorID& swapped_vec_id, Vector<T, _DIM>& swapped_vec) {
        AssertFatal(_bucket.Size() > _MIN_SIZE, LOG_TAG_DEFAULT, 
                    "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);

        _bucket.Delete(vec_id, swapped_vec_id, swapped_vec);
        return RetStatus::Success();
    }

    inline RetStatus MigrateTo(VectorID vec_id, Copper_Node<T, _DIM, _MIN_SIZE, _MAX_SIZE, DIST_TYPE, _DIST>* other, 
                               VectorID& swapped_vec_id, Vector<T, _DIM>& swapped_vec) {
        
        // todo
    }

    inline RetStatus MigrateLastVectorTo(Copper_Node<T, _DIM, _MIN_SIZE, _MAX_SIZE, DIST_TYPE, _DIST>* other) {

        // todo
    }

    inline RetStatus ApproximateKNearestNeighbours(const Vector<T, _DIM>& query, size_t k, uint16_t sample_size,
            std::vector<std::pair<VectorID, DIST_TYPE>>& neighbours) const {

        AssertFatal(k > 0, LOG_TAG_DEFAULT, "Number of neighbours should not be 0");
        AssertFatal(_bucket.Size() >= _MIN_SIZE, LOG_TAG_DEFAULT, 
            "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        AssertFatal(_bucket.Size() <= _MAX_SIZE, LOG_TAG_DEFAULT, 
            "Node has too many elements: size=%hu, _MAX_SIZE=%hu.", _bucket.Size(), _MAX_SIZE);

        AssertFatal(sample_size > 0, LOG_TAG_DEFAULT, 
                "sample_size cannot be 0");
    
        AssertFatal(neighbours.size() <= k, LOG_TAG_DEFAULT, 
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

        AssertFatal(neighbours.size() <= k, LOG_TAG_DEFAULT, 
            "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

        return RetStatus::Success();
    }
    
    inline VectorID Find_Nearest(const Vector<T, _DIM>& query) {
        AssertFatal(_bucket.Size() >= _MIN_SIZE, LOG_TAG_DEFAULT, 
            "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        AssertFatal(_bucket.Size() <= _MAX_SIZE, LOG_TAG_DEFAULT, 
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

    inline bool Is_Leaf() const {
        return _centroid_id._level == 1;
    }

    inline bool Is_Full() const {
        return _bucket.Size() >= _MAX_SIZE;
    }

    inline bool Is_Almost_Empty() const {
        return _bucket.Size() <= _MIN_SIZE;
    }

    inline uint8_t Level() const {
        return _centroid_id._level;
    }

    inline bool Contains(VectorID id) const {
        return _bucket.Contains(id)
    }

    inline Vector<T, _DIM> Compute_Current_Centroid() const {
        // todo
    }

protected:
    _DIST _dist;
    _DIST_ID_PAIR_SIMILARITY _more_similar{_dist};

    VectorID _centroid_id;
    Vector<T, _DIM> _centroid;
    VectorSet<T, _DIM, _MAX_SIZE> _bucket;

// friend class VectorIndex;
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
protected:
    typedef Copper_Node<T, _DIM, KI_MIN, KI_MAX, DIST_TYPE, _DIST> Internal_Node;
    typedef Copper_Node<T, _DIM, KL_MIN, KL_MAX, DIST_TYPE, _DIST> Leaf_Node;

    
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
    std::vector<VectorID> _last_id_per_level;
    uint8_t _levels;
    uint16_t _split_internal;
    uint16_t _split_leaf;

    inline Leaf_Node* Find_Leaf(const Vector<T, _DIM>& query) {
        AssertFatal(_root != INVALID_VECTOR_ID, LOG_TAG_DEFAULT, "Invalid root ID.");
        AssertFatal(_root.Is_Centroid(), LOG_TAG_DEFAULT, "Invalid root ID -> root should be a centroid.");
        AssertFatal(_levels > 1, LOG_TAG_DEFAULT, "Height of the tree should be at least two but is %hhu.", _levels);
        AssertFatal(_levels == (uint8_t)(_last_id_per_level.size()), LOG_TAG_DEFAULT, "_levels:%hhu dose not represent num levels:%lu.", _levels, _last_id_per_level.size());

        if (_root.Is_Leaf()) {
            return _bufmgr.Get_Leaf(_root);
        }
        VectorID next = _root;

        while (!next.Is_Leaf()) {
            AssertFatal(next != INVALID_VECTOR_ID, LOG_TAG_DEFAULT, "Invalid vector id:%lu", next._id);
            Internal_Node* node = _bufmgr.Get_Node(next);
            AssertFatal(node != nullptr, LOG_TAG_DEFAULT, "Invisible node with id %lu.", next._id);
            next = node->Find_Nearest(query);
        }

        AssertFatal(next != INVALID_VECTOR_ID, LOG_TAG_DEFAULT, "Invalid vector id:%lu", next._id);
        AssertFatal(next.Is_Leaf(), LOG_TAG_DEFAULT, "Invalid leaf vector id:%lu", next._id);
        return _bufmgr.Get_Leaf(next);
    }

    inline VectorID Next_ID(uint8_t level) {
        AssertFatal(_levels == _last_id_per_level.size(), LOG_TAG_DEFAULT, "Height info is corrupted: _levels=%hhu, Height=%lu.", _levels, _last_id_per_level.size());
        AssertFatal(_levels > 1, LOG_TAG_DEFAULT, "Height of the tree should be at least two but is %hhu.", _levels);
        AssertFatal(level < _levels, LOG_TAG_DEFAULT, "Input level(%hhu) should be less than height of the tree(%hhu)."
                , level, _levels);

        _last_id_per_level[level] = _last_id_per_level[level].Get_Next_ID();
        return _last_id_per_level[level];
    }
    
    inline Internal_Node* Internal_Split(Internal_Node* node, const Vector<T, _DIM>& vec) {
        
    }

    // TODO: a clustring algorithm implementation
    inline Leaf_Node* Split(Leaf_Node* node, const Vector<T, _DIM>& vec) {
        AssertFatal(node != nullptr, LOG_TAG_DEFAULT, "node should not be nullptr.");
        AssertFatal(node->Is_Full(), LOG_TAG_DEFAULT, "node should be full.");

        uint16_t split = (KL_MAX / _split_leaf < KL_MIN ? KL_MAX / KL_MIN : _split_leaf);
        if (split < 2) {
            split = 2;
        }

        uint16_t num_vec_per_node = KL_MAX / split;
        uint16_t num_vec_rem = KL_MAX % split;
        Leaf_Node* new_leaves[split] = {nullptr};
        new_leaves[0] = node;
        uint16_t leaf_idx = 0;
        Internal_Node* Parent = _bufmgr->Get_Parent(node->CentroidID());

        for (uint16_t i = num_vec_per_node + num_vec_rem; i < KL_MAX; ++i) {
            if ((i - num_vec_rem) % num_vec_per_node == 0) {
                leaf_idx++;
                AssertFatal(leaf_idx < split, LOG_TAG_DEFAULT, "Too many leaves.");
                VectorID leaf_id = Next_ID(1);
                // we should pass the centroid address to the new_leaf function
                // or we can remove the centroid vector from the nodes as we do not use them
                // we use buffer manager and their id to get the vector
                // also consumes less memory
                new_leaves[leaf_idx] = _bufmgr->New_Leaf(leaf_id);
            }
            node->MigrateLastVectorTo(new_leaves[leaf_idx]);
            if ((i + 1 - num_vec_rem) % num_vec_per_node == 0) {
                // compute centroid
                // insert to parent
                // update buffer -> parent address and centroid address
            }
        }

        // compute centroid for new_leaves[0] and update buffer

    }

public:

    inline RetStatus Insert(const Vector<T, _DIM>& vec, VectorID& vec_id) {
        vec_id = Next_ID(0); // vectors are at level 0
        Leaf_Node* leaf = Find_Leaf(vec);
        RetStatus rc = RetStatus::Success();
        AssertFatal(leaf != nullptr, LOG_TAG_DEFAULT, "Leaf not found.");
        
        if (leaf->Is_Full()) {
            // todo split
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "Split is not implemented.");
        }

        rc = leaf->Insert(vec, vec_id);
        if (rc.Is_OK()) {
            ++_size;
        }

        return rc;
    }

    inline RetStatus Delete(VectorID vec_id) {
        AssertFatal(_root != INVALID_VECTOR_ID, LOG_TAG_DEFAULT, "Invalid root ID.");
        AssertFatal(_root.Is_Centroid(), LOG_TAG_DEFAULT, "Invalid root ID -> root should be a centroid.");
        AssertFatal(vec_id != INVALID_VECTOR_ID, LOG_TAG_DEFAULT, "Invalid input vector id.");
        AssertFatal(!vec_id.Is_Centroid(), LOG_TAG_DEFAULT, "Invalid input vector id -> vector should not be a centroid.");
        AssertFatal(_levels > 1, LOG_TAG_DEFAULT, "Height of the tree should be at least two but is %hhu.", _levels);

        RetStatus rc = RetStatus::Success();
        Leaf_Node* leaf = _bufmgr->Get_Container_Leaf(vec_id);
        AssertFatal(leaf != nullptr, LOG_TAG_DEFAULT, "Container leaf not found.");

        if (leaf->Is_Almost_Empty()) {
            // todo handle merge
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "Merge is not implemented.");
        }

        Vector<T, _DIM> swapped_vector = Vector<T, _DIM>::NEW_INVALID();
        VectorID swapped_vector_id = INVALID_VECTOR_ID;

        rc = leaf->Delete(vec_id, swapped_vector_id, swapped_vector);
        if (!rc.Is_OK()) {
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "Recovery in case of delete failure not implemented.");
        }

        // todo handle the swapped vector
        CLOG(LOG_LEVEL_ERROR, LOG_TAG_NOT_IMPLEMENTED, "Handling swapped vectors is not implemented.");
        rc = RetStatus::Fail("Not implemented");
        return rc;
    }

    inline RetStatus ApproximateKNearestNeighbours(const Vector<T, _DIM>& query, size_t k,
                                        uint16_t _internal_k, uint16_t _leaf_k,
                                        std::vector<std::pair<VectorID, DIST_TYPE>>& neighbours, 
                                        uint16_t _internal_search = KI_MAX,
                                        uint16_t _leaf_search = KI_MAX, uint16_t _vector_search = KL_MAX, 
                                        bool sort = true, bool sort_from_more_similar_to_less = true) {

        AssertFatal(_root != INVALID_VECTOR_ID, LOG_TAG_DEFAULT, "Invalid root ID.");
        AssertFatal(_root.Is_Centroid(), LOG_TAG_DEFAULT, "Invalid root ID -> root should be a centroid.");
        AssertFatal(query.Is_Valid(), LOG_TAG_DEFAULT, "Invalid query vector.");
        AssertFatal(k > 0, LOG_TAG_DEFAULT, "Number of neighbours cannot be 0.");
        AssertFatal(_levels > 1, LOG_TAG_DEFAULT, "Height of the tree should be at least two but is %hhu.", _levels);

        AssertFatal(_internal_k > 0, LOG_TAG_DEFAULT, "Number of internal node neighbours cannot be 0.");
        AssertFatal(_leaf_k > 0, LOG_TAG_DEFAULT, "Number of leaf neighbours cannot be 0.");

        AssertFatal(_internal_search > 0, LOG_TAG_DEFAULT, "Internal search sample size cannot be 0.");
        AssertFatal(_leaf_search > 0, LOG_TAG_DEFAULT, "Leaf search sample size cannot be 0.");
        AssertFatal(_vector_search > 0, LOG_TAG_DEFAULT, "Vector search sample size cannot be 0.");
        
        std::vector<std::pair<VectorID, DIST_TYPE>> heap_stack;
        heap_stack.reserve(k + 1);
        neighbours.clear();
        neighbours.reserve(k + 1);

        RetStatus rc = RetStatus::Success();

        VectorID node_id = _root;
        uint8_t level = (uint8_t)node_id._level;
        Vector<T, _DIM> node_centroid_vector = _bufmgr->Get_Vector(node_id);
        AssertFatal(node_centroid_vector.Is_Valid(), LOG_TAG_DEFAULT, "Could not get vector with id %lu.", node_id._id);
        
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
                
                AssertFatal(dist_id_pair.first != INVALID_VECTOR_ID, LOG_TAG_DEFAULT,
                            "Invalid vector id at top of the stack.");
                AssertFatal(level == (uint8_t)(dist_id_pair.first._level), LOG_TAG_DEFAULT, 
                    "Level mismatch detected: level(%hhu) != dist_id_pair.level(%lu).",
                    level, dist_id_pair.first._level);

                // TODO pin and unpin for eviction in disaggregated setup
                Internal_Node* node = _bufmgr.Get_Node(dist_id_pair.first);
                AssertFatal(node != nullptr, LOG_TAG_DEFAULT, "Failed to get node with id %lu.",
                            dist_id_pair.first);
                rc = node->ApproximateKNearestNeighbours(query, search_k, search_sampling, neighbours, _more_similar);
                AssertFatal(rc.Is_OK(), LOG_TAG_DEFAULT, "ANN search failed at node(%lu) with err(%s).", 
                            dist_id_pair.first, rc.Msg());
            }

            std::swap(heap_stack, neighbours);
            --level;
        }

NEIGHBOUR_EXTRACTION:
        
        AssertFatal(!heap_stack.empty(), LOG_TAG_DEFAULT, "Heap stack should not be empty.");
        
        AssertFatal(heap_stack.top().first._level == 1ul, LOG_TAG_DEFAULT, 
                    "node level of the current stack top(%lu) is not 1", heap_stack.top().first._level);
        AssertFatal(level == 1hhu, LOG_TAG_DEFAULT, 
            "level(%hhu) is not 1", level);
        
        while (!heap_stack.empty()) {
            std::pop_heap(heap_stack.begin(), heap_stack.end(), _more_similar);
            std::pair<VectorID, DIST_TYPE> dist_id_pair = heap_stack.back();
            heap_stack.pop_back();
            
            AssertFatal(dist_id_pair.first != INVALID_VECTOR_ID, LOG_TAG_DEFAULT,
                "Invalid vector id at top of the stack.");
            AssertFatal(level == (uint8_t)(dist_id_pair.first._level), LOG_TAG_DEFAULT, 
                    "Level mismatch detected: level(%hhu) != dist_id_pair.level(%lu).",
                    level, dist_id_pair.first._level);


            Leaf_Node* leaf = _bufmgr.Get_Leaf(dist_id_pair.first);
            AssertFatal(leaf != nullptr, LOG_TAG_DEFAULT, "Failed to get leaf with id %lu.",
                            dist_id_pair.first);
            rc = leaf->ApproximateKNearestNeighbours(query, k, _vector_search, neighbours, _more_similar);
            AssertFatal(rc.Is_OK(), LOG_TAG_DEFAULT, "ANN search failed at leaf(%lu) with err(%s).", 
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
    
};

};

#endif