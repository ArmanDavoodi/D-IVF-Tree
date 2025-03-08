#ifndef COPPER_H_
#define COPPER_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"

#include <algorithm>

namespace copper {

template <typename T, uint16_t _DIM, uint16_t _MIN_SIZE, uint16_t _MAX_SIZE>
class Copper_Node {
static_assert(_MIN_SIZE > 0);
static_assert(_MAX_SIZE > _MIN_SIZE);
static_assert(_DIM > 0);
public:
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

    inline RetStatus ApproximateKNearestNeighbours(const Vector<T, _DIM>& query, size_t k, uint16_t sample_size,
            std::vector<std::pair<double, VectorID>>& neighbours, Reverse_Similarity<T, _DIM> _cmp) const {

        AssertFatal(k > 0, LOG_TAG_DEFAULT, "Number of neighbours should not be 0");
        AssertFatal(_bucket.Size() >= _MIN_SIZE, LOG_TAG_DEFAULT, 
            "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        AssertFatal(_bucket.Size() <= _MAX_SIZE, LOG_TAG_DEFAULT, 
            "Node has too many elements: size=%hu, _MAX_SIZE=%hu.", _bucket.Size(), _MAX_SIZE);

        AssertFatal(sample_size > 0, LOG_TAG_DEFAULT, 
                "sample_size cannot be 0");
    
        AssertFatal(neighbours.size() <= k, LOG_TAG_DEFAULT, 
            "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);
        
        if (sample_size > _bucket.Size()) {
            sample_size = _bucket.Size()
        }

        // TODO a more efficient and accurate sampling method should be implemented rather than only checking the n first elements
        // TODO after using a more efficient sampling method, we should also handle the case where there is no sampling
        for (uint16_t i = 0; i < sample_size; ++i) {
            const VectorPair<T, _DIM> vec = _bucket[i];
            double distance = (*_dist)(query, vec.vector);
            neighbours.emplace_back(distance vec.id);
            std::push_heap(neighbours.begin(), neighbours.end(), _cmp);
            if (neighbours.size() > k) {
                std::pop_heap(neighbours.begin(), neighbours.end(), _cmp)
                neighbours.pop_back();
            }
        }

        AssertFatal(neighbours.size() <= k, LOG_TAG_DEFAULT, 
            "Nummber of neighbours cannot be larger than k. # neighbours=%lu, k=%lu", neighbours.size(), k);

        return RetStatus::Success();
    }
 
    inline uint16_t Size() const {
        return _bucket.Size();
    }

    inline bool Is_Leaf() {
        return _centroid_id._level == 1;
    }

    inline bool Is_Full() {
        return _bucket.Size() == _MAX_SIZE;
    }

    inline bool Is_Almost_Empty() {
        return _bucket.Size() == _MIN_SIZE;
    }

    inline VectorID Find_Nearest(const Vector<T, _DIM>& query) {
        AssertFatal(_bucket.Size() >= _MIN_SIZE, LOG_TAG_DEFAULT, 
            "Node does not have enough elements: size=%hu, _MIN_SIZE=%hu.", _bucket.Size(), _MIN_SIZE);
        AssertFatal(_bucket.Size() <= _MAX_SIZE, LOG_TAG_DEFAULT, 
            "Node has too many elements: size=%hu, _MAX_SIZE=%hu.", _bucket.Size(), _MAX_SIZE);

        VectorPair<T, _DIM> best_vec = _bucket[0];
        double best_dist = (*_dist)(query, best_vec.vector);
        
        for (uint16_t i = 1; i < _bucket.Size(); ++i) {
            VectorPair<T, _DIM> next_vec = _bucket[i];
            double new_dist = (*_dist)(query, next_vec.vector);
            if ((*_dist)(new_dist, best_dist)) {
                best_dist = new_dist;
                best_vec = std::move(next_vec);
            }
        }

        return best_vec.id;
    }

    inline void* Get_Parent_Node() {
        return parent;
    }

    inline bool Is_Leaf() const {
        return _centroid_id.Is_Leaf();
    }

    inline uint8_t Level() const {
        return _centroid_id._level;
    }

protected:

    Distance<T, _DIM>* _dist;
    VectorID _centroid_id;
    Vector<T, _DIM> _centroid;
    VectorSet<T, _DIM, _MAX_SIZE> _bucket;
    void* parent;

// friend class VectorIndex;
};

template <typename T, uint16_t _DIM, uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX>
class VectorIndex {
static_assert(KI_MIN > 0);
static_assert(KI_MAX > KI_MIN);
static_assert(KL_MIN > 0);
static_assert(KL_MAX > KL_MIN);

protected:
    typedef Copper_Node<T, _DIM, KI_MIN, KI_MAX> Internal_Node;
    typedef Copper_Node<T, _DIM, KL_MIN, KL_MAX> Leaf_Node;

    Distance<T, _DIM>* _dist;
    size_t _size;
    Buffer_Manager<T, _DIM, KI_MIN, KI_MAX, KL_MIN, KL_MAX> _bufmgr;
    VectorID _root;
    std::vector<VectorID> _last_id_per_level;
    uint8_t _levels;

    inline Leaf_Node* Find_Leaf(const Vector<T, _DIM>& query) {
        AssertFatal(_root != INVALID_VECTOR_ID, LOG_TAG_DEFAULT, "Invalid root ID.");
        AssertFatal(_root.Is_Centroid(), LOG_TAG_DEFAULT, "Invalid root ID -> root should be a centroid.");
        AssertFatal(_levels > 1, LOG_TAG_DEFAULT, "Height of the tree should be at least two but is %hhu.", _levels);

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
                                        std::vector<VectorID>& neighbours, 
                                        uint16_t _internal_search = KI_MAX,
                                        uint16_t _leaf_search = KI_MAX, uint16_t _vector_search = KL_MAX, 
                                        bool sort = true) {

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
        
        Reverse_Similarity<T, _DIM> _rcmp{_dist};
        
        std::vector<std::pair<double, VectorID>> heap_stack;
        heap_stack.reserve(k + 1);
        neighbours.clear();
        neighbours.reserve(k + 1);

        RetStatus rc = RetStatus::Success();

        VectorID node_id;
        Internal_Node* node;
        uint8_t level;

        if (_root.Is_Leaf()) {
            Leaf_Node* root = _bufmgr.Get_Leaf(_root);
            AssertFatal(root != nullptr, LOG_TAG_DEFAULT, "Root vector is null.");

            rc = root->ApproximateKNearestNeighbours(query, k, _vector_search, neighbours, _rcmp);
            AssertFatal(rc.Is_OK(), LOG_TAG_DEFAULT, "ANN search failed at leaf root with err(%s).", rc.Msg());
            if (sort) {
                std::sort_heap(neighbours.begin(), neighbours.end(), Similarity<T, _DIM>{_dist})
            }
            return rc;
        }

        node_id = _root;
        node = _bufmgr.Get_Node(node_id);
        AssertFatal(node != nullptr, LOG_TAG_DEFAULT, "Failed to get node with id %lu.", node_id._id);
        heap_stack.push_back({(*_dist)(query, node), node_id});
        std::push_heap(heap_stack.begin(), heap_stack.end(), _rcmp);
        level = node_id._level;

        while (level > 1) {
            uint16_t search_k = _internal_k;
            uint16_t search_sampling = _internal_search;
            if (level == 2) { // todo unlikely
                search_k = _leaf_k;
                search_sampling = _leaf_search;
            }

            while (!heap_stack.empty()) {
                std::pop_heap(heap_stack.begin(), heap_stack.end(), _rcmp);
                std::pair<double, VectorID> dist_id_pair = heap_stack.back();
                heap_stack.pop_back();

                AssertFatal(level == dist_id_pair.second._level, LOG_TAG_DEFAULT, 
                    "Level mismatch detected: level(%hhu) != dist_id_pair.level(%hhu).",
                    level, dist_id_pair.second._level);

                AssertFatal(dist_id_pair.second != INVALID_VECTOR_ID, LOG_TAG_DEFAULT,
                            "Invalid vector id at top of the stack.");

                node = _bufmgr.Get_Node(dist_id_pair.second);
                AssertFatal(node != nullptr, LOG_TAG_DEFAULT, "Failed to get node with id %lu.",
                            dist_id_pair.second);
                rc = node->ApproximateKNearestNeighbours(query, search_k, search_sampling, neighbours, _rcmp);
                AssertFatal(rc.Is_OK(), LOG_TAG_DEFAULT, "ANN search failed at node(%lu) with err(%s).", 
                            dist_id_pair.second, rc.Msg());
            }

            std::swap(heap_stack, neighbours);
            --level;
        }
        
        AssertFatal(!heap_stack.empty(), LOG_TAG_DEFAULT, "Heap stack should not be empty.");
        
        AssertFatal(heap_stack.top().second._level == 1, LOG_TAG_DEFAULT, 
                    "node level of the current stack top(%hhu) is not 1", heap_stack.top().second._level);
        AssertFatal(level == 1, LOG_TAG_DEFAULT, 
            "level(%hhu) is not 1", level);

        while (!heap_stack.empty()) {
            std::pop_heap(heap_stack.begin(), heap_stack.end(), _rcmp);
            std::pair<double, VectorID> dist_id_pair = heap_stack.back();
            heap_stack.pop_back();

            AssertFatal(level == dist_id_pair.second._level, LOG_TAG_DEFAULT, 
                    "Level mismatch detected: level(%hhu) != dist_id_pair.level(%hhu).",
                    level, dist_id_pair.second._level);

            AssertFatal(dist_id_pair.second != INVALID_VECTOR_ID, LOG_TAG_DEFAULT,
                "Invalid vector id at top of the stack.");

            node = _bufmgr.Get_Leaf(dist_id_pair.second);
            AssertFatal(node != nullptr, LOG_TAG_DEFAULT, "Failed to get node with id %lu.",
                            dist_id_pair.second);
            rc = node->ApproximateKNearestNeighbours(query, k, _vector_search, neighbours, _rcmp);
            AssertFatal(rc.Is_OK(), LOG_TAG_DEFAULT, "ANN search failed at node(%lu) with err(%s).", 
                            dist_id_pair.second, rc.Msg());
        }
        
        if (sort) {
            std::sort_heap(neighbours.begin(), neighbours.end(), Similarity<T, _DIM>{_dist})
        }
        return rc;
    }

    inline size_t Size() const {
        return _size;
    }
    
};

};

#endif