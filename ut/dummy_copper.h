#ifndef COPPER_H_
#define COPPER_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"

#ifdef TESTING
namespace UT {
class Test;
};
#endif

namespace copper {

template <typename T, uint16_t _DIM, uint16_t _MIN_SIZE, uint16_t _MAX_SIZE,
            typename DIST_TYPE, typename _DIST>
class Copper_Node {
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

    Copper_Node(VectorID id) : _centroid_id(id), _parent_id(INVALID_VECTOR_ID) {}

    inline RetStatus Assign_Parent(VectorID parent_id) {
        return RetStatus::Success();
    }

    inline Address Insert(const Vector<T, _DIM>& vec, VectorID vec_id) {
        return INVALID_ADDRESS;
    }

    inline VectorUpdate MigrateLastVectorTo(Copper_Node<T, _DIM, _MIN_SIZE, _MAX_SIZE, DIST_TYPE, _DIST>* _dest) {
        VectorUpdate update;
        return update;
    }

    inline RetStatus ApproximateKNearestNeighbours(const Vector<T, _DIM>& query, size_t k, uint16_t sample_size,
            std::vector<std::pair<VectorID, DIST_TYPE>>& neighbours) const {
        return RetStatus::Success();
    }

    inline VectorID Find_Nearest(const Vector<T, _DIM>& query) {
        return INVALID_VECTOR_ID;
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
        return _centroid_id.Is_Leaf();
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
        return _dist.Compute_Centroid(_bucket.Get_Typed_Address(), _bucket.Size())
    }

protected:
    _DIST _dist;
    _DIST_ID_PAIR_SIMILARITY _more_similar{_dist};

    VectorID _centroid_id;
    VectorID _parent_id;
    VectorSet<T, _DIM, _MAX_SIZE> _bucket;

friend class UT::Test;
};

template <typename T, uint16_t _DIM,
            uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX,
            typename DIST_TYPE, typename _DIST>
class VectorIndex {
public:

    inline RetStatus Insert(const Vector<T, _DIM>& vec, VectorID& vec_id) {
        RetStatus rc = RetStatus::Success();
        return rc;
    }

    inline RetStatus Delete(VectorID vec_id) {

        RetStatus rc = RetStatus::Success();
        return rc;
    }

    inline RetStatus ApproximateKNearestNeighbours(const Vector<T, _DIM>& query, size_t k,
                                        uint16_t _internal_k, uint16_t _leaf_k,
                                        std::vector<std::pair<VectorID, DIST_TYPE>>& neighbours,
                                        uint16_t _internal_search = KI_MAX,
                                        uint16_t _leaf_search = KI_MAX, uint16_t _vector_search = KL_MAX,
                                        bool sort = true, bool sort_from_more_similar_to_less = true) {
        return RetStatus::Success();
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
        return nullptr;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline VectorID Record_Into(const Vector<T, _DIM>& vec, Node<_K_MIN, _K_MAX>* container_node,
                                Node<_K_MIN, _K_MAX>* node = nullptr) {
        return INVALID_VECTOR_ID;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline RetStatus Clustering(std::vector<Node<_K_MIN, _K_MAX>*>& nodes, size_t node_idx,
                                std::vector<Vector<T, _DIM>>& centroids) {
        RetStatus rs = RetStatus::Success();
        return rs;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline RetStatus Expand_Tree(Node<_K_MIN, _K_MAX>* root, const Vector<T, _DIM>& centroid) {
        RetStatus rs = RetStatus::Success();
        return rs;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline size_t Find_Closest_Cluster(const std::vector<Node<_K_MIN, _K_MAX>*>& candidates,
                                       const Vector<T, _DIM>& vec) {
        return 0;
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline RetStatus Split(std::vector<Node<_K_MIN, _K_MAX>*>& candidates, size_t node_idx) {
        return RetStatus::Success();
    }

    inline RetStatus Split(Leaf_Node* leaf) {
        return RetStatus::Success();
    }

friend class UT::Test;
};

};

#endif