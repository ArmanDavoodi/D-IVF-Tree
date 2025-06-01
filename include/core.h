#ifndef CORE_H_
#define CORE_H_

#include "common.h"
#include "vector_utils.h"

#include <vector>

namespace copper {

template<typename V_TYPE, uint16_t _DIM, typename D_TYPE = double>
class L2_Distance {
public:
    D_TYPE operator()(const Vector<V_TYPE, _DIM>& a, const Vector<V_TYPE, _DIM>& b) const {
        FatalAssert(a.Is_Valid(), LOG_TAG_BASIC, "a is invalid");
        FatalAssert(b.Is_Valid(), LOG_TAG_BASIC, "b is invalid");

        D_TYPE dist = 0;
        for (size_t i = 0; i < _DIM; ++i) {
            dist += ((D_TYPE)a[i] - (D_TYPE)b[i]) * ((D_TYPE)a[i] - (D_TYPE)b[i]);
        }
        return dist;
    }

    bool operator()(const D_TYPE& a, const D_TYPE& b) const {
        return a < b;
    }

    Vector<V_TYPE, _DIM> Compute_Centroid(const V_TYPE* vectors, size_t size) const {
        FatalAssert(size > 0, LOG_TAG_BASIC, "size cannot be 0");
        FatalAssert(vectors != nullptr, LOG_TAG_BASIC, "size cannot be 0");
        Vector<V_TYPE, _DIM> centroid(vectors);
        for (size_t v = 1; v < size; ++v) {
            for (uint16_t e = 0; e < _DIM; ++e) {
                centroid[e] += vectors[v * _DIM + e];
            }
        }

        for (uint16_t e = 0; e < _DIM; ++e) {
            centroid[e] /= size;
        }

        return centroid;
    }
};

template <typename DIST_TYPE, typename _DIST>
struct _DIST_ID_PAIR_SIMILARITY {
    _DIST _cmp;

    _DIST_ID_PAIR_SIMILARITY(_DIST _d) : _cmp(_d) {}
    inline bool operator()(const std::pair<VectorID, DIST_TYPE>& a, const std::pair<VectorID, DIST_TYPE>& b) const {
        return _cmp(a.second, b.second);
    }
};

template <typename DIST_TYPE, typename _DIST>
struct _DIST_ID_PAIR_REVERSE_SIMILARITY {
    _DIST _cmp;

    _DIST_ID_PAIR_REVERSE_SIMILARITY(_DIST _d) : _cmp(_d) {}
    inline bool operator()(const std::pair<VectorID, DIST_TYPE>& a, const std::pair<VectorID, DIST_TYPE>& b) const {
        return !_cmp(a.second, b.second);
    }
};

/*
 * Usually, a Core should not get a _DIST template as the clustering algorithm may depend on the _DIST itself
 * but in this case, Simple_Divide can be used with any distance function.
 */
template <typename T, uint16_t _DIM, typename DIST_TYPE, template<typename, uint16_t, typename> class _DIST>
class Simple_Divide { /* better naming */
static_assert(
    requires(const _DIST<T, _DIM, DIST_TYPE>& _dist, const Vector<T, _DIM>& a, const Vector<T, _DIM>& b) {
        { _dist(a, b) } -> std::same_as<DIST_TYPE>;
    },
    "_DIST must be callable on two vectors and return DIST_TYPE"
);
static_assert(
    requires(const _DIST<T, _DIM, DIST_TYPE>& _dist, const DIST_TYPE& a, const DIST_TYPE& b) {
        { _dist(a, b) } -> std::same_as<bool>;
    },
    "_DIST must be callable on two distances and return bool"
);

public:
    template<typename V_TYPE, uint16_t DIMENTION, typename D_TYPE>
    using CORE = Simple_Divide<V_TYPE, DIMENTION, D_TYPE, _DIST>;
    template<uint16_t _K_MIN, uint16_t _K_MAX>
    using Node = Copper_Node<T, _DIM, _K_MIN, _K_MAX, DIST_TYPE, CORE>;

    inline const _DIST_ID_PAIR_SIMILARITY<DIST_TYPE, _DIST<T, _DIM, DIST_TYPE>>& More_Similar_Comp() const {
        return _more_similar_cmp;
    }

    inline const _DIST_ID_PAIR_REVERSE_SIMILARITY<DIST_TYPE, _DIST<T, _DIM, DIST_TYPE>>& Less_Similar_Comp() const {
        return _less_similar_cmp;
    }

    inline DIST_TYPE Distance(const Vector<T, _DIM>& a, const Vector<T, _DIM>& b) const {
        return _dist(a, b);
    }

    inline bool More_Similar(const DIST_TYPE& a, const DIST_TYPE& b) const {
        return _dist(a, b);
    }

    inline Vector<T, _DIM> Compute_Centroid(const T* vectors, size_t size) const {
        return _dist.Compute_Centroid(vectors, size);
    }

    template<uint16_t _K_MIN, uint16_t _K_MAX>
    inline RetStatus Cluster(std::vector<Node<_K_MIN, _K_MAX>*>& nodes, size_t node_idx,
                             std::vector<Vector<T, _DIM>>& centroids, uint16_t _split_leaf,
                             Buffer_Manager<T, _DIM, _K_MIN, _K_MAX, _K_MIN, _K_MAX, DIST_TYPE, CORE>& _bufmgr) const {
        static_assert(_K_MIN > 0);
        static_assert(_K_MAX > _K_MIN);
        static_assert(_K_MAX / 2 >= _K_MIN);

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
                centroids.emplace_back(nodes.back()->Compute_Current_Centroid());
            }
        }
        centroids[0] = node->Compute_Current_Centroid();

        return rs;
    }

protected:
    _DIST<T, _DIM, DIST_TYPE> _dist;
    _DIST_ID_PAIR_SIMILARITY<DIST_TYPE, _DIST<T, _DIM, DIST_TYPE>> _more_similar_cmp{_dist};
    _DIST_ID_PAIR_REVERSE_SIMILARITY<DIST_TYPE, _DIST<T, _DIM, DIST_TYPE>> _less_similar_cmp{_dist};
};

template <typename T, uint16_t _DIM, typename DIST_TYPE = double>
using Simple_Divide_L2 = Simple_Divide<T, _DIM, DIST_TYPE, L2_Distance>;

};

#endif