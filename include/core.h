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
template <typename T, uint16_t _DIM, typename DIST_TYPE = double>
class Simple_Divide_L2 { /* better naming */
static_assert(
    requires(const L2_Distance<T, _DIM, DIST_TYPE>& _dist, const Vector<T, _DIM>& a, const Vector<T, _DIM>& b) {
        { _dist(a, b) } -> std::same_as<DIST_TYPE>;
    },
    "L2_Distance must be callable on two vectors and return DIST_TYPE"
);
static_assert(
    requires(const L2_Distance<T, _DIM, DIST_TYPE>& _dist, const DIST_TYPE& a, const DIST_TYPE& b) {
        { _dist(a, b) } -> std::same_as<bool>;
    },
    "L2_Distance must be callable on two distances and return bool"
);

public:
    // template<typename V_TYPE, uint16_t DIMENTION, typename D_TYPE>
    // using CORE = Simple_Divide<V_TYPE, DIMENTION, D_TYPE, _DIST>;
    template<uint16_t _K_MIN, uint16_t _K_MAX>
    using Node = CopperNode<T, _DIM, _K_MIN, _K_MAX, DIST_TYPE, Simple_Divide_L2>;

    inline const _DIST_ID_PAIR_SIMILARITY<DIST_TYPE, L2_Distance<T, _DIM, DIST_TYPE>>& More_Similar_Comp() const {
        return _more_similar_cmp;
    }

    inline const _DIST_ID_PAIR_REVERSE_SIMILARITY<DIST_TYPE, L2_Distance<T, _DIM, DIST_TYPE>>& Less_Similar_Comp() const {
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

    template<typename NodeType, uint16_t _KI_MIN, uint16_t _KI_MAX, uint16_t _KL_MIN, uint16_t _KL_MAX>
    inline RetStatus Cluster(std::vector<NodeType*>& nodes, size_t node_idx,
                             std::vector<Vector<T, _DIM>>& centroids, uint16_t split_into,
                             BufferManager<T, _DIM, _KI_MIN, _KI_MAX, _KL_MIN, _KL_MAX, DIST_TYPE, Simple_Divide_L2>& _bufmgr)
                             const {

        static_assert(
            std::is_same_v<NodeType, Node<_KI_MIN, _KI_MAX>> || std::is_same_v<NodeType, Node<_KL_MIN, _KL_MAX>>,
            "NodeType must be either Node<_KI_MIN, _KI_MAX> or Node<_KL_MIN, _KL_MAX>"
        );

        FatalAssert(nodes.size() > node_idx, LOG_TAG_VectorIndex, "nodes should contain node.");
        NodeType* node = nodes[node_idx];
        FatalAssert(node != nullptr, LOG_TAG_VectorIndex, "node should not be nullptr.");
        FatalAssert(node->Is_Full(), LOG_TAG_VectorIndex, "node should be full.");
        FatalAssert(split_into > 0, LOG_TAG_VectorIndex, "split_into should be greater than 0.");

        RetStatus rs = RetStatus::Success();

        split_into = (NodeType::_MAX_SIZE_ / split_into < NodeType::_MIN_SIZE_ ?
                      NodeType::_MAX_SIZE_ / NodeType::_MIN_SIZE_ : split_into);
        if (split_into < 2) {
            split_into = 2;
        }

        uint16_t num_vec_per_node = NodeType::_MAX_SIZE_ / split_into;
        uint16_t num_vec_rem = NodeType::_MAX_SIZE_ % split_into;
        nodes.reserve(nodes.size() + split_into - 1);
        centroids.reserve(split_into);
        centroids.emplace_back(nullptr);
        for (uint16_t i = num_vec_per_node + num_vec_rem; i < NodeType::_MAX_SIZE_; ++i) {
            if ((i - num_vec_rem) % num_vec_per_node == 0) {
                VectorID vector_id = _bufmgr.Record_Vector(node->Level());
                FatalAssert(vector_id.Is_Valid(), LOG_TAG_VectorIndex, "Failed to record vector.");
                nodes.emplace_back(new NodeType(vector_id));
                _bufmgr.UpdateClusterAddress(vector_id, nodes.back());
            }
            VectorUpdate update = node->MigrateLastVectorTo(nodes.back());
            // todo check update is ok and everything is successfull

            _bufmgr.UpdateVectorAddress(update.vector_id, update.vector_data);
            if (update.vector_id.Is_Leaf()) {
                _bufmgr.template Get_Node<Node<_KL_MIN, _KL_MAX>>(update.vector_id)->Assign_Parent(nodes.back()->CentroidID());
                // todo check  successfull
            } else if (update.vector_id.Is_Internal_Node()) {
                _bufmgr.template Get_Node<Node<_KI_MIN, _KI_MAX>>(update.vector_id)->Assign_Parent(nodes.back()->CentroidID());
                // todo check  successfull
            }

            if ((i + 1 - num_vec_rem) % num_vec_per_node == 0) {
                centroids.emplace_back(nodes.back()->Compute_Current_Centroid());
                CLOG(LOG_LEVEL_DEBUG, LOG_TAG_CORE,
                    "Simple Cluster: Created Node " NODE_LOG_FMT, " Centroid:%s",
                    NODE_PTR_LOG(nodes.back(), true),
                    centroids.back().to_string().c_str());
            }
        }
        centroids[0] = node->Compute_Current_Centroid();

        return rs;
    }

protected:
    L2_Distance<T, _DIM, DIST_TYPE> _dist;
    _DIST_ID_PAIR_SIMILARITY<DIST_TYPE, L2_Distance<T, _DIM, DIST_TYPE>> _more_similar_cmp{_dist};
    _DIST_ID_PAIR_REVERSE_SIMILARITY<DIST_TYPE, L2_Distance<T, _DIM, DIST_TYPE>> _less_similar_cmp{_dist};
};

// template <typename T, uint16_t _DIM, typename DIST_TYPE = double>
// using Simple_Divide_L2 = Simple_Divide<T, _DIM, DIST_TYPE, L2_Distance>;

};

#endif