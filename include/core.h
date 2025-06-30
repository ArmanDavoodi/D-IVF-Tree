#ifndef CORE_H_
#define CORE_H_

#include "common.h"
#include "vector_utils.h"

#include "interface/copper.h"
#include "interface/buffer.h"

#include <vector>

namespace copper {

class DIST_ID_PAIR_SIMILARITY_INTERFACE {
public:
    virtual bool operator()(const std::pair<VectorID, DTYPE>& a, const std::pair<VectorID, DTYPE>& b) const = 0;
};

namespace L2 {

inline static DTYPE Distance(const Vector& a, const Vector& b, uint16_t dim) {
    FatalAssert(a.IsValid(), LOG_TAG_BASIC, "a is invalid");
    FatalAssert(b.IsValid(), LOG_TAG_BASIC, "b is invalid");

    DTYPE dist = 0;
    for (size_t i = 0; i < dim; ++i) {
        dist += ((DTYPE)a[i] - (DTYPE)b[i]) * ((DTYPE)a[i] - (DTYPE)b[i]);
    }
    return dist;
}

inline static bool MoreSimilar(const DTYPE& a, const DTYPE& b) {
    return a < b;
}

inline static Vector ComputeCentroid(const VTYPE* vectors, size_t size, uint16_t dim) {
    FatalAssert(size > 0, LOG_TAG_BASIC, "size cannot be 0");
    FatalAssert(vectors != nullptr, LOG_TAG_BASIC, "size cannot be 0");
    Vector centroid(vectors, dim);
    for (size_t v = 1; v < size; ++v) {
        for (uint16_t e = 0; e < dim; ++e) {
            centroid[e] += vectors[v * dim + e];
        }
    }

    for (uint16_t e = 0; e < dim; ++e) {
        centroid[e] /= size;
    }

    return centroid;
}

/* todo: A better method(compared to polymorphism) to allow inlining for optimization */
struct DIST_ID_PAIR_SIMILARITY : public DIST_ID_PAIR_SIMILARITY_INTERFACE {
    inline bool operator()(const std::pair<VectorID, DTYPE>& a, const std::pair<VectorID, DTYPE>& b) const override {
        return MoreSimilar(a.second, b.second);
    }
};

struct DIST_ID_PAIR_REVERSE_SIMILARITY : public DIST_ID_PAIR_SIMILARITY_INTERFACE {
    inline bool operator()(const std::pair<VectorID, DTYPE>& a, const std::pair<VectorID, DTYPE>& b) const override {
        return !(MoreSimilar(a.second, b.second));
    }
};

};

// namespace SimpleDivide { /* better naming */

// inline RetStatus Cluster(std::vector<CopperNodeInterface*>& nodes, size_t target_node_index,
//                          std::vector<Vector>& centroids, uint16_t split_into,
//                          BufferManagerInterface& _bufmgr) {

//     FatalAssert(nodes.size() > target_node_index, LOG_TAG_CORE, "nodes should contain node.");
//     CopperNodeInterface* target = nodes[target_node_index];
//     FatalAssert(target != nullptr, LOG_TAG_CORE, "node should not be nullptr.");
//     FatalAssert(target->IsFull(), LOG_TAG_CORE, "node should be full.");
//     FatalAssert(split_into > 0, LOG_TAG_CORE, "split_into should be greater than 0.");

//     RetStatus rs = RetStatus::Success();

//     split_into = (target->MaxSize() / split_into < target->MinSize() ? target->MaxSize() / target->MinSize() :
//                                                                        split_into);
//     if (split_into < 2) {
//         split_into = 2;
//     }

//     uint16_t num_vec_per_node = target->MaxSize() / split_into;
//     uint16_t num_vec_rem = target->MaxSize() % split_into;
//     nodes.reserve(nodes.size() + split_into - 1);
//     centroids.reserve(split_into);
//     centroids.emplace_back(nullptr);
//     for (uint16_t i = num_vec_per_node + num_vec_rem; i < target->MaxSize(); ++i) {
//         if ((i - num_vec_rem) % num_vec_per_node == 0) {
//             VectorID vector_id = _bufmgr.RecordVector(target->Level());
//             FatalAssert(vector_id.IsValid(), LOG_TAG_CORE, "Failed to record vector.");
//             CopperNodeInterface* new_node = target->CreateSibling(vector_id);
//             FatalAssert(new_node != nullptr, LOG_TAG_CORE, "Failed to create sibling node.");
//             nodes.emplace_back(new_node);
//             _bufmgr.UpdateClusterAddress(vector_id, nodes.back());
//         }
//         VectorUpdate update = target->MigrateLastVectorTo(nodes.back());
//         // todo check update is ok and everything is successfull

//         _bufmgr.UpdateVectorAddress(update.vector_id, update.vector_data);
//         _bufmgr.GetNode(update.vector_id)->AssignParent(nodes.back()->CentroidID());

//         if ((i + 1 - num_vec_rem) % num_vec_per_node == 0) {
//             centroids.emplace_back(nodes.back()->ComputeCurrentCentroid());
//             CLOG(LOG_LEVEL_DEBUG, LOG_TAG_CORE,
//                 "Simple Cluster: Created Node " NODE_LOG_FMT, " Centroid:%s",
//                 NODE_PTR_LOG(nodes.back()), centroids.back().ToString(target->VectorDimention()).ToCStr());
//         }
//     }
//     centroids[0] = target->ComputeCurrentCentroid();

//     return rs;
// }

// };

};

#endif