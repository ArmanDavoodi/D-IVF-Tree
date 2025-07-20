#ifndef DIVFTREE_INTERFACE_H_
#define DIVFTREE_INTERFACE_H_

#include "common.h"
#include "vector_utils.h"
#include "distance.h"

#include <map>

namespace divftree {

struct DIVFTreeCoreAttributes {
    // DataType vtype;
    uint16_t dimension;
    // DataType dtype;
    ClusteringType clusteringAlg;
    DistanceType distanceAlg;
};

struct DIVFTreeVertexAttributes {
    DIVFTreeCoreAttributes core;
    VPairComparator similarityComparator;
    VPairComparator reverseSimilarityComparator;
    uint64_t version;
    uint16_t min_size;
    uint16_t max_size;
    VectorID centroid_id;
    Vector centroid_copy;
    uint8_t cluster_owner;
};

struct DIVFTreeAttributes {
    DIVFTreeCoreAttributes core;
    uint16_t leaf_min_size;
    uint16_t leaf_max_size;
    uint16_t internal_min_size;
    uint16_t internal_max_size;
    uint16_t split_internal;
    uint16_t split_leaf;

    String ToString() const {
        return String("{dimension=%hu, clusteringAlg=%s, distanceAlg=%s, "
                      "leaf_min_size=%hu, leaf_max_size=%hu, "
                      "internal_min_size=%hu, internal_max_size=%hu, "
                      "split_internal=%hu, split_leaf=%hu}",
                      core.dimension, CLUSTERING_TYPE_NAME[core.clusteringAlg], DISTANCE_TYPE_NAME[core.distanceAlg],
                      leaf_min_size, leaf_max_size, internal_min_size, internal_max_size, split_internal, split_leaf);
    }
};

#define CHECK_CORE_ATTRIBUTES(attr, tag) \
    FatalAssert((attr).core.dimension > 0, (tag), "Dimension must be greater than 0."); \
    FatalAssert(IsValid((attr).core.clusteringAlg), (tag), "Clustering algorithm is invalid."); \
    FatalAssert(IsValid((attr).core.distanceAlg), (tag), "Distance algorithm is invalid.")

#define CHECK_VERTEX_ATTRIBUTES(attr, tag) \
    CHECK_CORE_ATTRIBUTES(attr, tag); \
    CHECK_MIN_MAX_SIZE(attr.min_size, attr.max_size, tag); \
    CHECK_VECTORID_IS_VALID(attr.centroid_id, tag); \
    CHECK_VECTORID_IS_CENTROID(attr.centroid_id, tag); \
    FatalAssert(attr.cluster_owner < MAX_COMPUTE_NODE_ID, tag, \
                "Cluster owner is invalid. cluster_owner=%hu", attr.cluster_owner); \
    FatalAssert(IsComputeNode(attr.cluster_owner), tag, \
                "Cluster owner is not a compute node. cluster_owner=%hu", attr.cluster_owner);

#define CHECK_DIVFTREE_ATTRIBUTES(attr, tag) \
    CHECK_CORE_ATTRIBUTES(attr.core, tag); \
    CHECK_MIN_MAX_SIZE(attr.leaf_min_size, attr.leaf_max_size, tag); \
    CHECK_MIN_MAX_SIZE(attr.internal_min_size, attr.internal_max_size, tag)

enum UpdateType : int8_t {
    VECTOR_INSERT,
    VECTOR_MIGRATE,
    VECTOR_DELETE,
    VECTOR_INPLACE_UPDATE,
    NUM_UPDATE_TYPES
};

struct BatchUpdateEntry {
    UpdateType type;
    bool is_urgent;
    VectorID vector_id;
    RetStatus *result;

    union {
        struct {
            ConstAddress vector_data; // for VECTOR_INSERT
        } insert_args;

        struct {
            ConstAddress vector_data; // for VECTOR_INPLACE_UPDATE
        } inplace_update_args;
    };
};

/* todo need to think more about the cases for inserting a new operation as it
might cause deadlock or unnecessary errors*/
struct BatchUpdate {
    std::map<VectorID, BatchUpdateEntry> updates;
    VectorID target_cluster;
    bool urgent = false;

    inline RetStatus AddInsert(const VectorID& vector_id, ConstAddress vector_data,
                               RetStatus *result = nullptr, bool is_urgent = false) {
        FatalAssert(vector_id.IsValid(), LOG_TAG_DIVFTREE, "Invalid VectorID: " VECTORID_LOG_FMT, VECTORID_LOG(vector_id));
        FatalAssert(vector_data != nullptr, LOG_TAG_DIVFTREE, "Vector data cannot be null.");
        FatalAssert(target_cluster.IsValid(), LOG_TAG_DIVFTREE, "Target cluster is not set.");
        FatalAssert(target_cluster.IsCentroid(), LOG_TAG_DIVFTREE, "Target cluster is not a centroid.");
        FatalAssert(target_cluster._level == vector_id._level + 1,
                    LOG_TAG_DIVFTREE, "Target cluster level (%hhu) does not match vector ID level (%hhu).",
                    target_cluster._level, vector_id._level + 1);
        auto& it = updates.find(vector_id);
        RetStatus rs = RetStatus::Success();
        if (it != updates.end()) {
            switch (it->second.type) {
            case VECTOR_INSERT:
            case VECTOR_MIGRATE:
            case VECTOR_INPLACE_UPDATE:
                CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                     "VectorID " VECTORID_LOG_FMT " already exists in the batch with conflicting operations.",
                     VECTORID_LOG(vector_id));
                rs = RetStatus{
                    .stat = RetStatus::BATCH_CONFLICTING_OPERATIONS
                };
                if (result != nullptr) {
                    *result = rs;
                }
                return rs;

            case VECTOR_DELETE:
                CLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE,
                     "VectorID " VECTORID_LOG_FMT
                     " already exists in the batch with a delete operation. Overwriting it with INPLACE_UPDATE_OPT.",
                     VECTORID_LOG(vector_id));
                it->second.type = VECTOR_INPLACE_UPDATE;
                it->second.is_urgent ||= is_urgent;
                it->second.vector_id = vector_id;
                urgent ||= is_urgent;
                return rs;
            default:
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
                     "Unknown update type for VectorID " VECTORID_LOG_FMT
                     ": %d", VECTORID_LOG(vector_id), it->second.type);
                return RetStatus::Fail("Unknown update type in batch");
            }
        }
        updates[vector_id] = {.type = VECTOR_INSERT,
                              .is_urgent = is_urgent,
                              .vector_id = vector_id,
                              .result = result,
                              .insert_args.vector_data = vector_data};
        urgent ||= is_urgent;
        return RetStatus::Success();
    }

    inline RetStatus AddInplaceUpdate(const VectorID& vector_id, ConstAddress vector_data,
                                      RetStatus *result = nullptr, bool is_urgent = false) {
        FatalAssert(vector_id.IsValid(), LOG_TAG_DIVFTREE, "Invalid VectorID: " VECTORID_LOG_FMT, VECTORID_LOG(vector_id));
        FatalAssert(vector_data != nullptr, LOG_TAG_DIVFTREE, "Vector data cannot be null.");
        FatalAssert(target_cluster.IsValid(), LOG_TAG_DIVFTREE, "Target cluster is not set.");
        FatalAssert(target_cluster.IsCentroid(), LOG_TAG_DIVFTREE, "Target cluster is not a centroid.");
        FatalAssert(target_cluster._level == vector_id._level + 1,
                    LOG_TAG_DIVFTREE, "Target cluster level (%hhu) does not match vector ID level (%hhu).",
                    target_cluster._level, vector_id._level + 1);
        auto& it = updates.find(vector_id);
        RetStatus rs = RetStatus::Success();
        if (it != updates.end()) {
            switch (it->second.type) {
            case VECTOR_INSERT:
            case VECTOR_MIGRATE:
            case VECTOR_DELETE:
            case VECTOR_INPLACE_UPDATE:
                CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                     "VectorID " VECTORID_LOG_FMT " already exists in the batch with conflicting operations.",
                     VECTORID_LOG(vector_id));
                rs = RetStatus{
                    .stat = RetStatus::BATCH_CONFLICTING_OPERATIONS
                };
                if (result != nullptr) {
                    *result = rs;
                }
                return rs;
            default:
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
                     "Unknown update type for VectorID " VECTORID_LOG_FMT
                     ": %d", VECTORID_LOG(vector_id), it->second.type);
                return RetStatus::Fail("Unknown update type in batch");
            }
        }
        updates[vector_id] = {.type = VECTOR_INPLACE_UPDATE,
                              .is_urgent = is_urgent,
                              .vector_id = vector_id,
                              .result = result,
                              .inplace_update_args.vector_data = vector_data};
        urgent ||= is_urgent;
        return RetStatus::Success();
    }

    inline RetStatus AddMigrate(const VectorID& vector_id, RetStatus *result = nullptr,
                                bool is_urgent = false) {
        FatalAssert(vector_id.IsValid(), LOG_TAG_DIVFTREE, "Invalid VectorID: " VECTORID_LOG_FMT, VECTORID_LOG(vector_id));
        FatalAssert(target_cluster.IsValid(), LOG_TAG_DIVFTREE, "Target cluster is not set.");
        FatalAssert(target_cluster.IsCentroid(), LOG_TAG_DIVFTREE, "Target cluster is not a centroid.");
        FatalAssert(target_cluster._level == vector_id._level + 1,
                    LOG_TAG_DIVFTREE, "Target cluster level (%hhu) does not match vector ID level (%hhu).",
                    target_cluster._level, vector_id._level + 1);
        auto& it = updates.find(vector_id);
        RetStatus rs = RetStatus::Success();
        if (it != updates.end()) {
            switch (it->second.type) {
            case VECTOR_INSERT:
            case VECTOR_MIGRATE:
            case VECTOR_DELETE:
            case VECTOR_INPLACE_UPDATE:
                CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                     "VectorID " VECTORID_LOG_FMT " already exists in the batch with conflicting operations.",
                     VECTORID_LOG(vector_id));
                rs = RetStatus{
                    .stat = RetStatus::BATCH_CONFLICTING_OPERATIONS
                };
                if (result != nullptr) {
                    *result = rs;
                }
                return rs;
            default:
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
                     "Unknown update type for VectorID " VECTORID_LOG_FMT
                     ": %d", VECTORID_LOG(vector_id), it->second.type);
                return RetStatus::Fail("Unknown update type in batch");
            }
        }
        updates[vector_id] = {.type = VECTOR_MIGRATE,
                              .is_urgent = is_urgent,
                              .vector_id = vector_id,
                              .result = result};
        urgent ||= is_urgent;
        return RetStatus::Success();
    }

    inline RetStatus AddDelete(const VectorID& vector_id, RetStatus *result = nullptr,
                                bool is_urgent = false) {
        FatalAssert(vector_id.IsValid(), LOG_TAG_DIVFTREE, "Invalid VectorID: " VECTORID_LOG_FMT, VECTORID_LOG(vector_id));
        FatalAssert(target_cluster.IsValid(), LOG_TAG_DIVFTREE, "Target cluster is not set.");
        FatalAssert(target_cluster.IsCentroid(), LOG_TAG_DIVFTREE, "Target cluster is not a centroid.");
        FatalAssert(target_cluster._level == vector_id._level + 1,
                    LOG_TAG_DIVFTREE, "Target cluster level (%hhu) does not match vector ID level (%hhu).",
                    target_cluster._level, vector_id._level + 1);
        auto& it = updates.find(vector_id);
        RetStatus rs = RetStatus::Success();
        if (it != updates.end()) {
            switch (it->second.type) {
            case VECTOR_INSERT:
            case VECTOR_MIGRATE:
            case VECTOR_DELETE:
            case VECTOR_INPLACE_UPDATE:
                CLOG(LOG_LEVEL_ERROR, LOG_TAG_DIVFTREE,
                     "VectorID " VECTORID_LOG_FMT " already exists in the batch with conflicting operations.",
                     VECTORID_LOG(vector_id));
                rs = RetStatus{
                    .stat = RetStatus::BATCH_CONFLICTING_OPERATIONS
                };
                if (result != nullptr) {
                    *result = rs;
                }
                return rs;
            default:
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
                     "Unknown update type for VectorID " VECTORID_LOG_FMT
                     ": %d", VECTORID_LOG(vector_id), it->second.type);
                return RetStatus::Fail("Unknown update type in batch");
            }
        }
        updates[vector_id] = {.type = VECTOR_DELETE,
                              .is_urgent = is_urgent,
                              .vector_id = vector_id,
                              .result = result};
        urgent ||= is_urgent;
        return RetStatus::Success();
    }
};

class DIVFTreeVertexInterface {
public:
    DIVFTreeVertexInterface() = delete;
    virtual ~DIVFTreeVertexInterface() = default;
    virtual RetStatus AssignParent(VectorID parent_id) = 0;

    /* should be handled by the buffer not index */
    // virtual void Pin() = 0;
    // virtual void Unpin() = 0;

    virtual uint16_t BatchInsert(const void** vector_data, const VectorID* vec_id,
                                 uint16_t num_vectors, bool& need_split, Address& offset) = 0;
    // virtual RetStatus StartVectorMigration(VectorID target) = 0;
    // virtual RetStatus EndVectorMigration(VectorID target) = 0;
    virtual RetStatus StartVectorMigration(uint16_t target_idx) = 0; /* need to pin vector id to make sure it does not change(maybe hold shared lock on vertex) */
    virtual RetStatus EndVectorMigration(uint16_t target_idx) = 0; /* need to pin vector id to make sure it does not change(maybe hold shared lock on vertex) */
    // virtual RetStatus DeleteVector(VectorID target) = 0;
    virtual RetStatus DeleteVector(uint16_t target_idx) = 0; /* need to pin vector id to make sure it does not change(maybe hold shared lock on vertex) */

    virtual RetStatus Search(const Vector& query, size_t k,
                             std::vector<std::pair<VectorID, DTYPE>>& neighbours) = 0;

    virtual VectorID CentroidID() const = 0;
    virtual VectorID ParentID() const = 0;
    virtual uint16_t Size() const = 0;

    virtual bool IsFull() const = 0;
    virtual bool IsAlmostEmpty() const = 0;
    virtual bool Contains(VectorID id) const = 0;

    virtual bool IsLeaf() const = 0;
    virtual uint8_t Level() const = 0;

    virtual Vector ComputeCurrentCentroid() const = 0;

    virtual uint16_t MinSize() const = 0;
    virtual uint16_t MaxSize() const = 0;
    virtual uint16_t VectorDimension() const = 0;

    /* todo: A better method(compared to polymorphism) to allow inlining for optimization */
    virtual VPairComparator GetSimilarityComparator(bool reverese) const = 0;
    virtual DTYPE Distance(const Vector& a, const Vector& b) const = 0;

    virtual String ToString() const = 0;
};

class DIVFTreeInterface {
public:
    DIVFTreeInterface() = default;
    virtual ~DIVFTreeInterface() = default;

    virtual RetStatus Insert(const Vector& vec, VectorID& vec_id, uint16_t vertex_per_layer) = 0;
    virtual RetStatus Delete(VectorID vec_id) = 0;

    virtual RetStatus ApproximateKNearestNeighbours(const Vector& query, size_t k,
                                                    uint16_t _internal_k, uint16_t _leaf_k,
                                                    std::vector<std::pair<VectorID, DTYPE>>& neighbours,
                                                    bool sort = true, bool sort_from_more_similar_to_less = true) = 0;

    virtual size_t Size() const = 0;

    virtual DTYPE Distance(const Vector& a, const Vector& b) const = 0;
    virtual size_t Bytes(bool is_internal_vertex) const = 0;

    virtual String ToString() = 0;

    inline static RetStatus KNearestNeighbours(const Vector& query, size_t k, uint16_t dim,
                                               const std::vector<std::pair<VectorID, Vector>>& _data,
                                               std::vector<std::pair<VectorID, DTYPE>>& neighbours,
                                               const DistanceType distanceAlg,
                                               bool sort = true, bool sort_from_more_similar_to_less = true) {
        FatalAssert(k > 0, LOG_TAG_BASIC, "k should be greater than 0.");
        FatalAssert(dim > 0, LOG_TAG_BASIC, "Vector dimension should be greater than 0.");
        FatalAssert(_data.size() > 0, LOG_TAG_BASIC, "Data should contain at least one vector.");
        FatalAssert(query.IsValid(), LOG_TAG_BASIC, "Query vector is invalid.");
        FatalAssert(neighbours.empty(), LOG_TAG_BASIC, "Neighbours vector should be empty.");
        CLOG_IF_TRUE(_data.size() <= k, LOG_LEVEL_WARNING, LOG_TAG_BASIC,
                     "Data size (%lu) is less than or equal to k (%lu).", _data.size(), k);

        for (const auto& pair : _data) {
            DTYPE distance = divftree::Distance(query, pair.second, dim, distanceAlg);
            neighbours.emplace_back(pair.first, distance);
            std::push_heap(neighbours.begin(), neighbours.end(),
                          GetDistancePairSimilarityComparator(distanceAlg, true));
            if (neighbours.size() > k) {
                std::pop_heap(neighbours.begin(), neighbours.end(),
                              GetDistancePairSimilarityComparator(distanceAlg, true));
                neighbours.pop_back();
            }
        }
        if (sort) {
            std::sort_heap(neighbours.begin(), neighbours.end(),
                           GetDistancePairSimilarityComparator(distanceAlg, true));
            if (!sort_from_more_similar_to_less) {
                std::reverse(neighbours.begin(), neighbours.end());
            }
        }

        return RetStatus::Success();
    }

protected:
    virtual RetStatus SearchVertexs(const Vector& query,
                                  const std::vector<std::pair<VectorID, DTYPE>>& upper_layer,
                                  std::vector<std::pair<VectorID, DTYPE>>& lower_layer, size_t n) = 0;

    virtual VectorID RecordInto(const Vector& vec, DIVFTreeVertexInterface* container_vertex,
                                DIVFTreeVertexInterface* vertex = nullptr) = 0;

    virtual RetStatus ExpandTree(DIVFTreeVertexInterface* root, const Vector& centroid) = 0;

    virtual size_t FindClosestCluster(const std::vector<DIVFTreeVertexInterface*>& candidates,
                                       const Vector& vec) = 0;

    virtual RetStatus Split(std::vector<DIVFTreeVertexInterface*>& candidates, size_t vertex_idx) = 0;
    virtual RetStatus Split(DIVFTreeVertexInterface* leaf) = 0;

    virtual DIVFTreeVertexInterface* CreateNewVertex(VectorID id) = 0;

    virtual RetStatus Cluster(std::vector<DIVFTreeVertexInterface*>& vertices, size_t target_vertex_index,
                              std::vector<Vector>& centroids, uint16_t split_into) = 0;
};

};

#endif