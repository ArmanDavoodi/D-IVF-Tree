#ifndef DIVFTREE_COMMON_H_
#define DIVFTREE_COMMON_H_

#include <vector>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <concepts>
#include <stdlib.h>

#include "utils/string.h"

#include "debug.h"

namespace divftree {

#if defined(__AVX512F__)
    #define HAVE_ATOMIC_128_LOAD_STORE 1
#else
    #define HAVE_ATOMIC_128_LOAD_STORE 0
#endif

#if defined(__x86_64__) && defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
    #define HAVE_CMPXCHG16B 1
#else
    #define HAVE_CMPXCHG16B 0
#endif

struct RetStatus {
    enum {
        SUCCESS,

        BATCH_CONFLICTING_OPERATIONS,
        BATCH_UNKNOWN_OPERATION,

        VECTOR_IS_INVALID,
        VECTOR_IS_MIGRATED,
        VECTOR_IS_INVISIBLE,
        VECTOR_NOT_FOUND,

        VERTEX_UPDATED,
        VERTEX_NEEDS_SPLIT, /* not enough space -> no vectors were placed in cluster -> split needed by caller */
        VERTEX_NEEDS_MERGE, /* may be solved with a single compaction? */
        VERTEX_NOT_ENOUGH_SPACE, /* returned when there is not enough space but someone else is splitting */

        FAIL
    } stat;

    const char* message;

    static inline RetStatus Success() {
        return RetStatus{SUCCESS, "OK"};
    }

    static inline RetStatus Success() {
        return RetStatus{SUCCESS, "OK"};
    }

    static inline RetStatus Fail(const char* msg) {
        return RetStatus{FAIL, msg};
    }

    inline bool IsOK() const {
        return stat == SUCCESS;
    }

    inline const char* Msg() const {
        return message;
    }
};

constexpr uint64_t INVALID_VECTOR_ID = UINT64_MAX;

union VectorID {
    uint64_t _id;
    struct {
        uint64_t _val : 48;
        uint64_t _level : 8; // == 0 for vectors, == 1 for leaves
        uint64_t _creator_node_id : 8;
    };

    static constexpr uint64_t MAX_ID_PER_LEVEL = 0x0000FFFFFFFFFFFF;
    static constexpr uint64_t VECTOR_LEVEL = 0;
    static constexpr uint64_t LEAF_LEVEL = 1;

    VectorID() : _id(INVALID_VECTOR_ID) {}
    VectorID(const uint64_t& ID) : _id(ID) {}
    VectorID(const VectorID& ID) : _id(ID._id) {}

    inline bool IsValid() const {
        return (_id != INVALID_VECTOR_ID) && (_val < MAX_ID_PER_LEVEL);
    }

    inline bool IsCentroid() const {
        return _level > VECTOR_LEVEL;
    }

    inline bool IsVector() const {
        return _level == VECTOR_LEVEL;
    }

    inline bool IsLeaf() const {
        return _level == LEAF_LEVEL;
    }

    inline bool IsInternalVertex() const {
        return _level > LEAF_LEVEL;
    }

    inline void operator=(const VectorID& ID) {
        _id = ID._id;
    }

    inline bool operator==(const VectorID& ID) const {
        return _id == ID._id;
    }

    inline bool operator!=(const VectorID& ID) const {
        return _id != ID._id;
    }

    inline void operator=(const uint64_t& ID) {
        _id = ID;
    }

    inline bool operator==(const uint64_t& ID) const {
        return _id == ID;
    }

    inline bool operator!=(const uint64_t& ID) const {
        return _id != ID;
    }
};

typedef void* Address;
typedef const void* AddressToConst;

constexpr Address INVALID_ADDRESS = nullptr;

class DIVFTreeVertexInterface;
class DIVFTreeInterface;
class BufferManagerInterface;
// class DIVFTreeVertex;
// class DIVFTree;
// class BufferManager;
// class Cluster;

enum ClusteringType : int8_t {
    InvalidC,
    SimpleDivide,
    NumTypesC
};
inline constexpr char* CLUSTERING_TYPE_NAME[ClusteringType::NumTypesC + 1] = {"Invalid", "SimpleDivide", "NumTypes"};
inline constexpr bool IsValid(ClusteringType type) {
    return ((type != ClusteringType::InvalidC) && (type != ClusteringType::NumTypesC));
}

enum DistanceType : int8_t {
    InvalidD,
    L2Distance,
    NumTypesD
};
inline constexpr char* DISTANCE_TYPE_NAME[DistanceType::NumTypesD + 1] = {"Invalid", "L2", "NumTypes"};
inline constexpr bool IsValid(DistanceType type) {
    return ((type != DistanceType::InvalidD) && (type != DistanceType::NumTypesD));
}


/*

    User local vector insert:
        1) Reserve a unique vector id at level 0
        2) Search for an appropriate leaf vertex to insert the vector and pin it
        3) TryLock the leaf in shared mode and try to reserve memory in cluster
            3.1) if lock failed it means there was an update and go to step 5
        4) If node is full split(*)
        5) If node is updating wait on cond var and retry based on the update type
            5.1)
        6) Create and insert entry in insertion state and increment _real_size
        7) Create buffer Entry for the entry
        8) change state from insertion to valid
        9) unlock leaf
        10) unpin leaf

    User local vector delete:
        1) Search for the vector in the buffer
        2) If not found return not found
        3) pin vertex and get lock in shared mode
            3.1) if lock failed it means there was an update and
                3.1.1)
        4) check the entry and CAS from Valid to Invalid
        5) if cas failed due to state:
            5.1)
        6) Create an empty buffer entry and cas it to the head of the version queue
            6.1) can cas fail here?
        7) Unpin vector and if pin is 0 delete it
            7.1) If deleted cas the new entry with nullptr and unpin it and if 0 delete
                7.2) can cas fail here?
        8) decrement _real_size and if it is less than min_size get exclusive, compact(*) and if needed merge(*)
        9) unlock vertex
        10) unpin vertex

    user local vector update:
        1) Search for the vector in the buffer
        2) If not found return not found
        3) pin vertex and get lock in shared mode
            3.1) if lock failed it means there was an update and
                3.1.1)
        4) check the entry and CAS from Valid to Valid|Move
        5) if cas failed due to state:
            5.1)
        6) Unlock vertex but do not unpin it!
        7) execute steps 2-7 of user local vector insert
        8) Cas the new buffer entry as the head of the version queue and make the current old version
            8.1) can cas fail? if not use exchange
            8.2) unpin the old version and if 0 delete it
        9) Unpin vector and if pin is 0 delete it
            9.1) If deleted cas the new entry with nullptr and unpin it and if 0 delete
        10) decrement _real_size and if it is less than min_size get exclusive, compact(*) and if needed merge(*)
        11) unlock vertex
        12) unpin vertex

*/
enum VertexUpdateType : int8_t {
    /* Main operations */
    VERTEX_INSERT,
    VERTEX_DELETE,
    VERTEX_MOVE,
    VERTEX_INPLACE_UPDATE,

    /* Insertion subtypes */
    VERTEX_MIGRATION_INSERT,
    VERTEX_UPDATE_INSERT,

    /* Deletion subtypes */
    VERTEX_MIGRATION_DELETE,
    VERTEX_UPDATE_DELETE,

    NUM_VERTEX_UPDATE_TYPES
};

struct BatchVertexUpdateEntry {
    VertexUpdateType type;
    bool is_urgent;
    VectorID vector_id;
    RetStatus *result;

    union {
        struct {
            AddressToConst vector_data; // for VERTEX_INSERT
            Address *cluster_entry_addr; // if successful, this will set to the address of final cluster entry where vector is inserted
        } insert_args;

        struct {
            AddressToConst vector_data; // for VERTEX_INPLACE_UPDATE
        } inplace_update_args;
    };
};

/* todo need to think more about the cases for inserting a new operation as it
might cause deadlock or unnecessary errors*/
struct BatchVertexUpdate {
    std::map<VectorID, BatchVertexUpdateEntry> updates[NUM_VERTEX_UPDATE_TYPES];
    VectorID target_cluster;
    bool urgent = false;

    inline RetStatus AddInsert(const VectorID& vector_id, AddressToConst vector_data,
                               RetStatus *result = nullptr, bool is_urgent = false) {
        FatalAssert(vector_id.IsValid(), LOG_TAG_DIVFTREE, "Invalid VectorID: " VECTORID_LOG_FMT, VECTORID_LOG(vector_id));
        FatalAssert(vector_data != nullptr, LOG_TAG_DIVFTREE, "Vector data cannot be null.");
        FatalAssert(target_cluster.IsValid(), LOG_TAG_DIVFTREE, "Target cluster is not set.");
        FatalAssert(target_cluster.IsCentroid(), LOG_TAG_DIVFTREE, "Target cluster is not a centroid.");
        FatalAssert(target_cluster._level == vector_id._level + 1,
                    LOG_TAG_DIVFTREE, "Target cluster level (%hhu) does not match vector ID level (%hhu).",
                    target_cluster._level, vector_id._level + 1);
        RetStatus rs = RetStatus::Success();
        if (updates[VERTEX_INSERT].find(vector_id) != updates[VERTEX_INSERT].end() ||
            updates[VERTEX_MIGRATION_MOVE].find(vector_id) != updates[VERTEX_MIGRATION_MOVE].end() ||
            updates[VERTEX_MIGRATION_DELETE].find(vector_id) != updates[VERTEX_MIGRATION_DELETE].end() ||
            updates[VERTEX_INPLACE_UPDATE].find(vector_id) != updates[VERTEX_INPLACE_UPDATE].end()) {
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
        }
        else if (updates[VERTEX_DELETE].find(vector_id) != updates[VERTEX_DELETE].end()) {
            /* delete should override insertion as it was either caused by user  */
            CLOG(LOG_LEVEL_WARNING, LOG_TAG_DIVFTREE,
                 "VectorID " VECTORID_LOG_FMT
                 " already exists in the batch with a delete operation. Overwriting it with INPLACE_UPDATE_OPT.",
                 VECTORID_LOG(vector_id));
            it->second.type = VERTEX_INPLACE_UPDATE;
            it->second.is_urgent ||= is_urgent;
            it->second.vector_id = vector_id;
            urgent ||= is_urgent;
            return rs;
        }

        updates[vector_id] = {.type = VERTEX_INSERT,
                              .is_urgent = is_urgent,
                              .vector_id = vector_id,
                              .result = result,
                              .insert_args.vector_data = vector_data};
        urgent ||= is_urgent;
        return RetStatus::Success();
    }

    inline RetStatus AddInplaceUpdate(const VectorID& vector_id, AddressToConst vector_data,
                                      RetStatus *result = nullptr, bool is_urgent = false) {
        FatalAssert(vector_id.IsValid(), LOG_TAG_DIVFTREE, "Invalid VectorID: " VECTORID_LOG_FMT, VECTORID_LOG(vector_id));
        FatalAssert(vector_data != nullptr, LOG_TAG_DIVFTREE, "Vector data cannot be null.");
        FatalAssert(target_cluster.IsValid(), LOG_TAG_DIVFTREE, "Target cluster is not set.");
        FatalAssert(target_cluster.IsCentroid(), LOG_TAG_DIVFTREE, "Target cluster is not a centroid.");
        FatalAssert(target_cluster._level == vector_id._level + 1,
                    LOG_TAG_DIVFTREE, "Target cluster level (%hhu) does not match vector ID level (%hhu).",
                    target_cluster._level, vector_id._level + 1);
        RetStatus rs = RetStatus::Success();
        if (updates[VERTEX_INSERT].find(vector_id) != updates[VERTEX_INSERT].end() ||
            updates[VERTEX_MIGRATION_MOVE].find(vector_id) != updates[VERTEX_MIGRATION_MOVE].end() ||
            updates[VERTEX_DELETE].find(vector_id) != updates[VERTEX_DELETE].end() ||
            updates[VERTEX_INPLACE_UPDATE].find(vector_id) != updates[VERTEX_INPLACE_UPDATE].end()) {
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
        }

        updates[vector_id] = {.type = VERTEX_INPLACE_UPDATE,
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
        RetStatus rs = RetStatus::Success();
        if (updates[VERTEX_INSERT].find(vector_id) != updates[VERTEX_INSERT].end() ||
            updates[VERTEX_MIGRATION_MOVE].find(vector_id) != updates[VERTEX_MIGRATION_MOVE].end() ||
            updates[VERTEX_DELETE].find(vector_id) != updates[VERTEX_DELETE].end() ||
            updates[VERTEX_INPLACE_UPDATE].find(vector_id) != updates[VERTEX_INPLACE_UPDATE].end()) {
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
        }

        updates[vector_id] = {.type = VERTEX_MIGRATION_MOVE,
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
        RetStatus rs = RetStatus::Success();
        if (updates[VERTEX_INSERT].find(vector_id) != updates[VERTEX_INSERT].end() ||
            updates[VERTEX_MIGRATION_MOVE].find(vector_id) != updates[VERTEX_MIGRATION_MOVE].end() ||
            updates[VERTEX_DELETE].find(vector_id) != updates[VERTEX_DELETE].end() ||
            updates[VERTEX_INPLACE_UPDATE].find(vector_id) != updates[VERTEX_INPLACE_UPDATE].end()) {
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
        }

        updates[vector_id] = {.type = VECTOR_DELETE,
                              .is_urgent = is_urgent,
                              .vector_id = vector_id,
                              .result = result};
        urgent ||= is_urgent;
        return RetStatus::Success();
    }
};

#ifndef VECTOR_TYPE
#define VECTOR_TYPE uint16_t
#define VTYPE_FMT "%hu"
#endif
#ifndef DISTANCE_TYPE
#define DISTANCE_TYPE double
#define DTYPE_FMT "%lf"
#endif

#if defined(__cpp_lib_hardware_interference_size) || defined(__GNUC__) || defined(_MSC_VER)
#include <new>
constexpr size_t CACHE_LINE_SIZE = std::hardware_destructive_interference_size;
#else
constexpr size_t CACHE_LINE_SIZE = 64; // Fallback to common cache line size
#endif

inline constexpr size_t ALLIGNED_SIZE(size_t size) {
    return (size % CACHE_LINE_SIZE == 0) ? size : (size + (CACHE_LINE_SIZE - (size % CACHE_LINE_SIZE)));
}

inline constexpr size_t ALLIGNEMENT(size_t size) {
    return (size % CACHE_LINE_SIZE == 0) ? 0 : (CACHE_LINE_SIZE - (size % CACHE_LINE_SIZE));
}

inline constexpr bool ALLIGNED(size_t size) {
    return (size % CACHE_LINE_SIZE == 0);
}

typedef VECTOR_TYPE VTYPE;
typedef DISTANCE_TYPE DTYPE;

// enum DataType : int8_t {
//     Invalid = -1,
//     UInt16,
//     Float,
//     Double,
//     NumTypes
// };
// inline constexpr char* DATA_TYPE_NAME[DataType::NumTypes] = {"uint16", "float", "double"};
// inline constexpr size_t SIZE_OF_TYPE[DataType::NumTypes] = {sizeof(uint16_t), sizeof(float), sizeof(double)};
// inline constexpr bool IsValid(DataType type) {
//     return ((type != DataType::Invalid) && (type != DataType::NumTypes));
// }

// String DataToString(const void* data, DataType type) {
//     if (data == nullptr) {
//         return String("NULL");
//     }
//     switch (type) {
//         case DataType::UInt16: {
//             return String("%hu", *(reinterpret_cast<const uint16_t*>(data)));
//         }
//         case DataType::Float: {
//             return String("%f", *(reinterpret_cast<const float*>(data)));
//         }
//         case DataType::Double: {
//             return String("%lf", *(reinterpret_cast<const double*>(data)));
//         }
//         default:
//             return String("Invalid DataType");
//     }
// }
// Todo: Log DIVFTree: DIVFTree(RootID:%s(%lu, %lu, %lu), # levels:lu, # vertices:lu, # vectors:lu, size:lu)

#ifndef PRINT_BUCKET
#define PRINT_BUCKET false
#endif

#define VECTORID_LOG_FMT "%s%lu(%lu, %lu, %lu)"
#define VECTORID_LOG(vid) (!((vid).IsValid()) ? "[INV]" : ""), (vid)._id, (vid)._creator_node_id, (vid)._level, (vid)._val

#define VECTOR_STATE_LOG_FMT "%s"
#define VECTOR_STATE_LOG(state) ((state).state.load().ToString().ToCStr())

// #define VERTEX_LOG_FMT "(%s<%hu, %hu>, ID:" VECTORID_LOG_FMT ", Size:%hu, ParentID:" VECTORID_LOG_FMT ", bucket:%s)"
// #define VERTEX_PTR_LOG(vertex)\
//     (((vertex) == nullptr) ? "NULL" :\
//         (!((vertex)->CentroidID().IsValid()) ? "INV" : ((vertex)->CentroidID().IsVector() ? "Non-Centroid" : \
//             ((vertex)->CentroidID().IsLeaf() ? "Leaf" : ((vertex)->CentroidID().IsInternalVertex() ? "Internal" \
//                 : "UNDEF"))))),\
//     (((vertex) == nullptr) ? 0 : (vertex)->MinSize()),\
//     (((vertex) == nullptr) ? 0 : (vertex)->MaxSize()),\
//     VECTORID_LOG((((vertex) == nullptr) ? divftree::INVALID_VECTOR_ID : (vertex)->CentroidID())),\
//     (((vertex) == nullptr) ? 0 : (vertex)->Size()),\
//     VECTORID_LOG((((vertex) == nullptr) ? divftree::INVALID_VECTOR_ID : (vertex)->ParentID())),\
//     ((PRINT_BUCKET) ? ((((vertex) == nullptr)) ? "NULL" : ((vertex)->BucketToString()).ToCStr()) : "OMITTED")

#define VECTOR_UPDATE_LOG_FMT "(ID:" VECTORID_LOG_FMT ", Address:%p)"
#define VECTOR_UPDATE_LOG(update) VECTORID_LOG((update).vector_id), (update).vector_data

#define CHECK_MIN_MAX_SIZE(min_size, max_size, tag) \
    do {
        FatalAssert((min_size) > 0, (tag), "Min size must be greater than 0."); \
        FatalAssert(((max_size) / 2) >= (min_size), (tag), \
                    "Max size must be at least twice the min size. Min size: %hu, Max size: %hu", \
                    (min_size), (max_size)); \
    } while(0)

#define CHECK_VECTORID_IS_VALID(vid, tag) \
    FatalAssert((vid).IsValid(), (tag), "Invalid VectorID: " VECTORID_LOG_FMT, VECTORID_LOG((vid)))
#define CHECK_VECTORID_IS_CENTROID(vid, tag) \
    FatalAssert((vid).IsCentroid(), (tag), "VectorID " VECTORID_LOG_FMT " is not a centroid", VECTORID_LOG((vid)))
#define CHECK_VECTORID_IS_VECTOR(vid, tag) \
    FatalAssert((vid).IsVector(), (tag), "VectorID " VECTORID_LOG_FMT " is not a vector", VECTORID_LOG((vid)))
#define CHECK_VECTORID_IS_LEAF(vid, tag) \
    FatalAssert((vid).IsLeaf(), (tag), "VectorID " VECTORID_LOG_FMT " is not a leaf", VECTORID_LOG((vid)))

#define CHECK_VERTEX_IS_LEAF(vertex, tag) \
    FatalAssert(((vertex) != nullptr) && (vertex)->IsLeaf(), (tag), \
                "Vertex is not a leaf: %s", (vertex)->ToString().ToCStr())
#define CHECK_VERTEX_IS_INTERNAL(vertex, tag) \
    FatalAssert(((vertex) != nullptr) && !((vertex)->IsLeaf()), (tag), \
                "Vertex is not an internal vertex: %s", (vertex)->ToString().ToCStr())

#define CHECK_NOT_NULLPTR(ptr, tag) \
    FatalAssert((ptr) != nullptr, (tag), "Pointer is nullptr")

#define CHECK_VERTEX_IS_VALID(vertex, tag, check_min_size) \
    do {
        CHECK_NOT_NULLPTR((vertex), (tag)); \
        CHECK_VECTORID_IS_VALID((vertex)->CentroidID(), (tag)); \
        CHECK_VECTORID_IS_CENTROID((vertex)->CentroidID(), (tag)); \
        FatalAssert((vertex)->VectorDimension() > 0, (tag), \
                    "Vertex has invalid vector dimension: %s", (vertex)->ToString().ToCStr()); \
        CHECK_MIN_MAX_SIZE((vertex)->MinSize(), (vertex)->MaxSize(), (tag)); \
        FatalAssert(((!(check_min_size)) || (vertex)->Size() >= (vertex)->MinSize()), (tag), \
                    "Vertex does not have enough elements: size=%hu, min_size=%hu.", \
                    (vertex)->Size(), (vertex)->MinSize()); \
        FatalAssert((vertex)->Size() <= (vertex)->MaxSize(), (tag), \
                    "Vertex has too many elements: size=%hu, max_size=%hu.", \
                    (vertex)->Size(), (vertex)->MaxSize()); \
    } while(0)

#define CHECK_VERTEX_SELF_IS_VALID(tag, check_min_size) \
    do { \
        FatalAssert(IsValid(_clusteringAlg), (tag), "Clustering algorithm is invalid."); \
        FatalAssert(IsValid(_distanceAlg), (tag), "Distance algorithm is invalid."); \
        CHECK_NOT_NULLPTR(_similarityComparator, (tag)); \
        CHECK_NOT_NULLPTR(_reverseSimilarityComparator, (tag)); \
        _cluster.CheckValid((check_min_size)); \
    } while(0)

#ifdef ENABLE_TEST_LOGGING
#define PRINT_VECTOR_PAIR_BATCH(vector, tag, msg, ...) \
    do { \
        divftree::String str(msg ": Batch Size: %lu, Vector Pair Batch: " __VA_OPT__(,) __VA_ARGS__,\
                           (vector).size()); \
        for (const auto& pair : (vector)) { \
            str += divftree::String("<" VECTORID_LOG_FMT ", Distance:" DTYPE_FMT "> ", \
                                  VECTORID_LOG(pair.first), pair.second); \
        } \
        CLOG(LOG_LEVEL_DEBUG, (tag), "%s", str.ToCStr()); \
    } while (0)

};

#else
#define PRINT_VECTOR_PAIR_BATCH(vector)
#endif

#endif