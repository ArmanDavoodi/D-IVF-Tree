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

struct RetStatus {
    enum {
        SUCCESS,
        FAIL
    } stat;

    const char* message;

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
typedef const void* ConstAddress;

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

#define VERTEX_LOG_FMT "(%s<%hu, %hu>, ID:" VECTORID_LOG_FMT ", Size:%hu, ParentID:" VECTORID_LOG_FMT ", bucket:%s)"
#define VERTEX_PTR_LOG(vertex)\
    (((vertex) == nullptr) ? "NULL" :\
        (!((vertex)->CentroidID().IsValid()) ? "INV" : ((vertex)->CentroidID().IsVector() ? "Non-Centroid" : \
            ((vertex)->CentroidID().IsLeaf() ? "Leaf" : ((vertex)->CentroidID().IsInternalVertex() ? "Internal" \
                : "UNDEF"))))),\
    (((vertex) == nullptr) ? 0 : (vertex)->MinSize()),\
    (((vertex) == nullptr) ? 0 : (vertex)->MaxSize()),\
    VECTORID_LOG((((vertex) == nullptr) ? divftree::INVALID_VECTOR_ID : (vertex)->CentroidID())),\
    (((vertex) == nullptr) ? 0 : (vertex)->Size()),\
    VECTORID_LOG((((vertex) == nullptr) ? divftree::INVALID_VECTOR_ID : (vertex)->ParentID())),\
    ((PRINT_BUCKET) ? ((((vertex) == nullptr)) ? "NULL" : ((vertex)->BucketToString()).ToCStr()) : "OMITTED")

#define VERTEX_SELF_LOG()\
    (!(_centroid_id.IsValid()) ? "INV" : (_centroid_id.IsVector() ? "Non-Centroid" : \
                                          (_centroid_id.IsLeaf() ? "Leaf" : \
                                                                   (_centroid_id.IsInternalVertex() ? "Internal" :\
                                                                                                    "UNDEF")))),\
    _min_size, _cluster.Capacity(), VECTORID_LOG(_centroid_id), _cluster.Size(), VECTORID_LOG(_parent_id),\
    ((PRINT_BUCKET) ? BucketToString().ToCStr() : "OMITTED")

#define VECTOR_UPDATE_LOG_FMT "(ID:" VECTORID_LOG_FMT ", Address:%p)"
#define VECTOR_UPDATE_LOG(update) VECTORID_LOG((update).vector_id), (update).vector_data

#define CHECK_MIN_MAX_SIZE(min_size, max_size, tag) \
    FatalAssert((min_size) > 0, (tag), "Min size must be greater than 0."); \
    FatalAssert(((max_size) / 2) >= (min_size), (tag), \
                "Max size must be at least twice the min size. Min size: %hu, Max size: %hu", \
                (min_size), (max_size))

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
                "Vertex is not a leaf: " VERTEX_LOG_FMT, VERTEX_PTR_LOG((vertex)))
#define CHECK_VERTEX_IS_INTERNAL(vertex, tag) \
    FatalAssert(((vertex) != nullptr) && !((vertex)->IsLeaf()), (tag), \
                "Vertex is not an internal vertex: " VERTEX_LOG_FMT, VERTEX_PTR_LOG((vertex)))

#define CHECK_NOT_NULLPTR(ptr, tag) \
    FatalAssert((ptr) != nullptr, (tag), "Pointer is nullptr")

#define CHECK_VERTEX_IS_VALID(vertex, tag, check_min_size) \
    CHECK_NOT_NULLPTR((vertex), (tag)); \
    CHECK_VECTORID_IS_VALID((vertex)->CentroidID(), (tag)); \
    CHECK_VECTORID_IS_CENTROID((vertex)->CentroidID(), (tag)); \
    FatalAssert((vertex)->VectorDimention() > 0, (tag), \
                "Vertex has invalid vector dimension: " VERTEX_LOG_FMT, VERTEX_PTR_LOG((vertex))); \
    CHECK_MIN_MAX_SIZE((vertex)->MinSize(), (vertex)->MaxSize(), (tag)); \
    FatalAssert(((!(check_min_size)) || (vertex)->Size() >= (vertex)->MinSize()), (tag), \
                "Vertex does not have enough elements: size=%hu, min_size=%hu.", \
                (vertex)->Size(), (vertex)->MinSize()); \
    FatalAssert((vertex)->Size() <= (vertex)->MaxSize(), (tag), \
                "Vertex has too many elements: size=%hu, max_size=%hu.", \
                (vertex)->Size(), (vertex)->MaxSize())

#define CHECK_VERTEX_SELF_IS_VALID(tag, check_min_size) \
    CHECK_VECTORID_IS_VALID(_centroid_id, (tag)); \
    CHECK_VECTORID_IS_CENTROID(_centroid_id, (tag)); \
    FatalAssert(_cluster.Dimension() > 0, (tag), \
                "Vertex has invalid vector dimension: " VERTEX_LOG_FMT, VERTEX_SELF_LOG()); \
    CHECK_MIN_MAX_SIZE(_min_size, _cluster.Capacity(), (tag)); \
    FatalAssert(((!(check_min_size)) || _cluster.Size() >= _min_size), (tag), \
                "Vertex does not have enough elements: size=%hu, min_size=%hu.", \
                _cluster.Size(), _min_size); \
    FatalAssert(_cluster.Size() <= _cluster.Capacity(), (tag), \
                "Vertex has too many elements: size=%hu, max_size=%hu.", \
                _cluster.Size(), _cluster.Capacity()); \
    FatalAssert(IsValid(_clusteringAlg), (tag), "Clustering algorithm is invalid."); \
    FatalAssert(IsValid(_distanceAlg), (tag), "Distance algorithm is invalid."); \
    CHECK_NOT_NULLPTR(_similarityComparator, (tag)); \
    CHECK_NOT_NULLPTR(_reverseSimilarityComparator, (tag))

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