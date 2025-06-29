#ifndef COPPER_COMMON_H_
#define COPPER_COMMON_H_

#include <vector>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <concepts>
#include <stdlib.h>

#include "utils/string.h"

#include "debug.h"

namespace copper {

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

    inline bool Is_OK() const {
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

    inline bool IsInternalNode() const {
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

typedef void* DTYPE;
typedef void* VTYPE;

constexpr Address INVALID_ADDRESS = nullptr;

class CopperNodeInterface;
class VectorIndexInterface;
class BufferManagerInterface;
class VectorSet;

enum ClusteringType : int8_t {
    Invalid = -1,
    Simple_Divide,
    NumTypes
};
inline constexpr char* CLUSTERING_TYPE_NAME[ClusteringType::NumTypes] = {"Simple_Divide"};
inline constexpr bool IsValid(ClusteringType type) {
    return ((type != ClusteringType::Invalid) && (type != ClusteringType::NumTypes));
}

enum DistanceType : int8_t {
    Invalid = -1,
    L2,
    NumTypes
};
inline constexpr char* DISTANCE_TYPE_NAME[DistanceType::NumTypes] = {"L2"};
inline constexpr bool IsValid(DistanceType type) {
    return ((type != DistanceType::Invalid) && (type != DistanceType::NumTypes));
}

enum DataType : int8_t {
    Invalid = -1,
    UInt16,
    Float,
    Double,
    NumTypes
};
inline constexpr char* DATA_TYPE_NAME[DataType::NumTypes] = {"uint16", "float", "double"};
inline constexpr size_t SIZE_OF_TYPE[DataType::NumTypes] = {sizeof(uint16_t), sizeof(float), sizeof(double)};
inline constexpr bool IsValid(DataType type) {
    return ((type != DataType::Invalid) && (type != DataType::NumTypes));
}

String DataToString(const void* data, DataType type) {
    if (data == nullptr) {
        return String("NULL");
    }
    switch (type) {
        case DataType::UInt16: {
            return String("%hu", *(reinterpret_cast<const uint16_t*>(data)));
        }
        case DataType::Float: {
            return String("%f", *(reinterpret_cast<const float*>(data)));
        }
        case DataType::Double: {
            return String("%lf", *(reinterpret_cast<const double*>(data)));
        }
        default:
            return String("Invalid DataType");
    }
}
// Todo: Log VectorIndex: VectorIndex(RootID:%s(%lu, %lu, %lu), # levels:lu, # nodes:lu, # vectors:lu, size:lu)

#define VECTORID_LOG_FMT "%s%lu(%lu, %lu, %lu)"
#define VECTORID_LOG(vid) (!((vid).Is_Valid()) ? "[INV]" : ""), (vid)._id, (vid)._creator_node_id, (vid)._level, (vid)._val

#define NODE_LOG_FMT "(%s<%hu, %hu>, ID:" VECTORID_LOG_FMT ", Size:%hu, ParentID:" VECTORID_LOG_FMT ", bucket:%s)"
// todo remove
#define NODE_PTR_LOG(node, print_bucket)\
    (((node) == nullptr) ? "NULL" :\
        (!((node)->CentroidID().IsValid()) ? "INV" : ((node)->CentroidID().IsVector() ? "Non-Centroid" : \
            ((node)->CentroidID().IsLeaf() ? "Leaf" : ((node)->CentroidID().IsInternalNode() ? "Internal" \
                : "UNDEF"))))),\
    (((node) == nullptr) ? 0 : std::remove_reference_t<decltype(*(node))>::_MIN_SIZE_),\
    (((node) == nullptr) ? 0 : std::remove_reference_t<decltype(*(node))>::_MAX_SIZE_),\
    VECTORID_LOG((((node) == nullptr) ? copper::INVALID_VECTOR_ID : (node)->CentroidID())),\
    (((node) == nullptr) ? 0 : (node)->Size()),\
    VECTORID_LOG((((node) == nullptr) ? copper::INVALID_VECTOR_ID : (node)->ParentID())),\
    ((print_bucket) ? ((((node) == nullptr)) ? "NULL" : ((node)->BucketToString()).ToCStr()) : "OMITTED")

#define VECTOR_UPDATE_LOG_FMT "(ID:" VECTORID_LOG_FMT ", Address:%p)"
#define VECTOR_UPDATE_LOG(update) VECTORID_LOG((update).vector_id), (update).vector_data

#ifdef ENABLE_TEST_LOGGING
#define PRINT_VECTOR_PAIR_BATCH(vector, type, msg) \
    do { \
        copper::String str = ""; \
        for (const auto& pair : (vector)) { \
            str += String(VECTORID_LOG_FMT ", Distance:%lu; ", \
                          VECTORID_LOG(pair.first), DataToString(&(pair.second), (type))); \
        } \
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_CopperNode, "%s: Batch Size: %lu, Vector Pair Batch: %s", \
            (msg), (vector).size(), str.ToCStr()); \
    } while (0)

};

#else
#define PRINT_VECTOR_PAIR_BATCH(vector)
#endif

#endif