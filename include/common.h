#ifndef COPPER_COMMON_H_
#define COPPER_COMMON_H_

#include <vector>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <concepts>

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

    inline bool Is_Valid() const {
        return (_id != INVALID_VECTOR_ID) && (_val < MAX_ID_PER_LEVEL);
    }

    inline bool Is_Centroid() const {
        return _level > VECTOR_LEVEL;
    }

    inline bool Is_Vector() const {
        return _level == VECTOR_LEVEL;
    }

    inline bool Is_Leaf() const {
        return _level == LEAF_LEVEL;
    }

    inline bool Is_Internal_Node() const {
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

    // inline bool operator<=(const VectorID& ID) const {
    //     return _id <= ID._id;
    // }

    // inline bool operator>=(const VectorID& ID) const {
    //     return _id >= ID._id;
    // }

    // inline bool operator<(const VectorID& ID) const {
    //     return _id < ID._id;
    // }

    // inline bool operator>(const VectorID& ID) const {
    //     return _id > ID._id;
    // }

    inline void operator=(const uint64_t& ID) {
        _id = ID;
    }

    inline bool operator==(const uint64_t& ID) const {
        return _id == ID;
    }

    inline bool operator!=(const uint64_t& ID) const {
        return _id != ID;
    }

    // inline bool operator<=(const uint64_t& ID) const {
    //     return _id <= ID;
    // }

    // inline bool operator>=(const uint64_t& ID) const {
    //     return _id >= ID;
    // }

    // inline bool operator<(const uint64_t& ID) const {
    //     return _id < ID;
    // }

    // inline bool operator>(const uint64_t& ID) const {
    //     return _id > ID;
    // }
};

typedef void* Address;

constexpr Address INVALID_ADDRESS = nullptr;

template <typename T, uint16_t _DIM, uint16_t _MIN_SIZE, uint16_t _MAX_SIZE,
          typename DIST_TYPE, template<typename, uint16_t, typename> class _CORE> class Copper_Node;

template <typename T, uint16_t _DIM, uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX,
          typename DIST_TYPE, template<typename, uint16_t, typename> class _CORE> class VectorIndex;

template <typename T, uint16_t _DIM, uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX,
          typename DIST_TYPE, template<typename, uint16_t, typename> class _CORE> class Buffer_Manager;

template<typename T, uint16_t _DIM>
struct VectorPair;

template<typename T, uint16_t _DIM, uint16_t _CAP>
class VectorSet;

// Todo: Log VectorIndex: VectorIndex(RootID:%s(%lu, %lu, %lu), # levels:lu, # nodes:lu, # vectors:lu, size:lu)

#define VECTORID_LOG_FMT "%s%lu(%lu, %lu, %lu)"
#define VECTORID_LOG(vid) (!((vid).Is_Valid()) ? "[INV]" : ""), (vid)._id, (vid)._creator_node_id, (vid)._level, (vid)._val

#define NODE_LOG_FMT "(%s<%hu, %hu>, ID:" VECTORID_LOG_FMT ", Size:%hu, ParentID:" VECTORID_LOG_FMT ")"
// todo remove
#define NODE_PTR_LOG(node)\
    ((node) == nullptr ? "NULL" :\
        (!((node)->CentroidID().Is_Valid()) ? "INV" : ((node)->CentroidID().Is_Vector() ? "Non-Centroid" : \
            ((node)->CentroidID().Is_Leaf() ? "Leaf" : ((node)->CentroidID().Is_Internal_Node() ? "Internal" \
                : "UNDEF"))))),\
    ((node) == nullptr ? 0 : std::remove_reference_t<decltype(*(node))>::_MIN_SIZE_),\
    ((node) == nullptr ? 0 : std::remove_reference_t<decltype(*(node))>::_MAX_SIZE_),\
    VECTORID_LOG(((node) == nullptr ? copper::INVALID_VECTOR_ID : (node)->CentroidID())),\
    ((node) == nullptr ? 0 : (node)->Size()),\
    VECTORID_LOG(((node) == nullptr ? copper::INVALID_VECTOR_ID : (node)->ParentID()))

/* #define NODE_VAL_LOG(node)\
    (!((node).CentroidID().Is_Valid()) ? "INV" : ((node).CentroidID().Is_Vector() ? "Non-Centroid" : \
            ((node).CentroidID().Is_Leaf() ? "Leaf" : ((node).CentroidID().Is_Internal_Node() ? "Internal" \
                : "UNDEF")))),\
    std::remove_reference_t<decltype((node))>::_MIN_SIZE_, std::remove_reference_t<decltype((node))>::_MAX_SIZE_,\
    VECTORID_LOG((node).CentroidID()), ((node).Size()), VECTORID_LOG((node).ParentID()) */

#define VECTOR_UPDATE_LOG_FMT "(ID:" VECTORID_LOG_FMT ", Address:%lu)"
#define VECTOR_UPDATE_LOG(update) VECTORID_LOG((update).vector_id), (update).vector_data

};

#endif