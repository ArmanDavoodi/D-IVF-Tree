#ifndef COPPER_COMMON_H
#define COPPER_COMMON_H

#include <vector>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "debug.h"

namespace copper {

struct RetStatus {
    bool OK;
    const char* message;

    static inline RetStatus Success() {
        return RetStatus{true, "OK"};
    }

    static inline RetStatus Fail(const char* msg) {
        return RetStatus{false, msg};
    }

    inline bool Is_OK() const {
        return OK;
    }

    inline const char* Msg() const {
        return message;
    }
};

constexpr uint64_t INVALID_VECTOR_ID = 0;

union VectorID {
    uint64_t _id;
    struct {
        uint64_t _creator_node_id : 8;
        uint64_t _level : 8; // == 0 for vectors, == 1 for leaves
        uint64_t val : 48;
    };

    static constexpr uint64_t MAX_ID_PER_LEVEL = 0x0000FFFFFFFFFFFF;
    static constexpr uint64_t VECTOR_LEVEL = 0;
    static constexpr uint64_t LEAF_LEVEL = 1;

    VectorID() : _id(INVALID_VECTOR_ID) {}
    VectorID(const uint64_t& ID) : _id(ID) {}
    VectorID(const VectorID& ID) : _id(ID._id) {}

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

};

#endif