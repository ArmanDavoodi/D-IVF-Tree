#ifndef DIVFTREE_VECTOR_UTILS_H_
#define DIVFTREE_VECTOR_UTILS_H_

#include "common.h"
#include "distributed_common.h"

#include <queue>

namespace divftree {

struct VectorUpdate {
    VectorID vector_id;
    Address vector_data;

    inline bool IsValid() const {
        return vector_id.IsValid() && (vector_data != INVALID_ADDRESS);
    }
};

class Vector {
public:
    Vector(Vector& other) = delete;
    Vector(const Vector& other) = delete;
    Vector& operator=(Vector& other) = delete;
    Vector& operator=(const Vector& other) = delete;

    Vector() : _data(nullptr), _delete_on_destroy(false) {}

    /* Copy Constructors */
    Vector(const Vector& other, uint16_t dim) : _data(other.IsValid() ? malloc(dim * sizeof(VTYPE)) : nullptr),
                                                _delete_on_destroy(other.IsValid())
#ifdef MEMORY_DEBUG
        , linkCnt(new std::atomic<uint64_t>(1))
#endif
    {
        FatalAssert(dim > 0, LOG_TAG_VECTOR, "Cannot create vector with dimension 0.");
        FatalAssert(!other.IsValid() || IsValid(), LOG_TAG_VECTOR, "Malloc failed");
        if (IsValid()) {
            memcpy(_data, other._data, dim * sizeof(VTYPE));
        }
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "copy constructor: other=%p, this=%p, address=%p", &other, this, _data);
    }

    Vector(AddressToConst other, uint16_t dim) : _data(other != nullptr ? malloc(dim * sizeof(VTYPE)) : nullptr),
                                               _delete_on_destroy(other != nullptr)
#ifdef MEMORY_DEBUG
        , linkCnt(new std::atomic<uint64_t>(1))
#endif
    {
        FatalAssert(dim > 0, LOG_TAG_VECTOR, "Cannot create vector with dimension 0.");
        FatalAssert((other == nullptr) || IsValid(), LOG_TAG_VECTOR, "Malloc failed");
        if (IsValid()) {
            memcpy(_data, other, dim * sizeof(VTYPE));
        }
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "copy constructor: this=%p, address=%p", &other, this, _data);
    }

    /* Link Constructors */
    Vector(Vector&& other) : _data(other._data), _delete_on_destroy(other._delete_on_destroy)
#ifdef MEMORY_DEBUG
        , linkCnt(other.linkCnt)
#endif
    {
        other._data = nullptr;
        other._delete_on_destroy = false;
#ifdef MEMORY_DEBUG
        other.linkCnt = nullptr;
#endif
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Move vector: other=%p to this=%p, address=%p, delete_on_destroy=%s",
             &other, this, _data, (_delete_on_destroy ? "T" : "F"));
    }

    /*
     * should not use AddressToConst as we would be able to change the data of that address.
     * The user should ensure that if the address is valid, it is not freed or deleted while
     * this vector(or other vectors) is linked to it.
     */
    Vector(Address data) : _data(data), _delete_on_destroy(false) {
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "link constructor: this=%p, address=%p",
             this, _data);
    }

    ~Vector() {
        if (!IsValid()) {
            FatalAssert(_delete_on_destroy == false, LOG_TAG_VECTOR,
                        "Vector is invalid but delete_on_destroy is true. This should not happen. this=%p", this);
            return;
        }

        if (_delete_on_destroy) {
            CLOG(LOG_LEVEL_LOG, LOG_TAG_MEMORY, "Destroy vector: this=%p, address=%p", this, _data);
#ifdef MEMORY_DEBUG
            FatalAssert((linkCnt == nullptr) || (linkCnt->load() == 1), LOG_TAG_VECTOR,
                        "Vector is linked to other vectors. Cannot delete it. this=%p, address=%p, linkCnt=%lu",
                        this, _data, linkCnt->load());
            if (linkCnt != nullptr) {
                delete linkCnt;
                linkCnt = nullptr;
            }
#endif
            free(_data);
        } else {
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Unlink vector: this=%p, address=%p", this, _data);
        }
        _data = nullptr;
        _delete_on_destroy = false;
    }

    Vector& operator=(Vector&& other) {
        FatalAssert(!(IsValid()), LOG_TAG_VECTOR, "Vector is not invalid.");
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Move vector: other=%p to this=%p, address=%p, delete_on_destroy=%s",
             &other, this, other._data, (other._delete_on_destroy ? "T" : "F"));
        _data = other._data;
        _delete_on_destroy = other._delete_on_destroy;
#ifdef MEMORY_DEBUG
        linkCnt = other.linkCnt;
        other.linkCnt = nullptr;
#endif
        other._data = nullptr;
        other._delete_on_destroy = false;
        return *this;
    }

    inline constexpr bool IsValid() const {
        return (_data != nullptr);
    }

    inline void Create(uint16_t dim) {
        FatalAssert(!(IsValid()), LOG_TAG_VECTOR, "Vector is not invalid.");
        FatalAssert(dim > 0, LOG_TAG_VECTOR, "Cannot create vector with dimension 0.");
        _data = malloc(dim * sizeof(VTYPE));
        _delete_on_destroy = true;
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Malloc failed");
#ifdef MEMORY_DEBUG
        linkCnt = new std::atomic<uint64_t>(1);
#endif
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Created vector: this=%p, address=%p, dimension=%hu", this, _data, dim);
    }

    /*
     * Will result in undefined behavior if another vector is linked to this vector.
     */
    inline void Destroy() {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        FatalAssert(_delete_on_destroy, LOG_TAG_MEMORY,
                    "Cannot destroy a linked vector. _delete_on_destroy is false. this=%p, address=%p", this, _data);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Destroy vector: this=%p, address=%p", this, _data);
#ifdef MEMORY_DEBUG
        FatalAssert((linkCnt == nullptr) || (linkCnt->load() == 1), LOG_TAG_VECTOR,
                    "Vector is linked to other vectors. Cannot delete it. this=%p, address=%p, linkCnt=%lu",
                    this, _data, linkCnt->load());
        if (linkCnt != nullptr) {
            delete linkCnt;
            linkCnt = nullptr;
        }
#endif
        free(_data);
        _data = nullptr;
        _delete_on_destroy = false;
    }

    /*
     * If the src vector is a linked vector, the user should ensure that the associated data is not freed or deleted
     * while this vector is linked to it.
     */
    inline void Link(Vector& src) {
        FatalAssert(!(IsValid()), LOG_TAG_VECTOR, "Vector is valid");
        _data = src._data;
        _delete_on_destroy = false;
#ifdef MEMORY_DEBUG
        linkCnt = src.linkCnt;
        if (linkCnt != nullptr) {
            (void)linkCnt->fetch_add(1);
        }
#endif
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Link vector: this=%p, address=%p", this, _data);
    }

    /*
     * If the is not null, the user should ensure that it is not freed or deleted
     * while this vector or other vectors are linked to it.
     */
    inline void Link(void* src) {
        FatalAssert(!(IsValid()), LOG_TAG_VECTOR, "Vector is valid");
        _data = src;
        _delete_on_destroy = false;
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Link vector: this=%p, address=%p", this, _data);
    }

    inline void Unlink() {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        FatalAssert(!_delete_on_destroy, LOG_TAG_MEMORY,
                    "Not a linked vector. _delete_on_destroy is true. this=%p, address=%p", this, _data);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Unlink vector: this=%p, address=%p", this, _data);
        _data = nullptr;
        _delete_on_destroy = false;
#ifdef MEMORY_DEBUG
        if (linkCnt != nullptr) {
            FatalAssert((linkCnt->load() > 1), LOG_TAG_VECTOR,
                        "This is the last linked vector and should not be unlinked. this=%p, address=%p, linkCnt=%lu",
                        this, _data, linkCnt->load());
            (void)linkCnt->fetch_sub(1);
        }
#endif
    }

    inline void CopyFrom(const Vector& src, uint16_t dim) {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        FatalAssert(src.IsValid(), LOG_TAG_VECTOR, "Source Vector is not invalid.");

        memcpy(_data, src._data, dim * sizeof(VTYPE));
    }

    inline void CopyFrom(AddressToConst src, uint16_t dim) {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        FatalAssert(src != INVALID_ADDRESS, LOG_TAG_VECTOR, "Source should not be invalid.");

        memcpy(_data, src, dim * sizeof(VTYPE));
    }

    inline Address GetData() {
        return _data;
    }

    inline AddressToConst GetData() const {
        return _data;
    }

    inline bool operator==(const Vector& other) const {
        return (_data == other._data);
    }

    inline bool operator!=(const Vector& other) const {
        return (_data != other._data);
    }

    inline bool Similar(const Vector& other, uint16_t dim) const {
        if (*this == other) {
            return true;
        }

        if (!(IsValid()) || !(other.IsValid())) {
            return false;
        }

        return !(memcmp(_data, other._data, dim * sizeof(VTYPE)));
    }

    inline VTYPE& operator[](size_t idx) {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        return (static_cast<VTYPE*>(_data))[idx];
    }

    inline const VTYPE& operator[](size_t idx) const {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        return (static_cast<const VTYPE*>(_data))[idx];
    }

    inline String ToString(uint16_t dim) const {
        if (!(IsValid())) {
            return String("INV");
        }
        String str("(address=%p, linked=%s)<", _data, _delete_on_destroy ? "T" : "F");
        for (uint16_t i = 0; i < dim; ++i) {
            str += String(VTYPE_FMT, (*this)[i]) + String((i < (dim - 1)) ? ", " : ">");
        }

        return str;
    }

protected:
    void* _data;
    bool _delete_on_destroy; // If true, the vector will be deleted when destroyed.
#ifdef MEMORY_DEBUG
    std::atomic<uint64_t>* linkCnt = nullptr;
#endif

TESTABLE;
};

enum VectorState : uint8_t {
    VECTOR_STATE_INVALID = 0,
    VECTOR_STATE_VALID = 1,
    VECTOR_STATE_MIGRATED = 2,
    VECTOR_STATE_OUTDATED = 3
};

inline String VectorStateToString(VectorState state) {
    switch (state)
    {
    case VECTOR_STATE_INVALID:
        return String("INVALID");
    case VECTOR_STATE_VALID:
        return String("VALID");
    case VECTOR_STATE_MIGRATED:
        return String("MIGRATED");
    case VECTOR_STATE_OUTDATED:
        return String("OUTDATED");
    default:
        return String("UNDEFINED");
    }
}


struct VectorMetaData {
    VectorID id;
    VectorState state;
};

struct CentroidMetaData {
    VectorID id;
    Version version;
    VectorState state;
};

struct ClusterHeader {
    const bool is_leaf;
    const uint16_t block_size;
    const uint16_t capacity;
    const uint16_t dimension;
    std::atomic<uint16_t> reserved_size;
    std::atomic<uint16_t> visible_size;
    std::atomic<uint16_t> num_deleted;
    std::atomic<uint64_t> reader_pin;
};
class Cluster {
public:
    inline static size_t Bytes(bool is_leaf_vertex, uint16_t cap, uint16_t dim) {
        const uint64_t header_bytes = (is_leaf_vertex ? sizeof(VectorMetaData) : sizeof(CentroidMetaData));
        const uint64_t data_bytes = sizeof(VTYPE) * (uint64_t)dim;
        return sizeof(ClusterHeader) + cap * (header_bytes + data_bytes);
    }

    Cluster(bool is_leaf_vertex, uint16_t block_cap, uint16_t cap, uint16_t dim, bool set_zero = true) :
            header{is_leaf_vertex, block_cap, cap, dim, 0, 0, 0, 0} {
        FatalAssert(block_cap > 0, LOG_TAG_CLUSTER, "Block size must be greater than 0. block_size=%hu", block_cap);
        FatalAssert(cap > 0, LOG_TAG_CLUSTER, "Capacity must be greater than 0. capacity=%hu", cap);
        FatalAssert(dim > 0, LOG_TAG_CLUSTER, "Dimension must be greater than 0. dimension=%hu", dim);
        const uint64_t header_bytes = (header.is_leaf ? sizeof(VectorMetaData) : sizeof(CentroidMetaData));
        const uint64_t data_bytes = sizeof(VTYPE) * (uint64_t)header.dimension;
        const uint16_t block_bytes = header.block_size * (header_bytes + data_bytes);
        const uint16_t total_blocks = (header.capacity + header.block_size - 1) / header.block_size;
        const uint64_t bytes = header.capacity * (header_bytes + data_bytes);
        if (set_zero) {
            memset(blocks, 0, bytes);
        }
    }

    inline const void* MetaData(uint16_t offset) const {
        FatalAssert(offset < header.capacity, LOG_TAG_CLUSTER, "Offset is out of bounds. offset=%hu, capacity=%hu",
                    offset, header.capacity);
        const uint16_t block_number = offset / header.block_size;
        const uint16_t block_offset = offset % header.block_size;
        const uint64_t header_bytes = (header.is_leaf ? sizeof(VectorMetaData) : sizeof(CentroidMetaData));
        const uint64_t data_bytes = sizeof(VTYPE) * (uint64_t)header.dimension;
        const uint16_t block_bytes = header.block_size * (header_bytes + data_bytes);
        FatalAssert(block_number == 0, LOG_TAG_CLUSTER,
                    "Only single block clusters are supported currently. offset=%hu, "
                    "block_number=%hu, block_size=%hu", offset, block_number, header.block_size);
        return reinterpret_cast<const void*>(blocks +
                                             block_number * block_bytes + block_offset * header_bytes);
    }

    inline void* MetaData(uint16_t offset) {
        FatalAssert(offset < header.capacity, LOG_TAG_CLUSTER, "Offset is out of bounds. offset=%hu, capacity=%hu",
                    offset, header.capacity);
        const uint16_t block_number = offset / header.block_size;
        const uint16_t block_offset = offset % header.block_size;
        const uint64_t header_bytes = (header.is_leaf ? sizeof(VectorMetaData) : sizeof(CentroidMetaData));
        const uint64_t data_bytes = sizeof(VTYPE) * (uint64_t)header.dimension;
        const uint16_t block_bytes = header.block_size * (header_bytes + data_bytes);
        FatalAssert(block_number == 0, LOG_TAG_CLUSTER,
                    "Only single block clusters are supported currently. offset=%hu, "
                    "block_number=%hu, block_size=%hu", offset, block_number, header.block_size);
        return reinterpret_cast<void*>(blocks +
                                       block_number * block_bytes + block_offset * header_bytes);
    }

    inline const VTYPE* Data(uint16_t offset) const {
        FatalAssert(offset < header.capacity, LOG_TAG_CLUSTER, "Offset is out of bounds. offset=%hu, capacity=%hu",
                    offset, header.capacity);
        const uint16_t block_number = offset / header.block_size;
        const uint16_t block_offset = offset % header.block_size;
        const uint64_t header_bytes = (header.is_leaf ? sizeof(VectorMetaData) : sizeof(CentroidMetaData));
        const uint64_t data_bytes = sizeof(VTYPE) * (uint64_t)header.dimension;
        const uint16_t block_bytes = header.block_size * (header_bytes + data_bytes);
        const uint16_t meta_data_bytes = (block_number == (header.block_size - 1) ?
                                          header.capacity % header.block_size : header.block_size) *
                                          header_bytes;
        FatalAssert(block_number == 0, LOG_TAG_CLUSTER,
                    "Only single block clusters are supported currently. offset=%hu, "
                    "block_number=%hu, block_size=%hu", offset, block_number, header.block_size);
        return reinterpret_cast<const VTYPE*>(blocks +
                                              block_number * block_bytes + meta_data_bytes + block_offset * data_bytes);
    }

    inline VTYPE* Data(uint16_t offset) {
        FatalAssert(offset < header.capacity, LOG_TAG_CLUSTER, "Offset is out of bounds. offset=%hu, capacity=%hu",
                    offset, header.capacity);
        const uint16_t block_number = offset / header.block_size;
        const uint16_t block_offset = offset % header.block_size;
        const uint64_t header_bytes = (header.is_leaf ? sizeof(VectorMetaData) : sizeof(CentroidMetaData));
        const uint64_t data_bytes = sizeof(VTYPE) * (uint64_t)header.dimension;
        const uint16_t block_bytes = header.block_size * (header_bytes + data_bytes);
        const uint16_t meta_data_bytes = (block_number == (header.block_size - 1) ?
                                          header.capacity % header.block_size : header.block_size) *
                                          header_bytes;
        FatalAssert(block_number == 0, LOG_TAG_CLUSTER,
                    "Only single block clusters are supported currently. offset=%hu, "
                    "block_number=%hu, block_size=%hu", offset, block_number, header.block_size);
        return reinterpret_cast<VTYPE*>(blocks +
                                        block_number * block_bytes + meta_data_bytes + block_offset * data_bytes);
    }
    /*
     * a cluster consists of a header and multiple blocks
     * ---------------------------------------------------
     * the blocks of an internal cluster have this structure:
     *
     * | VectorID | Version | State | ...
     * ---------------------------------------------------
     * | VectorData | ...
     *
     * the blocks of a leaf cluster have this structure:
     * | VectorID | State | ...
     * ---------------------------------------------------
     * | VectorData | ...
     */
    ClusterHeader header;
protected:
    char blocks[];

TESTABLE;
}
// struct ClusterEntry {
//     std::atomic<VectorState> state;
//     VectorID id; // should this also be atomic?
//     char vector[];

//     inline void SanityCheck() const {
//         SanityCheck(state.load(), id);
//     }

//     inline static void SanityCheck(VectorState _state, VectorID _id) const {
//         UNUSED_VARIABLE(_state);
//         UNUSED_VARIABLE(copy);
//         FatalAssert(!(_state.IsValid()) || _id.IsValid(), LOG_TAG_CLUSTER,
//                     "VectorID is not valid when state is valid. state=%s, id=" VECTORID_LOG_FMT ", entry=%s",
//                     _state.ToString().ToCStr(), VECTORID_LOG(_id), ToString().ToCStr());
//         FatalAssert(!(_state.IsMoving()) || !(_state.IsInserting()), LOG_TAG_CLUSTER,
//                     "Vector cannot be both in the moving and insertion state at the same time! state=%s, id="
//                     VECTORID_LOG_FMT ", entry=%s",
//                     _state.ToString().ToCStr(), VECTORID_LOG(_id), ToString().ToCStr());
//         FatalAssert(!(_state.IsValid()) || !(_state.IsInserting()), LOG_TAG_CLUSTER,
//                     "Vector cannot be both valid and inserting at the same time! state=%s, id="
//                     VECTORID_LOG_FMT ", entry=%s",
//                     _state.ToString().ToCStr(), VECTORID_LOG(_id), ToString().ToCStr());
//         FatalAssert((_state.IsValid()) || !(_state.IsMoving()), LOG_TAG_CLUSTER,
//                     "Vector cannot move invalid vector! state=%s, id="
//                     VECTORID_LOG_FMT ", entry=%s",
//                     _state.ToString().ToCStr(), VECTORID_LOG(_id), ToString().ToCStr());
//     }

//     inline bool IsValid() const {
//         VectorState _state = state.load();
//         SanityCheck(_state, id);
//         return _state.IsValid();
//     }

//     inline bool IsVisible() const {
//         VectorState _state = state.load();
//         SanityCheck(_state, id);
//         return _state.IsVisible();
//     }

//     /* Should only be called when the vertex lock is held(either in shared or exclusive mode) */
//     inline bool Move(VectorState& current_state) {
//         VectorState expected = VectorState::Valid();
//         current_state = VectorState::Valid() | VectorState::Move();
//         if (state.compare_exchange_strong(expected, current_state)) {
//             return *true;
//         }
//         current_state = expected;
//         return false;
//     }

//     /* Should only be called when the vertex lock is held(either in shared or exclusive mode) */
//     inline bool Delete(VectorState& old_state) {
//         old_state = state.exchange(VectorState::Invalid());
//         return !(old_state.IsValid());
//     }

//     inline bool operator==(const ClusterEntry& other) const {
//         return (this == &other);
//     }

//     inline bool operator!=(const ClusterEntry& other) const {
//         return (this != &other);
//     }

//     inline bool Similar(const ClusterEntry& other, uint16_t dim) const {
//         if (*this == other) {
//             return true;
//         }

//         return !(memcmp(vector, other.vector, dim * sizeof(VTYPE)));
//     }

//     inline VTYPE& operator[](size_t idx) {
//         return reinterpret_cast<VTYPE[]>(vector)[idx];
//     }

//     inline const VTYPE& operator[](size_t idx) const {
//         return reinterpret_cast<const VTYPE[]>(vector)[idx];
//     }

//     inline String ToString() const {
//         String str = String("<Address=%p, State: ", (void*)this) +
//                      state.load().ToString() + String("ID=" VECTORID_LOG_FMT ">", VECTORID_LOG(id));

//         return str;
//     }

//     inline String ToString(uint16_t dim) const {
//         String str = String("<Address=%p, State: ", (void*)this) +
//                      state.load().ToString() + String("ID=" VECTORID_LOG_FMT ">[", VECTORID_LOG(id));
//         for (uint16_t i = 0; i < dim; ++i) {
//             str += String(VTYPE_FMT, (*this)[i]) + String((i < (dim - 1)) ? ", " : "]");
//         }

//         return str;
//     }
// };

// class Cluster {
// public:
//     static inline constexpr size_t SingleEntryBytes(uint16_t dim) {
//         return sizeof(ClusterEntry) + (sizeof(VTYPE) * (uint64_t)dim);
//     }

//     static inline constexpr size_t AllignedDataBytes(uint16_t dim, uint16_t cap) {
//         return ALLIGNED_SIZE(SingleEntryBytes(dim) * (uint64_t)cap);
//     }

//     static inline constexpr size_t AllignedHeaderBytes() {
//         return ALLIGNED_SIZE(sizeof(Cluster));
//     }

//     static inline constexpr size_t AllignedBytes(uint16_t dim, uint16_t cap) {
//         return AllignedHeaderBytes() + AllignedDataBytes(dim, cap);
//     }

//     static Cluster* CreateCluster(uint64_t version, VectorID centroid_id,
//                                   uint16_t min_size, uint16_t max_size,
//                                   uint16_t dimension, uint8_t cluster_owner,
//                                   uint8_t num_dist_pins) {

//         Cluster* cluster = static_cast<Cluster*>(std::aligned_alloc(CACHE_LINE_SIZE,
//                                                                     AllignedBytes(dimension, max_size)));
//         FatalAssert(cluster != nullptr, LOG_TAG_TEST, "Failed to allocate memory for Cluster.");
//         new (cluster) Cluster(sizeof(Cluster), version, centroid_id, min_size, max_size, dimension,
//                               cluster_owner, num_dist_pins);
//         CLOG(LOG_LEVEL_DEBUG, LOG_TAG_TEST, "Created Cluster: %s",
//              cluster->ToString().ToCStr());
//         return cluster;
//     }

//     Cluster(uint64_t raw_header_bytes, uint64_t version, VectorID centroid_id,
//             uint16_t min_size, uint16_t max_size,
//             uint16_t dimension, uint8_t cluster_owner,
//             uint8_t num_dist_pins) : _header_bytes(ALLIGNED_SIZE(raw_header_bytes)),
//                                          _version(version), _centroid_id(centroid_id),
//                                          _min_size(min_size), _max_size(max_size),
//                                          _dim(dimension), _collectable(0), _owner(cluster_owner),
//                                          _distributed_pin(num_dist_pins), _real_size(0),
//                                          _reserved_size(0), _parent_id(INVALID_VECTOR_ID),
//                                          _entry(ALLIGNEMENT(raw_header_bytes) + (void*)_data) {
//         FatalAssert(num_dist_pins > 0, LOG_TAG_CLUSTER,
//                     "Number of distributed pins should be greater than 0. num_dist_pins=%d", num_dist_pins);
//         FatalAssert(num_dist_pins <= NUM_COMPUTE_NODES, LOG_TAG_CLUSTER,
//                     "Number of distributed pins should be less than or equal to number of compute nodes. "
//                     "num_dist_pins=%d, NUM_COMPUTE_NODES=%d", num_dist_pins, NUM_COMPUTE_NODES);
//         FatalAssert(raw_header_bytes >= sizeof(Cluster), LOG_TAG_CLUSTER,
//                     "Raw header bytes(%lu) does not contain enough space(%lu).",
//                     raw_header_bytes, sizeof(Cluster));
//         FatalAssert(_header_bytes >= raw_header_bytes, LOG_TAG_CLUSTER,
//                     "Cluster bytes(%lu) does not contain enough space(%lu).",
//                     _header_bytes, raw_header_bytes);
//         FatalAssert(_header_bytes >= AllignedHeaderBytes(), LOG_TAG_CLUSTER,
//                     "Cluster bytes(%lu) does not contain enough space(%lu).",
//                     _header_bytes, AllignedHeaderBytes());
//         FatalAssert(ALLIGNED(_header_bytes), LOG_TAG_CLUSTER,
//                     "Cluster bytes(%lu) is not aligned to cache line size(%lu).",
//                     _header_bytes, CACHE_LINE_SIZE);
//         FatalAssert(_centroid_id.IsValid(), LOG_TAG_CLUSTER, "Invalid centroid ID.");
//         CHECK_MIN_MAX_SIZE(_min_size, _max_size, LOG_TAG_CLUSTER);
//         FatalAssert(_dim > 0, LOG_TAG_CLUSTER, "Cannot create a Cluster with dimension of 0.");
//         FatalAssert(_entry + _max_size + 1 == (void*)this + _header_bytes + AllignedDataBytes(_dim, _max_size),
//                     LOG_TAG_CLUSTER, "First entry is not in the right place. _entry=%p, this=%p",
//                     _entry, this);
//         FatalAssert(_max_size < UINT16_MAX, LOG_TAG_CLUSTER,
//                     "Cluster size is too large. _max_size=%hu", _max_size);
//         FatalAssert(_owner < MAX_COMPUTE_NODE_ID, LOG_TAG_CLUSTER,
//                     "Cluster owner is invalid. _owner=%hu", _owner);
//         FatalAssert(IsComputeNode(_owner), LOG_TAG_CLUSTER,
//                     "Cluster owner is not a compute node. _owner=%hu", _owner);

//         memset(_data, 0, ALLIGNEMENT(raw_header_bytes) + AllignedDataBytes(_dim, _max_size));
//         for (uint16_t i = 0; i < _max_size; ++i) {
//             _entry[i].id = INVALID_VECTOR_ID;
//         }
//     }

//     Cluster(const Cluster& other) = delete;
//     Cluster(Cluster&& other) = delete;
//     ~Cluster() = default;

//     inline void CheckValid(bool check_min_size) const {
// #ifdef ENABLE_ASSERTS
//         FatalAssert(_header_bytes >= AllignedHeaderBytes(), LOG_TAG_CLUSTER,
//                     "Cluster bytes(%lu) does not contain enough space(%lu).",
//                     _header_bytes, AllignedHeaderBytes());
//         FatalAssert(ALLIGNED(_header_bytes), LOG_TAG_CLUSTER,
//                     "Cluster bytes(%lu) is not aligned to cache line size(%lu).",
//                     _header_bytes, CACHE_LINE_SIZE);
//         CHECK_VECTORID_IS_VALID(_centroid_id, LOG_TAG_CLUSTER);
//         CHECK_VECTORID_IS_CENTROID(_centroid_id, LOG_TAG_CLUSTER);
//         CHECK_MIN_MAX_SIZE(_min_size, _max_size, LOG_TAG_CLUSTER);
//         FatalAssert(_dim > 0, LOG_TAG_CLUSTER, "Cannot create a Cluster with dimension of 0.");
//         FatalAssert(_entry + _max_size + 1 == (void*)this + _header_bytes + AllignedDataBytes(_dim, _max_size),
//                     LOG_TAG_CLUSTER, "First entry is not in the right place. _entry=%p, this=%p",
//                     _entry, this);
//         FatalAssert(_max_size < UINT16_MAX, LOG_TAG_CLUSTER,
//                     "Cluster size is too large. _max_size=%hu", _max_size);
//         FatalAssert(_owner < MAX_COMPUTE_NODE_ID, LOG_TAG_CLUSTER,
//                     "Cluster owner is invalid. _owner=%hu", _owner);
//         FatalAssert(IsComputeNode(_owner), LOG_TAG_CLUSTER,
//                     "Cluster owner is not a compute node. _owner=%hu", _owner);
//         uint16_t size = _real_size.load();
//         if (check_min_size) {
//             FatalAssert(size >= _min_size, LOG_TAG_CLUSTER,
//                         "Cluster size(%hu) is less than minimum size(%hu).", size, _min_size);
//         }
//         FatalAssert(size <= _max_size, LOG_TAG_CLUSTER,
//                     "Cluster size(%hu) is larger than maximum size(%hu).", size, _max_size);
//         uint16_t real_size = 0;
//         for (uint16_t i = 0; i < _max_size; ++i) {
//             _entry[i].SanityCheck();
//         }
// #endif
//     }

//     /* Should only be called when vertex lock is held(either shared or exclusive) */
//     /* Will return invalid address to indicate that cluster is full */
//     inline RetStatus BatchInsert(std::map<VectorID, BatchVertexUpdateEntry>& updates) {
//         FatalAssert(updates.size() > 0, LOG_TAG_CLUSTER,
//                     "No updates to insert. updates.size()=%lu",
//                     updates.size());
//         uint16_t num_vectors = updates.size();
//         need_split = false;
//         uint16_t offset;
//         uint16_t num_reserved = Reserve(num_vectors, offset);
//         FatalAssert(num_reserved <= num_vectors, LOG_TAG_CLUSTER,
//                     "Reserved %hu vectors but requested %hu vectors.", num_reserved, num_vectors);
//         if (num_reserved < num_vectors) {
//             /* Cluster is full and someone should be splitting it so release your lock if you couldn't CAS change_IP */
//             return RetStatus{.stat = VERTEX_NOT_ENOUGH_SPACE};
//         }

//         FatalAssert(offset + num_reserved <= _max_size, LOG_TAG_CLUSTER,
//                     "Offset(%hu) + num_reserved(%hu) is larger than max size(%hu).",
//                     offset, num_reserved, _max_size);
//         FatalAssert(offset < _max_size, LOG_TAG_CLUSTER,
//                     "Offset(%hu) is larger than max size(%hu).", offset, _max_size);
//         entry = &_entry[offset];
//         std::map<VectorID, BatchVertexUpdateEntry>::iterator it = updates.begin();
//         for (uint16_t i = 0; i < updates.size(); ++i, ++it) {
//             FatalAssert(it != updates.end(), LOG_TAG_CLUSTER,
//                         "Not enough updates for the number of reserved vectors. i=%hu, updates.size()=%lu",
//                         i, updates.size());
//             FatalAssert(i + offset <= _max_size, LOG_TAG_CLUSTER,
//                         "Index(%hu) + offset(%hu) is larger than max size(%hu).",
//                         i, offset, _max_size);
//             FatalAssert(it->second.type == VERTEX_INSERT, LOG_TAG_CLUSTER,
//                         "Invalid update type!");
//             FatalAssert(it->second.insert_args.vector_data != nullptr, LOG_TAG_CLUSTER,
//                         "Invalid vector data!");
//             FatalAssert(it->second.insert_args.cluster_entry_addr != nullptr, LOG_TAG_CLUSTER,
//                         "Invalid cluster entry address pointer!");
//             FatalAssert(it->second.result != nullptr, LOG_TAG_CLUSTER,
//                         "Invalid result pointer!");
//             FatalAssert(it->second.vector_id.IsValid(), LOG_TAG_CLUSTER, "Invalid VectorID in ids[%hu] = " VECTORID_LOG_FMT,
//                         i, VECTORID_LOG(it->second.vector_id));
//             FatalAssert(_entry[offset + i].state.load().IsValid() == false, LOG_TAG_CLUSTER,
//                         "Cluster entry is valid at offset %hu", offset + i);
//             memcpy(_entry[offset + i].vector, (it->second.insert_args.vector_data),
//                    _dim * sizeof(VTYPE));
//             _entry[offset + i].id = it->second.vector_id;
//             _entry[offset + i].state.store(VectorState::Valid()); /* todo memory order */
//             *(it->second.insert_args.cluster_entry_addr) = &_entry[offset + i];
//             it->second.result->stat = RetStatus::SUCCESS;
//         }

//         return RetStatus::Success();
//     }

//     /* Should only be called when vertex is pinned */
//     inline const ClusterEntry* FindEntry(VectorID id, bool onlyConsiderVisible = true) const {
//         uint16_t size = _reserved_size.load();
//         /* todo may need to fine tune so that if it is moving we return and don't move forward */
//         for (uint16_t index = 0; index < size; ++index) {
//             if ((onlyConsiderVisible ? _entry[index].state.load().IsVisible() :
//                                        _entry[index].state.load().IsValid()) &&
//                 (_entry[index].id == id)) {
//                 return &_entry[index];
//             }
//         }

//         return nullptr;
//     }

//     /* Should only be called when vertex is pinned */
//     inline ClusterEntry* FindEntry(VectorID id, bool onlyConsiderVisible = true) {
//         uint16_t size = _reserved_size.load();
//         /* todo may need to fine tune so that if it is moving we return and don't move forward */
//         for (uint16_t index = 0; index < size; ++index) {
//             if ((onlyConsiderVisible ? _entry[index].state.load().IsVisible() :
//                                        _entry[index].state.load().IsValid()) &&
//                 (_entry[index].id == id)) {
//                 return &_entry[index];
//             }
//         }

//         return nullptr;
//     }

//     /* Should only be called when vertex is pinned */
//     inline bool Contains(VectorID id, bool onlyConsiderVisible = true) const {
//         return (FindEntry(id, onlyConsiderVisible) != nullptr);
//     }

//     inline ClusterEntry& operator[](uint16_t idx) {
//         FatalAssert(idx < _max_size, LOG_TAG_CLUSTER, "idx(%hu) >= _max_size(%hu)", idx, _max_size);
//         return _entry[idx];
//     }

//     inline const ClusterEntry& operator[](uint16_t idx) const {
//         FatalAssert(idx < _max_size, LOG_TAG_CLUSTER, "idx(%hu) >= _max_size(%hu)", idx, _max_size);
//         return _entry[idx];
//     }

//     inline uint16_t FilledSize() const {
//         return _reserved_size.load();
//     }

//     /* Has to be called with exclusive lock held if you need accurate results.
//        Cluster should be pinned */
//     inline uint16_t ComputeRealSize() const {
//         size_t size = 0;
//         for (uint16_t i = 0; i < _max_size; ++i) {
//             size += (_entry[i].state.load().IsVisible() ? 1 : 0);
//         }

//         size_t cur_size = _reserved_size.load();
//         UNUSED_VARIABLE(cur_size);
//         FatalAssert(size <= cur_size, LOG_TAG_CLUSTER, "Computed size(%lu) is larger than current size(%hu)",
//                     size, cur_size);
//         return size;
//     }

//     /* May return inconsistent data if SXLock is not held in exclusive mode */
//     String ToString(bool detailed = false) const {
//         String str("<ClusterAddress=%p, StartDataAddress=%p, StartEntryAddress=%p, "
//                    "EndEntryAddress=%p, ClusterHeaderBytes=%lu, TotalAllignedHeaderBytes=%lu,"
//                    " TotalBytes=%lu, Version=%lu, SizeLimit=[%hu %hu], "
//                    "Dim=%hu, CachedParentID=" VECTORID_LOG_FMT
//                    ", CentroidID=" VECTORID_LOG_FMT ", ReservedSize=%hu, Size=%hhu, Owner=%hu",
//                    this, _data, _entry, _entry + _max_size, sizeof(Cluster),
//                    _header_bytes, _header_bytes + AllignedBytes(_dim, _max_size),
//                    _version, _min_size, _max_size, _dim, VECTORID_LOG(_parent_id),
//                    VECTORID_LOG(_centroid_id), _reserved_size.load(), _real_size.load(), _owner);
//         if (detailed) {
//             for (uint16_t i = 0; i < _max_size; ++i) {
//                 str += _entry[i].ToString(_dim);
//                 if (i < (_max_size - 1)) {
//                     str += ", ";
//                 }
//             }
//             str += "]>";
//         }
//         else {
//             str += ">";
//         }
//         return str;
//     }

//     const uint64_t _header_bytes; /* Size of cluster header including vertex header and the padding */
//     const uint64_t _version; /* Incremented during split and compaction */
//     const VectorID _centroid_id;
//     /* only read during split -> holding a shared lock on it for migration of cluster should suffice */
//     VectorID _parent_id; /* may be outdated in a multi node setup */
//     const uint16_t _min_size;
//     const uint16_t _max_size;
//     const uint16_t _dim;
//     /* number of invalid vectors in the cluster that can be removed during compaction */
//     std::atomic<uint16_t> _collectable;
//     uint8_t _owner;
//     std::atomic<uint8_t> _distributed_pin;
//     /* responsibility of the user may change if we only have the shared lock */
//     /* Is used to determine if merge/compaction is necessary */
//     std::atomic<uint16_t> _real_size;
//     ClusterEntry * const _entry;
//     // const DataType _vtype;

// protected:
//     uint16_t Reserve(uint16_t numVectors, uint16_t& offset) {
//         FatalAssert(numVectors > 0, LOG_TAG_CLUSTER, "Cannot reserve 0 vectors.");
//         offset = UINT16_MAX;
//         uint16_t cur_size = _reserved_size.load();
//         while (cur_size < _max_size) {
//             numVectors = std::min(numVectors, _max_size - cur_size);
//             if (_reserved_size.compare_exchange_strong(cur_size, cur_size + numVectors)) {
//                 offset = cur_size;
//                 return numVectors;
//             }
//         }
//         return 0; /* Cluster is full */
//     }

//     std::atomic<uint16_t> _reserved_size;

//     char _data[];

// TESTABLE;
// };
};
#endif