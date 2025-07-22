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

    Vector(ConstAddress other, uint16_t dim) : _data(other != nullptr ? malloc(dim * sizeof(VTYPE)) : nullptr),
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
     * should not use ConstAddress as we would be able to change the data of that address.
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

    inline void CopyFrom(ConstAddress src, uint16_t dim) {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        FatalAssert(src != INVALID_ADDRESS, LOG_TAG_VECTOR, "Source should not be invalid.");

        memcpy(_data, src, dim * sizeof(VTYPE));
    }

    inline Address GetData() {
        return _data;
    }

    inline ConstAddress GetData() const {
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

struct VectorState {
    uint8_t valid : 1; /* If 0 the vector either does not exists, is deleted, or is in middle of insertion */
    /*
     * If set to 1, it means that the vector is being moved and should not be read or deleted.
     * When move is complete it will be set to 0 and valid bit will be unset.
     */
    uint8_t move : 1;
    uint8_t unused : 6;

    inline bool IsVisible() const {
        return (((valid == 1) && (move == 0)) ? true : false);
    }

    inline bool IsValid() const {
        return (valid == 1 ? true : false);
    }

    inline bool IsMoving() const {
        return (move == 1 ? true : false);
    }

    inline VectorState operator|(const VectorState& other) const {
        VectorState state{0};
        state.valid = valid | other.valid;
        state.move = move | other.move;
        return state;
    }

    inline VectorState& operator|=(const VectorState& other) {
        valid |= other.valid;
        move |= other.move;
        return *this;
    }

    inline VectorState operator&(const VectorState& other) const {
        VectorState state{0};
        state.valid = valid & other.valid;
        state.move = move & other.move;
        return state;
    }

    inline VectorState& operator&=(const VectorState& other) {
        valid &= other.valid;
        move &= other.move;
        return *this;
    }

    inline VectorState operator~() const {
        VectorState state{0};
        state.valid = ~valid;
        state.move = ~move;
        return state;
    }

    inline bool operator==(const VectorState& other) const {
        return (valid == other.valid) && (move == other.move);
    }

    inline bool operator!=(const VectorState& other) const {
        return !(*this == other);
    }

    inline static VectorState Invalid() {
        return VectorState{0};
    }

    inline static VectorState Valid() {
        VectorState state{0};
        state.valid = 1;
        return state;
    }

    inline static VectorState Move() {
        VectorState state{0};
        state.move = 1;
        return state;
    }

    inline String ToString() const {
        return String("(valid=%d, move=%d)", valid, move);
    }
};

struct ClusterEntry {
    std::atomic<VectorState> state;
    VectorID id;
    char vector[];

    inline bool IsValid() const {
        VectorState _state = state.load();
        VectorID copy = id;
        UNUSED_VARIABLE(copy);
        FatalAssert(!(_state.IsValid()) || copy.IsValid(), LOG_TAG_CLUSTER,
                    "VectorID is not valid when state is valid. state=%s, id=" VECTORID_LOG_FMT " entry=%s",
                    _state.ToString().ToCStr(), VECTORID_LOG(copy), ToString().ToCStr());
        return _state.IsValid();
    }

    inline bool IsVisible() const {
        VectorState _state = state.load();
        VectorID copy = id;
        UNUSED_VARIABLE(copy);
        FatalAssert(!(_state.IsValid()) || copy.IsValid(), LOG_TAG_CLUSTER,
                    "VectorID is not valid when state is valid. state=%s, id=" VECTORID_LOG_FMT " entry=%s",
                    _state.ToString().ToCStr(), VECTORID_LOG(copy), ToString().ToCStr());
        return _state.IsVisible();
    }

    /* Should only be called when the vertex lock is held(either in shared or exclusive mode) */
    inline bool Move(VectorState& current_state) {
        VectorState expected = VectorState::Valid();
        current_state = VectorState::Valid() | VectorState::Move();
        if (state.compare_exchange_strong(expected, current_state)) {
            return *true;
        }
        current_state = expected;
        return false;
    }

    /* Should only be called when the vertex lock is held(either in shared or exclusive mode) */
    inline bool Delete(VectorState& old_state) {
        old_state = state.exchange(VectorState::Invalid());
        return !(old_state.IsValid());
    }

    inline bool operator==(const ClusterEntry& other) const {
        return (this == &other);
    }

    inline bool operator!=(const ClusterEntry& other) const {
        return (this != &other);
    }

    inline bool Similar(const ClusterEntry& other, uint16_t dim) const {
        if (*this == other) {
            return true;
        }

        return !(memcmp(vector, other.vector, dim * sizeof(VTYPE)));
    }

    inline VTYPE& operator[](size_t idx) {
        return reinterpret_cast<VTYPE[]>(vector)[idx];
    }

    inline const VTYPE& operator[](size_t idx) const {
        return reinterpret_cast<const VTYPE[]>(vector)[idx];
    }

    inline String ToString() const {
        String str = String("<Address=%p, State: ", (void*)this) +
                     state.load().ToString() + String("ID=" VECTORID_LOG_FMT ">", VECTORID_LOG(id));

        return str;
    }

    inline String ToString(uint16_t dim) const {
        String str = String("<Address=%p, State: ", (void*)this) +
                     state.load().ToString() + String("ID=" VECTORID_LOG_FMT ">[", VECTORID_LOG(id));
        for (uint16_t i = 0; i < dim; ++i) {
            str += String(VTYPE_FMT, (*this)[i]) + String((i < (dim - 1)) ? ", " : "]");
        }

        return str;
    }
};

class Cluster {
public:
    static inline constexpr size_t SingleEntryBytes(uint16_t dim) {
        return sizeof(ClusterEntry) + (sizeof(VTYPE) * (uint64_t)dim);
    }

    static inline constexpr size_t AllignedDataBytes(uint16_t dim, uint16_t cap) {
        return ALLIGNED_SIZE(SingleEntryBytes(dim) * (uint64_t)cap);
    }

    static inline constexpr size_t AllignedHeaderBytes() {
        return ALLIGNED_SIZE(sizeof(Cluster));
    }

    static inline constexpr size_t AllignedBytes(uint16_t dim, uint16_t cap) {
        return AllignedHeaderBytes() + AllignedDataBytes(dim, cap);
    }

    static Cluster* CreateCluster(uint64_t version, VectorID centroid_id,
                                  uint16_t min_size, uint16_t max_size,
                                  uint16_t dimension, uint8_t cluster_owner,
                                  uint8_t num_dist_pins) {

        Cluster* cluster = static_cast<Cluster*>(std::aligned_alloc(CACHE_LINE_SIZE,
                                                                    AllignedBytes(dimension, max_size)));
        FatalAssert(cluster != nullptr, LOG_TAG_TEST, "Failed to allocate memory for Cluster.");
        new (cluster) Cluster(sizeof(Cluster), version, centroid_id, min_size, max_size, dimension,
                              cluster_owner, num_dist_pins);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_TEST, "Created Cluster: %s",
             cluster->ToString().ToCStr());
        return cluster;
    }

    Cluster(uint64_t raw_header_bytes, uint64_t version, VectorID centroid_id,
            uint16_t min_size, uint16_t max_size,
            uint16_t dimension, uint8_t cluster_owner,
            uint8_t num_dist_pins) : _header_bytes(ALLIGNED_SIZE(raw_header_bytes)),
                                         _version(version), _centroid_id(centroid_id),
                                         _min_size(min_size), _max_size(max_size),
                                         _dim(dimension), _collectable(0), _owner(cluster_owner),
                                         _distributed_pin(num_dist_pins),
                                         _size(0), _parent_id(INVALID_VECTOR_ID),
                                         _entry(ALLIGNEMENT(raw_header_bytes) + (void*)_data) {
        FatalAssert(num_dist_pins > 0, LOG_TAG_CLUSTER,
                    "Number of distributed pins should be greater than 0. num_dist_pins=%d", num_dist_pins);
        FatalAssert(num_dist_pins <= NUM_COMPUTE_NODES, LOG_TAG_CLUSTER,
                    "Number of distributed pins should be less than or equal to number of compute nodes. "
                    "num_dist_pins=%d, NUM_COMPUTE_NODES=%d", num_dist_pins, NUM_COMPUTE_NODES);
        FatalAssert(raw_header_bytes >= sizeof(Cluster), LOG_TAG_CLUSTER,
                    "Raw header bytes(%lu) does not contain enough space(%lu).",
                    raw_header_bytes, sizeof(Cluster));
        FatalAssert(_header_bytes >= raw_header_bytes, LOG_TAG_CLUSTER,
                    "Cluster bytes(%lu) does not contain enough space(%lu).",
                    _header_bytes, raw_header_bytes);
        FatalAssert(_header_bytes >= AllignedHeaderBytes(), LOG_TAG_CLUSTER,
                    "Cluster bytes(%lu) does not contain enough space(%lu).",
                    _header_bytes, AllignedHeaderBytes());
        FatalAssert(ALLIGNED(_header_bytes), LOG_TAG_CLUSTER,
                    "Cluster bytes(%lu) is not aligned to cache line size(%lu).",
                    _header_bytes, CACHE_LINE_SIZE);
        FatalAssert(_centroid_id.IsValid(), LOG_TAG_CLUSTER, "Invalid centroid ID.");
        CHECK_MIN_MAX_SIZE(_min_size, _max_size, LOG_TAG_CLUSTER);
        FatalAssert(_dim > 0, LOG_TAG_CLUSTER, "Cannot create a Cluster with dimension of 0.");
        FatalAssert(_entry + _max_size + 1 == (void*)this + _header_bytes + AllignedDataBytes(_dim, _max_size),
                    LOG_TAG_CLUSTER, "First entry is not in the right place. _entry=%p, this=%p",
                    _entry, this);
        FatalAssert(_max_size < UINT16_MAX, LOG_TAG_CLUSTER,
                    "Cluster size is too large. _max_size=%hu", _max_size);
        FatalAssert(_owner < MAX_COMPUTE_NODE_ID, LOG_TAG_CLUSTER,
                    "Cluster owner is invalid. _owner=%hu", _owner);
        FatalAssert(IsComputeNode(_owner), LOG_TAG_CLUSTER,
                    "Cluster owner is not a compute node. _owner=%hu", _owner);

        memset(_data, 0, ALLIGNEMENT(raw_header_bytes) + AllignedDataBytes(_dim, _max_size));
        for (uint16_t i = 0; i < _max_size; ++i) {
            _entry[i].id = INVALID_VECTOR_ID;
        }
    }

    Cluster(const Cluster& other) = delete;
    Cluster(Cluster&& other) = delete;
    ~Cluster() = default;

    inline void CheckValid(bool check_min_size) const {
#ifdef ENABLE_ASSERTS
        FatalAssert(_header_bytes >= AllignedHeaderBytes(), LOG_TAG_CLUSTER,
                    "Cluster bytes(%lu) does not contain enough space(%lu).",
                    _header_bytes, AllignedHeaderBytes());
        FatalAssert(ALLIGNED(_header_bytes), LOG_TAG_CLUSTER,
                    "Cluster bytes(%lu) is not aligned to cache line size(%lu).",
                    _header_bytes, CACHE_LINE_SIZE);
        CHECK_VECTORID_IS_VALID(_centroid_id, LOG_TAG_CLUSTER);
        CHECK_VECTORID_IS_CENTROID(_centroid_id, LOG_TAG_CLUSTER);
        CHECK_MIN_MAX_SIZE(_min_size, _max_size, LOG_TAG_CLUSTER);
        FatalAssert(_dim > 0, LOG_TAG_CLUSTER, "Cannot create a Cluster with dimension of 0.");
        FatalAssert(_entry + _max_size + 1 == (void*)this + _header_bytes + AllignedDataBytes(_dim, _max_size),
                    LOG_TAG_CLUSTER, "First entry is not in the right place. _entry=%p, this=%p",
                    _entry, this);
        FatalAssert(_max_size < UINT16_MAX, LOG_TAG_CLUSTER,
                    "Cluster size is too large. _max_size=%hu", _max_size);
        FatalAssert(_owner < MAX_COMPUTE_NODE_ID, LOG_TAG_CLUSTER,
                    "Cluster owner is invalid. _owner=%hu", _owner);
        FatalAssert(IsComputeNode(_owner), LOG_TAG_CLUSTER,
                    "Cluster owner is not a compute node. _owner=%hu", _owner);
        uint16_t size = _size.load();
        if (check_min_size) {
            FatalAssert(size >= _min_size, LOG_TAG_CLUSTER,
                        "Cluster size(%hu) is less than minimum size(%hu).", size, _min_size);
        }
        FatalAssert(size <= _max_size, LOG_TAG_CLUSTER,
                    "Cluster size(%hu) is larger than maximum size(%hu).", size, _max_size);
        uint16_t real_size = 0;
        for (uint16_t i = 0; i < _max_size; ++i) {
            if (_entry[i].IsVisible()) {
                real_size++;
            }
        }
        if (check_min_size) {
            FatalAssert(real_size >= _min_size, LOG_TAG_CLUSTER,
                        "Cluster real size(%hu) is less than minimum size(%hu).", real_size, _min_size);
        }
        FatalAssert(real_size <= _max_size, LOG_TAG_CLUSTER,
                    "Cluster real size(%hu) is larger than maximum size(%hu).", real_size, _max_size);
#endif
    }

    /* Should only be called when vertex lock is held(either shared or exclusive) */
    /* Will return invalid address to indicate that cluster is full */
    inline uint16_t BatchInsert(const void** vectors, const VectorID* ids, uint16_t num_vectors,
                                bool& need_split, ClusterEntry*& entry) {
        FatalAssert(vectors != nullptr, LOG_TAG_CLUSTER, "Cannot insert null vector.");
        FatalAssert(ids != nullptr, LOG_TAG_CLUSTER, "ids cannot be null!");
        FatalAssert(num_vectors > 0, LOG_TAG_CLUSTER, "Cannot insert 0 vectors.");
        FatalAssert(num_vectors < _max_size - _min_size, LOG_TAG_CLUSTER,
                    "Cannot insert more vectors than available space.");
        need_split = false;
        uint16_t offset;
        uint16_t num_reserved = Reserve(num_vectors, offset);
        FatalAssert(num_reserved <= num_vectors, LOG_TAG_CLUSTER,
                    "Reserved %hu vectors but requested %hu vectors.", num_reserved, num_vectors);
        if (num_reserved < num_vectors) {
            bool expected = false;
            need_split = _change_in_progress.compare_exchange_strong(expected, true);
            /* Cluster is full and someone should be splitting it so release your lock if you couldn't CAS change_IP */
        }

        if (num_reserved == 0) {
            entry = nullptr;
            return 0; /* No space left */
        }
        FatalAssert(offset + num_reserved <= _max_size, LOG_TAG_CLUSTER,
                    "Offset(%hu) + num_reserved(%hu) is larger than max size(%hu).",
                    offset, num_reserved, _max_size);
        FatalAssert(offset < _max_size, LOG_TAG_CLUSTER,
                    "Offset(%hu) is larger than max size(%hu).", offset, _max_size);
        entry = &_entry[offset];
        for (uint16_t i = 0; i < num_reserved; ++i) {
            FatalAssert(vectors[i] == nullptr, LOG_TAG_CLUSTER,
                        "Invalid vector at id %hu", i);
            FatalAssert(ids[i].IsValid(), LOG_TAG_CLUSTER, "Invalid VectorID in ids[%hu] = " VECTORID_LOG_FMT,
                        i, VECTORID_LOG(ids[i]));
            FatalAssert(_entry[offset + i].state.load().IsValid() == false, LOG_TAG_CLUSTER,
                        "Cluster entry is valid at offset %hu", offset + i);
            memcpy(_entry[offset + i].vector, static_cast<const char*>(vectors[i]) + (i * _dim * sizeof(VTYPE)),
                   _dim * sizeof(VTYPE));
            _entry[offset + i].id = ids[i];
            _entry[offset + i].state.store(VectorState::Valid()); /* todo memory order */
        }

        return num_reserved;
    }

    /* Should only be called when vertex is pinned */
    inline const ClusterEntry* FindEntry(VectorID id, bool onlyConsiderVisible = true) const {
        uint16_t size = _size.load();
        /* todo may need to fine tune so that if it is moving we return and don't move forward */
        for (uint16_t index = 0; index < size; ++index) {
            if ((onlyConsiderVisible ? _entry[index].state.load().IsVisible() :
                                       _entry[index].state.load().IsValid()) &&
                (_entry[index].id == id)) {
                return &_entry[index];
            }
        }

        return nullptr;
    }

    /* Should only be called when vertex is pinned */
    inline ClusterEntry* FindEntry(VectorID id, bool onlyConsiderVisible = true) {
        uint16_t size = _size.load();
        /* todo may need to fine tune so that if it is moving we return and don't move forward */
        for (uint16_t index = 0; index < size; ++index) {
            if ((onlyConsiderVisible ? _entry[index].state.load().IsVisible() :
                                       _entry[index].state.load().IsValid()) &&
                (_entry[index].id == id)) {
                return &_entry[index];
            }
        }

        return nullptr;
    }

    /* Should only be called when vertex is pinned */
    inline bool Contains(VectorID id, bool onlyConsiderVisible = true) const {
        return (FindEntry(id, onlyConsiderVisible) != nullptr);
    }

    inline ClusterEntry& operator[](uint16_t idx) {
        FatalAssert(idx < _max_size, LOG_TAG_CLUSTER, "idx(%hu) >= _max_size(%hu)", idx, _max_size);
        return _entry[idx];
    }

    inline const ClusterEntry& operator[](uint16_t idx) const {
        FatalAssert(idx < _max_size, LOG_TAG_CLUSTER, "idx(%hu) >= _max_size(%hu)", idx, _max_size);
        return _entry[idx];
    }

    inline uint16_t FilledSize() const {
        return _size.load();
    }

    /* Has to be called with exclusive lock held if you need accurate results.
       Cluster should be pinned */
    inline uint16_t ComputeRealSize() const {
        size_t size = 0;
        for (uint16_t i = 0; i < _max_size; ++i) {
            size += (_entry[i].state.load().IsVisible() ? 1 : 0);
        }

        size_t cur_size = _size.load();
        UNUSED_VARIABLE(cur_size);
        FatalAssert(size <= cur_size, LOG_TAG_CLUSTER, "Computed size(%lu) is larger than current size(%hu)",
                    size, cur_size);
        return size;
    }

    /* May return inconsistent data if SXLock is not held in exclusive mode */
    String ToString(bool detailed = false) const {
        String str("<ClusterAddress=%p, StartDataAddress=%p, StartEntryAddress=%p, "
                   "EndEntryAddress=%p, ClusterHeaderBytes=%lu, TotalAllignedHeaderBytes=%lu,"
                   " TotalBytes=%lu, Version=%lu, SizeLimit=[%hu %hu], "
                   "Dim=%hu, CachedParentID=" VECTORID_LOG_FMT
                   ", CentroidID=" VECTORID_LOG_FMT ", Size=%hu",
                   this, _data, _entry, _entry + _max_size, sizeof(Cluster),
                   _header_bytes, _header_bytes + AllignedBytes(_dim, _max_size),
                   _version, _min_size, _max_size, _dim, VECTORID_LOG(_parent_id),
                   VECTORID_LOG(_centroid_id), _size);
        if (detailed) {
            for (uint16_t i = 0; i < _max_size; ++i) {
                str += _entry[i].ToString(_dim);
                if (i < (_max_size - 1)) {
                    str += ", ";
                }
            }
            str += "]>";
        }
        else {
            str += ">";
        }
        return str;
    }

    const uint64_t _header_bytes; /* Size of cluster header including vertex header and the padding */
    const uint64_t _version; /* Incremented during split and compaction */
    const VectorID _centroid_id;
    /* only read during split -> holding a shared lock on it for migration of cluster should suffice */
    VectorID _parent_id; /* may be outdated in a multi node setup */
    const uint16_t _min_size;
    const uint16_t _max_size;
    const uint16_t _dim;
    /* number of invalid vectors in the cluster that can be removed during compaction */
    std::atomic<uint16_t> _collectable;
    uint8_t _owner;
    std::atomic<bool> _change_in_progress;
    std::atomic<uint8_t> _distributed_pin;
    ClusterEntry * const _entry;
    // const DataType _vtype;

protected:
    uint16_t Reserve(uint16_t numVectors, uint16_t& offset) {
        FatalAssert(numVectors > 0, LOG_TAG_CLUSTER, "Cannot reserve 0 vectors.");
        offset = UINT16_MAX;
        uint16_t cur_size = _size.load();
        while (cur_size < _max_size) {
            numVectors = std::min(numVectors, _max_size - cur_size);
            if (_size.compare_exchange_strong(cur_size, cur_size + numVectors)) {
                offset = cur_size;
                return numVectors;
            }
        }
        return 0; /* Cluster is full */
    }

    std::atomic<uint16_t> _size;

    char _data[];

TESTABLE;
};
};
#endif