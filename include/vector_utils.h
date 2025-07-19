#ifndef DIVFTREE_VECTOR_UTILS_H_
#define DIVFTREE_VECTOR_UTILS_H_

#include "common.h"

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
        FatalAssert(dim > 0, LOG_TAG_VECTOR, "Cannot create vector with dimention 0.");
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
        FatalAssert(dim > 0, LOG_TAG_VECTOR, "Cannot create vector with dimention 0.");
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
        FatalAssert(dim > 0, LOG_TAG_VECTOR, "Cannot create vector with dimention 0.");
        _data = malloc(dim * sizeof(VTYPE));
        _delete_on_destroy = true;
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Malloc failed");
#ifdef MEMORY_DEBUG
        linkCnt = new std::atomic<uint64_t>(1);
#endif
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Created vector: this=%p, address=%p, dimention=%hu", this, _data, dim);
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
    inline VectorState Move() {
        VectorState expected = VectorState::Valid();
        if (state.compare_exchange_strong(expected, VectorState::Valid() | VectorState::Move())) {
            return *this;
        }
        return expected;
    }

    /* Should only be called when the vertex lock is held(either in shared or exclusive mode) */
    inline VectorState Delete() {
        return state.exchange(VectorState::Invalid());
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
    static inline constexpr size_t EntryBytes(uint16_t dim) {
        return sizeof(ClusterEntry) + (sizeof(VTYPE) * (uint64_t)dim);
    }

    static inline constexpr size_t AllignedDataBytes(uint16_t dim, uint16_t cap) {
        return ALLIGNED_SIZE(EntryBytes(dim) * (uint64_t)cap);
    }

    static inline constexpr size_t AllignedBytes(uint16_t dim, uint16_t cap) {
        return ALLIGNED_SIZE(sizeof(Cluster)) + AllignedDataBytes(dim, cap);
    }

    static Cluster* CreateCluster(uint64_t version, VectorID centroid_id,
                                  uint16_t min_size, uint16_t max_size,
                                  uint16_t dimension) {
        size_t bytes = AllignedBytes(dimension, max_size);

        Cluster* cluster = static_cast<Cluster*>(std::aligned_alloc(CACHE_LINE_SIZE, bytes));
        FatalAssert(cluster != nullptr, LOG_TAG_TEST, "Failed to allocate memory for Cluster.");
        new (cluster) Cluster(version, centroid_id, min_size, max_size, dimension);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_TEST, "Created Cluster: %s",
             cluster->ToString().ToCStr());
        return cluster;
    }

    Cluster(const Cluster& other) = delete;
    Cluster(Cluster&& other) = delete;
    ~Cluster() = default;

    /* Should only be called when vertex lock is held(either shared or exclusive) */
    /* Will return invalid address to indicate that cluster is full */
    inline ClusterEntry* Insert(const void* new_vector, VectorID id) {
        FatalAssert(new_vector != nullptr, LOG_TAG_CLUSTER, "Cannot insert null vector.");
        FatalAssert(id.IsValid(), LOG_TAG_CLUSTER, "Cannot insert vector with invalid ID: " VECTORID_LOG_FMT,
                    VECTORID_LOG(id));
        uint16_t offset = Reserve(1);
        if (offset == UINT16_MAX) {
            return nullptr; /* Cluster is full and someone should be splitting it so release your lock */
        }

        FatalAssert(!(_entry[offset].state.load().IsValid()), LOG_TAG_CLUSTER, "Cluster entry is valid!");
        memcpy(_entry[offset].vector, new_vector, _dim * sizeof(VTYPE));
        _entry[offset].id = id;
        _entry[offset].state.store(VectorState::Valid()); /* todo memory order */
        return &(_entry[offset]);
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
    String ToString() const {
        String str("<CurAddress= %p, Bytes=%lu, Version=%lu, SizeLimit=[%hu %hu], "
                   "Dim=%hu, CachedParentID=" VECTORID_LOG_FMT
                   ", CentroidID=" VECTORID_LOG_FMT ", Size=%hu, Entries: [",
                   this, _bytes, _version, _min_size, _max_size, _dim, VECTORID_LOG(_parent_id),
                   VECTORID_LOG(_centroid_id), _size);
        for (uint16_t i = 0; i < _max_size; ++i) {
            str += _entry[i].ToString(_dim);
            if (i < (_max_size - 1)) {
                str += ", ";
            }
        }
        str += "]>";
        return str;
    }

    const uint64_t _version; /* Incremented during split and compaction */
    const VectorID _centroid_id;
    const uint16_t _min_size;
    const uint16_t _max_size;
    const uint16_t _dim;
    const size_t _bytes;
    // const DataType _vtype;

protected:
    Cluster(uint64_t version, VectorID centroid_id, uint16_t min_size, uint16_t max_size,
            uint16_t dimention) : _version(version), _centroid_id(centroid_id),
                                  _min_size(min_size), _max_size(max_size), _dim(dimention),
                                  _bytes(AllignedBytes(_dim, _max_size)),
                                  _entry(ALLIGNEMENT(sizeof(Cluster)) + (void*)_data),
                                  _parent_id(INVALID_VECTOR_ID), _size(0) {
        FatalAssert(_centroid_id.IsValid(), LOG_TAG_CLUSTER, "Invalid centroid ID.");
        CHECK_MIN_MAX_SIZE(_min_size, _max_size, LOG_TAG_CLUSTER);
        FatalAssert(_dim > 0, LOG_TAG_CLUSTER, "Cannot create a Cluster with dimentiom of 0.");
        FatalAssert(_entry == ALLIGNED_SIZE(sizeof(Cluster)) + (void*)this,
                    LOG_TAG_CLUSTER, "First entry is not in the right place. _entry=%p, this=%p",
                    _entry, this);
        FatalAssert(_max_size < UINT16_MAX, LOG_TAG_CLUSTER,
                    "Cluster size is too large. _max_size=%hu", _max_size);
        memset(_data, 0, AllignedBytes(_dim, _max_size) - sizeof(Cluster));
        for (uint16_t i = 0; i < _max_size; ++i) {
            _entry[i].id = INVALID_VECTOR_ID;
        }
    }

    uint16_t Reserve(uint16_t numVectors) {
        FatalAssert(numVectors > 0, LOG_TAG_CLUSTER, "Cannot reserve 0 vectors.");
        uint16_t cur_size = _size.load();
        while (cur_size + numVectors <= _max_size) {
            if (_size.compare_exchange_strong(cur_size, cur_size + numVectors)) {
                return cur_size;
            }
        }
        return UINT16_MAX; /* Cluster is full */
    }

    ClusterEntry * const _entry;

    VectorID _parent_id; /* may be outdated in a multi node setup */
    std::atomic<uint16_t> _size;

    char _data[];

TESTABLE;
};
};
#endif