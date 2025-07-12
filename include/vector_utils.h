#ifndef COPPER_VECTOR_UTILS_H_
#define COPPER_VECTOR_UTILS_H_

#include "common.h"

#include <queue>

namespace copper {

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

struct VectorPair {
    VectorID id;
    Vector vec;

    VectorPair(VectorID vector_id, Vector&& vector_data) : id(vector_id), vec(std::move(vector_data)) {}
};

struct ConstVectorPair {
    const VectorID id;
    const Vector vec;

    ConstVectorPair(VectorID vector_id, const Vector& vector_data) : id(vector_id),
                                                                     vec(std::move(const_cast<Vector&>(vector_data))) {}
};

class VectorSet {
public:
    VectorSet(const VectorSet& other) = delete;
    VectorSet(VectorSet&& other) = delete;
    ~VectorSet() = default;

    VectorSet(uint16_t dimention, uint16_t capacity) : _size(0), _cap(capacity), _dim(dimention) {
        FatalAssert(_dim > 0, LOG_TAG_VECTOR_SET, "Cannot create a VectorSet with dimentiom of 0.");
        FatalAssert(_cap > 0, LOG_TAG_VECTOR_SET, "Cannot create a VectorSet with capacity of 0.");
        memset(GetVectors(), 0, sizeof(VTYPE) * _dim * _cap);
        memset(static_cast<Address>(GetIDs()), -1, sizeof(VectorID) * _cap);
    }

    inline Address GetVectors() {
        return static_cast<Address>(_data);
    }

    inline ConstAddress GetVectors() const {
        return static_cast<ConstAddress>(_data);
    }

    inline VectorID* GetIDs() {
        return static_cast<VectorID*>(GetVectors() + sizeof(VTYPE) * _dim * _cap);
    }

    inline const VectorID* GetIDs() const {
        return static_cast<const VectorID*>(GetVectors() + sizeof(VTYPE) * _dim * _cap);
    }

    inline Address Insert(const Vector& new_vector, VectorID id) {
        FatalAssert(new_vector.IsValid(), LOG_TAG_VECTOR_SET, "Cannot insert invalid vector.");
        FatalAssert(_size < _cap, LOG_TAG_VECTOR_SET, "VectorSet is full.");

        Address loc = GetVectors() + (_size * _dim * sizeof(VTYPE));
        memcpy(loc, new_vector.GetData(), _dim * sizeof(VTYPE));
        (GetIDs())[_size] = id;
        ++_size;
        return loc;
    }

    inline Address Insert(const void* _src, VectorID id) {
        FatalAssert(_src != nullptr, LOG_TAG_VECTOR_SET, "Cannot insert null vector.");
        FatalAssert(_size < _cap, LOG_TAG_VECTOR_SET, "VectorSet is full.");

        Address loc = GetVectors() + (_size * _dim * sizeof(VTYPE));
        memcpy(loc, _src, _dim * sizeof(VTYPE));
        GetIDs()[_size] = id;
        ++_size;
        return loc;
    }

    inline VectorID GetVectorID(uint16_t idx) const {
        FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);
        return GetIDs()[idx];
    }

    inline VectorID GetLastVectorID() const {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Vector set is empty");
        return GetIDs()[_size - 1];
    }

    inline bool Contains(VectorID id) const {
        const VectorID* ids = GetIDs();
        for (uint16_t index = 0; index < _size; ++index) {
            if (ids[index] == id) {
                return true;
            }
        }

        return false;
    }

    inline uint16_t GetIndex(VectorID id) const {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Bucket is Empty");
        const VectorID* ids = GetIDs();
        uint16_t index = 0;
        for (; index < _size; ++index) {
            if (ids[index] == id) {
                break;
            }
        }

        FatalAssert(index < _size, LOG_TAG_VECTOR_SET, "vector id:" VECTORID_LOG_FMT " not found", VECTORID_LOG(id));
        return index;
    }

    inline Vector GetLastVector() {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Vector set is empty");
        Address loc = GetVectors() + ((_size - 1) * _dim * sizeof(VTYPE));
        return Vector(loc);
    }

    inline const Vector GetLastVector() const {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Vector set is empty");
        Address loc = const_cast<Address>(GetVectors() + ((_size - 1) * _dim * sizeof(VTYPE)));
        return Vector(loc);
    }

    inline Vector GetVector(uint16_t idx) {
        FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);
        Address loc = GetVectors() + (idx * _dim * sizeof(VTYPE));
        return Vector(loc);
    }

    inline const Vector GetVector(uint16_t idx) const {
        FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);
        Address loc = const_cast<Address>(GetVectors() + (idx * _dim * sizeof(VTYPE)));
        return Vector(loc);
    }

    inline Vector GetVectorByID(VectorID id) {
        return GetVector(GetIndex(id));
    }

    inline const Vector GetVectorByID(VectorID id) const {
        return GetVector(GetIndex(id));
    }

    inline VectorPair operator[](uint16_t idx) {
        return VectorPair(GetVectorID(idx), GetVector(idx));
    }

    inline ConstVectorPair operator[](uint16_t idx) const {
        return ConstVectorPair(GetVectorID(idx), GetVector(idx));
    }

    inline void DeleteLast() {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Vector set is empty");
        --_size;
    }

    inline uint16_t Size() const {
        return _size;
    }

    inline uint16_t Capacity() const {
        return _cap;
    }

    inline uint16_t Dimension() const {
        return _dim;
    }

    static inline size_t DataBytes(uint16_t dim, uint16_t cap) {
        return ((sizeof(VTYPE) * (uint64_t)dim) + sizeof(VectorID)) * (uint64_t)cap;
    }

    String ToString() const {
        String str("<Size=%hu, Cap=%hu, Dim=%hu, Vectors: [", _size, _cap, _dim);
        for (uint16_t i = 0; i < _size; ++i) {
            str += GetVector(i).ToString(_dim);
            if (i != _size - 1)
                str += ", ";
        }
        str += "], IDs: [";
        for (uint16_t i = 0; i < _size; ++i) {
            str += String(VECTORID_LOG_FMT, VECTORID_LOG(GetIDs()[i]));
            if (i != _size - 1)
                str += ", ";
        }
        str += "]>";
        return str;
    }

protected:
    uint16_t _size;
    const uint16_t _cap;
    const uint16_t _dim;
    // DataType _vtype;
    char _data[]; // This is a flexible array member, it will be allocated with the size of _cap * (_dim * sizeof(VTYPE) + sizeof(VectorID))

TESTABLE;
};
};
#endif