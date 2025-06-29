#ifndef COPPER_VECTOR_UTILS_H_
#define COPPER_VECTOR_UTILS_H_

#include "common.h"

#include <queue>

namespace copper {

struct VectorUpdate {
    VectorID vector_id;
    Address vector_data;
};

class Vector {
public:
    Vector(Vector& other) = delete;
    Vector(const Vector& other) = delete;
    Vector& operator=(Vector& other) = delete;
    Vector& operator=(const Vector& other) = delete;

    Vector() : _data(nullptr), _delete_on_destroy(false) {}

    /* Copy Constructors */
    Vector(const Vector& other, DataType vtype, uint16_t dim) : _data(other.IsValid() ?
                                                                      malloc(dim * SIZE_OF_TYPE[vtype]) : nullptr),
                                                                _delete_on_destroy(other.IsValid()) {
        FatalAssert(copper::IsValid(vtype), LOG_TAG_VECTOR, "Cannot create vector with invalid type. vtype=%d", vtype);
        FatalAssert(dim > 0, LOG_TAG_VECTOR, "Cannot create vector with dimention 0.");
        FatalAssert(!other.IsValid() || IsValid(), LOG_TAG_VECTOR, "Malloc failed");
        if (IsValid()) {
            memcpy(_data, other._data, dim * SIZE_OF_TYPE[vtype]);
        }
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "copy constructor: other=%p, this=%p, address=%p", &other, this, _data);
    }

    Vector(const void* other, DataType vtype, uint16_t dim) : _data(other != nullptr ?
                                                                    malloc(dim * SIZE_OF_TYPE[vtype]) : nullptr),
                                                                _delete_on_destroy(other != nullptr) {
        FatalAssert(copper::IsValid(vtype), LOG_TAG_VECTOR, "Cannot create vector with invalid type. vtype=%d", vtype);
        FatalAssert(dim > 0, LOG_TAG_VECTOR, "Cannot create vector with dimention 0.");
        FatalAssert((other == nullptr) || IsValid(), LOG_TAG_VECTOR, "Malloc failed");
        if (IsValid()) {
            memcpy(_data, other, dim * SIZE_OF_TYPE[vtype]);
        }
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "copy constructor: this=%p, address=%p", &other, this, _data);
    }

    /* Link Constructors */
    Vector(Vector&& other) : _data(other._data), _delete_on_destroy(other._delete_on_destroy) {
        other._data = nullptr;
        other._delete_on_destroy = false;
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Move vector: other=%p to this=%p, address=%p, delete_on_destroy=%s",
             &other, this, _data, (_delete_on_destroy ? "T" : "F"));
    }

    /* should not use ConstAddress as we would be able to change the data of that address. */
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
        other._data = nullptr;
        other._delete_on_destroy = false;
        return *this;
    }

    inline bool IsValid() const {
        return (_data != nullptr);
    }

    inline void Create(DataType vtype, uint16_t dim) {
        FatalAssert(!(IsValid()), LOG_TAG_VECTOR, "Vector is not invalid.");
        FatalAssert(dim > 0, LOG_TAG_VECTOR, "Cannot create vector with dimention 0.");
        FatalAssert(copper::IsValid(vtype), LOG_TAG_VECTOR, "Cannot create vector with invalid type. vtype=%d", vtype);
        _data = malloc(dim * SIZE_OF_TYPE[vtype]);
        _delete_on_destroy = true;
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Malloc failed");

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Created vector: this=%p, address=%p, dimention=%hu, type=%s",
                                              this, _data, dim, DATA_TYPE_NAME[vtype]);
    }

    inline void Destroy() {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        FatalAssert(_delete_on_destroy, LOG_TAG_MEMORY,
                    "Cannot destroy a linked vector. _delete_on_destroy is false. this=%p, address=%p", this, _data);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Destroy vector: this=%p, address=%p", this, _data);
        free(_data);
        _data = nullptr;
        _delete_on_destroy = false;
    }

    inline void Link(Vector& src) {
        FatalAssert(!(IsValid()), LOG_TAG_VECTOR, "Vector is valid");
        _data = src._data;
        _delete_on_destroy = false;
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Link vector: this=%p, address=%p", this, _data);
    }

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
    }

    inline void Copy(const Vector& src, DataType vtype, uint16_t dim) {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        FatalAssert(src.IsValid(), LOG_TAG_VECTOR, "Source Vector is not invalid.");

        memcpy(_data, src._data, dim * SIZE_OF_TYPE[vtype]);
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

    inline bool Similar(const Vector& other, DataType vtype, uint16_t dim) const {
        if (*this == other) {
            return true;
        }

        if (!(IsValid()) || !(other.IsValid())) {
            return false;
        }

        return !(memcmp(_data, other._data, dim * SIZE_OF_TYPE[vtype]));
    }

    template<typename T>
    inline T& operator[](size_t idx) {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        return (static_cast<T*>(_data))[idx];
    }

    template<typename T>
    inline const T& operator[](size_t idx) const {
        FatalAssert(IsValid(), LOG_TAG_VECTOR, "Vector is invalid.");
        return (static_cast<const T*>(_data))[idx];
    }

    inline String ToString(uint16_t dim, DataType type) const {
        if (!(IsValid())) {
            return String("INV");
        }
        String str("(address=%p, linked=%s)<", _data, _delete_on_destroy ? "T" : "F");
        for (uint16_t i = 0; i < dim; ++i) {
            str += DataToString(_data + i * SIZE_OF_TYPE[type], type) + String((i < (dim - 1)) ? ", " : ">");
        }

        return str;
    }

protected:
    bool _delete_on_destroy; // If true, the vector will be deleted when destroyed.
    void* _data;

TESTABLE;
};

struct VectorPair {
    VectorID id;
    Vector vec;

    VectorPair(VectorID vector_id, Vector&& vector_data) : id(vector_id), vec(std::move(vector_data)) {}
};

class VectorSet {
public:
    VectorSet(DataType vtype, uint16_t dimention, uint16_t capacity) : _size(0), _cap(capacity),
                                                                       _dim(dimention), _vtype(vtype) {
        FatalAssert(IsValid(_vtype), LOG_TAG_VECTOR_SET, "Cannot initialize a VectorSet with invalid vector type."
                    " vtype=%d", _vtype);
        FatalAssert(_dim > 0, LOG_TAG_VECTOR_SET, "Cannot initialize a VectorSet with dimentiom of 0.");
        FatalAssert(_cap > 0, LOG_TAG_VECTOR_SET, "Cannot initialize a VectorSet with capacity of 0.");
        memset(_data, 0, ((SIZE_OF_TYPE[_vtype] * _dim) + sizeof(VectorID)) * _cap);
    }

    ~VectorSet() {}

    inline Address GetVectors() {
        return _data;
    }

    inline ConstAddress GetVectors() const {
        return _data;
    }

    inline VectorID* GetIDs() {
        return static_cast<VectorID*>(static_cast<void*>(_data) + SIZE_OF_TYPE[_vtype] * _dim * _cap);
    }

    inline const VectorID* GetIDs() const {
        return static_cast<const VectorID*>(static_cast<const void*>(_data) + SIZE_OF_TYPE[_vtype] * _dim * _cap);
    }

    inline Address Insert(const Vector& new_vector, VectorID id) {
        FatalAssert(new_vector.IsValid(), LOG_TAG_VECTOR_SET, "Cannot insert invalid vector.");
        FatalAssert(_size < _cap, LOG_TAG_VECTOR_SET, "VectorSet is full.");

        Address loc = _data + (_size * _dim * SIZE_OF_TYPE[_vtype]);
        memcpy(loc, new_vector.GetData(), _dim * SIZE_OF_TYPE[_vtype]);
        GetIDs()[_size] = id;
        ++_size;
        return loc;
    }

    inline Address Insert(const void* _src, VectorID id) {
        FatalAssert(_src != nullptr, LOG_TAG_VECTOR_SET, "Cannot insert null vector.");
        FatalAssert(_size < _cap, LOG_TAG_VECTOR_SET, "VectorSet is full.");

        Address loc = _data + (_size * _dim * SIZE_OF_TYPE[_vtype]);
        memcpy(loc, _src, _dim * SIZE_OF_TYPE[_vtype]);
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

    /* we cannot use the link constructor for the constant version so we do not declare it. */
    inline Vector GetLastVector() {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Vector set is empty");
        Address loc = _data + ((_size - 1) * _dim * SIZE_OF_TYPE[_vtype]);
        return Vector(loc);
    }

    inline Vector GetVector(uint16_t idx) {
        FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);
        Address loc = _data + (idx * _dim * SIZE_OF_TYPE[_vtype]);
        return Vector(loc);
    }

    inline Vector GetVectorByID(VectorID id) {
        return GetVector(GetIndex(id));
    }

    /* we cannot use the link constructor for the constant version so we do not declare it. */
    // inline const Vector GetVector(uint16_t idx) const {
    //     FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);
    //     return Vector<const T, _dim>((_beg + (idx * _dim)), false);
    // }

    // inline const Vector<T, _dim> Get_Vector_By_ID(VectorID id) const {
    //     return Get_Vector(Get_Index(id));
    // }

    inline VectorPair operator[](uint16_t idx) {
        return VectorPair(GetVectorID(idx), GetVector(idx));
    }

    // inline const VectorPair operator[](uint16_t idx) const {
    //     return VectorPair(Get_VectorID(idx), _beg + (idx * _dim), false);
    // }


    /* Not Tested */
    // inline Vector<T, _dim> Get_Vector_Copy(uint16_t idx) const {
    //     FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);

    //     return Vector<T, _dim>(_beg + (idx * _dim));
    // }

    // inline Vector<T, _dim> Get_Vector_Copy_By_ID(VectorID id) const {
    //     return Get_Vector_Copy(Get_Index(id));
    // }

    // inline VectorUpdate Delete(VectorID id) {
    //     FatalAssert(id.Is_Valid(), LOG_TAG_VECTOR_SET, "cannot delete invalide vector id.");

    //     uint16_t idx = Get_Index(id);
    //     VectorUpdate swapped{INVALID_VECTOR_ID, INVALID_ADDRESS};

    //     if (idx != _size - 1) {
    //         swapped.vector_id = Get_Last_VectorID(); // ID of the last vector
    //         swapped.vector_data = _beg + (idx * _dim); // new address of the last vector
    //         memcpy(_beg + (idx * _dim), _beg + ((_size - 1) * _dim), _dim * sizeof(T));
    //         GetIDs()[idx] = swapped.vector_id;
    //     }

    //     --_size;

    //     return swapped;
    // }

    inline void DeleteLast() {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Vector set is empty");
        --_size;
    }

    inline Address GetAddress() {
        return _data;
    }

    inline ConstAddress GetAddress() const {
        return _data;
    }

    inline uint16_t Size() const {
        return _size;
    }

    String ToString() {
        String str = "<Vectors: [";
        for (uint16_t i = 0; i < _size; ++i) {
            str += GetVector(i).ToString(_dim, _vtype);
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
    uint16_t _cap;
    uint16_t _dim;
    DataType _vtype;
    char _data[1];

TESTABLE;
};
};
#endif