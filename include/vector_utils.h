#ifndef COPPER_VECTOR_UTILS_H_
#define COPPER_VECTOR_UTILS_H_

#include "common.h"

#include <queue>

namespace copper {

struct VectorUpdate {
    VectorID vector_id;
    Address vector_data;
    // Address cluster_address;

    // VectorUpdate(VectorID _id, Address _data, Address _cluster) :
    //     vector_id(_id), vector_data(_data), cluster_address(_cluster) {}
};

template<typename T, uint16_t _DIM>
class Vector {
static_assert(_DIM > 0);

public:
    Vector() : _data(new T[_DIM]), _delete_on_destroy(true) {
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR, "Default Constructor: Created vector. _data=%lu. this=%lu, _delete_on_destroy=%s.",
            _data, this, _delete_on_destroy ? "T" : "F");
    }

    explicit Vector(const T* data) : _data(data != nullptr ? new T[_DIM] : nullptr),
                                     _delete_on_destroy(data != nullptr) {
        if (data != nullptr) {
            memcpy(_data, data, _DIM * sizeof(T));
        }
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR,
            "Created vector based on pointer data. _data=%lu. this=%lu, _delete_on_destroy=%s.",
            _data, this, _delete_on_destroy ? "T" : "F");
    }

    /* Not tested */
    // explicit Vector(const std::vector<T>& vec) : _data(vec.size() >= _DIM ? new T[_DIM] : nullptr),
    //                                              _delete_on_destroy(vec.size() >= _DIM) {
    //     if (vec.size() >= _DIM) {
    //         memcpy(_data, &vec[0], _DIM * sizeof(T));
    //     }
    // }

    Vector(const Vector<T, _DIM>& _vec) : _data(_vec.Is_Valid() ? new T[_DIM] : nullptr),
                                          _delete_on_destroy(_vec.Is_Valid()) {
        if (_vec.Is_Valid()) {
            memcpy(_data, _vec._data, _DIM * sizeof(T));
        }

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR,
            "Created vector based on another vector. _data=%lu. this=%lu, _delete_on_destroy=%s.",
            _data, this, _delete_on_destroy ? "T" : "F");
    }

    Vector(Vector<T, _DIM>&& _vec)
        : _data(_vec._data), _delete_on_destroy(_vec._delete_on_destroy) {
        _vec._data = nullptr;
        _vec._delete_on_destroy = false;

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR,
            "Created vector based on rvalue vector. _data=%lu. this=%lu, _delete_on_destroy=%s.",
            _data, this, _delete_on_destroy ? "T" : "F");
    }

    inline static Vector<T, _DIM> NEW_INVALID() {
        return Vector<T, _DIM>(nullptr, false);
    }

    inline void Invalidate() {
        if (_data != nullptr) {
            if (_delete_on_destroy) {
                CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR, "Deleted vector _data=%lu. this=%lu", _data, this);
                delete[] _data;
            }
            else {
                CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR, "Existing data not deleted. this=%lu, _data=%lu", this, _data);
            }
        }

        _data = nullptr;
        _delete_on_destroy = false;
    }

    ~Vector() {
        Invalidate();
    }

    Vector<T, _DIM>& operator=(const Vector<T, _DIM>& other)  {
        if (other.Is_Valid()) {
            if (_data == nullptr) {
                _data = new T[_DIM];
                _delete_on_destroy = true;
            }

            memcpy(_data, other._data, _DIM * sizeof(T));
        }
        else {
            Invalidate();
        }

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR,
            "assigned vector to another vector. _data=%lu. this=%lu, _delete_on_destroy=%s.",
            _data, this, _delete_on_destroy ? "T" : "F");

        return *this;
    }

    Vector<T, _DIM>& operator=(Vector<T, _DIM>&& other) {
        if (other.Is_Valid()) {
            if (_data == nullptr) {
                _data = other._data;
                _delete_on_destroy = other._delete_on_destroy;
                other._data = nullptr;
                other._delete_on_destroy = false;
            }
            else {
                memcpy(_data, other._data, _DIM * sizeof(T));
            }
        }
        else {
            Invalidate();
        }

        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR,
            "assigned vector to another rvalue vector. _data=%lu. this=%lu, _delete_on_destroy=%s.",
            _data, this, _delete_on_destroy ? "T" : "F");

        return *this;
    }

    inline bool Is_Valid() const {
        return _data != nullptr;
    }

    inline bool Delete_On_Destroy() const {
        return _delete_on_destroy;
    }

    inline bool Are_The_Same(const Vector<T, _DIM>& other) const {
        return _data == other._data;
    }

    inline T& operator[](uint16_t i) {
        FatalAssert(i < _DIM, LOG_TAG_VECTOR, "index %hu out of bounds! dimention = %hu", i, _DIM);
        return _data[i];
    }

    inline const T& operator[](uint16_t i) const {
        FatalAssert(i < _DIM, LOG_TAG_VECTOR, "index %hu out of bounds! dimention = %hu", i, _DIM);
        return _data[i];
    }

    inline bool operator==(const Vector<T, _DIM>& other) const {
        if (Are_The_Same(other)) {
            return true;
        }

        if (!Is_Valid() || !other.Is_Valid()) {
            return false;
        }

        for (uint16_t i = 0; i < _DIM; ++i) {
            if (_data[i] != other._data[i]) {
                return false;
            }
        }

        return true;
    }

    inline bool operator!=(const Vector<T, _DIM>& other) const {
        return !(*this == other);
    }

    /* Not tested. */
    // inline Vector<T, _DIM> Shallow_Copy() {
    //     return Vector<T, _DIM>(_data, false);
    // }

    /* Not tested. */
    // inline const Vector<T, _DIM> Shallow_Copy() const {
    //     return Vector<T, _DIM>(_data, false);
    // }

    /* Not tested. */
    // inline Vector<T, _DIM> Deep_Copy() const {
    //     return Vector<T, _DIM>(*this, nullptr, true);
    // }

    inline Address Get_Address() {
        return _data;
    }

    inline std::string to_string() {
        if (!Is_Valid()) {
            return std::string("INVALID");
        }

        std::string str = "<" + std::to_string((uint64_t)_data) + ", " + std::to_string(_delete_on_destroy) + ">[";
        for (uint16_t i = 0; i < _DIM; ++i) {
            str += std::to_string(_data[i]);
            if (i < _DIM - 1)
                str += ", ";
        }
        str += "]";
        return str;
    }

protected:
    T* _data = nullptr;
    bool _delete_on_destroy;

    Vector(T* data, bool delete_on_destroy) : _data(data), _delete_on_destroy(delete_on_destroy) {
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_VECTOR,
            "Created vector using private constructor. _data=%lu. this=%lu, _delete_on_destroy=%s.",
            _data, this, _delete_on_destroy ? "T" : "F");
    }

    /* Not tested. */
    // Vector(const Vector<T, _DIM>& _vec, T* loc, bool delete_on_destroy)
    //         : _delete_on_destroy(delete_on_destroy) {
    //     ErrorAssert((_vec.Is_Valid() || loc == nullptr), LOG_TAG_VECTOR, "Input vector is invalid. this=%lu, _vec=%lu.", this, &_vec);
    //     if (!_vec.Is_Valid()) {
    //         _data = nullptr;
    //     }
    //     else {
    //         CLOG_IF_TRUE(loc == _vec._data ,LOG_LEVEL_WARNING, LOG_TAG_VECTOR, "Deep copy on the same location. this=%lu, _vec=%lu, loc=%lu.", this, &_vec, loc);
    //         CLOG_IF_TRUE((loc != nullptr && delete_on_destroy), LOG_LEVEL_WARNING, LOG_TAG_VECTOR, "Delete on destroy is true with valid location! this=%lu, _vec=%lu, loc=%lu.", this, &_vec, loc);
    //         _data = (loc ? loc : new T[_DIM]);
    //         memcpy(_data, _vec._data, _DIM * sizeof(T));
    //     }
    // }

    // Vector(const std::vector<T>& vec, T* loc, bool delete_on_destroy)
    //         : _delete_on_destroy(delete_on_destroy) {
    //     FatalAssert((vec.size() >= _DIM), LOG_TAG_VECTOR, "Input vector is too small.");

    //     CLOG_IF_TRUE((loc != nullptr && delete_on_destroy), LOG_LEVEL_WARNING, LOG_TAG_VECTOR, "Delete on destroy is true with valid location! this=%lu, loc=%lu.", this, loc);
    //     _data = (loc ? loc : new T[_DIM]);
    //     memcpy(_data, &vec[0], _DIM * sizeof(T));
    // }

    /* Not tested. */
    // inline Vector<T, _DIM> Deep_Copy(T* loc, bool delete_on_destroy = true) const {
    //     return Vector<T, _DIM>(*this, loc, delete_on_destroy);
    // }

template<typename, uint16_t, uint16_t>
friend class VectorSet;
friend class VectorPair<T, _DIM>;
template <typename, uint16_t, uint16_t, uint16_t, uint16_t, uint16_t, typename,
          template<typename, uint16_t, typename> class>
friend class Buffer_Manager;

TESTABLE;
};

template<typename T, uint16_t _DIM>
struct VectorPair {
public:
    VectorPair(const VectorPair<T, _DIM>& other) : id(other.id), vector(other.vector) {}
    VectorPair(VectorPair<T, _DIM>&& other) noexcept : id(other.id), vector(std::move(other.vector)) {}

    inline VectorPair<T, _DIM>& operator=(const VectorPair<T, _DIM>& other) {
        id = other.id;
        vector = other.vector;
        return *this;
    }

    inline VectorPair<T, _DIM>& operator=(VectorPair<T, _DIM>&& other) {
        id = other.id;
        vector.Invalidate();
        vector = std::move(other.vector);
        return *this;
    }

    VectorID id;
    Vector<T, _DIM> vector;
protected:
    VectorPair(VectorID _id, T* data, bool delete_on_destroy=true) : id(_id), vector(data, delete_on_destroy) {}
    // VectorPair(VectorID _id, const T* data, bool delete_on_destroy=true) : id(_id), vector(data) {}


template<typename, uint16_t, uint16_t>
friend class VectorSet;
};

template<typename T, uint16_t _DIM, uint16_t _CAP>
class VectorSet {
static_assert(_DIM > 0);
static_assert(_CAP > 0);

public:
    VectorSet() : _size(0) {}
    ~VectorSet() {}

    inline Address Insert(const Vector<T, _DIM>& _data, VectorID id) {
        FatalAssert(_data.Is_Valid(), LOG_TAG_VECTOR_SET, "Cannot insert invalid vector.");
        FatalAssert(_size < _CAP, LOG_TAG_VECTOR_SET, "VectorSet is full.");

        memcpy(_beg + (_size * _DIM), _data._data, _DIM * sizeof(T));
        _ids[_size] = id;
        ++_size;
        return (Address)(_beg + ((_size - 1) * _DIM));
    }

    inline Address Insert(const T* _data, VectorID id) {
        FatalAssert(_data != nullptr, LOG_TAG_VECTOR_SET, "Cannot insert null vector.");
        FatalAssert(_size < _CAP, LOG_TAG_VECTOR_SET, "VectorSet is full.");

        memcpy(_beg + (_size * _DIM), _data, _DIM * sizeof(T));
        _ids[_size] = id;
        ++_size;
        return (Address)(_beg + ((_size - 1) * _DIM));
    }

    // inline Address Insert(const std::vector<T>& _data, VectorID id) {
    //     FatalAssert(_data.size() == _DIM, LOG_TAG_VECTOR_SET, "Input vector dimention dose not match, input.dim=%hu, _DIM=%hu.", _data.size(), _DIM);
    //     FatalAssert(_size < _CAP, LOG_TAG_VECTOR_SET, "VectorSet is full.");

    //     memcpy(_beg + (_size * _DIM), &_data[0], _DIM * sizeof(T));
    //     _ids[_size] = id;
    //     ++_size;
    //     return _beg + ((_size - 1) * _DIM);
    // }

    inline VectorID Get_VectorID(uint16_t idx) const {
        FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);
        return _ids[idx];
    }

    inline VectorID Get_Last_VectorID() const {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Vector set is empty");
        return _ids[_size - 1];
    }

    inline bool Contains(VectorID id) const {
        for (uint16_t index = 0; index < _size; ++index) {
            if (_ids[index] == id) {
                return true;
            }
        }

        return false;
    }

    inline uint16_t Get_Index(VectorID id) const {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Bucket is Empty");

        uint16_t index = 0;
        for (; index < _size; ++index) {
            if (_ids[index] == id) {
                break;
            }
        }

        FatalAssert(index < _size, LOG_TAG_VECTOR_SET, "vector id:%lu not found", id._id);
        FatalAssert(_ids[index] == id, LOG_TAG_VECTOR_SET, "_ids[%hu](%lu) != id(%lu)", index, _ids[index]._id, id._id);
        return index;
    }

    inline Vector<T, _DIM> Get_Last_Vector() {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Vector set is empty");

        return Vector<T, _DIM>(_beg + ((_size - 1) * _DIM), false);
    }

    inline Vector<T, _DIM> Get_Vector(uint16_t idx) {
        FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);

        return Vector<T, _DIM>(_beg + (idx * _DIM), false);
    }

    inline Vector<T, _DIM> Get_Vector_By_ID(VectorID id) {
        return Get_Vector(Get_Index(id));
    }

    inline const Vector<T, _DIM> Get_Vector(uint16_t idx) const {
        FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);

        return Vector<T, _DIM>(_beg + (idx * _DIM), false);
    }

    inline const Vector<T, _DIM> Get_Vector_By_ID(VectorID id) const {
        return Get_Vector(Get_Index(id));
    }

    inline VectorPair<T, _DIM> operator[](uint16_t idx) {
        return VectorPair<T, _DIM>(Get_VectorID(idx), _beg + (idx * _DIM), false);
    }

    inline const VectorPair<T, _DIM> operator[](uint16_t idx) const {
        return VectorPair<T, _DIM>(Get_VectorID(idx), _beg + (idx * _DIM), false);
    }


    /* Not Tested */
    // inline Vector<T, _DIM> Get_Vector_Copy(uint16_t idx) const {
    //     FatalAssert(idx < _size, LOG_TAG_VECTOR_SET, "idx(%hu) >= _size(%hu)", idx, _size);

    //     return Vector<T, _DIM>(_beg + (idx * _DIM));
    // }

    // inline Vector<T, _DIM> Get_Vector_Copy_By_ID(VectorID id) const {
    //     return Get_Vector_Copy(Get_Index(id));
    // }

    // inline VectorUpdate Delete(VectorID id) {
    //     FatalAssert(id.Is_Valid(), LOG_TAG_VECTOR_SET, "cannot delete invalide vector id.");

    //     uint16_t idx = Get_Index(id);
    //     VectorUpdate swapped{INVALID_VECTOR_ID, INVALID_ADDRESS};

    //     if (idx != _size - 1) {
    //         swapped.vector_id = Get_Last_VectorID(); // ID of the last vector
    //         swapped.vector_data = _beg + (idx * _DIM); // new address of the last vector
    //         memcpy(_beg + (idx * _DIM), _beg + ((_size - 1) * _DIM), _DIM * sizeof(T));
    //         _ids[idx] = swapped.vector_id;
    //     }

    //     --_size;

    //     return swapped;
    // }

    inline void Delete_Last() {
        FatalAssert(_size > 0, LOG_TAG_VECTOR_SET, "Vector set is empty");
        --_size;
    }

    inline T* Get_Typed_Address() {
        return _beg;
    }

    inline uint16_t Size() const {
        return _size;
    }

    std::string to_string() {
        std::string str = "<Vectors: [";
        for (uint16_t i = 0; i < _size; ++i) {
            str += Get_Vector(i).to_string();
            if (i != _size - 1)
                str += ", ";
        }
        str += "], IDs: [";
        for (uint16_t i = 0; i < _size; ++i) {
            str += std::to_string(_ids[i]._id);
            if (i != _size - 1)
                str += ", ";
        }
        str += "]>";
        return str;
    }

protected:
    T _beg[_DIM * _CAP];
    VectorID _ids[_DIM * _CAP];
    uint16_t _size;

TESTABLE;
};
};
#endif