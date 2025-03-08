#ifndef COPPER_VECTOR_UTILS_H
#define COPPER_VECTOR_UTILS_H

#include "common.h"
#include "queue"

namespace copper {

template<typename T, uint16_t _DIM>
class Vector {
static_assert(_DIM > 0);

public:
    Vector() : _data(new T[_DIM]), _delete_on_destroy(true) {}

    Vector(const T* data) : _data(data != nullptr ? new T[_DIM] : nullptr), _delete_on_destroy(data != nullptr) {
        if (data != nullptr) {
            memcpy(_data, data, _DIM * sizeof(T));
        }
    }

    Vector(const std::vector<T>& vec) : _data(vec.size() >= _DIM ? new T[_DIM] : nullptr), _delete_on_destroy(vec.size() >= _DIM) {
        if (vec.size() >= _DIM) {
            memcpy(_data, &vec[0], _DIM * sizeof(T));
        }
    }
    
    Vector(const Vector<T, _DIM>& _vec) : _data(_vec.Is_Valid() ? new T[_DIM] : nullptr), _delete_on_destroy(_vec.Is_Valid()) {
        if (_vec.Is_Valid()) {
            memcpy(_data, _vec._data, _DIM * sizeof(T));
        }
    }

    Vector(Vector<T, _DIM>&& _vec) 
        : _data(_vec._data), _delete_on_destroy(_vec.delete_on_destroy) {
        _vec._data = nullptr;
        _vec._delete_on_destroy = false;
    }

    inline static Vector<T, _DIM> NEW_INVALID() {
        return Vector<T, _DIM>(nullptr, false);
    }

    ~Vector() {
        if (_delete_on_destroy) {
            if (_data != nullptr) {
                CLOG(LOG_LEVEL_DEBUG, LOG_TAG_BASIC, "Deleted vector with _data=%lu. this=%lu", _data, this);
                delete[] _data;
            }
            else {
                CLOG(LOG_LEVEL_WARNING, LOG_TAG_BASIC, "Could not delete invalid vector. this=%lu", this);
            }
        }
        _data = nullptr;
        _delete_on_destroy = false;
    }

    Vector<T, _DIM>& operator=(const Vector<T, _DIM>& other)  {
        if (_data == nullptr) {
            _data = new T[_DIM];
            _delete_on_destroy = true;
        }

        memcpy(_data, other._data, _DIM * sizeof(T));

        return *this;
    }

    Vector<T, _DIM>& operator=(Vector<T, _DIM>&& other) {
        if (_data != nullptr) {
            if (_delete_on_destroy) {
                delete[] _data;
            }
            else {
                CLOG(LOG_LEVEL_WARNING, LOG_TAG_BASIC, "Existing data not deleted. this=%lu, _data=%lu", this, _data);
            }
        }

        _data = other._data;
        _delete_on_destroy = other._delete_on_destroy;
        other._data = nullptr;
        other._delete_on_destroy = false;

        return *this;
    }

    inline void Invalidate() {
        if (_data != nullptr) {
            if (_delete_on_destroy) {
                CLOG(LOG_LEVEL_DEBUG, LOG_TAG_BASIC, "Deleted vector _data=%lu. this=%lu", _data, this);
                delete[] _data;
            }
            else {
                CLOG(LOG_LEVEL_WARNING, LOG_TAG_BASIC, "Existing data not deleted. this=%lu, _data=%lu", this, _data);
            }
        }

        _data = nullptr;
        _delete_on_destroy = false;
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
        AssertFatal(i < _DIM, LOG_TAG_BASIC, "index %hu out of bounds! dimention = %hu", i, _DIM);
        return _data[i];
    }

    inline const T& operator[](uint16_t i) const {
        AssertFatal(i < _DIM, LOG_TAG_BASIC, "index %hu out of bounds! dimention = %hu", i, _DIM);
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

    inline Vector<T, _DIM> Shallow_Copy() {
        return Vector<T, _DIM>(_data, false);
    }

    inline const Vector<T, _DIM> Shallow_Copy() const {
        return Vector<T, _DIM>(_data, false);
    }

    inline Vector<T, _DIM> Deep_Copy() const {
        return Vector<T, _DIM>(*this, nullptr, true);
    }

    inline std::string to_string() {
        if (!Is_Valid()) {
            return std::string("INVALID");
        }

        std::string str = "[";
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

    Vector(T* data, bool delete_on_destroy) : _data(data), _delete_on_destroy(delete_on_destroy) {}

    Vector(const Vector<T, _DIM>& _vec, T* loc, bool delete_on_destroy) 
            : _delete_on_destroy(delete_on_destroy) {
        AssertError((_vec.Is_Valid() || loc == nullptr), LOG_TAG_BASIC, "Input vector is invalid. this=%lu, _vec=%lu.", this, &_vec);
        if (!_vec.Is_Valid()) {
            _data = nullptr;
        }
        else {
            CLOG_IF_TRUE(loc == _vec._data ,LOG_LEVEL_WARNING, LOG_TAG_BASIC, "Deep copy on the same location. this=%lu, _vec=%lu, loc=%lu.", this, &_vec, loc);
            CLOG_IF_TRUE((loc != nullptr && delete_on_destroy), LOG_LEVEL_WARNING, LOG_TAG_BASIC, "Delete on destroy is true with valid location! this=%lu, _vec=%lu, loc=%lu.", this, &_vec, loc);
            _data = (loc ? loc : new T[_DIM]);
            memcpy(_data, _vec._data, _DIM * sizeof(T));
        }
    }

    Vector(const std::vector<T>& vec, T* loc, bool delete_on_destroy) 
            : _delete_on_destroy(delete_on_destroy) {
        AssertFatal((vec.size() >= _DIM), LOG_TAG_BASIC, "Input vector is too small.");

        CLOG_IF_TRUE((loc != nullptr && delete_on_destroy), LOG_LEVEL_WARNING, LOG_TAG_BASIC, "Delete on destroy is true with valid location! this=%lu, loc=%lu.", this, loc);
        _data = (loc ? loc : new T[_DIM]);
        memcpy(_data, &vec[0], _DIM * sizeof(T));
    }

    inline Vector<T, _DIM> Deep_Copy(T* loc = nullptr, bool delete_on_destroy = true) const {
        return Vector<T, _DIM>(*this, loc, delete_on_destroy);
    }

friend class VectorSet;
friend class VectorPair;
};

template<typename T, uint16_t _DIM>
struct VectorPair {
    VectorPair(VectorID _id, T* data) : id(_id), vector(data, false) {}
    VectorPair(const VectorPair<T, _DIM>& other) : id(other.id), vector(other.vector) {}
    VectorPair(VectorPai<T, _DIM>&& other) noexcept : id(other.id), vector(std::move(other.vector)) {}

    inline VectorPai<T, _DIM>& operator=(const VectorPair<T, _DIM>& other) {
        id = other.id;
        vector = other.vector;
        return *this;
    }

    inline VectorPai<T, _DIM>& operator=(VectorPair<T, _DIM>&& other) {
        id = other.id;
        vector = std::move(other.vector);
        return *this;
    }

    VectorID id;
    Vector<T, _DIM> vector;
};

template<typename T, uint16_t _DIM, uint16_t _CAP>
class VectorSet {
static_assert(_DIM > 0);
static_assert(_CAP > 0);

public:
    VectorSet() : _beg(new T[_DIM * _CAP]), _ids(new VectorID[_DIM * _CAP]), _size(0) {}
    ~VectorSet() {
        AssertError(_beg != nullptr, LOG_TAG_BASIC, "_beg cannot be nullptr.");
        AssertError(_ids != nullptr, LOG_TAG_BASIC, "_ids cannot be nullptr.");

        delete[] _beg;
        delete[] _ids;
        _beg = nullptr;
        _ids = nullptr;
    }

    inline void Insert(const Vector<T, _DIM>& _data, VectorID id) {
        AssertFatal(_data.Is_Valid(), LOG_TAG_BASIC, "Cannot insert invalid vector.");
        AssertFatal(_size < _CAP, LOG_TAG_BASIC, "VectorSet is full.");

        memcpy(beg + (_size * _DIM), _data._data, _DIM * sizeof(T));
        _ids[_size] = id;
        ++_size;
    }

    inline void Insert(const T* _data, VectorID id) {
        AssertFatal(_data != nullptr, LOG_TAG_BASIC, "Cannot insert null vector.");
        AssertFatal(_size < _CAP, LOG_TAG_BASIC, "VectorSet is full.");

        memcpy(beg + (_size * _DIM), _data, _DIM * sizeof(T));
        _ids[_size] = id;
        ++_size;
    }

    inline void Insert(const std::vector<T>& _data, VectorID id) {
        AssertFatal(_data.size() == _DIM, LOG_TAG_BASIC, "Input vector dimention dose not match, input.dim=%hu, _DIM=%hu.", _data.size(), _DIM);
        AssertFatal(_size < _CAP, LOG_TAG_BASIC, "VectorSet is full.");

        memcpy(beg + (_size * _DIM), &_data[0], _DIM * sizeof(T));
        _ids[_size] = id;
        ++_size;
    }

    inline VectorID Get_VectorID(uint16_t idx) const {
        AssertFatal(idx < _size, LOG_TAG_ANY, "idx(%hu) >= _size(%hu)", idx, _size);
        return _ids[idx]; 
    }

    inline uint16_t Get_Index(VectorID id) const {
        AssertFatal(_size > 0, LOG_TAG_ANY, "Bucket is Empty");

        uint16_t index = 0;
        for (; index < _size; ++index) {
            if (_ids[index] == id) {
                break;
            }
        }

        AssertFatal(_ids[index] == id, LOG_TAG_ANY, "_ids[%hu](%lu) != id(%lu)", index, _ids[index]._id, id._id);
        return index;
    }

    inline Vector<T, _DIM> Get_Vector(uint16_t idx) {
        AssertFatal(idx < _size, LOG_TAG_ANY, "idx(%hu) >= _size(%hu)", idx, _size);
        
        return Vector<T, _DIM>(_beg + idx, false);
    }

    inline Vector<T, _DIM> Get_Vector_By_ID(VectorID id) {
        return Get_Vector(Get_Index(id)); 
    }

    inline const Vector<T, _DIM> Get_Vector(uint16_t idx) const {
        AssertFatal(idx < _size, LOG_TAG_ANY, "idx(%hu) >= _size(%hu)", idx, _size);
        
        return Vector<T, _DIM>(_beg + idx, false);
    }

    inline const Vector<T, _DIM> Get_Vector_By_ID(VectorID id) const {
        return Get_Vector(Get_Index(id)); 
    }

    inline VectorPair<T, _DIM> operator[](uint16_t idx) {
        return VectorPair<T, _DIM>(Get_VectorID(idx), _beg + idx, false)
    }

    inline const VectorPair<T, _DIM> operator[](uint16_t idx) const {
        return VectorPair<T, _DIM>(Get_VectorID(idx), _beg + idx, false)
    }

    inline Vector<T, _DIM> Get_Vector_Copy(uint16_t idx) const {
        AssertFatal(idx < _size, LOG_TAG_ANY, "idx(%hu) >= _size(%hu)", idx, _size);
        
        return Vector<T, _DIM>(_beg + idx);
    }

    inline Vector<T, _DIM> Get_Vector_Copy_By_ID(VectorID id) const {
        return Get_Vector_Copy(Get_Index(id)); 
    }

    inline void Delete(VectorID id, VectorID& swapped_vec_id, Vector<T, _DIM>& swapped_vec) {
        AssertFatal(!swapped_vec.Is_Valid(), LOG_TAG_BASIC, "inputed swapped_vec is valid.");

        uint16_t idx = Get_Index(id);
        
        if (idx != _size - 1) {
            swapped_vec = Get_Vector(_size - 1, _DIM);
            swapped_vec_id = Get_VectorID(_size - 1);

            memcpy(_beg + (idx * _DIM * sizeof(T)), swapped_vec._data, _DIM * sizeof(T));
            _ids[idx] = swapped_vec_id;
        }
        else {
            swapped_vec_id = INVALID_VECTOR_ID;
            swapped_vec.Invalidate();
        }

        --_size;
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
    T* _beg;
    VectorID* _ids;
    uint16_t _size;
};

template<typename T, uint16_t _DIM>
class Distance {
public:
    virtual double operator()(const Vector<T, _DIM>& a, const Vector<T, _DIM>& b) const = 0;
    virtual bool operator()(const double& a, const double& b) const = 0;
};

template<typename T, uint16_t _DIM>
class L2_Distance : public Distance<T, _DIM> {
public:
    double operator()(const Vector<T, _DIM>& a, const Vector<T, _DIM>& b) const {
        AssertFatal(a.Is_Valid(), LOG_TAG_ANY, "a is invalid");
        AssertFatal(b.Is_Valid(), LOG_TAG_ANY, "b is invalid");

        double dist = 0;
        for (size_t i = 0; i < _DIM; ++i) {
            dist += (double)(a[i] - b[i]) * (double)(a[i] - b[i]);
        }
        return dist;
    }

    bool operator()(const double& a, const double& b) const {
        return a < b;
    }
};

template<typename T, uint16_t _DIM>
struct Reverse_Similarity {
public:
    Distance<T, _DIM>* _dist;

    Reverse_Similarity(Distance<T, _DIM>* dist) : _dist(dist) {}

    bool operator()(const std::pair<double, VectorID>& a, const std::pair<double, VectorID>& b) const {
        return (*_dist)(a.first, b.first);
    }
};

template<typename T, uint16_t _DIM>
struct Similarity {
public:
    Distance<T, _DIM>* _dist;

    Similarity(Distance<T, _DIM>* dist) : _dist(dist) {}

    bool operator()(const std::pair<double, VectorID>& a, const std::pair<double, VectorID>& b) const {
        return !((*_dist)(a.first, b.first));
    }
};

};

#endif