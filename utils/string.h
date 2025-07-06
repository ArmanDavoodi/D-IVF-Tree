#ifndef STRING_H_
#define STRING_H_

#include <cstring>
#include <stdarg.h>
#include <stdio.h>
#include <string>

namespace copper {

class String {
public:
    String() : _data(nullptr), size(0) {}

    String(const String& other) : _data(((other.size == 0) ?
                                  nullptr : static_cast<char*>(malloc((other.size + 1) * sizeof(char*))))),
                                  size(other.size) {
        if (other.size > 0) {
            memcpy(_data, other._data, sizeof(char) * (size + 1));
        }
    }

    String(String&& other) : _data(other._data), size(other.size) {
        other._data = nullptr;
        other.size = 0;
    }

    String(const std::string& other) :  _data(((other.size() == 0) ?
                                        nullptr : static_cast<char*>(malloc((other.size() + 1) * sizeof(char*))))),
                                        size(other.size()) {
        if (size > 0) {
            memcpy(_data, &(other[0]), sizeof(char) * (size));
            _data[size] = 0;
        }
    }

    String(const char* fmt, ...) : _data(nullptr), size(0) {
        if (fmt == nullptr) {
            return;
        }

        va_list argptr;
        va_start(argptr, fmt);

        va_list arg_copy;
        va_copy(arg_copy, argptr);
        int _size = vsnprintf(NULL, 0, fmt, arg_copy);
        va_end(arg_copy);
        if (_size < 0) {
            size = 0;
            return;
        }
        size = _size;
        _data = static_cast<char*>(malloc((size + 1) * sizeof(char*)));

        int num_writen = vsnprintf(_data, size + 1, fmt, argptr);
        va_end(argptr);
        if ((num_writen < 0) || ((size_t)(num_writen) != size)) {
            free(_data);
            _data = nullptr;
            size = 0;
            return;
        }
    }

    String& operator=(const String& other) {
        if (_data != nullptr) {
            free(_data);
            _data = nullptr;
            size = 0;
        }

        if (other.size > 0) {
            size = other.size;
            _data = static_cast<char*>(malloc((size + 1) * sizeof(char*)));
            memcpy(_data, other._data, sizeof(char) * (size + 1));
        }

        return *this;
    }

    String& operator=(String&& other) {
        if (_data != nullptr) {
            free(_data);
            _data = nullptr;
            size = 0;
        }

        if (other.size > 0) {
            size = other.size;
            _data = other._data;
            other._data = nullptr;
            other.size = 0;
        }

        return *this;
    }

    String& operator=(const char* other) {
        if (_data != nullptr) {
            free(_data);
            _data = nullptr;
            size = strlen(other);
        }

        if (size > 0) {
            _data = static_cast<char*>(malloc((size + 1) * sizeof(char*)));
            memcpy(_data, other, sizeof(char) * (size + 1));
        }

        return *this;
    }

    String& operator=(const std::string& other) {
        if (_data != nullptr) {
            free(_data);
            _data = nullptr;
            size = other.size();
        }

        if (size > 0) {
            _data = static_cast<char*>(malloc((size + 1) * sizeof(char*)));
            memcpy(_data, &(other[0]), sizeof(char) * (size));
            _data[size] = 0;
        }

        return *this;
    }

    ~String() {
        if (_data != nullptr) {
            free(_data);
            _data = nullptr;
            size = 0;
        }
    }

    inline char& operator[](size_t index) {
        return _data[index];
    }

    inline const char& operator[](size_t index) const {
        return _data[index];
    }

    inline String operator+(const String& other) const {
        String res;
        res.size = size + other.size;
        if (res.size == 0) {
            return res;
        }

        res._data = static_cast<char*>(malloc((size + 1) * sizeof(char*)));
        size_t offset = 0;
        if (size > 0) {
            memcpy(res._data, _data, sizeof(char) * size);
            offset = size;
        }

        if (other.size > 0) {
            memcpy(res._data + offset, other._data, sizeof(char) * other.size);
        }

        res._data[res.size] = 0;
        return res;
    }

    inline String& operator+=(const String& other) {
        return (*this = (*this + other));
    }

    inline bool SameAs(const String& other) const {
        return ((_data == other._data) && (size == other.size));
    }

    inline bool operator==(const String& other) const {
        if (SameAs(other)) {
            return true;
        }

        if (other._data == nullptr || _data == nullptr) {
            return false;
        }

        return !(strcmp(_data, other._data));
    }

    inline bool operator!=(const String& other) const {
        return !(*this == other);
    }

    inline bool operator>(const String& other) const {
        if (SameAs(other)) {
            return false;
        }

        if (other._data == nullptr) {
            return true;
        }

        if (_data == nullptr) {
            return false;
        }

        return (strcmp(_data, other._data) > 0);
    }

    inline bool operator<(const String& other) const {
        return (other > *this);
    }

    inline bool operator<=(const String& other) const {
        return ((*this < other) || (*this == other));
    }

    inline bool operator>=(const String& other) const {
        return ((*this > other) || (*this == other));
    }

    const char* ToCStr() const {
        return _data;
    }
protected:
    char* _data;
    size_t size;
};

};

#endif