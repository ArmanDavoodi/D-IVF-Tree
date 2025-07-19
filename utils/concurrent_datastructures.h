#ifndef CONCURRENT_DATASTRUCTURES_H_
#define CONCURRENT_DATASTRUCTURES_H_

#include <mutex>
#include <set>
#include <queue>

#include "utils/string.h"

namespace divftree {

/* Todo better implementations -> lockfree */

template<typename T, typename Compare = std::less<T>, typename Alloc = std::allocator<T>>
class ConcurrentSet {
public:
    bool Insert(const T& value) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_set.find(value) != _set.end()) {
            return false;
        }
        _set.insert(value);
        return true;
    }

    bool Erase(const T& value) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _set.find(value);
        if (it == _set.end()) {
            return false;
        }
        _set.erase(it);
        return true;
    }

    bool Contains(const T& value) {
        std::lock_guard<std::mutex> lock(_mutex);
        return _set.find(value) != _set.end();
    }

    size_t Size() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _set.size();
    }

    bool Empty() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _set.empty();
    }

    void Clear() {
        std::lock_guard<std::mutex> lock(_mutex);
        _set.clear();
    }

    divftree::String ToString(divftree::String (*ConvertToString)(const T&)) {
        std::lock_guard<std::mutex> lock(_mutex);
        divftree::String result = divftree::String("ConcurrentSet<size=%lu, elements=[");
        uint64_t size = _set.size();
        uint64_t index = 0;
        for (const T& item : _set) {
            result += ConvertToString(item);
            if (index < size - 1) {
                result += ", ";
            }
            index++;
        }
        result += "]>";
        return result;
    }
protected:
    std::set<T, Compare, Alloc> _set;
    std::mutex _mutex;
};

template<typename T, typename seq = std::deque<T>>
class ConcurrentQueue {
public:
    void Push(const T& value) {
        std::lock_guard<std::mutex> lock(_mutex);
        _queue.push(value);
    }

    bool PushIfNotInQueue(const T& value) {
        std::lock_guard<std::mutex> lock(_mutex);
        for (const T& item : _queue) {
            if (item == value) {
                return false; // Value already in queue
            }
        }
        _queue.push(value);
        return true;
    }

    bool PopHead(T& value) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_queue.empty()) {
            return false;
        }
        value = _queue.front();
        _queue.pop();
        return true;
    }

    bool Empty() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _queue.empty();
    }

    size_t Size() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _queue.size();
    }

    void Clear() {
        std::lock_guard<std::mutex> lock(_mutex);
        while (!_queue.empty()) {
            _queue.pop();
        }
    }

    bool Contains(const T& value) {
        std::lock_guard<std::mutex> lock(_mutex);
        for (const T& item : _queue) {
            if (item == value) {
                return true; // Value already in queue
            }
        }
        return false;
    }

    divftree::String ToString(divftree::String (*ConvertToString)(const T&)) {
        std::lock_guard<std::mutex> lock(_mutex);
        divftree::String result = divftree::String("ConcurrentQueue<size=%lu, elements=[");
        uint64_t size = _queue.size();
        uint64_t index = 0;
        for (const T& item : _queue) {
            result += ConvertToString(item);
            if (index < size - 1) {
                result += ", ";
            }
            index++;
        }
        result += "]>";
        return result;
    }
protected:
    std::queue<T, seq> _queue;
    std::mutex _mutex;
};

};

#endif