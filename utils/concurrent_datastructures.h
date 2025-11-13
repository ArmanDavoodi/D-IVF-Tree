#ifndef CONCURRENT_DATASTRUCTURES_H_
#define CONCURRENT_DATASTRUCTURES_H_

#include <mutex>
#include <set>
#include <queue>

#include "utils/string.h"

#include "third_party/moodycamel/concurrentqueue/concurrentqueue.h"
#include "third_party/moodycamel/concurrentqueue/blockingconcurrentqueue.h"

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

template<typename T>
class BlockingQueue {
public:
    BlockingQueue() : q() {}

    BlockingQueue(size_t initial) : q(initial) {}

    inline bool Push(T&& value) {
        return q.enqueue(std::forward<T>(value));
    }

    inline bool Push(const T& value) {
        return q.enqueue(value);
    }

    inline bool BatchPush(const T* value, size_t size) {
        return q.enqueue_bulk(value, size);
    }

    inline bool PopHead(T& value) {
        return q.wait_dequeue_timed(value, std::chrono::microseconds(10));
    }

    inline bool TryPopHead(T& value) {
        return q.try_dequeue(value);
    }

    inline size_t BatchPopHead(T* arr, size_t size) {
        return q.wait_dequeue_bulk_timed(arr, size, std::chrono::microseconds(10));
    }

    inline size_t TryBatchPopHead(T* arr, size_t size) {
        return q.try_dequeue_bulk(arr, size);
    }

    inline bool Empty() {
        return q.size_approx() == 0;
    }

    inline size_t Size() {
        return q.size_approx();
    }

protected:
    moodycamel::BlockingConcurrentQueue<T> q;
};

};

#endif