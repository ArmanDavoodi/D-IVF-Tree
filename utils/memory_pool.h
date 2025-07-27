#ifndef MEMORY_POOL_H_
#define MEMORY_POOL_H_

#include <cstddef>
#include <new>
#include <utility>
#include <unordered_set>

template<typename T>
struct MemoryChunk {
    const size_t size;
    T data[];

    /* todo correct */
    static inline MemoryChunk<T> CreateNewChunk(size_t num_bytes) {
        size_t aligned_size = (num_bytes % alignof(T) == 0) ? num_bytes : (num_bytes + (alignof(T) - (num_bytes % alignof(T))));
        MemoryChunk<T>* chunk = static_cast<MemoryChunk<T>*>(std::aligned_alloc(alignof(T), aligned_size));
        if (!chunk) {
            throw std::bad_alloc();
        }
        return *new (chunk) MemoryChunk<T>(aligned_size);
    }

protected:
    MemoryChunk(size_t sz) : size(sz) {}
};

template<typename T, size_t size = sizeof(T)>
class MemoryPool {
public:
protected:
    std::unordered_set<MemoryChunk<T>*> pool;
};


#endif