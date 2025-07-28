#ifndef MEMORY_POOL_H_
#define MEMORY_POOL_H_

#include <cstddef>
#include <new>
#include <utility>
#include <set>
#include <vector>
#include <shared_mutex>

#include "debug.h"

union SlotFlag {
    char flag; // 1 byte for the flag
    struct {
        uint8_t need_call_destructor : 1; // 1 bit for destructor call flag
        uint8_t unused : 7; // 7 bits unused
    };
};

template<size_t slot_size>
struct MemoryChunk {
    const size_t num_slots;
    char data[];
    /* Before each object we should have 1 byte flag which indicates that we have to call destructor or not */

    /* the user should make sure the data is alligned */
    static inline MemoryChunk<slot_size>* CreateNewChunk(size_t slots) {
        size_t bytes = sizeof(MemoryChunk<slot_size>) + slots * (slot_size + 1);
        MemoryChunk<slot_size>* new_chunk = reinterpret_cast<MemoryChunk<slot_size>*>(std::malloc(bytes));
        if (!new_chunk) {
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_MEMORY, "Failed to allocate memory chunk of size %zu", bytes);
        }
        memset(new_chunk, 0, bytes);
        new (new_chunk) MemoryChunk<slot_size>(slots);
        return new_chunk;
    }

    ~MemoryChunk() = default;

    inline void* GetSlot(size_t offset) {
        return reinterpret_cast<void*>(data + offset * (slot_size + 1) + 1);
    }

    inline SlotFlag* GetSlotFlag(size_t offset) {
        return reinterpret_cast<SlotFlag*>(data + offset * (slot_size + 1));
    }

protected:
    MemoryChunk(size_t sz) : size(sz) {}
};

struct SlotRange {
    size_t start;
    size_t end;
};

template<typename T, size_t size = sizeof(T)>
class MemoryPool {
public:
    MemoryPool(size_t initial_slots = 1024) : num_free_slots(0),
                                              last_chunk_size(std::min(initial_slots, MAX_SLOTS_PER_CHUNK)),
                                              num_total_slots(0) {
        if (initial_slots == 0) {
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_MEMORY, "Memory pool initialized with zero slots.");
        }
        size_t num_chunks = initial_slots / MAX_SLOTS_PER_CHUNK + (initial_slots % MAX_SLOTS_PER_CHUNK != 0);
        for (size_t i = 0; i < num_chunks; i++) {
            Expand(last_chunk_size, MAX_SLOTS_PER_CHUNK * i);
        }
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Memory pool initialized with %zu slots.", num_total_slots);
    }

    ~MemoryPool() {
        std::unique_lock lock(mutex);
        for (auto& chunk : chunks) {
            if (chunk != nullptr) {
                for (size_t i = 0; i < chunk->num_slots; i++) {
                    if (chunk->GetSlotFlag(i)->need_call_destructor == 1) {
                        T* obj = reinterpret_cast<T*>(chunk->GetSlot(i));
                        obj->~T(); // Call destructor if needed
                    }
                }
                std::free(chunk);
                chunk = nullptr;
            }
        }
        chunks.clear();
        free_slots.clear();
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Memory pool destroyed, all chunks freed.");
    }

    inline void Expand(size_t slots_needed, size_t num_current_slots) {
        std::unique_lock lock(mutex);

        if (slots_needed == 0 || slots_needed > MAX_SLOTS_PER_CHUNK) {
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_MEMORY, "Cannot expand memory pool with %zu slots, max is %zu.", slots_needed, MAX_SLOTS_PER_CHUNK);
        }

        if (num_current_slots < num_free_slots.load()) {
            return; // some memory was allocated. retry and check if more is needed.
        }

        for (size_t i = 0; i < slots_needed; i++) {
            MemoryChunk<T>* new_chunk = MemoryChunk<T>::CreateNewChunk(slots_needed);
            chunks.insert(new_chunk);
            free_slots.push_back({num_total_slots, num_total_slots + slots_needed - 1});
            num_total_slots += slots_needed;
            num_free_slots.fetch_add(slots_needed);
        }
    }

    inline T* Malloc() {
        std::shared_lock lock(mutex);
        while (num_free_slots.load() == 0) {
            lock.unlock();
            Expand(std::min(last_chunk_size * GROWTH_RATE, MAX_SLOTS_PER_CHUNK), num_free_slots.load());
            lock.lock();
        }
    }

    inline T* MallocZero() {

    }

    inline T* New() {

    }

    inline void Free(T* ptr) {
    }

    inline void Delete(T* ptr) {
    }

    inline bool TryFree(T* ptr) {
    }

    inline bool TryDelete(T* ptr) {
    }

protected:
    constexpr size_t MAX_SLOTS_PER_CHUNK = 8192;
    constexpr size_t GROWTH_RATE = 2;

    std::shared_mutex mutex;
    std::set<MemoryChunk<T>*> chunks;
    /* Todo: use map if it uses too much memory */
    std::vector<SlotRange> free_slots;
    std::atomic<size_t> num_free_slots;
    std::atomic<size_t> last_chunk_size;
    size_t num_total_slots;
};


#endif