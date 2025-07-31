#ifndef MEMORY_POOL_H_
#define MEMORY_POOL_H_

#include <cstddef>
#include <new>
#include <utility>
#include <set>
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <mutex>

#include "debug.h"

#include "utils/thread.h"

union SlotFlag {
    struct {
        uint8_t is_free : 1; // if the slot is free
        uint8_t marked_by_gc : 1; // if the slot is marked by garbage collector
        uint8_t need_call_destructor : 1; // if the object needs to call destructor
        uint8_t reserved : 5; // reserved for future use
    };
    uint8_t value;

    SlotFlag() : value(0) {}

    inline void SetFree() {
        is_free = 1;
    }

    inline void SetUsed() {
        is_free = 0;
    }

    inline void SetMarkedByGC() {
        marked_by_gc = 1;
    }

    inline void ClearMarkedByGC() {
        marked_by_gc = 0;
    }

    inline void SetNeedCallDestructor() {
        need_call_destructor = 1;
    }

    inline void ClearNeedCallDestructor() {
        need_call_destructor = 0;
    }

    inline void Clear() {
        value = 0;
    }
};

struct Slot {
    SlotFlag flag; // flags for the slot
    char data[]; // pointer to the allocated memory
};

struct SlotRange {
    std::atomic<size_t> startOffset;
    const size_t endOffset; // exclusive

    SlotRange* next; // pointer to the next range in the linked list
};

struct SlotList {
    std::atomic<SlotRange*> head;
    std::atomic<SlotRange*> tail;
};

/* Todo: implement an efficent arena later  */
template<typename T, size_t DataSize = sizeof(T)>
class MemoryPool {

constexpr SLOT_SIZE = DataSize + sizeof(SlotFlag);

public:

    ~MemoryPool() {
        std::unique_lock lock(mutex);
        SlotRange* current = free_slots.head.load(std::memory_order_relaxed);
        while (current != nullptr) {
            for (size_t i = current->startOffset.load(std::memory_order_relaxed); i < current->endOffset; ++i) {
                Slot& slot = *(Slot*)(_data + (SLOT_SIZE * i));
                if (slot.flag.need_call_destructor) {
                    (reinterpret_cast<T*>(slot.data))->~T();
                }
            }
            free_slots.head.store(current->next, std::memory_order_relaxed);
            delete current;
            current = free_slots.head.load(std::memory_order_relaxed);
        }

        std::free(_data);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_MEMORY, "Memory pool destroyed, all chunks freed.");
    }

    inline T* Malloc() {
        std::shared_lock lock(mutex);
        SlotRange* current = free_slots.head.load(std::memory_order_acquire);
        while (current != nullptr) {
            size_t offset = current->startOffset.load(std::memory_order_acquire);
            if (offset >= current->endOffset) {
                SlotRange* old = current;
                do {
                    current = free_slots.head.load(std::memory_order_acquire); // move to the next range
                    DIVFTREE_YIELD();
                    DIVFTREE_YIELD();
                    DIVFTREE_YIELD();
                } while (current == old);
                continue;
            }
            while (!current->startOffset.compare_exchange_strong(offset, offset + 1, std::memory_order_acq_rel)) {
                if (offset >= current->endOffset) {
                    break; // no more slots in this range
                }
            }

            if (offset == current->endOffset - 1) {
                free_slots.head.store(current->next, std::memory_order_release);
                do {
                    current->next = garbage_ptrs.head.load(std::memory_order_relaxed);
                } while(!garbage_ptrs.head.compare_exchange_strong(current->next, current, std::memory_order_acq_rel));
            }

            if (offset < current->endOffset) {
                Slot& slot = *(Slot*)(_data + (SLOT_SIZE * offset));
                FatalAssert(slot.flag.is_free, LOG_TAG_MEMORY, "Slot is not free when trying to allocate memory.");
                slot.flag.Clear();
                slot.flag.SetUsed();
                return reinterpret_cast<T*>(slot.data);
            }
            current = free_slots.head.load(std::memory_order_acquire);
        }

        return nullptr;
    }

    inline T* MallocZero() {

    }

    template<typename... Args>
    inline T* New(Args&&... args) {

    }

    inline void Free(T* ptr) {
    }

    inline void Delete(T* ptr) {
    }

protected:
    SlotList free_slots;
    SlotList garbage_ptrs;
    std::shared_mutex mutex; // for thread-safe operations
    const size_t num_slots;
    void* _data;
};


#endif