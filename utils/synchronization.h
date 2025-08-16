#ifndef SYNCHRONIZATION_H_
#define SYNCHRONIZATION_H_

#include "configs/support.h"

#include <shared_mutex>
#include <atomic>
#include <condition_variable>

#include "debug.h"

#include "utils/string.h"
#include "utils/thread.h"
#include "utils/concurrent_datastructures.h"

namespace divftree {

inline bool compare_exchange128(void* dest, void* expected, const void* desired,
                                bool relaxed = false) {
    FatalAssert(TYPE_ALIGNED(dest, 16), LOG_TAG_BASIC, "Destination pointer is not 16-byte aligned");
    // FatalAssert(TYPE_ALIGNED(expected, 16), LOG_TAG_BASIC, "Expected pointer is not 16-byte aligned");
    // FatalAssert(TYPE_ALIGNED(desired, 16), LOG_TAG_BASIC, "Desired pointer is not 16-byte aligned");

#ifdef __DIVFTREE_CMPXCHG128__
    bool result;
    uint64_t* cmp = reinterpret_cast<uint64_t*>(expected);
    uint64_t* with = reinterpret_cast<uint64_t*>(desired);
    if (relaxed) {
        asm volatile
        (
            "lock cmpxchg16b %1\n\t"
            "setz %0"       // on gcc6 and later, use a flag output constraint instead
            : "=q" ( result )
            , "+m" ( *dest )
            , "+d" ( cmp[1] )
            , "+a" ( cmp[0] )
            : "c" ( with[1] )
            , "b" ( with[0] )
            : "cc"
        );
    }
    else {
        asm volatile
        (
            "lock cmpxchg16b %1\n\t"
            "setz %0"       // on gcc6 and later, use a flag output constraint instead
            : "=q" ( result )
            , "+m" ( *dest )
            , "+d" ( cmp.hi )
            , "+a" ( cmp.lo )
            : "c" ( with.hi )
            , "b" ( with.lo )
            : "cc", "memory" // compile-time memory barrier.  Omit if you want memory_order_relaxed compile-time ordering.
        );
    }
    return result; /* May need to use atomic fences later? */
#else
    std::atomic<__int128_t>* target = reinterpret_cast<std::atomic<__int128_t>*>(const_cast<void*>(dest));
    target->compare_exchange_strong(*reinterpret_cast<__int128_t*>(expected),
                                   *reinterpret_cast<const __int128_t*>(desired),
                                   relaxed ? std::memory_order_relaxed : std::memory_order_seq_cst);
#endif
}

// Atomic store: No memory fence/ordering!
inline void atomic_store128(void* dest, const void* src) {
    FatalAssert(TYPE_ALIGNED(dest, 16), LOG_TAG_BASIC, "Destination pointer is not 16-byte aligned");
    FatalAssert(TYPE_ALIGNED(src, 16), LOG_TAG_BASIC, "Source pointer is not 16-byte aligned");

#if (defined(__DIVFTREE_ATOMIC128__) && (defined(__i386__) || defined(__x86_64__)))
    _mm_store_si128((__m128i*)dest, *(const __m128i*)src);
#elif defined(__DIVFTREE_CMPXCHG128__)
    alignas(16) uint64_t expected[2];
    std::memcpy(expected, src, 16);
    while (!compare_exchange128(dest, expected, src)) {
        DIVFTREE_YIELD();
    }
#else
    std::atomic<__int128_t>* target = reinterpret_cast<std::atomic<__int128_t>*>(dest);
    target->store(*reinterpret_cast<const __int128_t*>(src), std::memory_order_relaxed);
#endif
}

// Atomic load: No memory fence/ordering!
inline void atomic_load128(const void* src, void* dest) {
    FatalAssert(TYPE_ALIGNED(src, 16), LOG_TAG_BASIC, "Source pointer is not 16-byte aligned");
    FatalAssert(TYPE_ALIGNED(dest, 16), LOG_TAG_BASIC, "Destination pointer is not 16-byte aligned");

#if (defined(__DIVFTREE_ATOMIC128__) && (defined(__i386__) || defined(__x86_64__)))
    (__m128i*)dest = _mm_load_si128((const __m128i*)src);
#elif defined(__DIVFTREE_CMPXCHG128__)
    std::memcpy(dest, src, 16);
    compare_exchange128(const_cast<void*>(src), dest, dest);
#else
    std::memcpy(dest, reinterpret_cast<const std::atomic<__int128_t>*>(src)->load(std::memory_order_relaxed),
                16);
#endif
}

enum LockMode {
    SX_SHARED, SX_EXCLUSIVE
};

String LockModeToString(LockMode mode) {
    switch (mode) {
        case SX_SHARED: return "SHARED";
        case SX_EXCLUSIVE: return "EXCLUSIVE";
        default: return "INVALIDE";
    }
}

/* Todo: a more efficent implementation */
class SXLock {
public:
    SXLock() : _mode(SX_SHARED), _num_shared_holders(0), _signal{true} {}
    ~SXLock() = default;

    void Lock(LockMode mode) {
        if (mode == SX_SHARED) {
            _m.lock_shared();
            FatalAssert(_mode != SX_EXCLUSIVE, LOG_TAG_BASIC,
                        "Cannot lock in SX_SHARED mode when already in SX_EXCLUSIVE mode!");
            _num_shared_holders.fetch_add(1);
        } else {
            FatalAssert(mode == SX_EXCLUSIVE, LOG_TAG_BASIC, "Non Shared Lock should be exclusive!");
            _m.lock();
            FatalAssert(_mode == SX_SHARED, LOG_TAG_BASIC,
                        "Cannot lock in SX_EXCLUSIVE mode when it is not free!");
            FatalAssert(_num_shared_holders.load() == 0,
                        LOG_TAG_BASIC, "Cannot lock in SX_EXCLUSIVE mode when there are shared holders!");
            _mode = SX_EXCLUSIVE;
        }
    }

    void Unlock() {
        if (_mode == SX_SHARED) {
            FatalAssert(_num_shared_holders.load() > 0,
                        LOG_TAG_BASIC, "Cannot unlock in SX_SHARED mode when no holders!");
            _num_shared_holders.fetch_sub(1);
            _m.unlock_shared();
        } else {
            FatalAssert(_mode == SX_EXCLUSIVE, LOG_TAG_BASIC, "Cannot unlock in SX_EXCLUSIVE mode when it is not locked!");
            FatalAssert(_num_shared_holders.load() == 0,
                        LOG_TAG_BASIC, "Cannot unlock in SX_EXCLUSIVE mode when there are shared holders!");
            _mode = SX_SHARED;
            _m.unlock();
        }
    }

    bool TryLock(LockMode mode) {
        bool locked = false;
        if (mode == SX_SHARED) {
            locked = _m.try_lock_shared();
            if (locked) {
                FatalAssert(_mode != SX_EXCLUSIVE, LOG_TAG_BASIC,
                            "Cannot lock in SX_SHARED mode when already in SX_EXCLUSIVE mode!");
                _num_shared_holders.fetch_add(1);
            }
        } else {
            FatalAssert(mode == SX_EXCLUSIVE, LOG_TAG_BASIC, "Non Shared Lock should be exclusive!");
            locked = _m.try_lock();
            if (locked) {
                FatalAssert(_mode == SX_SHARED, LOG_TAG_BASIC,
                        "Cannot lock in SX_EXCLUSIVE mode when it is not free!");
                FatalAssert(_num_shared_holders.load() == 0,
                            LOG_TAG_BASIC, "Cannot lock in SX_EXCLUSIVE mode when there are shared holders!");
                _mode = SX_EXCLUSIVE;
            }
        }
        return locked;
    }

    /* better naming */
    bool LockWithBlockCheck(LockMode mode) {
        bool blocked = !(TryLock(mode));
        if (blocked) {
            Lock(mode);
        }
        return blocked;
    }

    void SleepTillSignalled(LockMode mode) {
        _signal.store(false);
        _signal.wait(false);
    }

    inline String ToString() const {
        return String("<LockMode=%s, SharedHolders=%lu>",
                      LockModeToString(_mode).ToCStr(), _num_shared_holders.load());
    }
protected:
    LockMode _mode;
    std::atomic<uint64_t> _num_shared_holders;
    std::shared_mutex _m;
    std::atomic<bool> _signal;
};

class SXSpinLock {
public:
    SXSpinLock() : _mode(SX_SHARED), _lock{0} {}
    ~SXSpinLock() = default;

    void Lock(LockMode mode) {
        do {
            while (_lock._data._exclusive_flag.load(std::memory_order_acquire) == LOCKED) {
                DIVFTREE_YIELD();
            }

            if (mode == SX_SHARED) {
                non_atomic_lock res{0};
                res._counter = _lock._counter.fetch_add(1, std::memory_order_seq_cst);
                if (res._data._exclusive_flag == LOCKED) {
                    _lock._counter.fetch_sub(1, std::memory_order_release);
                    continue;
                }
                FatalAssert(_mode == SX_SHARED, LOG_TAG_BASIC, "Lock mode is exclusive!");
                break;
            }
            else {
                non_atomic_lock expected{0};
                if (!_lock._data._exclusive_flag.compare_exchange_strong(expected._data._exclusive_flag,
                                                                         LOCKED, std::memory_order_seq_cst)) {
                    continue;
                }

                while (_lock._data._shared_counter.load(std::memory_order_acquire) > 0) {
                    DIVFTREE_YIELD();
                }
                FatalAssert(_mode == SX_SHARED, LOG_TAG_BASIC, "Lock mode is exclusive!");
                _mode = SX_EXCLUSIVE;
                break;
            }
        } while(true);
    }

    void Unlock() {
        if (_mode == SX_SHARED) {
            FatalAssert(_lock._data._shared_counter.load(std::memory_order_acquire) > 0,
                        LOG_TAG_BASIC, "Cannot unlock in SX_SHARED mode when no holders!");
            _lock._data._shared_counter.fetch_sub(1, std::memory_order_release);
        } else {
            FatalAssert(_mode == SX_EXCLUSIVE, LOG_TAG_BASIC, "Cannot unlock in SX_EXCLUSIVE mode when it is not locked!");
            FatalAssert(_lock._data._shared_counter.load(std::memory_order_acquire) == 0,
                        LOG_TAG_BASIC, "Cannot unlock in SX_EXCLUSIVE mode when there are shared holders!");
            FatalAssert(_lock._data._exclusive_flag.load(std::memory_order_acquire) == LOCKED,
                        LOG_TAG_BASIC, "Cannot unlock in SX_EXCLUSIVE mode when it is not locked!");
            _mode = SX_SHARED;
            _lock._data._exclusive_flag.store(0, std::memory_order_release);
        }
    }

    /* May return false negative for exclusive lock as we might cas sooner than a FAA but see the result of FAA */
    bool TryLock(LockMode mode) {
        if (mode == SX_SHARED) {
            non_atomic_lock res{0};
            res._counter = _lock._counter.fetch_add(1, std::memory_order_seq_cst);
            if (res._data._exclusive_flag == LOCKED) {
                _lock._counter.fetch_sub(1, std::memory_order_release);
                return false;
            }
            FatalAssert(_mode == SX_SHARED, LOG_TAG_BASIC, "Lock mode is exclusive!");
            return true;
        }
        else {
            non_atomic_lock expected{0};
            if (!_lock._data._exclusive_flag.compare_exchange_strong(expected._data._exclusive_flag,
                                                                        LOCKED, std::memory_order_seq_cst)) {
                return false;
            }

            if (_lock._data._shared_counter.load(std::memory_order_acquire) > 0) {
                _lock._data._exclusive_flag.store(0, std::memory_order_release);
                return false;
            }

            FatalAssert(_mode == SX_SHARED, LOG_TAG_BASIC, "Lock mode is exclusive!");
            _mode = SX_EXCLUSIVE;
            return true;
        }
    }

    /* better naming */
    bool LockWithBlockCheck(LockMode mode) {
        bool blocked = !(TryLock(mode));
        if (blocked) {
            Lock(mode);
        }
        return blocked;
    }

    inline String ToString() const {
        return String("<LockMode=%s, SharedHolders=%u>",
                      LockModeToString(_mode).ToCStr(), _lock._data._shared_counter.load());
    }
protected:
    constexpr static uint32_t LOCKED = INT32_MIN;
    LockMode _mode;

    union non_atomic_lock {
        struct {
            uint32_t _exclusive_flag;
            uint32_t _shared_counter;
        } _data;
        uint64_t _counter;
    };
    union {
        struct {
            std::atomic<uint32_t> _exclusive_flag;
            std::atomic<uint32_t> _shared_counter;
        } _data;
        std::atomic<uint64_t> _counter;
    } _lock;
};

template<LockMode mode>
class LockWrapper {
public:
    LockWrapper(SXLock& lock) : _lock(lock) {}
    ~LockWrapper() = default;

    void lock() {
        _lock.Lock(mode);
    }

    void unlock() {
        _lock.Unlock();
    }
protected:
    SXLock& _lock;
};

class CondVar {
public:
    CondVar() = default;
    ~CondVar() = default;

    void NotifyOne() {
        _cond_var.notify_one();
    }

    void NotifyAll() {
        _cond_var.notify_all();
    }

    template<LockMode mode>
    void Wait(LockWrapper<mode> lock) {
        _cond_var.wait(lock);
    }

protected:
    std::condition_variable_any _cond_var;
};

};

#endif