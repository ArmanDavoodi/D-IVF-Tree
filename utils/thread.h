#ifndef THREAD_H_
#define THREAD_H_

#include "configs/support.h"

#include <thread>
#include <cstdint>
#include <unordered_set>

#include "debug.h"

#include "utils/string.h"

#if defined(_MSC_VER) && (defined(_M_IX86) || defined(_M_X64))

#include <immintrin.h>
#define DIVFTREE_YIELD()  _mm_pause()

#elif defined(__i386__) || defined(__x86_64__)

#include <immintrin.h>
#define DIVFTREE_YIELD()  __builtin_ia32_pause()

#elif defined(__aarch64__) || defined(__arm__)

#define DIVFTREE_YIELD()  asm volatile("yield" ::: "memory")

#else
#define DIVFTREE_YIELD()
#endif
namespace divftree {

enum LockMode {
    SX_SHARED, SX_EXCLUSIVE
};

String LockModeToString(LockMode mode) {
    switch (mode) {
        case SX_SHARED: return "SHARED";
        case SX_EXCLUSIVE: return "EXCLUSIVE";
        default: return "INVALID";
    }
}

typedef uint64_t DIVFThreadID;
inline constexpr DIVFThreadID INVALID_DIVF_THREAD_ID = UINT64_MAX;

inline thread_local Thread* threadSelf = nullptr;
class Thread {
public:
    Thread() {}
    ~Thread() {}

    inline const DIVFThreadID ID() const {
        return _id;
    }

    inline void AcquireLockSanityLog(void* lockAddr, LockMode mode) {
        UNUSED_VARIABLE(lockAddr);
        UNUSED_VARIABLE(mode);
#ifdef LOCK_DEBUG
        SanityCheckLockNotHeldByMe(lockAddr);
        if (mode == SX_EXCLUSIVE) {
            heldExclusive.insert(lockAddr);
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_LOCK, "Acquired EXCLUSIVE lock %p", _id, lockAddr);
        } else {
            heldShared.insert(lockAddr);
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_LOCK, "Acquired SHARED lock %p", _id, lockAddr);
        }
#endif
    }

    inline void DowngradeLockSanityLog(void* lockAddr) {
        UNUSED_VARIABLE(lockAddr);
#ifdef LOCK_DEBUG
        SanityCheckLockHeldInModeByMe(lockAddr, SX_EXCLUSIVE);
        heldExclusive.erase(lockAddr);
        heldShared.insert(lockAddr);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_LOCK, "Downgraded EXCLUSIVE to SHARED lock %p", _id, lockAddr);
#endif
    }

    inline void UpgradeLockSanityLog(void* lockAddr) {
        UNUSED_VARIABLE(lockAddr);
#ifdef LOCK_DEBUG
        SanityCheckLockHeldInModeByMe(lockAddr, SX_SHARED);
        heldShared.erase(lockAddr);
        heldExclusive.insert(lockAddr);
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_LOCK, "Upgraded SHARED to EXCLUSIVE lock %p", _id, lockAddr);
#endif
    }

    inline void ReleaseLockSanityLog(void* lockAddr, LockMode mode) {
        UNUSED_VARIABLE(lockAddr);
        UNUSED_VARIABLE(mode);
#ifdef LOCK_DEBUG
        SanityCheckLockHeldInModeByMe(lockAddr, mode);
        if (mode == SX_EXCLUSIVE) {
            heldExclusive.erase(lockAddr);
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_LOCK, "Released EXCLUSIVE lock %p", _id, lockAddr);
        } else {
            heldShared.erase(lockAddr);
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_LOCK, "Released SHARED lock %p", _id, lockAddr);
        }
#endif
    }

    inline void SanityCheckLockNotHeldInBothModesByMe(void* lockAddr) {
        UNUSED_VARIABLE(lockAddr);
#ifdef LOCK_DEBUG
        FatalAssert((heldExclusive.find(lockAddr) == heldExclusive.end()) ||
                    (heldShared.find(lockAddr) == heldShared.end()),
                    LOG_TAG_LOCK, "Lock %p is held in both modes by thread %lu", lockAddr, _id);
#endif
    }

    inline void SanityCheckLockHeldByMe(void* lockAddr) {
        UNUSED_VARIABLE(lockAddr);
#ifdef LOCK_DEBUG
        SanityCheckLockNotHeldInBothModesByMe(lockAddr);
        FatalAssert((heldExclusive.find(lockAddr) != heldExclusive.end()) ||
                    (heldShared.find(lockAddr) != heldShared.end()),
                    LOG_TAG_LOCK, "Lock %p is not held by thread %lu", lockAddr, _id);
#endif
    }

    inline void SanityCheckLockHeldInModeByMe(void* lockAddr, LockMode mode) {
        UNUSED_VARIABLE(lockAddr);
        UNUSED_VARIABLE(mode);
#ifdef LOCK_DEBUG
        SanityCheckLockNotHeldInBothModesByMe(lockAddr);
        if (mode == SX_EXCLUSIVE) {
            FatalAssert(heldExclusive.find(lockAddr) != heldExclusive.end(),
                        LOG_TAG_LOCK, "Lock %p is not held in EXCLUSIVE mode by thread %lu",
                        lockAddr, _id);
        } else {
            FatalAssert(heldShared.find(lockAddr) != heldShared.end(),
                        LOG_TAG_LOCK, "Lock %p is not held in SHARED mode by thread %lu",
                        lockAddr, _id);
        }
#endif
    }

    inline void SanityCheckLockNotHeldByMe(void* lockAddr) {
        UNUSED_VARIABLE(lockAddr);
#ifdef LOCK_DEBUG
        SanityCheckLockNotHeldInBothModesByMe(lockAddr);
        FatalAssert(heldShared.find(lockAddr) == heldShared.end() &&
                    heldExclusive.find(lockAddr) == heldExclusive.end(),
                    LOG_TAG_LOCK, "Lock %p is held by thread %lu",
                    lockAddr, _id);
#endif
    }

    inline void SanityCheckLockNotHeldInModeByMe(void* lockAddr, LockMode mode) {
        UNUSED_VARIABLE(lockAddr);
        UNUSED_VARIABLE(mode);
#ifdef LOCK_DEBUG
        SanityCheckLockNotHeldInBothModesByMe(lockAddr);
        if (mode == SX_EXCLUSIVE) {
            FatalAssert(heldExclusive.find(lockAddr) == heldExclusive.end(),
                        LOG_TAG_LOCK, "Lock %p is held in EXCLUSIVE mode by thread %lu",
                        lockAddr, _id);
        } else {
            FatalAssert(heldShared.find(lockAddr) == heldShared.end(),
                        LOG_TAG_LOCK, "Lock %p is held in SHARED mode by thread %lu",
                        lockAddr, _id);
        }
#endif
    }
protected:
    // Thread implementation details
    DIVFThreadID _id;
#ifdef LOCK_DEBUG
    std::unordered_set<void*> heldShared;
    std::unordered_set<void*> heldExclusive;
#endif
};

};

#endif