#ifndef _LOCK_H_
#define _LOCK_H_

#include <shared_mutex>
#include <atomic>

#include "debug.h"

#include "utils/string.h"
#include "utils/thread.h"
#include "utils/concurrent_datastructures.h"

namespace divftree {

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
    SXLock() : _mode(SX_SHARED), _num_shared_holders(0) {}
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
protected:
    LockMode _mode;
    std::atomic<uint64_t> _num_shared_holders;
    std::shared_mutex _m;
};

};

#endif