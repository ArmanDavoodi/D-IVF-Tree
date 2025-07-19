#ifndef THREAD_H_
#define THREAD_H_

#include <thread>
#include <cstdint>

namespace divftree {

typedef uint64_t DIVFThreadID;
inline constexpr DIVFThreadID INVALID_DIVF_THREAD_ID = UINT64_MAX;

inline thread_local DIVFThreadID _cur_thread_id = INVALID_DIVF_THREAD_ID;

class Thread {
public:
    Thread();
    ~Thread();

    void Start();
    void Join();

protected:
    // Thread implementation details
};

};

#endif