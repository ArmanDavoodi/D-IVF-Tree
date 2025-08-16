#ifndef THREAD_H_
#define THREAD_H_

#include "configs/support.h"

#include <thread>
#include <cstdint>

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