#ifndef COPPER_DEBUG_H_
#define COPPER_DEBUG_H_

#include <cstdint>

#ifndef BUILD
#define BUILD RELEASE
#endif

#ifdef TESTING
#undef BUILD
#define BUILD DEBUG
#define FAULT_INJECTION // todo: add to compile time flags
#endif

// might add other modes as well later and consider not completly disabling the logs in release
#if BUILD==DEBUG
#define ENABLE_TEST_LOGGING
#define ENABLE_ASSERTS
#elif BUILD==RELEASE
#define ENABLE_ASSERTS
#endif

enum LOG_LEVELS : uint8_t {
    LOG_LEVEL_ZERO,
    LOG_LEVEL_PANIC,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_LOG,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_NUM
};

enum LOG_TAG_BITS : uint64_t {
    LOG_TAG_BASIC_BIT,
    LOG_TAG_VECTOR_BIT,
    LOG_TAG_VECTOR_SET_BIT,
    LOG_TAG_BUFFER_BIT,
    LOG_TAG_COPPER_NODE_BIT,
    LOG_TAG_VECTOR_INDEX_BIT,
    LOG_TAG_NOT_IMPLEMENTED_BIT,
    LOG_TAG_TEST_BIT,
    NUM_TAGS
};

#define LOG_TAG_BASIC (1ul << (uint64_t)(LOG_TAG_BITS::LOG_TAG_BASIC_BIT))
#define LOG_TAG_VECTOR (1ul << (uint64_t)(LOG_TAG_BITS::LOG_TAG_VECTOR_BIT))
#define LOG_TAG_VECTOR_SET (1ul << (uint64_t)(LOG_TAG_BITS::LOG_TAG_VECTOR_SET_BIT))
#define LOG_TAG_BUFFER (1ul << (uint64_t)(LOG_TAG_BITS::LOG_TAG_BUFFER_BIT))
#define LOG_TAG_COPPER_NODE (1ul << (uint64_t)(LOG_TAG_BITS::LOG_TAG_COPPER_NODE_BIT))
#define LOG_TAG_VECTOR_INDEX (1ul << (uint64_t)(LOG_TAG_BITS::LOG_TAG_VECTOR_INDEX_BIT))
#define LOG_TAG_NOT_IMPLEMENTED (1ul << (uint64_t)(LOG_TAG_BITS::LOG_TAG_NOT_IMPLEMENTED_BIT))
#define LOG_TAG_TEST (1ul << (uint64_t)(LOG_TAG_BITS::LOG_TAG_TEST_BIT))

#define LOG_TAG_ANY (-1ul)

#ifndef LOG_MIN_LEVEL
#define LOG_MIN_LEVEL (LOG_LEVEL_PANIC)
#endif

#ifndef LOG_LEVEL
#define LOG_LEVEL (LOG_LEVEL_LOG)
#endif

#ifndef LOG_TAG
#define LOG_TAG (LOG_TAG_ANY)
#endif

#ifndef OUT
#define OUT (stdout)
#endif

#define _COLORF_BLACK "\033[0;30m"
#define _COLORF_RED "\033[0;31m"
#define _COLORF_GREEN "\033[0;32m"
#define _COLORF_YELLOW "\033[0;33m"
#define _COLORF_BLUE "\033[0;34m"
#define _COLORF_PURPLE "\033[0;35m"
#define _COLORF_CYAN "\033[0;36m"
#define _COLORF_WHITE "\033[0;37m"
#define _COLORF_RESET "\033[0m"

#ifdef COLORED
#define COLORF_BLACK _COLORF_BLACK
#define COLORF_RED _COLORF_RED
#define COLORF_GREEN _COLORF_GREEN
#define COLORF_YELLOW _COLORF_YELLOW
#define COLORF_BLUE _COLORF_BLUE
#define COLORF_PURPLE _COLORF_PURPLE
#define COLORF_CYAN _COLORF_CYAN
#define COLORF_WHITE _COLORF_WHITE
#define COLORF_RESET _COLORF_RESET
#else
#define COLORF_BLACK
#define COLORF_RED
#define COLORF_GREEN
#define COLORF_YELLOW
#define COLORF_BLUE
#define COLORF_PURPLE
#define COLORF_CYAN
#define COLORF_WHITE
#define COLORF_RESET
#endif

#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <assert.h>
#include <thread>
#include <chrono>
#include <ctime>
#include <cstdio>
#include <stdarg.h>
#include <string.h>
#include <atomic>
#include <semaphore>
#include <map>

#ifdef __clang__
#include <experimental/source_location>
    using SourceLoc = std::experimental::source_location;
#else
#include <source_location>
    using SourceLoc = std::source_location;
#endif

namespace copper {

struct Log_Msg {
    char _msg[500] = "";

    Log_Msg(const char* msg, ...) {
        va_list argptr;
        va_start(argptr, msg);
        vsprintf(_msg, msg, argptr);
        va_end(argptr);
    }
};

inline void nsleep(uint64_t nsec) {
    std::this_thread::sleep_for(std::chrono::nanoseconds(nsec));
}

inline void usleep(uint64_t usec) {
    std::this_thread::sleep_for(std::chrono::microseconds(usec));
}

inline void msleep(uint64_t msec) {
    std::this_thread::sleep_for(std::chrono::milliseconds(msec));
}

inline void sleep(uint64_t sec) {
    std::this_thread::sleep_for(std::chrono::seconds(sec));
}

inline void timetostr(const std::chrono::_V2::system_clock::time_point _time, char* mbstr)
{
    std::time_t _t = std::chrono::system_clock::to_time_t(_time);
    auto _m = _time - std::chrono::time_point_cast<std::chrono::seconds>(_time);
    size_t num_chars = std::strftime(mbstr, 100, "%F:%T", std::localtime(&_t));
    sprintf(mbstr + num_chars, ".%lu", std::chrono::duration_cast<std::chrono::microseconds>(_m).count());
}

inline const char* leveltostr(LOG_LEVELS level)
{
    switch (level)
    {
    case LOG_LEVEL_PANIC:
        return COLORF_PURPLE "PANIC" COLORF_RESET;
    case LOG_LEVEL_ERROR:
        return COLORF_RED "ERROR" COLORF_RESET;
    case LOG_LEVEL_WARNING:
        return COLORF_YELLOW "WARNING" COLORF_RESET;
    case LOG_LEVEL_LOG:
        return COLORF_CYAN "LOG" COLORF_RESET;
    case LOG_LEVEL_DEBUG:
        return COLORF_GREEN "DEBUG" COLORF_RESET;
    default:
        return "UNDEFINED";
    }
}

inline const char* tagtostr(uint64_t tag)
{
    switch (tag)
    {
    case LOG_TAG_BASIC:
        return "Basic" ;
    case LOG_TAG_VECTOR:
        return "Vector" ;
    case LOG_TAG_VECTOR_SET:
        return "Vector Set" ;
    case LOG_TAG_BUFFER:
        return "Buffer" ;
    case LOG_TAG_COPPER_NODE:
        return "Copper Node" ;
    case LOG_TAG_VECTOR_INDEX:
        return "Vector Index" ;
    case LOG_TAG_NOT_IMPLEMENTED:
        return "Not Implemented" ;
    case LOG_TAG_TEST:
        return "Test" ;
    default:
        return "Undefined" ;
    }
}

inline void Log(LOG_LEVELS level, uint64_t tag, const Log_Msg& msg, const std::chrono::_V2::system_clock::time_point _time,
                const char* file_name, const char* func_name, size_t line,
                size_t thread_id) {

    char time_str[100];
    timetostr(_time, time_str); // todo add coloring if needed
    fprintf(OUT, "%s | %s | %s | %s:%lu | %s | Thread(%lu) | Message: %s\n",
        leveltostr(level), tagtostr(tag), time_str, file_name, line, func_name, thread_id, msg._msg);
    fflush(OUT);
    if (level == LOG_LEVEL_PANIC) {
        sleep(1); // wait to make sure everything is flushed
        assert(false);
    }
}

inline bool Pass_Min_Level(LOG_LEVELS level) {
    return ((uint8_t)(level) <= LOG_MIN_LEVEL);
}

inline bool Pass_Tag(uint64_t tag) {
    return ((LOG_TAG & tag) != 0);
}

inline bool Pass_Level(LOG_LEVELS level) {
    return ((uint8_t)(level) <= (uint8_t)(LOG_LEVEL));
}

};

#ifdef ENABLE_TEST_LOGGING

#define CLOG(level, tag, msg, ...) \
    do {\
        if (copper::Pass_Min_Level((level)) || copper::Pass_Tag((tag))) {\
            if (copper::Pass_Level((level))){\
                copper::Log((level), (tag), (copper::Log_Msg((msg) __VA_OPT__(,) __VA_ARGS__)), \
                            std::chrono::system_clock::now(),\
                            __FILE__, __PRETTY_FUNCTION__, __LINE__,\
                            std::hash<std::thread::id>{}(std::this_thread::get_id()));\
            }\
        }\
    } while(0)

#define CLOG_IF_TRUE(cond, level, tag, msg, ...) \
    do {\
        if ((cond)){\
            char _TMP_DEBUG[sizeof((#cond))+sizeof((msg))+19] = "Condition True(" #cond "): ";\
            strcat(_TMP_DEBUG+sizeof((#cond)), (msg));\
            CLOG((level), (tag),  _TMP_DEBUG __VA_OPT__(,) __VA_ARGS__);\
        }\
    } while(0)

#define CLOG_IF_FALSE(cond, level, tag, msg, ...) \
    do {\
        if (!(cond)){\
            char _TMP_DEBUG[sizeof((#cond))+sizeof((msg))+20] = "Condition False(" #cond "): ";\
            strcat(_TMP_DEBUG+sizeof((#cond)), (msg));\
            CLOG((level), (tag),  _TMP_DEBUG __VA_OPT__(,) __VA_ARGS__);\
        }\
    } while(0)

// todo add unlikely to assert conditions
#ifdef ENABLE_ASSERTS
#define FatalAssert(cond, tag, msg, ...) \
    do {\
        if (!(cond)){\
            if (sizeof((msg)) == 0){\
                CLOG(LOG_LEVEL_PANIC, (tag),  "Assertion \'" #cond "\' Failed.");\
            }\
            else{\
                char _TMP_DEBUG[sizeof((#cond))+sizeof((msg))+22] = "Assertion \'" #cond "\' Failed: ";\
                strcat(_TMP_DEBUG+sizeof((#cond)), (msg));\
                CLOG(LOG_LEVEL_PANIC, (tag),  _TMP_DEBUG __VA_OPT__(,) __VA_ARGS__);\
            }\
        }\
    } while(0)

#ifdef ASSERT_ERROR_PANIC
#define ErrorAssert(cond, tag, msg, ...) FatalAssert((cond), (tag), (msg)__VA_OPT__(,) __VA_ARGS__)
#else
#define ErrorAssert(cond, tag, msg, ...) \
    do {\
        if (!(cond)) {\
            if (sizeof((msg)) == 0){\
                CLOG(LOG_LEVEL_ERROR, (tag),  "Assertion \'" #cond "\' Failed.");\
            }\
            else{\
                char _TMP_DEBUG[sizeof((#cond))+sizeof((msg))+22] = "Assertion \'" #cond "\' Failed: ";\
                strcat(_TMP_DEBUG+sizeof((#cond)), (msg));\
                CLOG(LOG_LEVEL_ERROR, (tag),  _TMP_DEBUG __VA_OPT__(,) __VA_ARGS__);\
            }\
        }\
    } while(0)
#endif

#else
#define FatalAssert(cond, tag, msg, ...)
#define ErrorAssert(cond, tag, msg, ...)
#endif
#else

#define CLOG(level, tag, msg, ...)
#define CLOG_IF_TRUE(cond, level, tag, msg, ...)
#define CLOG_IF_FALSE(cond, level, tag, msg, ...)

#ifdef ENABLE_ASSERTS
#define FatalAssert(cond, tag, msg, ...) assert((cond))
#else
#define FatalAssert(cond, tag, msg, ...)
#endif

#ifdef ASSERT_ERROR_PANIC
#define ErrorAssert(cond, tag, msg, ...) FatalAssert((cond), (tag), (msg)__VA_OPT__(,) __VA_ARGS__)
#else
#define ErrorAssert(cond, tag, msg, ...)
#endif
#endif

#ifdef FAULT_INJECTION
static inline std::map<std::string, std::binary_semaphore> FI_MAP;
#define FAULT_INJECTION_INIT(name) FI_MAP.try_emplace((name), 0)
#define FAULT_INJECTION_DELETE(name) FI_MAP.erase((name))
#define FAULT_INJECTION_WAIT(name) FI_MAP[(name)].acquire()
#define FAULT_INJECTION_SIGNAL(name) FI_MAP[(name)].release()
// todo:
// todo: add backslash at the end of lines
// #define FAULT_INJECTION_WAIT_UNTIL(name, duration) FI_MAP[(name)].try_acquire_for((duration))
// #define FAULT_INJECTION_WAIT_UNTIL_ERROR(name, duration, tag, msg, ...)
//     do {
//         if (sizeof((msg)) == 0){
//             CLOG(LOG_LEVEL_ERROR, (tag),  "Fault Injection timeout error on \'%s\' after waiting for %lu %s.", );
//         }
//         else{
//             char _TMP_DEBUG[sizeof((#cond))+sizeof((msg))+22] = "Assertion \'" #cond "\' Failed: ";
//             strcat(_TMP_DEBUG+sizeof((#cond)), (msg));
//             CLOG(LOG_LEVEL_ERROR, (tag),  _TMP_DEBUG __VA_OPT__(,) __VA_ARGS__);
//         }
//     } while(0)
// #define FAULT_INJECTION_WAIT_UNTIL_PANIC(name, duration) FI_MAP[(name)].try_acquire_for((duration))
#else
#define FAULT_INJECTION_INIT(name)
#define FAULT_INJECTION_DELETE(name)
#define FAULT_INJECTION_WAIT(name)
#define FAULT_INJECTION_SIGNAL(name)
#endif

#endif