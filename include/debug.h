#ifndef COPPER_DEBUG_H_
#define COPPER_DEBUG_H_

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


#define LOG_LEVEL_ZERO 0
#define LOG_LEVEL_PANIC 1
#define LOG_LEVEL_ERROR 2
#define LOG_LEVEL_WARNING 3
#define LOG_LEVEL_LOG 4
#define LOG_LEVEL_DEBUG 5

#define LOG_TAG_BASIC 0b1
#define LOG_TAG_NOT_IMPLEMENTED 0b10
#define LOG_TAG_TEST 0b100
#define LOG_TAG_COPPER_NODE 0b1000
#define NUM_TAGS 4

#define LOG_TAG_ANY 0b1111


#ifndef LOG_MIN_LEVEL
#define LOG_MIN_LEVEL LOG_LEVEL_ZERO
#endif

#ifndef LOG_LEVEL
#define LOG_LEVEL LOG_LEVEL_LOG
#endif

#ifndef LOG_TAG
#define LOG_TAG LOG_TAG_ANY
#endif

#ifndef OUT
#define OUT stdout
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

inline const char* leveltostr(uint8_t level)
{
    switch (level)
    {
    case LOG_LEVEL_PANIC:
        return COLORF_PURPLE "Panic";
    case LOG_LEVEL_ERROR:
        return COLORF_RED "Error";
    case LOG_LEVEL_WARNING:
        return COLORF_YELLOW "Warning";
    case LOG_LEVEL_LOG:
        return COLORF_CYAN "Log";
    case LOG_LEVEL_DEBUG:
        return COLORF_GREEN "Debug";
    default:
        return "Undefined";
    }
}

inline const char* tagtostr(uint8_t tag)
{
    switch (tag)
    {
    case LOG_TAG_BASIC:
        return "(Basic)" COLORF_RESET;
    case LOG_TAG_NOT_IMPLEMENTED:
        return "(Not Implemented)" COLORF_RESET;
    case LOG_TAG_TEST:
        return "(Test)" COLORF_RESET;
    case LOG_TAG_COPPER_NODE:
        return "(Copper Node)" COLORF_RESET;
    default:
        return "(Undefined)" COLORF_RESET;
    }
}

inline void Log(uint8_t level, uint8_t tag, const Log_Msg& msg, const std::chrono::_V2::system_clock::time_point _time,
                const char* file_name, const char* func_name, size_t line, 
                size_t thread_id) {

    char time_str[100];
    timetostr(_time, time_str); // todo add coloring if needed 
    fprintf(OUT, "%s%s | %s | %s:%lu | %s | Thread(%lu) | Message: %s\n", 
        leveltostr(level), tagtostr(tag), time_str, file_name, line, func_name, thread_id, msg._msg);
    fflush(OUT);
    if (level == LOG_LEVEL_PANIC) {
        sleep(1); // wait to make sure everything is flushed
        assert(false);
    }
}

inline bool Pass_Min_Level(uint8_t level) {
    return (level <= LOG_MIN_LEVEL);
}

inline bool Pass_Tag(uint8_t tag) {
    return ((LOG_TAG & tag) != 0);
}

inline bool Pass_Level(uint8_t level) {
    return (level <= LOG_LEVEL);
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
            char _TMP_DEBUG[sizeof((#cond))+sizeof((msg))+20] = "Condition True(" #cond ") | ";\
            strcat(_TMP_DEBUG+sizeof((#cond)), (msg));\
            CLOG((level), (tag),  _TMP_DEBUG __VA_OPT__(,) __VA_ARGS__);\
        }\
    } while(0)

#define CLOG_IF_FALSE(cond, level, tag, msg, ...) \
    do {\
        if (!(cond)){\
            char _TMP_DEBUG[sizeof((#cond))+sizeof((msg))+21] = "Condition False(" #cond ") | ";\
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