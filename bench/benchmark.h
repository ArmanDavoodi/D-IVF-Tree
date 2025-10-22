#ifndef BENCHMARK_H_
#define BENCHMARK_H_

#include "configurations.h"
#include "divftree.h"

#include <cstdint>
#include <atomic>

inline divftree::DIVFTreeAttributes index_attr;
inline uint8_t default_leaf_search_span;
inline uint8_t default_internal_search_span;
inline uint8_t default_k;
inline size_t num_threads;
inline uint32_t write_ratio;
inline uint32_t delete_ratio;
inline uint32_t build_size;
inline uint32_t warmup_time;
inline uint32_t run_time;
inline uint32_t throughput_report_time;

#endif