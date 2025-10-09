#include "benchmark.h"

#include "config_reader.h"
#include "dataset.h"

inline std::atomic<uint32_t> next_vec;
inline divftree::DIVFTree* vector_index = nullptr;
inline std::atomic<bool> stop = false;
inline std::atomic<bool> batch_ready = true;
inline std::atomic<uint32_t> signal = 0;
inline std::atomic<uint32_t> readers = 0;
inline bool dataset_finished = false;

enum Task {
    BUILD_INDEX, WARMUP, RUN
};

inline std::atomic<Task> current_task = BUILD_INDEX;

/* todo: instead of this get a batch per thread and make reading the file atomic? */
void worker() {
    bool readNextBatch = false;
    uint32_t idx;
    while (!stop.load(std::memory_order_acquire)) {
        if (readNextBatch) {
            if (readers.load(std::memory_order_acquire) > 0) {
                DIVFTREE_YIELD();
                continue;
            }

            if (next_vec.load(std::memory_order_acquire) - 1 != current_batch_size.load(std::memory_order_acquire)) {
                DIVFTREE_YIELD();
                continue;
            }

            readers.fetch_add(1);
            readNextBatch = false;
            if (!ReadNextBatch()) {
                dataset_finished = true;
                stop.store(true, std::memory_order_release);
                continue;
            }
            next_vec.store(1, std::memory_order_release);
            batch_ready.store(true, std::memory_order_release);
            idx = 0;
        } else {
            /* todo: current batch size and next_vec are not atomically updated together. */
            if (!batch_ready.load(std::memory_order_acquire)) {
                DIVFTREE_YIELD();
                continue;
            }

            idx = next_vec.fetch_add(1);
            if (idx == current_batch_size) {
                batch_ready.store(false, std::memory_order_release);
                readNextBatch = true;
                continue;
            }

            if (idx > current_batch_size) {
                next_vec.fetch_sub(1);
                DIVFTREE_YIELD();
                continue;
            }

            readers.fetch_add(1);
        }

        divftree::VTYPE* vector = &vector_buffer[idx * DIMENSION];

        readers.fetch_sub(1);
    }
}

int main() {
    ReadConfigs();
    ParseConfigs();
    OpenDataFile();
    (void)ReadNextBatch();

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Starting benchmark for %s(type:%s, dimension:%hu, distance:%s) "
            "with %lu threads for (build:%u, warmup:%u, run:%u) durations with %u percent writes"
            "and search span of %hhu and %hhu for leaf and internal vertices...",
            DIVF_MACRO_TO_STR(DATASET), DIVF_MACRO_TO_STR(VECTOR_TYPE), DIMENSION,
            divftree::DISTANCE_TYPE_NAME[(int8_t)DISTANCE_ALG], num_threads, build_time, warmup_time, run_time,
            write_ratio, default_leaf_search_span, default_internal_search_span);

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Initing the index with the attributes: %s",
            index_attr.ToString().ToCStr());

    vector_index = new divftree::DIVFTree(index_attr);

}