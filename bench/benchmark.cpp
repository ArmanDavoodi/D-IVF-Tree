#include "benchmark.h"

#include "config_reader.h"
#include "dataset.h"

inline divftree::DIVFTree* vector_index = nullptr;
inline std::atomic<bool> stop = false;
inline std::atomic<bool> dataset_finished = false;
inline size_t* search_queries = nullptr;
inline size_t* insert_queries = nullptr;
inline size_t* delete_queries = nullptr;

constexpr size_t K = 10;

enum Task {
    BUILD_INDEX, WARMUP, RUN
};

inline std::atomic<Task> current_task = BUILD_INDEX;

/* todo: instead of this get a batch per thread and make reading the file atomic? */
void worker(divftree::Thread* self, size_t thread_idx) {
    self->InitDIVFThread();
    divftree::VTYPE buffer[BATCH_SIZE * DIMENSION];
    FILE* input_file_ptr = nullptr;
    OpenDataFile(input_file_ptr, false);
    size_t num_read = 0;
    size_t total_read = 0;

    while (!stop.load(std::memory_order_acquire)) {
        if (num_read == total_read) {
            total_read = ReadNextBatch(input_file_ptr, buffer);
            num_read = 0;
        }

        if (total_read == 0) {
            dataset_finished.store(true, std::memory_order_release);
            break;
        }

        divftree::VectorID id;
        std::unordered_set<divftree::VectorID, divftree::VectorIDHash> id_set;
        std::vector<divftree::ANNVectorInfo> neighbours;
        divftree::RetStatus rs;
        switch (current_task.load(std::memory_order_acquire))
        {
        case BUILD_INDEX:
            /* todo: make sure that when we return from this it is safe to reuse buffer! */
            rs = vector_index->Insert(&buffer[num_read * DIMENSION], id, default_internal_search_span);
            if (!rs.IsOK()) {
                DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Error during insert: %s", rs.Msg());
            }
            id_set.insert(id);
            ++num_read;
            break;
        case WARMUP:
            neighbours.clear();
            rs = vector_index->ApproximateKNearestNeighbours(&buffer[num_read * DIMENSION], K,
                                                             default_internal_search_span, default_leaf_search_span,
                                                             divftree::SortType::Unsorted, neighbours);
            if (!rs.IsOK()) {
                DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Error during search: %s", rs.Msg());
            }
            ++num_read;
            break;
        case RUN:
            if(!self->UniformBinary(write_ratio)) {
                neighbours.clear();
                rs = vector_index->ApproximateKNearestNeighbours(&buffer[num_read * DIMENSION], K,
                                                                default_internal_search_span, default_leaf_search_span,
                                                                divftree::SortType::Unsorted, neighbours);
                if (!rs.IsOK()) {
                    DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Error during search: %s", rs.Msg());
                }
                ++search_queries[thread_idx];
                ++num_read;
            } else if (self->UniformBinary(delete_ratio)) {
                divftree::VectorID target = divftree::INVALID_VECTOR_ID;
                target._creator_node_id = 0;
                target._level = 0;
                target._val = self->UniformRange64(0, id._val);
                if (id_set.find(target) != id_set.end()) {
                    rs = vector_index->Delete(target);
                    if (!rs.IsOK()) {
                        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Error during delete: %s", rs.Msg());
                    } else {
                        id_set.erase(target);
                    }
                    ++delete_queries[thread_idx];
                } else {
                    rs = vector_index->Insert(&buffer[num_read * DIMENSION], id, default_internal_search_span);
                    if (!rs.IsOK()) {
                        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Error during insert: %s", rs.Msg());
                    } else {
                        id_set.insert(id);
                    }
                    ++insert_queries[thread_idx];
                    ++num_read;
                }
            } else {
                rs = vector_index->Insert(&buffer[num_read * DIMENSION], id, default_internal_search_span);
                if (!rs.IsOK()) {
                    DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Error during insert: %s", rs.Msg());
                } else {
                    id_set.insert(id);
                }
                ++insert_queries[thread_idx];
                ++num_read;
            }
            break;
        default:
            DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Unknown task!");
            break;
        }
    }

    CloseDataFile(input_file_ptr);
    self->DestroyDIVFThread();
}

int main() {
    ReadConfigs();
    ParseConfigs();
    FILE* file = nullptr;
    OpenDataFile(file, true);
    CloseDataFile(file);

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Starting benchmark for %s(type:%s, dimension:%hu, distance:%s) "
            "with %lu threads for (build:%u, warmup:%u, run:%u) durations with %u percent writes"
            "and search span of %hhu and %hhu for leaf and internal vertices...",
            DIVF_MACRO_TO_STR(DATASET), DIVF_MACRO_TO_STR(VECTOR_TYPE), DIMENSION,
            divftree::DISTANCE_TYPE_NAME[(int8_t)DISTANCE_ALG], num_threads, build_time, warmup_time, run_time,
            write_ratio, default_leaf_search_span, default_internal_search_span);

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Initing the index with the attributes: %s",
            index_attr.ToString().ToCStr());

    vector_index = new divftree::DIVFTree(index_attr);

    search_queries = new size_t[num_threads];
    insert_queries = new size_t[num_threads];
    delete_queries = new size_t[num_threads];
    memset(search_queries, 0, sizeof(size_t) * num_threads);
    memset(insert_queries, 0, sizeof(size_t) * num_threads);
    memset(delete_queries, 0, sizeof(size_t) * num_threads);

    std::vector<divftree::Thread*> threads(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        threads[i] = new divftree::Thread(100);
    }

    current_task.store(BUILD_INDEX, std::memory_order_release);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Starting %lu worker threads...", num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        threads[i]->Start(worker, i);
    }

    divftree::sleep(build_time);
    current_task.store(WARMUP, std::memory_order_release);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Switching to warmup phase...");
    divftree::sleep(warmup_time);
    current_task.store(RUN, std::memory_order_release);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Switching to run phase...");
    divftree::sleep(run_time);
    stop.store(true, std::memory_order_release);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Stopping all threads...");
    for (size_t i = 0; i < num_threads; ++i) {
        threads[i]->Join();
        delete threads[i];
        threads[i] = nullptr;
    }
    delete vector_index;

    size_t total_search = 0;
    size_t total_insert = 0;
    size_t total_delete = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_search += search_queries[i];
        total_insert += insert_queries[i];
        total_delete += delete_queries[i];
    }

    if (dataset_finished.load(std::memory_order_acquire)) {
        DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_TEST, "All data set has been processed!");
    }

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Total search queries: %lu", total_search);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Total insert queries: %lu", total_insert);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Total delete queries: %lu", total_delete);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Search QPS: %.2f", ((double)total_search) / run_time);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Insert QPS: %.2f", ((double)total_insert) / run_time);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Delete QPS: %.2f", ((double)total_delete) / run_time);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Total QPS: %.2f",
            ((double)(total_search + total_insert + total_delete)) / run_time);
    delete[] search_queries;
    delete[] insert_queries;
    delete[] delete_queries;
    search_queries = nullptr;
    insert_queries = nullptr;
    delete_queries = nullptr;
}