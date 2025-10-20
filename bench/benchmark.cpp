#include "bench/benchmark.h"

#include "bench/config_reader.h"
#include "bench/dataset.h"

#include <vector>
#include <unordered_map>

inline divftree::DIVFTree* vector_index = nullptr;
inline std::atomic<bool> stop = false;
inline std::atomic<bool> dataset_finished = false;
inline size_t* search_queries = nullptr;
inline size_t* insert_queries = nullptr;
inline size_t* delete_queries = nullptr;
inline size_t* search_errors = nullptr;
inline size_t* insert_errors = nullptr;
inline size_t* delete_errors = nullptr;

constexpr size_t K = 10;

enum Task {
    BUILD_INDEX, WARMUP, RUN
};

inline std::atomic<Task> current_task = BUILD_INDEX;

struct IDSet {
    std::unordered_map<divftree::VectorID, size_t, divftree::VectorIDHash> _map;
    std::vector<divftree::VectorID> _data;

    void insert(divftree::VectorID id) {
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_TEST);
        CHECK_VECTORID_IS_VECTOR(id, LOG_TAG_TEST);
        FatalAssert(_map.find(id) == _map.end(), LOG_TAG_TEST, "id should not exist in the set!");
        _map[id] = _data.size() - 1;
        _data.push_back(id);
    }

    void remove(divftree::VectorID id) {
        size_t idx = _map[id];
        if (idx != _data.size() - 1) {
            _data[idx] = _data.back();
            _map[_data[idx]] = idx;
        }
        _data.pop_back();
        _map.erase(id);
    }

    bool exists(divftree::VectorID id) const {
        return (_map.find(id) != _map.end());
    }

    bool empty() const {
        return _data.empty();
    }

    divftree::VectorID get_random_id() const {
        return _data[divftree::threadSelf->UniformRange64(0, _data.size() - 1)];
    }
};

divftree::RetStatus Insert(FILE*& input_file_ptr, size_t& num_read, size_t& total_read, divftree::VTYPE* buffer,
                           divftree::VectorID& id, IDSet& id_set) {
    if (num_read == total_read) {
        total_read = ReadNextBatch(input_file_ptr, buffer);
        num_read = 0;
    }

    if (total_read == 0) {
        if (!dataset_finished.load()) {
            dataset_finished.store(true, std::memory_order_release);
            DIVFLOG(LOG_LEVEL_WARNING, LOG_TAG_TEST, "Dataset finished!");
        }
        return divftree::RetStatus::Fail(nullptr);
    }

    /* todo: make sure that when we return from this it is safe to reuse buffer! */
    divftree::RetStatus rs = vector_index->Insert(&buffer[num_read * DIMENSION], id, default_internal_search_span);
    if (!rs.IsOK()) {
        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Error during insert: %s", rs.Msg());
    }
    id_set.insert(id);
    ++num_read;
    return rs;
}

divftree::RetStatus Delete(IDSet& id_set) {
    divftree::RetStatus rs;
    if (id_set.empty()) {
        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "id_set is empty!");
        return divftree::RetStatus::Fail(nullptr);
    }

    divftree::VectorID target = id_set.get_random_id();
    rs = vector_index->Delete(target);
    if (!rs.IsOK()) {
        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Error during delete: %s", rs.Msg());
    } else {
        id_set.remove(target);
    }
    return rs;
}

divftree::RetStatus Search(std::vector<divftree::ANNVectorInfo>& neighbours) {
    divftree::RetStatus rs;
    neighbours.clear();
    size_t idx = divftree::threadSelf->UniformRange64(0, total_num_queries - 1);
    rs = vector_index->ApproximateKNearestNeighbours(&search_query_vectors[idx * DIMENSION], K,
                                                    default_internal_search_span, default_leaf_search_span,
                                                    divftree::SortType::Unsorted, neighbours);
    if (!rs.IsOK()) {
        DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Error during search: %s", rs.Msg());
    }
    return rs;
}

/* todo: instead of this get a batch per thread and make reading the file atomic? */
void worker(divftree::Thread* self, size_t thread_idx) {
    self->InitDIVFThread();
    divftree::VTYPE buffer[BATCH_SIZE * DIMENSION];
    FILE* input_file_ptr = nullptr;
    OpenDataFile(input_file_ptr, false);
    size_t num_read = 0;
    size_t total_read = 0;

    while (!stop.load(std::memory_order_acquire)) {
        divftree::VectorID id;
        IDSet id_set;
        std::vector<divftree::ANNVectorInfo> neighbours;
        divftree::RetStatus rs;
        switch (current_task.load(std::memory_order_acquire))
        {
        case BUILD_INDEX:
            rs = Insert(input_file_ptr, num_read, total_read, buffer, id, id_set);
            break;
        case WARMUP:
            rs = Search(neighbours);
            break;
        case RUN:
            if(!self->UniformBinary(write_ratio)) {
                rs = Search(neighbours);
                if (rs.IsOK()) {
                    ++search_queries[thread_idx];
                } else {
                    ++search_errors[thread_idx];
                }
            } else if (!id_set.empty() && self->UniformBinary(delete_ratio)) {
                rs = Delete(id_set);
                if (rs.IsOK()) {
                    ++delete_queries[thread_idx];
                } else {
                    ++delete_errors[thread_idx];
                }
            } else {
                rs = Insert(input_file_ptr, num_read, total_read, buffer, id, id_set);
                if (rs.IsOK()) {
                    ++insert_queries[thread_idx];
                } else {
                    ++insert_errors[thread_idx];
                }
            }
            break;
        default:
            DIVFLOG(LOG_LEVEL_ERROR, LOG_TAG_TEST, "Unknown task!");
            break;
        }
    }

    CloseFile(input_file_ptr);
    self->DestroyDIVFThread();
}

int main() {
    ReadConfigs();
    ParseConfigs();
    FILE* file = nullptr;
    OpenDataFile(file, true);
    CloseFile(file);
    LoadQueryVectors();

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
    search_errors = new size_t[num_threads];
    insert_errors = new size_t[num_threads];
    delete_errors = new size_t[num_threads];
    memset(search_queries, 0, sizeof(size_t) * num_threads);
    memset(insert_queries, 0, sizeof(size_t) * num_threads);
    memset(delete_queries, 0, sizeof(size_t) * num_threads);
    memset(search_errors, 0, sizeof(size_t) * num_threads);
    memset(insert_errors, 0, sizeof(size_t) * num_threads);
    memset(delete_errors, 0, sizeof(size_t) * num_threads);

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
    size_t total_search_err = 0;
    size_t total_insert_err = 0;
    size_t total_delete_err = 0;
    for (size_t i = 0; i < num_threads; ++i) {
        total_search += search_queries[i];
        total_insert += insert_queries[i];
        total_delete += delete_queries[i];
        total_search_err += search_errors[i];
        total_insert_err += insert_errors[i];
        total_delete_err += delete_errors[i];
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

    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Total search errors: %lu", total_search_err);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Total insert errors: %lu", total_insert_err);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Total delete errors: %lu", total_delete_err);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Search EPS: %.2f", ((double)total_search_err) / run_time);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Insert EPS: %.2f", ((double)total_insert_err) / run_time);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Delete EPS: %.2f", ((double)total_delete_err) / run_time);
    DIVFLOG(LOG_LEVEL_LOG, LOG_TAG_TEST, "Total EPS: %.2f",
            ((double)(total_search_err + total_insert_err + total_delete_err)) / run_time);

    delete[] search_queries;
    delete[] insert_queries;
    delete[] delete_queries;
    delete[] search_errors;
    delete[] insert_errors;
    delete[] delete_errors;
    search_queries = nullptr;
    insert_queries = nullptr;
    delete_queries = nullptr;
    search_errors = nullptr;
    insert_errors = nullptr;
    delete_errors = nullptr;

    delete[] search_query_vectors;
    search_query_vectors = nullptr;
}