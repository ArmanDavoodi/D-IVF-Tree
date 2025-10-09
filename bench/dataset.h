#ifndef DATASET_H_
#define DATASET_H_

#include "benchmark.h"

inline FILE * vector_input_file = nullptr;

inline uint32_t total_num_vectors = 0;
inline uint32_t num_dimensions = 0;
inline uint32_t num_read = 0;

/* todo: for every 10 writes, use the next vector for search ->
   not the best benchmark as it does not take the search skew in account */
inline constexpr uint32_t BATCH_SIZE = 1024 * 128;
inline std::atomic<uint32_t> current_batch_size = 0;
inline divftree::VTYPE vector_buffer[BATCH_SIZE * DIMENSION]; /* each time we read 128K vectors */

void OpenDataFile() {
    if (vector_input_file != nullptr) {
        fclose(vector_input_file);
        vector_input_file = nullptr;
    }
    vector_input_file = fopen(DATA_PATH, "rb");
    if (vector_input_file == nullptr) {
        throw std::runtime_error(
                divftree::String("Could not open the output data file! errno %d, errno msg: %s",
                                    errno, strerror(errno)).ToCStr());
    }

    size_t ret_code = fread(&total_num_vectors, sizeof(uint32_t), 1, vector_input_file);
    if (ret_code != 1) {
        if (feof(vector_input_file))
            throw std::runtime_error("Error reading data file: unexpected end of file!");
        else if (ferror(vector_input_file))
            throw std::runtime_error("Error reading data file: unexpected error!");
    }

    if (total_num_vectors == 0) {
        throw std::runtime_error("Error: num vectors cannot be 0!");
    }

    size_t ret_code = fread(&num_dimensions, sizeof(uint32_t), 1, vector_input_file);
    if (ret_code != 1) {
        if (feof(vector_input_file))
            throw std::runtime_error("Error reading data file: unexpected end of file!");
        else if (ferror(vector_input_file))
            throw std::runtime_error("Error reading data file: unexpected error!");
    }

    if (num_dimensions != DIMENSION) {
        throw std::runtime_error("Error: dimension mismatch!");
    }
}

/* todo: if num total vectors is not divisible by batch size, then the last batch will have duplicate elements */
bool ReadNextBatch() {
    if (vector_input_file == nullptr) {
        throw std::runtime_error("Error: file is not open!");
    }

    if (num_read >= total_num_vectors) {
        current_batch_size.store(0, std::memory_order_release);
        return false;
    }

    num_read = std::max(num_read + BATCH_SIZE, total_num_vectors);
    size_t ret_code = fread(vector_buffer, sizeof(divftree::VTYPE) * DIMENSION, BATCH_SIZE, vector_input_file);
    if (ret_code < BATCH_SIZE && num_read < total_num_vectors) {
        if (feof(vector_input_file))
            throw std::runtime_error("Error reading data file: unexpected end of file!");
        else if (ferror(vector_input_file))
            throw std::runtime_error("Error reading data file: unexpected error!");
    }

    current_batch_size.store(ret_code, std::memory_order_release);
    return true;
}

void CloseDataFile() {
    if (vector_input_file != nullptr) {
        fclose(vector_input_file);
        vector_input_file = nullptr;
    }
}

#endif