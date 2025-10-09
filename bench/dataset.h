#ifndef DATASET_H_
#define DATASET_H_

#include "benchmark.h"

inline uint32_t total_num_vectors = 0;
inline std::atomic<uint32_t> next_offset = 0;

/* todo: for every 10 writes, use the next vector for search ->
   not the best benchmark as it does not take the search skew in account */
inline constexpr uint32_t BATCH_SIZE = 1024 * 16;

void OpenDataFile(FILE* input_file_ptr, bool read_header = false) {
    if (input_file_ptr != nullptr) {
        fclose(input_file_ptr);
        input_file_ptr = nullptr;
    }
    input_file_ptr = fopen(DATA_PATH, "rb");
    if (input_file_ptr == nullptr) {
        throw std::runtime_error(
                divftree::String("Could not open the output data file! errno %d, errno msg: %s",
                                    errno, strerror(errno)).ToCStr());
    }

    uint32_t dummy_num_v = 0;
    uint32_t* num_vec = nullptr;
    if (read_header) {
        num_vec = &dummy_num_v;
    } else {
        num_vec = &total_num_vectors;
    }

    size_t ret_code = fread(num_vec, sizeof(uint32_t), 1, input_file_ptr);
    if (ret_code != 1) {
        if (feof(input_file_ptr))
            throw std::runtime_error("Error reading data file: unexpected end of file!");
        else if (ferror(input_file_ptr))
            throw std::runtime_error("Error reading data file: unexpected error!");
    }

    if (*num_vec == 0) {
        throw std::runtime_error("Error: num vectors cannot be 0!");
    }

    if (!read_header && (total_num_vectors != dummy_num_v)) {
        throw std::runtime_error("Error: total num vectors mismatch!");
    }

    uint32_t num_dimensions = 0;
    size_t ret_code = fread(&num_dimensions, sizeof(uint32_t), 1, input_file_ptr);
    if (ret_code != 1) {
        if (feof(input_file_ptr))
            throw std::runtime_error("Error reading data file: unexpected end of file!");
        else if (ferror(input_file_ptr))
            throw std::runtime_error("Error reading data file: unexpected error!");
    }

    if (num_dimensions != DIMENSION) {
        throw std::runtime_error("Error: dimension mismatch!");
    }
}

size_t ReadNextBatch(FILE* input_file_ptr, divftree::VTYPE* buffer) {
    if (input_file_ptr == nullptr) {
        throw std::runtime_error("Error: file is not open!");
    }

    if (buffer == nullptr) {
        throw std::runtime_error("Error: buffer is null!");
    }

    uint32_t offset = next_offset.fetch_add(BATCH_SIZE * DIMENSION * sizeof(divftree::VTYPE));

    if (offset >= total_num_vectors) {
        return 0;
    }

    size_t ret_code = fseek(input_file_ptr, sizeof(uint32_t) * 2 + offset, SEEK_SET);
    if (ret_code != 0) {
        throw std::runtime_error("Error seeking data file!");
    }

    ret_code = fread(buffer, sizeof(divftree::VTYPE) * DIMENSION, BATCH_SIZE, input_file_ptr);
    if (ret_code < BATCH_SIZE && offset < total_num_vectors) {
        if (feof(input_file_ptr))
            throw std::runtime_error("Error reading data file: unexpected end of file!");
        else if (ferror(input_file_ptr))
            throw std::runtime_error("Error reading data file: unexpected error!");
    }

    return ret_code;
}

void CloseDataFile(FILE* input_file_ptr) {
    if (input_file_ptr != nullptr) {
        fclose(input_file_ptr);
        input_file_ptr = nullptr;
    }
}

#endif