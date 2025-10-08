#ifndef BIGANN_CONFIGURATIONS_H_
#define BIGANN_CONFIGURATIONS_H_

#include <cstdint>

#ifdef VECTOR_TYPE
#undef VECTOR_TYPE
#endif
#ifdef VTYPE_FMT
#undef VTYPE_FMT
#endif
#ifdef VECTOR_TYPE
#undef VECTOR_TYPE
#endif
#ifdef DTYPE_FMT
#undef DTYPE_FMT
#endif

#define VECTOR_TYPE uint8_t
#define VTYPE_FMT "%hhu"
#define DISTANCE_TYPE uint32_t
#define DTYPE_FMT "%u"

#define DIMENSION ((uint16_t)128)
#define DISTANCE_ALG (divftree::DistanceType::L2)

#define DATA_PATH "bench/datasets/bigann/raw_data/BIGANN100M.u8bin"

#endif