#ifndef SMALLNORMALSAMPLE_CONFIGURATIONS_H_
#define SMALLNORMALSAMPLE_CONFIGURATIONS_H_

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

#define DIMENSION ((uint16_t)3)
#define DISTANCE_ALG (divftree::DistanceType::L2)

#define EXCESS_LOGING
#define DATASET_NAME "SMALLNORMAL"
#define DATA_PATH "bench/datasets/smallnormalsample/raw_data/smallnormalsample128.u8bin"
#define QUERY_PATH "bench/datasets/smallnormalsample/raw_data/smallnormalQ32.u8bin"

#endif