#ifndef CONFIGURATIONS_H_
#define CONFIGURATIONS_H_

#define BIGANN100M 0
#define BIGANN1B 1
#define SMALLNORMAL 2

#if defined(DATASET)
    #if ((DATASET == BIGANN100M) || (DATASET == BIGANN1B))
    #include "bench/datasets/bigann/configurations.h"
    #elif (DATASET == SMALLNORMAL)
    #include "bench/datasets/smallnormalsample/configurations.h"
    #else
    #error UNDEFINED DATASET!
    #endif
#else
    #error DATASET IS NOT DEFINED!
#endif

#endif