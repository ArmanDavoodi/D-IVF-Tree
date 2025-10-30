#ifndef CONFIGURATIONS_H_
#define CONFIGURATIONS_H_

#define BIGANN100M 1
#define BIGANN1B 2
#define SMALLNORMAL 3
#define HUGENORMAL 4

#if defined(DATASET)
    #if ((DATASET == BIGANN100M) || (DATASET == BIGANN1B))
    #include "bench/datasets/bigann/configurations.h"
    #elif (DATASET == SMALLNORMAL)
    #include "bench/datasets/smallnormalsample/configurations.h"
    #elif (DATASET == HUGENORMAL)
    #include "bench/datasets/hugenormalsample/configurations.h"
    #else
    #error UNDEFINED DATASET!
    #endif
#else
    #error DATASET IS NOT DEFINED!
#endif

#endif