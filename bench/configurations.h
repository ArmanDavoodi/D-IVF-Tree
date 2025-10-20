#ifndef CONFIGURATIONS_H_
#define CONFIGURATIONS_H_

#if defined(DATASET) && ((DATASET == BIGANN100M) || (DATASET == BIGANN1B))
#include "bench/datasets/bigann/configurations.h"
#else
    #error UNDEFINED DATASET!
#endif

#endif