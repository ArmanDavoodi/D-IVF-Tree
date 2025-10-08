#ifndef CONFIGURATIONS_H_
#define CONFIGURATIONS_H_

#ifndef DATASET
#define DATASET BIGANN100M
#endif

#if DATASET == BIGANN100M
#include "datasets/bigann/configurations.h"
#else
    #error UNDEFINED DATASET!
#endif

#endif