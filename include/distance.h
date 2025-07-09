#ifndef DISTANCE_H_
#define DISTANCE_H_

#include "common.h"
#include "vector_utils.h"

#include <vector>

namespace copper {

class DIST_ID_PAIR_SIMILARITY_INTERFACE {
public:
    virtual bool operator()(const std::pair<VectorID, DTYPE>& a, const std::pair<VectorID, DTYPE>& b) const = 0;
};

namespace L2 {

inline static DTYPE Distance(const Vector& a, const Vector& b, uint16_t dim) {
    FatalAssert(a.IsValid(), LOG_TAG_BASIC, "a is invalid");
    FatalAssert(b.IsValid(), LOG_TAG_BASIC, "b is invalid");

    DTYPE dist = 0;
    for (size_t i = 0; i < dim; ++i) {
        dist += ((DTYPE)a[i] - (DTYPE)b[i]) * ((DTYPE)a[i] - (DTYPE)b[i]);
    }
    return dist;
}

inline static bool MoreSimilar(const DTYPE& a, const DTYPE& b) {
    return a < b;
}

inline static Vector ComputeCentroid(const VTYPE* vectors, size_t size, uint16_t dim) {
    FatalAssert(size > 0, LOG_TAG_BASIC, "size cannot be 0");
    FatalAssert(vectors != nullptr, LOG_TAG_BASIC, "size cannot be 0");
    Vector centroid(vectors, dim);
    for (size_t v = 1; v < size; ++v) {
        for (uint16_t e = 0; e < dim; ++e) {
            centroid[e] += vectors[v * dim + e];
        }
    }

    for (uint16_t e = 0; e < dim; ++e) {
        centroid[e] /= size;
    }

    return centroid;
}

/* todo: A better method(compared to polymorphism) to allow inlining for optimization */
struct DIST_ID_PAIR_SIMILARITY : public DIST_ID_PAIR_SIMILARITY_INTERFACE {
    inline bool operator()(const std::pair<VectorID, DTYPE>& a, const std::pair<VectorID, DTYPE>& b) const override {
        return MoreSimilar(a.second, b.second);
    }
};

struct DIST_ID_PAIR_REVERSE_SIMILARITY : public DIST_ID_PAIR_SIMILARITY_INTERFACE {
    inline bool operator()(const std::pair<VectorID, DTYPE>& a, const std::pair<VectorID, DTYPE>& b) const override {
        return !(MoreSimilar(a.second, b.second));
    }
};

};

};

#endif