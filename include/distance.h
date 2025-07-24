#ifndef DISTANCE_H_
#define DISTANCE_H_

#include "common.h"
#include "vector_utils.h"

#include <type_traits>
#include <vector>

namespace divftree {

using VPairComparator = bool (*)(const std::pair<VectorID, DTYPE>&, const std::pair<VectorID, DTYPE>&);

namespace L2 {

inline constexpr DTYPE Distance(const Vector& a, const Vector& b, uint16_t dim) {
    FatalAssert(a.IsValid(), LOG_TAG_BASIC, "a is invalid");
    FatalAssert(b.IsValid(), LOG_TAG_BASIC, "b is invalid");

    DTYPE dist = 0;
    for (size_t i = 0; i < dim; ++i) {
        dist += ((DTYPE)a[i] - (DTYPE)b[i]) * ((DTYPE)a[i] - (DTYPE)b[i]);
    }
    return dist;
}

/* todo: A better method(compared to passing a pointer) to allow inlining for optimization */
inline constexpr bool MoreSimilar(const DTYPE& a, const DTYPE& b) {
    return a < b;
}

inline constexpr bool MoreSimilarVPair(const std::pair<VectorID, DTYPE>& a, const std::pair<VectorID, DTYPE>& b) {
    return MoreSimilar(a.second, b.second);
}

inline constexpr bool LessSimilarVPair(const std::pair<VectorID, DTYPE>& a, const std::pair<VectorID, DTYPE>& b) {
    return MoreSimilar(b.second, a.second);
}

inline Vector ComputeCentroid(const ClusterEntry* vectors, size_t size, uint16_t dim) {
    FatalAssert(size > 0, LOG_TAG_BASIC, "size cannot be 0");
    FatalAssert(vectors != nullptr, LOG_TAG_BASIC, "size cannot be 0");
    FatalAssert(vectors[0].IsVisible(), LOG_TAG_BASIC, "First vector is not visible!");
    Vector centroid(vectors[0].vector, dim);
    for (size_t v = 1; v < size; ++v) {
        FatalAssert(vectors[v].IsVisible(), LOG_TAG_BASIC, "Vector is not visible!");
        Vector current_vector(vectors[v].vector);
        for (uint16_t e = 0; e < dim; ++e) {
            centroid[e] += current_vector[e];
        }
    }

    for (uint16_t e = 0; e < dim; ++e) {
        centroid[e] /= size;
    }

    return centroid;
}

};

inline Vector ComputeCentroid(const ClusterEntry* vectors, size_t size, uint16_t dim, DistanceType distanceAlg) {
    switch (distanceAlg) {
    case DistanceType::L2Distance:
        return L2::ComputeCentroid(vectors, size, dim);
    default:
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_BASIC,
             "ComputeCentroid: Invalid distance type: %s", DISTANCE_TYPE_NAME[distanceAlg]);
    }
    return Vector(); // Return an empty vector if the distance type is invalid
}

inline constexpr DTYPE Distance(const Vector& a, const Vector& b, uint16_t dim, DistanceType distanceAlg) {
    switch (distanceAlg) {
    case DistanceType::L2Distance:
        return L2::Distance(a, b, dim);
    default:
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_BASIC,
             "Distance: Invalid distance type: %s", DISTANCE_TYPE_NAME[distanceAlg]);
    }
    return 0; // Return 0 if the distance type is invalid
}

inline constexpr bool MoreSimilar(const DTYPE& a, const DTYPE& b, DistanceType distanceAlg) {
    switch (distanceAlg) {
    case DistanceType::L2Distance:
        return L2::MoreSimilar(a, b);
    default:
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_BASIC,
             "MoreSimilar: Invalid distance type: %s", DISTANCE_TYPE_NAME[distanceAlg]);
    }
    return false; // Return false if the distance type is invalid
}

inline constexpr VPairComparator GetDistancePairSimilarityComparator(DistanceType distanceAlg, bool reverse) {
    switch (distanceAlg) {
    case DistanceType::L2Distance:
        return (reverse ? L2::MoreSimilarVPair : L2::LessSimilarVPair);
    default:
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_BASIC,
             "MoreSimilarVPair: Invalid distance type: %s", DISTANCE_TYPE_NAME[distanceAlg]);
    }
    return nullptr; // Return nullptr if the distance type is invalid
}

};

#endif