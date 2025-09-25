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

inline void ComputeCentroid(const VTYPE* vectors1, size_t size1, const VTYPE* vectors2, size_t size2, uint16_t dim,
                            VTYPE* centroid) {
    FatalAssert(size1 > 0, LOG_TAG_BASIC, "size cannot be 0");
    CHECK_NOT_NULLPTR(vectors1, LOG_TAG_BASIC);
    CHECK_NOT_NULLPTR(centroid, LOG_TAG_BASIC);
    FatalAssert((vectors2 == nullptr) == (size2 == 0), LOG_TAG_BASIC, "mismatch for second batch of vectors");

    memcpy(centroid, vectors1, sizeof(VTYPE) * dim);
    /* todo: use AVX for this operation? */
    for (size_t v = 1; v < size1; ++v) {
        for (uint16_t e = 0; e < dim; ++e) {
            centroid[e] += vectors1[(v * dim) + e];
        }
    }

    for (size_t v = 0; v < size2; ++v) {
        for (uint16_t e = 0; e < dim; ++e) {
            centroid[e] += vectors2[(v * dim) + e];
        }
    }

    for (uint16_t e = 0; e < dim; ++e) {
        centroid[e] /= size1 + size2;
    }
}

inline void ComputeCentroid(const Cluster& cluster, uint16_t block_size, uint16_t cluster_cap, bool is_leaf,
                            const VTYPE* vectors, size_t size, uint16_t dim, VTYPE* centroid) {
    uint16_t cluster_size = cluster.header.visible_size.load(std::memory_order_acquire);
    FatalAssert(cluster_size > 0, LOG_TAG_BASIC, "cluster_size cannot be 0");
    CHECK_NOT_NULLPTR(centroid, LOG_TAG_BASIC);
    FatalAssert((vectors == nullptr) == (size == 0), LOG_TAG_BASIC, "mismatch for second batch of vectors");
    const uint16_t num_blocks = cluster.NumBlocks(block_size, cluster_cap);
    FatalAssert(num_blocks == 1, LOG_TAG_NOT_IMPLEMENTED, "currently not possible with more than 1 block!");
    UNUSED_VARIABLE(num_blocks);
    uint16_t real_size = 0;
    const VTYPE* data = cluster.Data(0, is_leaf, block_size, cluster_cap, dim);
    const void* meta = cluster.MetaData(0, is_leaf, block_size, cluster_cap, dim);
    /* todo: use AVX for this operation? */
    for (uint16_t offset = 0; offset < cluster_size; ++offset) {
        if (is_leaf) {
            VectorMetaData* vmt = static_cast<VectorMetaData*>(meta);
            if (vmt[offset].state.load(std::memory_order_acquire) != VECTOR_STATE_VALID) {
                continue;
            }
        } else {
            CentroidMetaData* vmt = static_cast<CentroidMetaData*>(meta);
            if (vmt[offset].state.load(std::memory_order_acquire) != VECTOR_STATE_VALID) {
                continue;
            }
        }

        if (real_size == 0) {
            memcpy(centroid, &data[offset * dim], sizeof(VTYPE) * dim);
        } else {
            for (uint16_t e = 0; e < dim; ++e) {
                centroid[e] += data[(offset * dim) + e];
            }
        }

        ++real_size;
    }

    size_t v = 0;
    if ((real_size == 0) && (size != 0)) {
        memcpy(centroid, vectors, sizeof(VTYPE) * dim);
        v = 1;
    }

    for (; v < size; ++v) {
        for (uint16_t e = 0; e < dim; ++e) {
            centroid[e] += vectors[(v * dim) + e];
        }
    }

    real_size += size;
    if (real_size == 0) {
        memset(centroid, 0, sizeof(VTYPE) * dim);
    } else {
        for (uint16_t e = 0; e < dim; ++e) {
            centroid[e] /= real_size;
        }
    }
}

};

inline void ComputeCentroid(const VTYPE* vectors1, size_t size1, const VTYPE* vectors2, size_t size2,
                              uint16_t dim, DistanceType distanceAlg, VTYPE* centroid) {
    switch (distanceAlg) {
    case DistanceType::L2Distance:
        return L2::ComputeCentroid(vectors1, size1, vectors2, size2, dim);
    default:
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_BASIC,
             "ComputeCentroid: Invalid distance type: %s", DISTANCE_TYPE_NAME[distanceAlg]);
    }
    return Vector(); // Return an empty vector if the distance type is invalid
}

/* cluster should be locked in S or X mode! This will only caount Valid vectors and it will use visible size not reserved size!*/
inline void ComputeCentroid(const Cluster& cluster, uint16_t block_size, uint16_t cluster_cap, bool is_leaf,
                            const VTYPE* vectors, size_t size, uint16_t dim,
                            DistanceType distanceAlg, VTYPE* centroid) {
    switch (distanceAlg) {
    case DistanceType::L2Distance:
        return L2::ComputeCentroid(cluster, block_size, cluster_cap, is_leaf, vectors, size, dim,
                                   distanceAlg, centroid);
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