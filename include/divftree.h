#ifndef DIVFTREE_H_
#define DIVFTREE_H_

#include "common.h"
#include "vector_utils.h"
#include "buffer.h"
#include "distance.h"
#include "distributed_common.h"

#include "utils/synchronization.h"

#include "interface/divftree.h"

#include <algorithm>
#include <atomic>

// Todo: better logs and asserts -> style: <Function name>(self data(a=?), input data): msg, additonal variables if needed

/*
 * Todo: There are two approaches we can take:
 * 1) Bottom-up nearest approach:
 *      This is our current approach in which we try to gurantee(lazily) that each vector is always in its closest vertex.
 *      However, this approach requires us to either have migration per insert or
 *      to check every possible leaf to see which one is closest.
 *      Imagin this example with 1 dim vectors:
 *      Layer 2:        1          2
 *      Layer 1:    0.9   1.1  1.6   2.6
 *      Layer 0:   ........................
 *      Now if we want to insert 1.4, it is closer to 1 so it will be inserted in leaf with centroid 1.1 while the closest
 *      leaf centroid to it is 1.6.
 *      probably better to have larger buckets.
 * 2) top-down nearest approach:
 *      In this approach, I gurantee that we can always take the greedy path from the root of the tree to
 *      find a vector.
 *      To do so, at the time of split of vertex k, we have to check migrataion for every vector in any layer in that subtree
 *      to other sibling vertices of vertex k.(sibling vertices are vertices with the same parent)
 *      As a result, a split in higher levels is more costly. and the cost only grows. lazy approach will cause us to have
 *      bad accuracy for a long time.
 *      probably having small buckets is better here.
 *
 */

namespace divftree {

class DIVFTreeVertex : public DIVFTreeVertexInterface {
public:
    DIVFTreeVertex() = delete;
    DIVFTreeVertex(DIVFTreeVertex&) = delete;
    DIVFTreeVertex(const DIVFTreeVertex&) = delete;
    DIVFTreeVertex(DIVFTreeVertex&&) = delete;

    DIVFTreeVertex(const DIVFTreeVertexAttributes& attributes) : attr(attributes), unpinCount{0},
                                                                 cluster(attributes.centroid_id.IsLeaf(),
                                                                         attributes.block_size,
                                                                         attributes.cap,
                                                                         attributes.index->GetAttributes().dimension,
                                                                         true) {
        CHECK_MIN_MAX_SIZE(attr.min_size, attr.cap, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_VECTORID_IS_VALID(attr.centroid_id, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_VECTORID_IS_CENTROID(attr.centroid_id, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_NOT_NULLPTR(attr.index, LOG_TAG_DIVFTREE_VERTEX);
        FatalAssert(attr.block_size > 0, LOG_TAG_DIVFTREE_VERTEX, "Block size cannot be 0!");
        FatalAssert(attr.version == 0, LOG_TAG_DIVFTREE_VERTEX, "The first version should be 0!");
    }

    DIVFTreeVertex(const DIVFTreeAttributes& attributes, VectorID id, DIVFTreeInterface* index) :
        attr(id, 0,
             id.IsLeaf() ? attributes.leaf_min_size : attributes.internal_min_size,
             id.IsLeaf() ? attributes.leaf_max_size: attributes.internal_max_size,
             id.IsLeaf() ? attributes.leaf_blck_size: attributes.internal_blck_size, index),
             unpinCount{0}, cluster(id.IsLeaf(),
                                    id.IsLeaf() ? attributes.leaf_blck_size: attributes.internal_blck_size,
                                    id.IsLeaf() ? attributes.leaf_max_size: attributes.internal_max_size,
                                    attributes.dimension, true) {
        CHECK_MIN_MAX_SIZE(attr.min_size, attr.cap, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_VECTORID_IS_VALID(attr.centroid_id, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_VECTORID_IS_CENTROID(attr.centroid_id, LOG_TAG_DIVFTREE_VERTEX);
        CHECK_NOT_NULLPTR(attr.index, LOG_TAG_DIVFTREE_VERTEX);
        FatalAssert(attr.block_size > 0, LOG_TAG_DIVFTREE_VERTEX, "Block size cannot be 0!");
        FatalAssert(attr.version == 0, LOG_TAG_DIVFTREE_VERTEX, "The first version should be 0!");
    }

    ~DIVFTreeVertex() override {
        FatalAssert(unpinCount.load(std::memory_order_relaxed) == 0, LOG_TAG_DIVFTREE_VERTEX,
                    "the vertex is still pinned!");
        if (attr.centroid_id.IsLeaf()) {
            return;
        }

        uint16_t size = cluster.header.reserved_size.load(std::memory_order_relaxed);
        FatalAssert(size == cluster.header.visible_size.load(std::memory_order_relaxed), LOG_TAG_DIVFTREE_VERTEX,
                    "mismatch between visited and reserved!");
        const uint16_t dim = attr.index->GetAttributes().dimension;
        CentroidMetaData* vmd =
            reinterpret_cast<CentroidMetaData*>(cluster.MetaData(0, attr.centroid_id.IsLeaf(),
                                                                 attr.block_size, attr.cap, dim));
        FatalAssert(cluster.NumBlocks(attr.block_size, attr.cap) == 1, LOG_TAG_NOT_IMPLEMENTED,
                    "cannot support more than one block!");
        for (uint16_t i = 0; i < size; ++i) {
            VectorState state = vmd[i].state.load(std::memory_order_relaxed);
            if (state == VECTOR_STATE_VALID || state == VECTOR_STATE_MIGRATED || state == VECTOR_STATE_OUTDATED) {
                FatalAssert(vmd[i].id != INVALID_VECTOR_ID, LOG_TAG_DIVFTREE_VERTEX, "cannot unpin invalid vector");
                BufferManager::GetInstance()->UnpinVertexVersion(vmd[i].id, vmd[i].version);
            }
        }
    }

    inline void Unpin() override {
        if (unpinCount.fetch_add(1) == UINT64_MAX) {
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Unpin and delete vertex " VECTORID_LOG_FMT,
                 VECTORID_LOG(attr.centroid_id));
            /* todo: we need to handle all the children(unpining their versions and stuff) in the destructor */
            this->~DIVFTreeVertex();
            BufferManager::GetInstance()->Recycle(this);
            return;
        }
        else {
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Unpin vertex " VECTORID_LOG_FMT,
                 VECTORID_LOG(attr.centroid_id));
        }
    }

    inline void MarkForRecycle(uint64_t pinCount) override {
        FatalAssert(pinCount >= unpinCount.load(), LOG_TAG_DIVFTREE_VERTEX,
                    "The cluster was unpinned more times than it was pinned. pinCount=%lu, unpinCount=%lu",
                    pinCount, unpinCount.load());
        uint64_t newPin = unpinCount.fetch_sub(pinCount) - pinCount;
        CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Mark vertex " VECTORID_LOG_FMT
            " for recycle with pin count %lu", VECTORID_LOG(attr.centroid_id), pinCount);
        if (newPin == 0) {
            CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE_VERTEX, "Delete vertex " VECTORID_LOG_FMT,
                 VECTORID_LOG(attr.centroid_id));
            /* todo: we need to handle all the children(unpining their versions and stuff) in the destructor */
            this->~DIVFTreeVertex();
            BufferManager::GetInstance()->Recycle(this);
            return;
        }
    }

    /* todo: make sure the vertex is locked in shared/exclusive mode when calling this function! */
    RetStatus BatchInsert(const ConstVectorBatch& batch, uint16_t marked_for_update = INVALID_OFFSET) override {
        // CHECK_VERTEX_SELF_IS_VALID(LOG_TAG_DIVFTREE_VERTEX, false);
        FatalAssert(batch.size > 0, LOG_TAG_DIVFTREE_VERTEX,
                    "Batch size must be greater than zero.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                    "Batch data must not be null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                    "Batch id must not be null.");
        uint16_t offset = cluster.header.reserved_size.fetch_add(batch.size);
        FatalAssert(offset < cluster.header.reserved_size.load(), LOG_TAG_DIVFTREE_VERTEX, "Overflow detected!");
        if (offset + batch.size > attr.cap) {
            cluster.header.reserved_size.fetch_sub(batch.size);
            return RetStatus{.stat = RetStatus::VERTEX_NOT_ENOUGH_SPACE, .message=nullptr};
        }
        BufferManager* bufferMgr = BufferManager::GetInstance();
        FatalAssert(bufferMgr != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                    "BufferManager is not initialized.");
        const uint16_t dim = attr.index->GetAttributes().dimension;
        VTYPE* dest = cluster.Data(offset, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        memcpy(dest, batch.data, batch.size * dim * sizeof(VTYPE));
        Address meta = cluster.MetaData(offset, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        for (uint16_t i = 0; i < batch.size; ++i) {
            if (attr.centroid_id.IsLeaf()) {
                VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(meta);
                vmd[i].id = batch.id[i];
                FatalAssert(vmd[i].state.load(std::memory_order_relaxed) == VECTOR_STATE_INVALID,
                            LOG_TAG_DIVFTREE_VERTEX, "Reserved space at offset %hu is not in INVALID state.",
                            offset + i);
                vmd[i].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            } else {
                FatalAssert(batch.version != nullptr, LOG_TAG_DIVFTREE_VERTEX,
                            "Batch version must not be null for centroid insertion.");
                CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(meta);
                vmd[i].id = batch.id[i];
                bufferMgr->PinVertexVersion(batch.id[i], batch.version[i]);
                vmd[i].version = batch.version[i];
                FatalAssert(vmd[i].state.load(std::memory_order_relaxed) == VECTOR_STATE_INVALID,
                            LOG_TAG_DIVFTREE_VERTEX, "Reserved space at offset %hu is not in INVALID state.",
                            offset + i);
                vmd[i].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            }
        }

        cluster.header.visible_size.fetch_add(batch.size);

        for (uint16_t i = 0; i < batch.size; ++i) {
            bufferMgr->
                UpdateVectorLocation(batch.id[i], VectorLocation(attr.centroid_id, attr.version, offset + i));
        }

        if (marked_for_update != INVALID_OFFSET) {
            FatalAssert(marked_for_update < offset, LOG_TAG_DIVFTREE_VERTEX,
                        "Marked for update offset %hu is not less than the start of newly inserted batch %hu.",
                        marked_for_update, offset);
            FatalAssert(attr.centroid_id.IsInternalVertex(), LOG_TAG_DIVFTREE_VERTEX,
                        "Marked for update offset can only be set for internal vertices.");
            CentroidMetaData* markedMeta = reinterpret_cast<CentroidMetaData*>(
                cluster.MetaData(marked_for_update, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim));
            markedMeta->state.store(VECTOR_STATE_OUTDATED);
            cluster.header.num_deleted.fetch_add(1);
            /*
             * we cannot unpin the outdated version here because if an ANN search has read this version but
             * has not pinned it, and the time it was not outdated,
             * the version will be lost.
             */
            // bufferMgr->UnpinVertexVersion(markedMeta->id, markedMeta->version);
        }

        return RetStatus::Success();
    }

    /*
     * if id.level is vertex/cluster and targetState is Invalid then the cluster should be locked in exclusive mode
     * otherwise if id.level is vertex/cluster then target should be locked in shared mode
     */
    inline RetStatus ChangeVectorState(VectorID target, uint16_t targetOffset,
                                       VectorState& expectedState, VectorState finalState,
                                       Version* version = nullptr) override {
        CHECK_VECTORID_IS_VALID(target, LOG_TAG_DIVFTREE_VERTEX);
        FatalAssert(target._level + 1 == attr.centroid_id._level, LOG_TAG_DIVFTREE_VERTEX, "level mismatch");
        FatalAssert(expectedState == VECTOR_STATE_VALID || expectedState == VECTOR_STATE_MIGRATED,
                    LOG_TAG_DIVFTREE_VERTEX, "expected state can be either VALID or MIGRATED");
        FatalAssert(finalState == VECTOR_STATE_VALID || finalState == VECTOR_STATE_MIGRATED ||
                    finalState == VECTOR_STATE_INVALID, LOG_TAG_DIVFTREE_VERTEX,
                    "final state can only be valid, invalid or migrated!");
        FatalAssert((expectedState == VECTOR_STATE_MIGRATED) == (finalState == VECTOR_STATE_VALID),
                    LOG_TAG_DIVFTREE_VERTEX, "excpected state is migrated is migrated if and only "
                    "if final state is valid. this can only happen when a migration fails and "
                    "we want to revert our state back to valid");
        uint16_t size = cluster.header.visible_size.load(std::memory_order_acquire);
        FatalAssert(targetOffset < size, LOG_TAG_DIVFTREE_VERTEX, "offset out of range!");

        Address meta = cluster.MetaData(targetOffset, attr.centroid_id.IsLeaf(),
                                        attr.block_size, attr.cap, attr.index->GetAttributes().dimension);
        VectorID* id = nullptr;
        std::atomic<VectorState>* state = nullptr;
        if (attr.centroid_id.IsLeaf()) {
            VectorMetaData* vmd = static_cast<VectorMetaData*>(meta);
            id = &(vmd->id);
            state = &(vmd->state);
        } else {
            CentroidMetaData* vmd = static_cast<CentroidMetaData*>(meta);
            id = &(vmd->id);
            if (version != nullptr) {
                *version = vmd->version;
            }
            state = &(vmd->state);
        }

        FatalAssert(*id == target, LOG_TAG_DIVFTREE_VERTEX, "id mismatch!");
        if (expectedState == VECTOR_STATE_MIGRATED) {
            FatalAssert((*state).load(std::memory_order_acquire) == VECTOR_STATE_MIGRATED, LOG_TAG_DIVFTREE_VERTEX,
                        "mismatch state!");
            state->store(finalState, std::memory_order_release);
            return RetStatus::Success();
        }

        if (target.IsCentroid()) {
            if (finalState == VECTOR_STATE_INVALID) {
                /* deletion */
                /* todo: check target should be locked in exclusive mode! */
                FatalAssert((*state).load(std::memory_order_acquire) == VECTOR_STATE_VALID, LOG_TAG_DIVFTREE_VERTEX,
                            "mismatch state!");
                state->store(finalState, std::memory_order_release);
                return RetStatus::Success();
            } else {
                /* todo: check target should be locked in shared mode! */
                FatalAssert((*state).load(std::memory_order_acquire) != VECTOR_STATE_INVALID, LOG_TAG_DIVFTREE_VERTEX,
                            "mismatch state!");
            }
        }

        return (state->compare_exchange_strong(expectedState, finalState) ?
                RetStatus::Success() :
                RetStatus{.stat=RetStatus::FAILED_TO_CAS_VECTOR_STATE, .message=nullptr});
    }

    void Search(const VTYPE* query, size_t k, std::vector<ANNVectorInfo>& neighbours,
                     std::unordered_set<std::pair<VectorID, Version>, VectorIDVersionPairHash>& seen) override {
        /* todo: should I also ignore same vectors with different versions? e.g. if it is newer replace o.w ignore */
        const uint16_t dim = attr.index->GetAttributes().dimension;
        const DistanceType dtype = attr.index->GetAttributes().distanceAlg;
        const SimilarityComparator cmp = attr.index->GetAttributes().reverseSimilarityComparator;
        VTYPE* data = cluster.Data(0, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        Address meta = cluster.MetaData(0, attr.centroid_id.IsLeaf(), attr.block_size, attr.cap, dim);
        uint16_t old_size = 0;
        uint16_t curr_size = cluster.header.visible_size.load(std::memory_order_acquire);
        std::unordered_map<VectorID, uint16_t, VectorIDHash> in_list;
        std::unordered_set<VectorID, VectorIDHash> in_cluster;
        while(curr_size != old_size) {
            /* this will overflow and i will get greater than size when it gets to 0 */
            uint16_t last_seen = old_size - 1;
            for (uint16_t i = curr_size - 1; i != last_seen; --i) {
                ANNVectorInfo new_vector;
                if (attr.centroid_id.IsLeaf()) {
                    VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(meta);
                    if (seen.find(std::make_pair(vmd[i].id, 0)) != seen.end()) {
                        continue;
                    }

                    switch (vmd[i].state.load(std::memory_order_acquire)) {
                    case VECTOR_STATE_VALID:
                    case VECTOR_STATE_MIGRATED:
                        new_vector = ANNVectorInfo(Distance(query, &data[i * dim], dim, dtype), vmd[i].id);
                        neighbours.emplace_back(new_vector);
                        seen.emplace(new_vector.id, new_vector.version);
                        std::push_heap(neighbours.begin(), neighbours.end(), cmp);
                        while (neighbours.size() > k) {
                            std::pop_heap(neighbours.begin(), neighbours.end(), cmp);
                            neighbours.pop_back();
                        }
                        break;
                    case VECTOR_STATE_OUTDATED:
                        FatalAssert(false, LOG_TAG_DIVFTREE_VERTEX, "a pure vector cannot become outdated!");
                    default:
                        continue;
                    }
                } else {
                    CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(meta);
                    if (seen.find(std::make_pair(vmd[i].id, vmd[i].version)) != seen.end()) {
                        continue;
                    }

                    std::unordered_map<divftree::VectorID, uint16_t, divftree::VectorIDHash>::iterator emplace_res;
                    switch (vmd[i].state.load(std::memory_order_acquire)) {
                    case VECTOR_STATE_VALID:
                    case VECTOR_STATE_MIGRATED:
                        auto check = in_list.find(vmd[i].id);
                        if (check != in_list.end()) { /* todo: we can handle this case much more efficiently! */
                            /* a vector was outdated in a previous pass */
                            FatalAssert((vmd[check->second].id == vmd[i].id) &&
                                        (vmd[check->second].state.load(std::memory_order_acquire) ==
                                        VECTOR_STATE_OUTDATED) && (vmd[check->second].version < vmd[i].version),
                                        LOG_TAG_DIVFTREE_VERTEX,
                                        "the one that we have seen before should be outdated!");
                            size_t j = 0;
                            for (; j < neighbours.size(); ++j) {
                                if (neighbours[j].id == vmd[i].id) {
                                    break;
                                }
                            }
                            FatalAssert((j < neighbours.size()) &&
                                        neighbours[j].version ==
                                        vmd[check->second].state.load(std::memory_order_acquire),
                                        LOG_TAG_DIVFTREE_VERTEX, "Could not find the outdated version!");
                            neighbours[j] = neighbours.back();
                            neighbours.pop_back();
                            std::make_heap(neighbours.begin(), neighbours.end());
                            check->second = i;
                            emplace_res = check;
                        } else {
                            emplace_res = in_list.emplace(new_vector.id, i).first;
                        }
                        new_vector = ANNVectorInfo(Distance(query, &data[i * dim], dim, dtype), vmd[i].id);
                        neighbours.emplace_back(new_vector);
                        in_cluster.emplace(new_vector.id);
                        seen.emplace(new_vector.id, new_vector.version);
                        std::push_heap(neighbours.begin(), neighbours.end(), cmp);
                        while (neighbours.size() > k) {
                            std::pop_heap(neighbours.begin(), neighbours.end(), cmp);
                            if (new_vector == neighbours.back()) {
                                in_list.erase(emplace_res);
                            } else {
                                auto it = in_list.find(new_vector.id);
                                if (it != in_list.end()) {
                                    in_list.erase(it);
                                }
                            }
                            neighbours.pop_back();
                        }
                        break;
                    case VECTOR_STATE_OUTDATED:
                        if (in_cluster.find(vmd[i].id) != in_cluster.end()) {
                            old_size = curr_size;
                        }
                    default:
                        continue;
                    }
                }
            }

            if (old_size == curr_size) {
                curr_size = cluster.header.visible_size.load(std::memory_order_acquire);
                FatalAssert(curr_size > old_size, LOG_TAG_DIVFTREE_VERTEX,
                            "if we have seen an outdated vector our size should have increased!");
            } else {
                old_size = curr_size;
            }
        }
    }

    // String ToString(bool detailed = false) const override {
    //     return String("{clustering algorithm=%s, distance=%s, centroid_copy=%s, LockState=%s, Cluster=",
    //                   CLUSTERING_TYPE_NAME[clusteringAlg], DISTANCE_TYPE_NAME[_distanceAlg],
    //                   _centroid_copy.ToString(cluster._dim).ToCStr(), lock.ToString().ToCStr()) +
    //            cluster.ToString(detailed) + String("}");
    // }


protected:
    const DIVFTreeVertexAttributes attr;
    std::atomic<uint64_t> unpinCount;
    alignas(CACHE_LINE_SIZE) Cluster cluster;

TESTABLE;
friend class DIVFTree;
};

class DIVFTree : public DIVFTreeInterface {
public:
    DIVFTree(DIVFTreeAttributes attributes) : attr(attributes), real_size(0) {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Create DIVFTree Index Start");
        VectorID root_id;
        BufferVertexEntry* root_entry =
            BufferManager::Init(sizeof(DIVFTreeVertex) - sizeof(ClusterHeader),
                                attr.leaf_max_size, attr.internal_max_size, attr.dimension);
        CHECK_NOT_NULLPTR(root_entry, LOG_TAG_DIVFTREE);
        (void)(new (root_entry->ReadLatestVersion(false)) DIVFTreeVertex(attr, root_entry->centroidMeta.selfId, this));
        BufferManager::GetInstance()->
            ReleaseBufferEntryIfNotNull(root_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});
        CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Create DIVFTree Index End");
    }

    ~DIVFTree() override {
        CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Shutdown DIVFTree Index Start");
        BufferManager::GetInstance()->Shutdown();
        CLOG(LOG_LEVEL_LOG, LOG_TAG_DIVFTREE, "Shutdown DIVFTree Index End");
    }

    /* todo: for now since it is single node, the create_completion_notification here does not do anything and returning
       from this function indicates that the vector is inserted. But we need to change
       this in the multi node environment */
    RetStatus Insert(const VTYPE* vec, VectorID& vec_id, uint8_t search_span,
                     bool create_completion_notification = false) override {
        CHECK_NOT_NULLPTR(vec, LOG_TAG_DIVFTREE);
        UNUSED_VARIABLE(create_completion_notification);
        FatalAssert(search_span > 0, LOG_TAG_DIVFTREE, "Number of neighbours cannot be 0.");
        RetStatus rs = RetStatus::Success();

        // CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
        //      "Insert BEGIN: Vector=%s, _size=%lu, _levels=%hhu",
        //      vec.ToString(core_attr.dimension).ToCStr(), _size, _levels);

        RetStatus rs = RetStatus::Success();
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        BufferVectorEntry* vector_entry = nullptr;
        bufferMgr->BatchCreateVectorEntry(1, &vector_entry, false);
        CHECK_NOT_NULLPTR(vector_entry, LOG_TAG_DIVFTREE);
        vec_id = vector_entry->selfId;
        CHECK_VECTORID_IS_VALID(vec_id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VECTOR(vec_id, LOG_TAG_DIVFTREE);
        const ConstVectorBatch batch(vec, &vec_id, nullptr, 1);

        std::vector<std::vector<ANNVectorInfo>&> layers;
        VectorID target_leaf;
        Version target_version;
        BufferVertexEntry* leaf_entry = nullptr;

        do {
            while (true) {
                DIVFTreeVertex* root =
                    static_cast<DIVFTreeVertex*>(bufferMgr->ReadAndPinRoot());
                CHECK_NOT_NULLPTR(root, LOG_TAG_DIVFTREE);

                if (root->attr.centroid_id.IsLeaf()) {
                    /* need to split the root */
                    target_leaf = root->attr.centroid_id;
                    target_version = root->attr.version;
                    root->Unpin();
                    break;
                }

                layers.reserve(root->attr.centroid_id._level + 1);
                for (uint64_t i = 0; i < root->attr.centroid_id._level; ++i) {
                    layers.emplace_back(*(new std::vector<ANNVectorInfo>));
                }
                layers.emplace_back(nullptr); /* this layer represents vectors and we do not need them */

                rs = ANNSearch(vec, 1, search_span, 1,
                            (uint8_t)(root->attr.centroid_id._level), (uint8_t)VectorID::LEAF_LEVEL,
                            SortType::DecreasingSimilarity, layers, root);
                root->Unpin();

                if (rs.IsOK()) {
                    FatalAssert(layers[VectorID::LEAF_LEVEL].size() == 1, LOG_TAG_DIVFTREE,
                                "if rs is OK we should have found the leaf!");
                    VectorID leaf_id = layers[VectorID::LEAF_LEVEL][0].id;
                    Version leaf_version = layers[VectorID::LEAF_LEVEL][0].version;
                }

                for (uint64_t i = 0; i < layers.size() - 1; ++i) {
                    std::vector<ANNVectorInfo>* vec = &layers[i];
                    delete vec;
                }
                layers.clear();

                if (rs.IsOK()) {
                    break;
                }
                /* todo: check how many retries! */
            }

            leaf_entry = bufferMgr->ReadBufferEntry(target_leaf, SX_SHARED);
            if (leaf_entry == nullptr || leaf_entry->currentVersion != target_version) {
                /* todo: if there is a difference in version we may be able to solve it using update logs! */
                /* todo: check how many retries! */
                bufferMgr->ReleaseBufferEntryIfNotNull(leaf_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                continue;
            }
        } while(!BatchInsertInto(leaf_entry, batch, true).IsOK());

        return rs;
    }

    // BatchInsertInto(BufferVertexEntry* container_entry, const ConstVectorBatch& batch, bool releaseEntry,
    //                           uint16_t marked_for_update = INVALID_OFFSET

    /* todo: for now since it is single node, the create_completion_notification here does not do anything and returning
       from this function indicates that the vector is deleted. But we need to change
       this in the multi node environment */
    RetStatus Delete(VectorID vec_id, bool create_completion_notification = false) override {
        UNUSED_VARIABLE(create_completion_notification);
        CHECK_VECTORID_IS_VALID(vec_id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VECTOR(vec_id, LOG_TAG_DIVFTREE);
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        BufferVectorEntry* entry = bufferMgr->GetVectorEntry(vec_id);
        if (entry == nullptr) {
            return RetStatus{.stat=RetStatus::VECTOR_NOT_FOUND, .message="Vector does not exist in this index"};
        }
        VectorLocation loc;
        BufferVertexEntry* parent = nullptr;
        do {
            parent = entry->ReadParent(loc);
            if (parent == nullptr) {
                return RetStatus{.stat=RetStatus::VECTOR_NOT_FOUND,
                                .message="Vector is either deleted or is not yet inserted."};
            }
        } while(!DeleteVectorAndReleaseContainer(vec_id, parent, loc).IsOK());

        return RetStatus::Success();
    }

    RetStatus ApproximateKNearestNeighbours(const VTYPE* query, size_t k,
                                            uint8_t internal_node_search_span, uint8_t leaf_node_search_span,
                                            SortType sort_type, std::vector<ANNVectorInfo>& neighbours) override {

        CHECK_NOT_NULLPTR(query, LOG_TAG_DIVFTREE);

        FatalAssert(k > 0, LOG_TAG_DIVFTREE, "Number of neighbours cannot be 0.");
        FatalAssert(internal_node_search_span > 0, LOG_TAG_DIVFTREE,
                    "Number of internal vertex neighbours cannot be 0.");
        FatalAssert(leaf_node_search_span > 0, LOG_TAG_DIVFTREE, "Number of leaf neighbours cannot be 0.");

        neighbours.clear();
        neighbours.reserve(k + 1);

        // CLOG(LOG_LEVEL_DEBUG, LOG_TAG_DIVFTREE,
        //      "ApproximateKNearestNeighbours BEGIN: query=%s, k=%lu, _internal_k=%hu, _leaf_k=%hu, index_size=%lu, "
        //      "num_levels=%hhu", query.ToString(core_attr.dimension).ToCStr(), k, _internal_k,
        //      _leaf_k, _size);

        RetStatus rs = RetStatus::Success();

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        std::vector<std::vector<ANNVectorInfo>&> layers;

        while (real_size.load(std::memory_order_acquire) > 0) {
            DIVFTreeVertex* root =
                static_cast<DIVFTreeVertex*>(bufferMgr->ReadAndPinRoot());
            CHECK_NOT_NULLPTR(root, LOG_TAG_DIVFTREE);

            layers.reserve(root->attr.centroid_id._level + 1);
            for (uint64_t i = 0; i < root->attr.centroid_id._level; ++i) {
                layers.emplace_back(*(new std::vector<ANNVectorInfo>));
            }
            layers.emplace_back(neighbours);

            rs = ANNSearch(query, k, internal_node_search_span, leaf_node_search_span,
                           (uint8_t)(root->attr.centroid_id._level), (uint8_t)VectorID::VECTOR_LEVEL, sort_type,
                           layers, root);
            root->Unpin();

            for (uint64_t i = 0; i < layers.size() - 1; ++i) {
                std::vector<ANNVectorInfo>* vec = &layers[i];
                delete vec;
            }
            layers.clear();

            if (rs.IsOK()) {
                break;
            }
        }

        return rs;
    }

    size_t Size() const override {
        return real_size.load(std::memory_order_acquire);
    }

    const DIVFTreeAttributes& GetAttributes() const override {
        return attr;
    }

    // inline String ToString() override {
    //     String out("{Attr=<Dim=%hu, ClusteringAlg=%s, DistanceAlg=%s, "
    //                "LeafMinSize=%hu, LeafMaxSize=%hu, InternalMinSize=%hu, InternalMaxSize=%hu, "
    //                "SplitInternal=%hu, SplitLeaf=%hu, Size=%lu, Levels=%lu>, RootID=" VECTORID_LOG_FMT
    //                ", Vertexs=[", core_attr.dimension, CLUSTERING_TYPE_NAME[core_attr.clusteringAlg],
    //                DISTANCE_TYPE_NAME[core_attr.distanceAlg], leaf_min_size, leaf_max_size,
    //                internal_min_size, internal_max_size, split_internal, split_leaf,
    //                _size, _levels, VECTORID_LOG(_root));
    //     std::vector<VectorID> curr_level_stack, next_level_stack;
    //     size_t seen = 0;
    //     if (_root.IsValid()) {
    //         curr_level_stack.push_back(_root);
    //     }
    //     bool go_to_next_level = true;
    //     uint64_t curr_level = 0;
    //     while (!curr_level_stack.empty()) {
    //         VectorID curr_vertexId = curr_level_stack.back();
    //         curr_level_stack.pop_back();
    //         CHECK_VECTORID_IS_VALID(curr_vertexId, LOG_TAG_DIVFTREE);
    //         if (go_to_next_level) {
    //             curr_level = curr_vertexId._level;
    //             out += String("Level %lu:{Size=%lu, Vertexs=[", curr_level, curr_level_stack.size() + 1);
    //             go_to_next_level = false;
    //         }
    //         else {
    //             FatalAssert(curr_vertexId._level == curr_level, LOG_TAG_DIVFTREE,
    //                         "Current vertex level (%hhu) does not match expected level (%lu).",
    //                         curr_vertexId._level, curr_level);
    //         }
    //         DIVFTreeVertex* vertex = static_cast<DIVFTreeVertex*>(_bufmgr.GetVertex(curr_vertexId));
    //         CHECK_VERTEX_IS_VALID(vertex, LOG_TAG_DIVFTREE, false);
    //         out += String("<VertexID=" VECTORID_LOG_FMT ", Bucket=%s>",
    //                     VECTORID_LOG(curr_vertexId), vertex->BucketToString().ToCStr());
    //         if (!vertex->IsLeaf()) {
    //             for (uint16_t i = 0; i < vertex->Size(); ++i) {
    //                 const VectorPair& vectorPair = vertex->cluster[i];
    //                 CHECK_VECTORID_IS_VALID(vectorPair.id, LOG_TAG_DIVFTREE);
    //                 FatalAssert(vectorPair.id._level == curr_vertexId._level - 1, LOG_TAG_DIVFTREE,
    //                             "VectorID level (%hhu) does not match current vertex level (%hhu).",
    //                             vectorPair.id._level, curr_vertexId._level - 1);
    //                 FatalAssert(vectorPair.vec.IsValid(), LOG_TAG_DIVFTREE,
    //                             "Invalid vector in vertex %s at index %hu.", VECTORID_LOG(curr_vertexId), i);
    //                 next_level_stack.emplace_back(vectorPair.id);
    //             }
    //         }
    //         else {
    //             seen += vertex->Size();
    //         }
    //         if (curr_level_stack.empty()) {
    //             out += String("]}");
    //             go_to_next_level = true;
    //             next_level_stack.swap(curr_level_stack);
    //             if (!curr_level_stack.empty()) {
    //                 out += String(", ");
    //             }
    //             else {
    //                 out += String("]}");
    //             }
    //         }
    //         else {
    //             out += String(", ");
    //         }
    //     }
    //     FatalAssert(seen == _size, LOG_TAG_DIVFTREE,
    //                 "Seen vertices (%lu) does not match expected size (%lu).", seen, _size);
    //     return out;
    // }

protected:
    const DIVFTreeAttributes attr;
    std::atomic<uint64_t> real_size;

    RetStatus CompactAndInsert(BufferVertexEntry* container_entry, const ConstVectorBatch& batch,
                               uint16_t marked_for_update = INVALID_OFFSET) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        FatalAssert(batch.size != 0, LOG_TAG_DIVFTREE, "Batch of vectors to insert is empty.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE, "Batch of vectors to insert is null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE, "Batch of vector IDs to insert is null.");
        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_EXCLUSIVE);

        DIVFTreeVertex* current = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(current, LOG_TAG_DIVFTREE);
        FatalAssert(current->attr.index == this, LOG_TAG_DIVFTREE,
                    "input entry does not belong to this index!");

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        DIVFTreeVertex* compacted =
            new(bufferMgr->AllocateMemoryForVertex(container_entry->centroidMeta.selfId._level))
            DIVFTreeVertex(current->attr);

        uint16_t size = current->cluster.header.reserved_size.load(std::memory_order_relaxed);
        const bool is_leaf = container_entry->centroidMeta.selfId.IsLeaf();
        const uint16_t dim = attr.dimension;
        const uint16_t cap = current->attr.cap;
        const uint16_t blckSize = current->attr.cap;
        /* Todo: this will not work if we have more than one block! */
        FatalAssert(current->cluster.NumBlocks(blckSize, cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "Currently cannot handle more than one block!");

        FatalAssert(!is_leaf || (batch.version != nullptr), LOG_TAG_DIVFTREE, "Batch of versions to insert is null!");

        Address srcMetaData = current->cluster.MetaData(0, is_leaf, blckSize, cap, dim);
        VTYPE* srcData = current->cluster.Data(0, is_leaf, blckSize, cap, dim);
        Address destMetaData = compacted->cluster.MetaData(0, is_leaf, blckSize, cap, dim);
        VTYPE* destData = compacted->cluster.Data(0, is_leaf, blckSize, cap, dim);
        uint16_t new_size = 0;
        /*
         * Note: During compaction, we will update vector locations but
         * we haven't updated the container cluster ptr yet. As a result, a reader may see that offset mismatch
         * caused by this. As long as we can get the container in S mode and we have ourself in X mode, our location
         * should not change. so in cases where only offset has changed, we can simply reread the location to
         * see the new one!
         * This may be a bit funky for vectors though as they do not have any locks associated with them.
         */
        for (uint16_t i = 0; i < size; ++i) {
            if (i == marked_for_update) {
                continue;
            }

            if (is_leaf) {
                VectorMetaData* src_vmd = reinterpret_cast<VectorMetaData*>(srcMetaData);
                VectorMetaData* dest_vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                VectorState vstate = src_vmd[i].state.load(std::memory_order_relaxed);
                if (vstate == VECTOR_STATE_VALID) {
                    memcpy(&destData[new_size * dim], &srcData[i * dim], dim * sizeof(VTYPE));
                    dest_vmd[new_size].id = src_vmd[i].id;
                    dest_vmd[new_size].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
                    ++new_size;
                } SANITY_CHECK( else if (vstate == VECTOR_STATE_MIGRATED) {
                    VectorLocation outdatedLoc(container_entry->centroidMeta.selfId,
                                               container_entry->currentVersion, i);
                    FatalAssert(outdatedLoc != bufferMgr->LoadCurrentVectorLocation(src_vmd[i].id), LOG_TAG_DIVFTREE,
                                "current location is migrated!");
                })
            } else {
                CentroidMetaData* src_vmd = reinterpret_cast<CentroidMetaData*>(srcMetaData);
                CentroidMetaData* dest_vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                VectorState vstate = src_vmd[i].state.load(std::memory_order_relaxed);
                if (vstate == VECTOR_STATE_VALID) {
                    memcpy(&destData[new_size * dim], &srcData[i * dim], dim * sizeof(VTYPE));
                    dest_vmd[new_size].id = src_vmd[i].id;
                    dest_vmd[new_size].version = src_vmd[i].version;
                    dest_vmd[new_size].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
                    ++new_size;
                } SANITY_CHECK( else if (vstate == VECTOR_STATE_MIGRATED) {
                    VectorLocation outdatedLoc(container_entry->centroidMeta.selfId,
                                               container_entry->currentVersion, i);
                    FatalAssert(outdatedLoc != bufferMgr->LoadCurrentVectorLocation(src_vmd[i].id), LOG_TAG_DIVFTREE,
                                "current location is migrated!");
                })
            }
        }
        FatalAssert(current->cluster.header.visible_size.load(std::memory_order_relaxed) ==
                    current->cluster.header.reserved_size.load(std::memory_order_relaxed), LOG_TAG_DIVFTREE,
                    "not all updates went through!");
        FatalAssert(new_size == current->cluster.header.reserved_size.load(std::memory_order_relaxed) -
                                current->cluster.header.num_deleted.load(std::memory_order_relaxed),
                    LOG_TAG_DIVFTREE, "not all deletes went through!");
        FatalAssert(new_size + batch.size > new_size, LOG_TAG_DIVFTREE, "overflow detected!");
        FatalAssert(new_size + batch.size < cap, LOG_TAG_DIVFTREE, "Cluster cannot contain all of these vectors!");

        uint16_t old_reserved = new_size;

        for (uint16_t i = 0; i < batch.size; ++i) {
            memcpy(&destData[new_size * dim], &batch.data[i * dim], dim * sizeof(VTYPE));
            if (is_leaf) {
                VectorMetaData* dest_vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                dest_vmd[new_size].id = batch.id[i];
                dest_vmd[new_size].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            } else {
                CentroidMetaData* dest_vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                dest_vmd[new_size].id = batch.id[i];
                dest_vmd[new_size].version = batch.version[i];
                dest_vmd[new_size].state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
                bufferMgr->PinVertexVersion(batch.id[i], batch.version[i]);
            }

            ++new_size;
        }

        compacted->cluster.header.num_deleted.store(0, std::memory_order_relaxed);
        compacted->cluster.header.reserved_size.store(new_size, std::memory_order_relaxed);
        compacted->cluster.header.visible_size.store(new_size, std::memory_order_relaxed);

        container_entry->UpdateClusterPtr(compacted);
        current = nullptr; // this is to make sure that we no longer access the current!

        for (uint16_t i = 0; i < old_reserved; ++i) {
            if (is_leaf) {
                VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                bufferMgr->UpdateVectorLocationOffset(vmd[i].id, i);
            } else {
                CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                bufferMgr->UpdateVectorLocationOffset(vmd[i].id, i);
            }
        }

        for (uint16_t i = old_reserved; i < new_size; ++i) {
            if (is_leaf) {
                VectorMetaData* vmd = reinterpret_cast<VectorMetaData*>(destMetaData);
                bufferMgr->UpdateVectorLocation(vmd[i].id,
                    VectorLocation(container_entry->centroidMeta.selfId, container_entry->currentVersion, new_size));
            } else {
                CentroidMetaData* vmd = reinterpret_cast<CentroidMetaData*>(destMetaData);
                bufferMgr->UpdateVectorLocation(vmd[i].id,
                    VectorLocation(container_entry->centroidMeta.selfId, container_entry->currentVersion, new_size));
            }
        }

        return RetStatus::Success();
    }

    inline void SimpleDivideClustering(const DIVFTreeVertex* base, const ConstVectorBatch& batch,
                                       BufferVertexEntry**& entries, DIVFTreeVertex**& clusters, VectorBatch& centroids,
                                       uint16_t marked_for_update = INVALID_OFFSET) {
        FatalAssert(base->attr.index == this, LOG_TAG_DIVFTREE, "index mismatch!");
        uint16_t num_vectors = base->cluster.header.reserved_size.load(std::memory_order_relaxed) -
                               base->cluster.header.num_deleted.load(std::memory_order_relaxed) + batch.size -
                               (marked_for_update != INVALID_OFFSET ? 1 : 0);
        const bool is_leaf = base->attr.centroid_id.IsLeaf();
        centroids.size = (is_leaf ? attr.split_leaf : attr.split_internal);
        if (num_vectors / centroids.size < base->attr.min_size) {
            centroids.size = num_vectors / base->attr.min_size + ((num_vectors % base->attr.min_size) > 0);
        }

        if (centroids.size < 2) {
            centroids.size = 2;
        }

        centroids.data = new VTYPE[attr.dimension * centroids.size];
        centroids.version = new Version[centroids.size];
        centroids.id = new VectorID[centroids.size];
        entries = new BufferVertexEntry*[centroids.size];
        clusters = new DIVFTreeVertex*[centroids.size];

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_CLUSTERING);
        centroids.id[0] = base->attr.centroid_id;
        centroids.version[0] = base->attr.version + 1;
        clusters[0] = new (bufferMgr->AllocateMemoryForVertex(centroids.id[0]._level))
            DIVFTreeVertex(DIVFTreeVertexAttributes(centroids.id[0], centroids.version[0], base->attr.min_size,
                                                    base->attr.cap, base->attr.block_size, base->attr.index));
        CHECK_NOT_NULLPTR(clusters[0], LOG_TAG_CLUSTERING);
        entries[0] = nullptr;
        DIVFTreeVertexInterface** clusterMemories = reinterpret_cast<DIVFTreeVertexInterface**>(&clusters[1]);
        bufferMgr->BatchCreateBufferEntry(centroids.size - 1, base->attr.centroid_id._level, &entries[1],
                                          clusterMemories, &centroids.id[1], &centroids.version[1]);

        for (uint16_t i = 1; i < centroids.size; ++i) {
            CHECK_NOT_NULLPTR(clusters[i], LOG_TAG_CLUSTERING);
            CHECK_NOT_NULLPTR(entries[i], LOG_TAG_CLUSTERING);
            new (clusters[i]) DIVFTreeVertex(
                DIVFTreeVertexAttributes(centroids.id[i], centroids.version[i], base->attr.min_size,
                                         base->attr.cap, base->attr.block_size, base->attr.index));
        }

        FatalAssert(base->cluster.NumBlocks(base->attr.block_size, base->attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "Currently cannot handle more than one block!");
        const VTYPE* vectors = base->cluster.Data(0, is_leaf, base->attr.block_size, base->attr.cap, attr.dimension);
        const void* meta = base->cluster.MetaData(0, is_leaf, base->attr.block_size, base->attr.cap, attr.dimension);
        uint16_t num_seen = 0;
        for (uint16_t i = 0; i < base->attr.cap; ++i) {
            if (i == marked_for_update) {
                continue;
            }

            const uint16_t destSize =
                clusters[num_seen % centroids.size]->cluster.header.reserved_size.load(std::memory_order_relaxed);
            void* destMeta =
                clusters[num_seen % centroids.size]->cluster.MetaData(destSize, is_leaf, base->attr.block_size,
                                                                    base->attr.cap, attr.dimension);
            VTYPE* destData =
                clusters[num_seen % centroids.size]->cluster.Data(destSize, is_leaf, base->attr.block_size,
                                                                base->attr.cap, attr.dimension);
            if (is_leaf) {
                const VectorMetaData* vmt = reinterpret_cast<const VectorMetaData*>(meta);
                VectorMetaData* destVmt = reinterpret_cast<VectorMetaData*>(destMeta);
                if (vmt[i].state.load(std::memory_order_relaxed) != VECTOR_STATE_VALID) {
                    continue;
                }
                memcpy(destData, &vectors[i * attr.dimension], sizeof(VTYPE) * attr.dimension);
                destVmt->id = vmt[i].id;
                destVmt->state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            } else {
                const CentroidMetaData* vmt = reinterpret_cast<const CentroidMetaData*>(meta);
                CentroidMetaData* destVmt = reinterpret_cast<CentroidMetaData*>(destMeta);
                if (vmt[i].state.load(std::memory_order_relaxed) != VECTOR_STATE_VALID) {
                    continue;
                }
                memcpy(destData, &vectors[i * attr.dimension], sizeof(VTYPE) * attr.dimension);
                destVmt->id = vmt[i].id;
                destVmt->version = vmt[i].version;
                destVmt->state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            }
            clusters[num_seen % centroids.size]->cluster.header.reserved_size.store(destSize + 1,
                                                                                    std::memory_order_relaxed);
            ++num_seen;
        }

        for (uint16_t i = 0; i < batch.size; ++i) {
            const uint16_t destSize =
                clusters[num_seen % centroids.size]->cluster.header.reserved_size.load(std::memory_order_relaxed);
            void* destMeta =
                clusters[num_seen % centroids.size]->cluster.MetaData(destSize, is_leaf, base->attr.block_size,
                                                                    base->attr.cap, attr.dimension);
            VTYPE* destData =
                clusters[num_seen % centroids.size]->cluster.Data(destSize, is_leaf, base->attr.block_size,
                                                                base->attr.cap, attr.dimension);
            if (is_leaf) {
                VectorMetaData* destVmt = reinterpret_cast<VectorMetaData*>(destMeta);
                memcpy(destData, &(batch.data[i * attr.dimension]), sizeof(VTYPE) * attr.dimension);
                destVmt->id = batch.id[i];
                destVmt->state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
            } else {
                CentroidMetaData* destVmt = reinterpret_cast<CentroidMetaData*>(destMeta);
                memcpy(destData, &(batch.data[i * attr.dimension]), sizeof(VTYPE) * attr.dimension);
                destVmt->id = batch.id[i];
                destVmt->version = batch.version[i];
                destVmt->state.store(VECTOR_STATE_VALID, std::memory_order_relaxed);
                bufferMgr->PinVertexVersion(batch.id[i], batch.version[i]);
            }
            clusters[num_seen % centroids.size]->cluster.header.reserved_size.store(destSize + 1,
                                                                                  std::memory_order_relaxed);
            ++num_seen;
        }

        /* Now we have to compute centroids and update num visible for each cluster and do a sanityt check for their size */
        for (uint16_t i = 0; i < centroids.size; ++i) {
            uint16_t size = clusters[i]->cluster.header.reserved_size.load(std::memory_order_relaxed);
            FatalAssert((base->attr.min_size <= size) && (size <= base->attr.cap), LOG_TAG_CLUSTERING, "invalid size!");
            clusters[i]->cluster.header.num_deleted.store(0, std::memory_order_relaxed);
            clusters[i]->cluster.header.visible_size.store(size, std::memory_order_relaxed);

            ComputeCentroid(clusters[i]->cluster.Data(0, is_leaf, base->attr.block_size, base->attr.cap, attr.dimension),
                            size, nullptr, 0, attr.dimension, attr.distanceAlg, &centroids.data[i * attr.dimension]);
        }

    }

    /*
     * will only fill in the raw centroid vectors to
     * the centroids batch and allocates memory for version and ids but does not fill them
     */
    inline void Clustering(const DIVFTreeVertex* base, const ConstVectorBatch& batch,
                           BufferVertexEntry**& entries, DIVFTreeVertex**& clusters, VectorBatch& centroids,
                           uint16_t marked_for_update = INVALID_OFFSET) {
        switch (attr.clusteringAlg)
        {
        case ClusteringType::SimpleDivide:
            SimpleDivideClustering(base, batch, entries, clusters, centroids, marked_for_update);
        default:
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE,
                 "Cluster: Invalid clustering type: %hhu", (uint8_t)(attr.clusteringAlg));
        }
    }

    inline BufferVertexEntry* ExpandTree() {
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        BufferVertexEntry* new_root = bufferMgr->CreateNewRootEntry();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        (void)(new (new_root->ReadLatestVersion(false)) DIVFTreeVertex(attr, new_root->centroidMeta.selfId, this));
        new_root->DowngradeAccessToShared();
        return new_root;
    }

    RetStatus SplitAndInsert(BufferVertexEntry* container_entry, const ConstVectorBatch& batch,
                             uint16_t marked_for_update = INVALID_OFFSET) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        FatalAssert(batch.size != 0, LOG_TAG_DIVFTREE, "Batch of vectors to insert is empty.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE, "Batch of vectors to insert is null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE, "Batch of vector IDs to insert is null.");
        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_EXCLUSIVE);

        RetStatus rs = RetStatus::Success();
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        DIVFTreeVertex* current = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(current, LOG_TAG_DIVFTREE);

        uint16_t size = current->cluster.header.reserved_size.load(std::memory_order_relaxed);
        const bool is_leaf = container_entry->centroidMeta.selfId.IsLeaf();
        const uint16_t dim = attr.dimension;
        const uint16_t cap = current->attr.cap;
        const uint16_t blckSize = current->attr.block_size;
        const uint8_t level = (uint8_t)(container_entry->centroidMeta.selfId._level);
        /* Todo: this will not work if we have more than one block! */
        FatalAssert(current->cluster.NumBlocks(blckSize, cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "Currently cannot handle more than one block!");

        FatalAssert(!is_leaf || (batch.version != nullptr), LOG_TAG_DIVFTREE, "Batch of versions to insert is null!");
        uint16_t total_size = current->cluster.header.reserved_size.load(std::memory_order_relaxed) -
                              current->cluster.header.num_deleted.load(std::memory_order_relaxed) +
                              batch.size;

        VectorBatch centroids;
        DIVFTreeVertex** clusters = nullptr;
        BufferVertexEntry** entries = nullptr;
        Clustering(current, batch, entries, clusters, centroids, marked_for_update);
        CHECK_NOT_NULLPTR(entries, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(clusters, LOG_TAG_DIVFTREE);
        FatalAssert(centroids.size >= 2, LOG_TAG_DIVFTREE, "numclusters should at least be 2!");
        CHECK_NOT_NULLPTR(centroids.data, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(centroids.id, LOG_TAG_DIVFTREE);
        CHECK_NOT_NULLPTR(centroids.version, LOG_TAG_DIVFTREE);
        entries[0] = container_entry;

        BufferVertexEntry* parent = nullptr;
        while (true) {
            VectorLocation currentLocation = INVALID_VECTOR_LOCATION;
            parent = container_entry->ReadParent(currentLocation);
            if (parent == nullptr) {
                FatalAssert(currentLocation == INVALID_VECTOR_LOCATION, LOG_TAG_DIVFTREE,
                            "null parent with valid location!");
                FatalAssert(container_entry->centroidMeta.selfId == bufferMgr->GetCurrentRootId(), LOG_TAG_DIVFTREE,
                            "null parent but we are not root!");
                parent = ExpandTree();
            }

            CHECK_NOT_NULLPTR(parent, LOG_TAG_DIVFTREE);
            FatalAssert(currentLocation != INVALID_VECTOR_LOCATION, LOG_TAG_DIVFTREE,
                        "null parent with valid location!");
            rs = BatchInsertInto(parent, centroids, false, currentLocation.entryOffset);
            if (!rs.IsOK()) {
                bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                parent = nullptr;
                continue;
            }

            break;
        }

        threadSelf->SanityCheckLockHeldInModeByMe(&parent->clusterLock, SX_SHARED);

        container_entry->UpdateClusterPtr(clusters[0], centroids.version[0]);
        bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
        parent = nullptr;

        for (uint16_t i = 0; i < centroids.size; ++i) {
            uint16_t size = clusters[i]->cluster.header.reserved_size.load(std::memory_order_relaxed);
            FatalAssert(size == clusters[i]->cluster.header.visible_size.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "mismatch between sizes when cluster is locked in X mode");
            FatalAssert(clusters[i]->cluster.header.num_deleted.load(std::memory_order_relaxed) == 0,
                        LOG_TAG_DIVFTREE, "num deleted should be 0 at thsi stage!");
            void* meta = clusters[i]->cluster.MetaData(0, is_leaf, blckSize, cap, dim);
            CHECK_NOT_NULLPTR(meta, LOG_TAG_DIVFTREE);
            for (uint16_t offset = 0; offset < size; ++offset) {
                if (is_leaf) {
                    VectorMetaData* vmt = reinterpret_cast<VectorMetaData*>(meta);
                    bufferMgr->UpdateVectorLocation(vmt[offset].id,
                                                    VectorLocation(centroids.id[i], centroids.version[i], offset));
                } else {
                    CentroidMetaData* vmt = reinterpret_cast<CentroidMetaData*>(meta);
                    bufferMgr->UpdateVectorLocation(vmt[offset].id,
                                                    VectorLocation(centroids.id[i], centroids.version[i], offset));
                }
            }
        }

        for (uint16_t i = centroids.size - 1; i != 0; --i) {
            bufferMgr->ReleaseBufferEntry(entries[i-1], ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});
        }
        bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});

        delete[] clusters;
        delete[] entries;
        delete[] centroids.data;
        delete[] centroids.id;
        delete[] centroids.version;

        return rs;
    }

    RetStatus BatchInsertInto(BufferVertexEntry* container_entry, const ConstVectorBatch& batch, bool releaseEntry,
                              uint16_t marked_for_update = INVALID_OFFSET) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        FatalAssert(batch.size != 0, LOG_TAG_DIVFTREE, "Batch of vectors to insert is empty.");
        FatalAssert(batch.data != nullptr, LOG_TAG_DIVFTREE, "Batch of vectors to insert is null.");
        FatalAssert(batch.id != nullptr, LOG_TAG_DIVFTREE, "Batch of vector IDs to insert is null.");
        RetStatus rs = RetStatus::Success();

        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_SHARED);
        FatalAssert(container_entry->state.load() != CLUSTER_DELETED, LOG_TAG_DIVFTREE, "Container vertex is deleted.");
        while (true) {
            /*
             * we do not need to pin the cluster here because the version pin should remain at least one as long as we
             * have a lock on it.
             */
            DIVFTreeVertex* container_vertex =
                static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
            CHECK_NOT_NULLPTR(container_vertex, LOG_TAG_DIVFTREE);
            // CHECK_VERTEX_IS_VALID(container_vertex, LOG_TAG_DIVFTREE, RetStatus::FAIL);
            rs = container_vertex->BatchInsert(batch, marked_for_update);
            if (rs.IsOK()) {
                break;
            }

            FatalAssert(rs.stat == RetStatus::VERTEX_NOT_ENOUGH_SPACE,
                        LOG_TAG_DIVFTREE, "Batch insert failed with error: %s", rs.Msg());
            BufferVertexEntryState state = CLUSTER_FULL;
            rs = container_entry->UpgradeAccessToExclusive(state);
            if (!rs.IsOK()) {
                FatalAssert(rs.stat == RetStatus::FAILED_TO_CAS_ENTRY_STATE, LOG_TAG_DIVFTREE,
                        "UpgradeAccessToExclusive failed with error: %s", rs.Msg());
                switch(state) {
                case BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS: /* Can this happen? */
                case BufferVertexEntryState::CLUSTER_FULL:
                case BufferVertexEntryState::CLUSTER_DELETED: /* Can this happen? */
                    /* it is possible that the update was not applied though! */
                    rs = RetStatus{.stat=RetStatus::VERTEX_UPDATED, .message=nullptr};
                    break;
                default:
                    CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "Invalid entry state!");
                    rs = RetStatus::Fail("Invalid State!");
                    break;
                }
                break;
            }

            FatalAssert((void*)(container_vertex) ==
                        (void*)(container_entry->ReadLatestVersion(false)), LOG_TAG_DIVFTREE,
                        "ClusterPtr has changed during the atomic upgrade from shared to exclusive!");
            FatalAssert(container_vertex->cluster.header.visible_size.load(std::memory_order_relaxed) ==
                        container_vertex->cluster.header.reserved_size.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "Some updates did not go through!");
            FatalAssert(container_vertex->cluster.header.reserved_size.load(std::memory_order_relaxed) >
                        container_vertex->cluster.header.num_deleted.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "More vectors are deleted than inserted!");
            uint16_t reserved_size =
                container_vertex->cluster.header.reserved_size.load(std::memory_order_relaxed) + batch.size;
            uint16_t real_size =
                reserved_size - container_vertex->cluster.header.num_deleted.load(std::memory_order_relaxed);
            if ((float)real_size * COMPACTION_FACTOR <= (float)container_vertex->attr.cap) {
                rs = CompactAndInsert(container_entry, batch, marked_for_update);
            } else if (reserved_size < container_vertex->attr.cap) {
                container_entry->DowngradeAccessToShared();
                continue;
            } else {
                rs = SplitAndInsert(container_entry, batch, marked_for_update);
            }

            FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "Major update failed!");
            if (releaseEntry) {
                BufferManager::GetInstance()->
                    ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});
            } else {
                container_entry->DowngradeAccessToShared();
            }

            return rs;
        }

        if (releaseEntry) {
            BufferManager::GetInstance()->
                ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
        }
        return rs;
    }

    inline void PruneAtRootAndRelease(BufferVertexEntry* container_entry) {
        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_EXCLUSIVE);
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        FatalAssert(container_entry->centroidMeta.selfId == bufferMgr->GetCurrentRootId(),
                    LOG_TAG_DIVFTREE, "we should be the root!");

        VectorID rootId = bufferMgr->GetCurrentRootId();
        CHECK_VECTORID_IS_VALID(container_entry->centroidMeta.selfId , LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_INTERNAL(container_entry->centroidMeta.selfId , LOG_TAG_DIVFTREE);

        DIVFTreeVertex* container = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(container, LOG_TAG_DIVFTREE);
        uint16_t size = container->cluster.header.reserved_size.load(std::memory_order_relaxed);
        uint16_t num_deleted = container->cluster.header.num_deleted.load(std::memory_order_relaxed);

        FatalAssert(size == container->cluster.header.visible_size.load(std::memory_order_relaxed), LOG_TAG_DIVFTREE,
                    "size should be stable!");
        FatalAssert(size >= num_deleted, LOG_TAG_DIVFTREE,
                    "we cannot delete more vectors than there are in the cluster!");
        if (size - num_deleted == 0) {
            /* all vectors in the index are deleted */
            /* todo: delete this node and then create a new root? what if an insertion comes in at this point? */
            CLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "we do not handle this case for now");
        }

        if (((size - num_deleted) > 1)) {
            /* no need to prune */
            bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});
            return;
        }

        /* we should see exactly one child in this cluster and that child will become the new root */
        FatalAssert(container->cluster.NumBlocks(container->attr.block_size, container->attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "cannot handle multiple blocks");
        CentroidMetaData* vmd =
            static_cast<CentroidMetaData*>(container->
                cluster.MetaData(0, false, container->attr.block_size, container->attr.cap, attr.dimension));
        VectorID newRootId = INVALID_VECTOR_ID;
        Version newRootVersion = 0;
        for (uint16_t i = 0; i < size; ++i) {
            if (vmd[i].state.load(std::memory_order_relaxed) == VECTOR_STATE_VALID) {
                newRootId = vmd[i].id;
                break;
            }
        }
        CHECK_VECTORID_IS_VALID(newRootId, LOG_TAG_DIVFTREE);
        bufferMgr->UpdateVectorLocation(newRootId, INVALID_VECTOR_LOCATION);
        bufferMgr->UpdateRoot(newRootId, newRootVersion, container_entry);
        container_entry->UnpinVersion(container_entry->currentVersion);
        bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=0});
    }

    inline void PruneEmptyVertexAndRelease(BufferVertexEntry* container_entry) {
        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_EXCLUSIVE);
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);

        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

        DIVFTreeVertex* container = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(container, LOG_TAG_DIVFTREE);
        uint16_t size = container->cluster.header.reserved_size.load(std::memory_order_relaxed);
        uint16_t num_deleted = container->cluster.header.num_deleted.load(std::memory_order_relaxed);

        FatalAssert(size == container->cluster.header.visible_size.load(std::memory_order_relaxed), LOG_TAG_DIVFTREE,
                    "size should be stable!");
        FatalAssert(size >= num_deleted, LOG_TAG_DIVFTREE,
                    "we cannot delete more vectors than there are in the cluster!");
        if (((size - num_deleted) >= 1)) {
            /* no need to prune */
            bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});
            return;
        }

        VectorLocation loc = bufferMgr->LoadCurrentVectorLocation(container_entry->centroidMeta.selfId);
        BufferVertexEntry* parent = nullptr;
        if ((loc == INVALID_VECTOR_LOCATION) || ((parent = container_entry->ReadParent(loc)) == nullptr)) {
            FatalAssert((parent == nullptr) && (loc == INVALID_VECTOR_LOCATION), LOG_TAG_DIVFTREE,
                        "mismatch between parent ptr and location");
            PruneAtRootAndRelease(container_entry);
            return;
        }
        FatalAssert((parent != nullptr) && (loc != INVALID_VECTOR_LOCATION), LOG_TAG_DIVFTREE,
                    "mismatch between parent ptr and location");
        FatalAssert(container_entry->centroidMeta.selfId != bufferMgr->GetCurrentRootId(),
                    LOG_TAG_DIVFTREE, "we should not be the root!");
        RetStatus rs = DeleteVectorAndReleaseContainer(container_entry->centroidMeta.selfId, parent, loc);
        UNUSED_VARIABLE(rs);
        FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "this deletion should not fail!");
        container_entry->UnpinVersion(container_entry->currentVersion);
        bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=0});
    }

    inline void PruneIfNeededAndRelease(BufferVertexEntry* container_entry) {
        while (true) {
            threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_SHARED);
            CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);

            BufferManager* bufferMgr = BufferManager::GetInstance();
            CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);

            VectorID rootId = bufferMgr->GetCurrentRootId();
            CHECK_VECTORID_IS_VALID(rootId, LOG_TAG_DIVFTREE);
            CHECK_VECTORID_IS_CENTROID(rootId, LOG_TAG_DIVFTREE);
            if (rootId.IsLeaf()) {
                FatalAssert(container_entry->centroidMeta.selfId == rootId, LOG_TAG_DIVFTREE, "we should be the root!");
                bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                return;
            }

            DIVFTreeVertex* container = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
            CHECK_NOT_NULLPTR(container, LOG_TAG_DIVFTREE);
            if (container_entry->centroidMeta.selfId == rootId) {
                /* we can be sure that we remain root as changing the current root requires exclusive lock on it */
                if ((container->cluster.header.reserved_size.load(std::memory_order_acquire) -
                    container->cluster.header.num_deleted.load(std::memory_order_acquire)) > 1) {
                    bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                    return;
                }
                BufferVertexEntryState state = BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS;
                if (container_entry->UpgradeAccessToExclusive(state).IsOK()) {
                    PruneAtRootAndRelease(container_entry);
                    return;
                } else if (state == BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS ||
                           state == BufferVertexEntryState::CLUSTER_DELETED) {
                    /* someone else has handled it */
                    bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                    return;
                } else if (state == BufferVertexEntryState::CLUSTER_FULL) {
                    /* recheck if pruning is needed */
                    continue;
                } else {
                    CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "we shouldn't get here! Invalid state");
                }
            }

            if ((container->cluster.header.reserved_size.load(std::memory_order_acquire) -
                container->cluster.header.num_deleted.load(std::memory_order_acquire)) >= 1) {
                bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                return;
            }
            BufferVertexEntryState state = BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS;
            if (container_entry->UpgradeAccessToExclusive(state).IsOK()) {
                if ((container->cluster.header.reserved_size.load(std::memory_order_acquire) -
                    container->cluster.header.num_deleted.load(std::memory_order_acquire)) >= 1) {
                    bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});
                    return;
                }

                rootId = bufferMgr->GetCurrentRootId();
                if (rootId.IsLeaf()) {
                    FatalAssert(container_entry->centroidMeta.selfId == rootId, LOG_TAG_DIVFTREE,
                                "we should be the root!");
                    bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=1, .stablize=1});
                    return;
                }
                if (container_entry->centroidMeta.selfId == rootId) {
                    PruneAtRootAndRelease(container_entry);
                } else {
                    PruneEmptyVertexAndRelease(container_entry);
                }
                return;
            } else if (state == BufferVertexEntryState::CLUSTER_DELETE_IN_PROGRESS ||
                       state == BufferVertexEntryState::CLUSTER_DELETED) {
                /* someone else has handled it */
                bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
            } else if (state == BufferVertexEntryState::CLUSTER_FULL) {
                /* recheck if pruning is needed */
                continue;
            } else {
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "we shouldn't get here! Invalid state");
            }
        }
    }

    RetStatus DeleteVectorAndReleaseContainer(VectorID target, BufferVertexEntry* container_entry, VectorLocation loc) {
        CHECK_NOT_NULLPTR(container_entry, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(target, LOG_TAG_DIVFTREE);
        FatalAssert(target._level + 1 == container_entry->centroidMeta.selfId._level, LOG_TAG_DIVFTREE,
                    "target cannot be child of container");
        FatalAssert(loc.containerId == container_entry->centroidMeta.selfId, LOG_TAG_DIVFTREE, "Location Id mismatch");
        FatalAssert(container_entry->currentVersion == loc.containerVersion, LOG_TAG_DIVFTREE,
                    "Location version mismatch");
        threadSelf->SanityCheckLockHeldInModeByMe(container_entry->clusterLock, SX_SHARED);
        DIVFTreeVertex* container = static_cast<DIVFTreeVertex*>(container_entry->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(container, LOG_TAG_DIVFTREE);
        Version version = 0;
        VectorState expState = VECTOR_STATE_VALID;
        RetStatus rs = container->ChangeVectorState(target, loc.entryOffset, expState, VECTOR_STATE_INVALID,
                                                    &version);
        FatalAssert(target.IsVector() || rs.IsOK(), LOG_TAG_DIVFTREE,
                    "state change will never fail if target is a vertex as it should be locked in X mode.");
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        if (rs.IsOK()) {
            bufferMgr->UpdateVectorLocation(target, INVALID_VECTOR_LOCATION);
            if (target.IsCentroid()) {
                bufferMgr->UnpinVertexVersion(target, version);
            }
            container->cluster.header.num_deleted.fetch_add(1);
            PruneIfNeededAndRelease(container_entry);
        } else {
            bufferMgr->ReleaseBufferEntry(container_entry, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
        }

        return rs;
    }

    inline RetStatus ReadParentAndCheckLocation(VectorID target, BufferVertexEntry* targetEntry,
                                                const VectorLocation& expTargetLocation,
                                                VectorLocation& currentTargetLocation, BufferVertexEntry** entries,
                                                uint8_t& num_entries) {
        BufferVertexEntry*& parentEntry = entries[3 - num_entries - 1];
        BufferManager* bufferMgr = BufferManager::GetInstance();
        RetStatus rs = RetStatus::Success();

        if (targetEntry != nullptr) {
            threadSelf->SanityCheckLockHeldByMe(targetEntry->clusterLock);
            parentEntry = targetEntry->ReadParent(currentTargetLocation);
            FatalAssert((parentEntry == nullptr) == (currentTargetLocation == INVALID_VECTOR_LOCATION),
                        LOG_TAG_DIVFTREE, "mismatch location and parentPtr");
            if (parentEntry == nullptr) {
                /* target is the new root */
                rs = RetStatus{.stat=RetStatus::TARGET_IS_ROOT, .message=nullptr};
            }
        } else {
            BufferVectorEntry* entry = bufferMgr->GetVectorEntry(target);
            CHECK_NOT_NULLPTR(entry, LOG_TAG_DIVFTREE);
            parentEntry = entry->ReadParent(currentTargetLocation);
            FatalAssert((parentEntry == nullptr) == (currentTargetLocation == INVALID_VECTOR_LOCATION),
                        LOG_TAG_DIVFTREE, "mismatch location and parentPtr");
            if (parentEntry == nullptr) {
                /* the vector is deleted */
                rs = RetStatus{.stat=RetStatus::TARGET_DELETED, .message=nullptr};
            }
        }

        if (rs.IsOK()) {
            CHECK_NOT_NULLPTR(parentEntry, LOG_TAG_DIVFTREE);
            ++num_entries;
            if ((currentTargetLocation.containerId != expTargetLocation.containerId) ||
                (currentTargetLocation.containerVersion != expTargetLocation.containerVersion)) {
                /* this means that either the parent had a major update that was not compaction or we were migrated */
                rs = RetStatus{.stat=RetStatus::TARGET_UPDATED, .message=nullptr};
            }
        }

        if (!rs.IsOK()) {
            bufferMgr->ReleaseEntriesIfNotNull(&entries[3 - num_entries], num_entries,
                                               ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
        }

        return rs;
    }

    inline RetStatus ReadAndCheckVersion(VectorID containerId, Version containerVersion,
                                         BufferVertexEntry** entries, uint8_t& num_entries, LockMode mode) {
        BufferVertexEntry*& containerEntry = entries[3 - num_entries - 1];
        BufferManager* bufferMgr = BufferManager::GetInstance();
        RetStatus rs = RetStatus::Success();

        /*
         * we do not care if we are blocked because the block does not necessarily lead to Major Updates +
         * Compaction does not change the version
         */
        containerEntry = bufferMgr->ReadBufferEntry(containerId, mode);
        if (containerEntry == nullptr) {
            rs = RetStatus{.stat=RetStatus::TARGET_DELETED, .message=nullptr};
        }

        if (rs.IsOK()) {
            ++num_entries;
            if (containerEntry->currentVersion != containerVersion) {
                rs = RetStatus{.stat=RetStatus::TARGET_UPDATED, .message=nullptr};
            }
        }

        if (!rs.IsOK()) {
            bufferMgr->ReleaseEntriesIfNotNull(&entries[3 - num_entries], num_entries,
                                               ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
        }

        return rs;
    }

    /* Todo: recheck to see if everything works fine */
    /* todo: need to refactor */
    RetStatus Migrate(VectorLocation targetLocation,
                      VectorID newContainerId, Version newContainerVersion,
                      VectorID target, Version targetVersion = 0) {
        CHECK_VECTORID_IS_VALID(target, LOG_TAG_DIVFTREE);
        FatalAssert(targetLocation != INVALID_VECTOR_LOCATION, LOG_TAG_DIVFTREE, "Invalid location");
        CHECK_VECTORID_IS_CENTROID(targetLocation.containerId, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(newContainerId, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(newContainerId, LOG_TAG_DIVFTREE);
        FatalAssert(newContainerId != targetLocation.containerId, LOG_TAG_DIVFTREE,
                    "containers should not be the same!");
        FatalAssert(newContainerId._level == targetLocation.containerId._level, LOG_TAG_DIVFTREE,
                    "containers should be on the same level!");
        FatalAssert(target._level + 1 == targetLocation.containerId._level, LOG_TAG_DIVFTREE,
                    "level mismatch between target and containers");

        RetStatus rs = RetStatus::Success();
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        BufferVertexEntry* entries[3];
        VectorLocation currentTargetLocation = INVALID_VECTOR_LOCATION;
        uint8_t num_entries = 0;
        BufferVertexEntry* dummy = nullptr;
        BufferVertexEntry** targetEntry;
        BufferVertexEntry** oldContainerEntry;
        BufferVertexEntry** newContainerEntry;
        if (target.IsCentroid()) {
            rs = ReadAndCheckVersion(target, targetVersion, entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }
            targetEntry = &entries[3 - num_entries];
        } else {
            targetEntry = &dummy;
        }

        /* Todo: if we have version logs, maybe we can use that to handle migration when there is an update */
        /* Todo: maybe we can do a simple distance computation in case there was any updates to avoid too many fails */
        /* Todo: collect stats on the failure percentages, the reason and the target level */

        /* We should first lock the child and then the parents in order of their values to avoid deadlocks! */
        if (targetLocation.containerId < newContainerId) {
            rs = ReadParentAndCheckLocation(target, (*targetEntry), targetLocation, currentTargetLocation, entries,
                                            num_entries);
            if (!rs.IsOK()) {
                return rs;
            }

            oldContainerEntry = &entries[3 - num_entries];

            rs = ReadAndCheckVersion(newContainerId, newContainerVersion, entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }
            newContainerEntry = &entries[3 - num_entries];
        } else {
            rs = ReadAndCheckVersion(newContainerId, newContainerVersion, entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }
            newContainerEntry = &entries[3 - num_entries];

            rs = ReadParentAndCheckLocation(target, (*targetEntry), targetLocation, currentTargetLocation, entries,
                                            num_entries);
            if (!rs.IsOK()) {
                return rs;
            }
            oldContainerEntry = &entries[3 - num_entries];
        }

        DIVFTreeVertex* oldContainer = static_cast<DIVFTreeVertex*>((*oldContainerEntry)->ReadLatestVersion(false));
        CHECK_NOT_NULLPTR(oldContainer, LOG_TAG_DIVFTREE);

        VectorState expState = VECTOR_STATE_VALID;
        rs = oldContainer->ChangeVectorState(target, currentTargetLocation.entryOffset,
                                             expState, VECTOR_STATE_MIGRATED);
        if (!rs.IsOK()) {
            bufferMgr->ReleaseEntriesIfNotNull(&entries[3 - num_entries], num_entries,
                                               ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
            FatalAssert((*targetEntry) == nullptr, LOG_TAG_DIVFTREE,
                        "if the target is a centroid the migration should not fail!");
            if (expState == VECTOR_STATE_MIGRATED) {
                rs = RetStatus{.stat=RetStatus::TARGET_MIGRATED, .message=nullptr};
            } else if (expState == VECTOR_STATE_INVALID) {
                rs = RetStatus{.stat=RetStatus::TARGET_DELETED, .message=nullptr};
            } else {
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_DIVFTREE, "we shouldn't get here!");
                rs = RetStatus{.stat=RetStatus::FAIL, .message=nullptr};
            }
            return rs;
        }

        VectorBatch batch;
        batch.size = 1;
        batch.id = new VectorID;
        *(batch.id) = target;
        batch.data = oldContainer->cluster.Data(currentTargetLocation.entryOffset, ((*targetEntry) == nullptr),
                                                oldContainer->attr.block_size, oldContainer->attr.cap, attr.dimension);
        if ((*targetEntry) == nullptr) {
            batch.version = new Version;
            *(batch.version) = targetVersion;
        }
        rs = BatchInsertInto((*newContainerEntry), batch, true);
        *newContainerEntry = nullptr;
        delete batch.id;
        batch.id = nullptr;
        if ((*targetEntry) == nullptr) {
            delete batch.version;
            batch.version = nullptr;
        }
        if (!rs.IsOK()) {
            VectorState expState = VECTOR_STATE_MIGRATED;
            rs = oldContainer->ChangeVectorState(target, currentTargetLocation.entryOffset,
                                                 expState, VECTOR_STATE_VALID);
            FatalAssert(rs.IsOK(), LOG_TAG_DIVFTREE, "this should not fail!");
            rs = RetStatus{.stat=RetStatus::NEW_CONTAINER_UPDATED, .message=nullptr};
            bufferMgr->ReleaseEntriesIfNotNull(&entries[3 - num_entries], num_entries,
                                           ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
        } else {
            oldContainer->cluster.header.num_deleted.fetch_add(1);
            PruneIfNeededAndRelease(*oldContainerEntry);
        }

        return rs;
    }

    /* todo: need to refactor */
    RetStatus Merge(VectorID srcId, Version srcVersion,
                    VectorID destId, Version destVersion) {
        CHECK_VECTORID_IS_VALID(srcId, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(srcId, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(destId, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(destId, LOG_TAG_DIVFTREE);
        FatalAssert(srcId != destId, LOG_TAG_DIVFTREE,
                    "clusters should be on the same level!");
        FatalAssert(srcId._level == destId._level, LOG_TAG_DIVFTREE,
                    "clusters should be on the same level!");

        RetStatus rs = RetStatus::Success();
        BufferManager* bufferMgr = BufferManager::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_DIVFTREE);
        BufferVertexEntry* entries[2];
        uint8_t num_entries = 0;
        BufferVertexEntry** srcEntry;
        BufferVertexEntry** destEntry;
        uint16_t reservedSize = 0;
        uint16_t totalSize = 0;
        DIVFTreeVertex* srcCluster = nullptr;

        /* Todo: if we have version logs, maybe we can use that to handle migration when there is an update */
        /* Todo: maybe we can do a simple distance computation in case there was any updates to avoid too many fails */
        /* Todo: collect stats on the failure percentages, the reason and the target level */

        /* We should first lock the child and then the parents in order of their values to avoid deadlocks! */
        if (srcId < destId) {
            rs = ReadAndCheckVersion(srcId, srcVersion, entries, num_entries, SX_EXCLUSIVE);
            if (!rs.IsOK()) {
                return rs;
            }
            srcEntry = &entries[3 - num_entries];
            srcCluster = static_cast<DIVFTreeVertex*>((*srcEntry)->ReadLatestVersion(false));
            CHECK_NOT_NULLPTR(srcCluster, LOG_TAG_DIVFTREE);
            reservedSize = srcCluster->cluster.header.reserved_size.load(std::memory_order_relaxed);
            FatalAssert(reservedSize == srcCluster->cluster.header.visible_size.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "reserved and visible size should be the same!");
            FatalAssert(reservedSize > srcCluster->cluster.header.num_deleted.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "size should be greater than to number of deleted entries!");
            /* size should not be 0 because the one who 0ed the size will also delete the cluster! */
            totalSize = reservedSize - srcCluster->cluster.header.num_deleted.load(std::memory_order_relaxed);
            if (totalSize >= srcCluster->attr.min_size) {
                bufferMgr->ReleaseEntriesIfNotNull(&entries[3 - num_entries], num_entries,
                                                   ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                return RetStatus{.stat=RetStatus::SRC_HAS_TOO_MANY_VECTORS, .message=nullptr};
            }

            rs = ReadAndCheckVersion(destId, destVersion, entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }
            destEntry = &entries[3 - num_entries];
        } else {
            rs = ReadAndCheckVersion(destId, destVersion, entries, num_entries, SX_SHARED);
            if (!rs.IsOK()) {
                return rs;
            }
            destEntry = &entries[3 - num_entries];

            rs = ReadAndCheckVersion(srcId, srcVersion, entries, num_entries, SX_EXCLUSIVE);
            if (!rs.IsOK()) {
                return rs;
            }

            srcEntry = &entries[3 - num_entries];
            srcCluster = static_cast<DIVFTreeVertex*>((*srcEntry)->ReadLatestVersion(false));
            CHECK_NOT_NULLPTR(srcCluster, LOG_TAG_DIVFTREE);
            reservedSize = srcCluster->cluster.header.reserved_size.load(std::memory_order_relaxed);
            FatalAssert(reservedSize == srcCluster->cluster.header.visible_size.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "reserved and visible size should be the same!");
            FatalAssert(reservedSize > srcCluster->cluster.header.num_deleted.load(std::memory_order_relaxed),
                        LOG_TAG_DIVFTREE, "size should be greater than to number of deleted entries!");
            /* size should not be 0 because the one who 0ed the size will also delete the cluster! */
            totalSize = reservedSize - srcCluster->cluster.header.num_deleted.load(std::memory_order_relaxed);
            if (totalSize >= srcCluster->attr.min_size) {
                bufferMgr->ReleaseEntriesIfNotNull(&entries[3 - num_entries], num_entries,
                                                   ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                return RetStatus{.stat=RetStatus::SRC_HAS_TOO_MANY_VECTORS, .message=nullptr};
            }
        }

        /* todo: use the original pointers to write these  */
        VectorBatch batch;
        batch.size = totalSize;
        batch.id = new VectorID[totalSize];
        batch.data = new VTYPE[totalSize * attr.dimension];
        bool is_leaf = srcId.IsLeaf();
        if (is_leaf) {
            batch.version = new Version[totalSize];
        }

        VTYPE* data = srcCluster->cluster.Data(0, is_leaf, srcCluster->attr.block_size, srcCluster->attr.cap,
                                               attr.dimension);
        void* meta = srcCluster->cluster.MetaData(0, is_leaf, srcCluster->attr.block_size, srcCluster->attr.cap,
                                               attr.dimension);
        FatalAssert(srcCluster->cluster.NumBlocks(srcCluster->attr.block_size, srcCluster->attr.cap) == 1,
                    LOG_TAG_NOT_IMPLEMENTED, "cannot handle more than 1 block!");
        uint16_t i = 0;
        for (uint16_t offset = 0; offset < reservedSize; ++offset) {
            FatalAssert(i < totalSize, LOG_TAG_DIVFTREE, "more elements than excpected!");
            if (is_leaf) {
                VectorMetaData* vmd = static_cast<VectorMetaData*>(meta);
                if (vmd[offset].state.load(std::memory_order_relaxed) == VECTOR_STATE_VALID) {
                    vmd[offset].state.store(VECTOR_STATE_MIGRATED, std::memory_order_release);
                    memcpy(&data[offset * attr.dimension], &batch.data[i * attr.dimension],
                           sizeof(VTYPE) * attr.dimension);
                    batch.id[i] = vmd[offset].id;
                    ++i;
                }
            } else {
                CentroidMetaData* vmd = static_cast<CentroidMetaData*>(meta);
                if (vmd[offset].state.load(std::memory_order_relaxed) == VECTOR_STATE_VALID) {
                    vmd[offset].state.store(VECTOR_STATE_MIGRATED, std::memory_order_release);
                    memcpy(&data[offset * attr.dimension], &batch.data[i * attr.dimension],
                           sizeof(VTYPE) * attr.dimension);
                    batch.id[i] = vmd[offset].id;
                    batch.version[i] = vmd[offset].version;
                    ++i;
                }
            }
        }
        FatalAssert(i == totalSize, LOG_TAG_DIVFTREE, "fewer elements than excpected!");

        rs = BatchInsertInto((*destEntry), batch, true);
        (*destEntry) = nullptr;
        if (rs.IsOK()) {
            srcCluster->cluster.header.num_deleted.store(reservedSize, std::memory_order_release);
            (*srcEntry)->state.store(CLUSTER_DELETE_IN_PROGRESS, std::memory_order_release);
            PruneEmptyVertexAndRelease(*srcEntry);
            delete[] batch.data;
            if (is_leaf) {
                delete[] batch.version;
            }
            delete batch.id;
            return rs;
        }

        i = 0;
        for (uint16_t offset = 0; offset < reservedSize; ++offset) {
            FatalAssert(i < totalSize, LOG_TAG_DIVFTREE, "more elements than excpected!");
            if (is_leaf) {
                VectorMetaData* vmd = static_cast<VectorMetaData*>(meta);
                if ((vmd[offset].id == batch.id[i]) &&
                    (vmd[offset].state.load(std::memory_order_relaxed) == VECTOR_STATE_MIGRATED)) {
                    vmd[offset].state.store(VECTOR_STATE_VALID, std::memory_order_release);
                    ++i;
                }
            } else {
                CentroidMetaData* vmd = static_cast<CentroidMetaData*>(meta);
                if ((vmd[offset].id == batch.id[i]) &&
                    (vmd[offset].state.load(std::memory_order_relaxed) == VECTOR_STATE_MIGRATED)) {
                    vmd[offset].state.store(VECTOR_STATE_VALID, std::memory_order_release);
                    ++i;
                }
            }
        }

        bufferMgr->ReleaseEntriesIfNotNull(&entries[3 - num_entries], num_entries,
                                           ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
        delete[] batch.data;
        if (is_leaf) {
            delete[] batch.version;
        }
        delete batch.id;

        return RetStatus{.stat=RetStatus::DEST_UPDATED, .message=nullptr};
    }

    void SearchRoot(const VTYPE* query, size_t span, std::vector<std::vector<ANNVectorInfo>&>& layers,
                         DIVFTreeVertex* pinned_root_version) {
        BufferManager* bufferMgr = BufferManager::GetInstance();
        FatalAssert(bufferMgr != nullptr, LOG_TAG_DIVFTREE, "BufferManager is not initialized.");
        CHECK_NOT_NULLPTR(pinned_root_version, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_VALID(pinned_root_version->attr.centroid_id, LOG_TAG_DIVFTREE);
        CHECK_VECTORID_IS_CENTROID(pinned_root_version->attr.centroid_id, LOG_TAG_DIVFTREE);
        uint8_t level = pinned_root_version->attr.centroid_id._level;
        FatalAssert(layers[level].size() == 1 &&
                    layers[level][0].id == pinned_root_version->attr.centroid_id &&
                    layers[level][0].version == pinned_root_version->attr.version, LOG_TAG_DIVFTREE,
                    "the highest level should only contain the pinned version of the root");
        FatalAssert(span > 0, LOG_TAG_DIVFTREE, "span cannot be 0");
        std::unordered_set<std::pair<VectorID, Version>, VectorIDVersionPairHash> seen;
        layers[level - 1].reserve(span + 1);
        pinned_root_version->Search(query, span, layers[level - 1], seen);
    }

    void SearchLayer(const VTYPE* query, size_t span, std::vector<std::vector<ANNVectorInfo>&>& layers,
                     uint8_t level) {
        BufferManager* bufferMgr = BufferManager::GetInstance();
        FatalAssert(bufferMgr != nullptr, LOG_TAG_DIVFTREE, "BufferManager is not initialized.");
        FatalAssert(level < layers.size(), LOG_TAG_DIVFTREE, "level out of bounds!");
        FatalAssert((uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_DIVFTREE, "level out of bounds!");
        FatalAssert(layers[level - 1].empty(), LOG_TAG_DIVFTREE, "next level should be empty!");
        layers[level - 1].reserve(span + 1);
        std::unordered_set<std::pair<VectorID, Version>, VectorIDVersionPairHash> seen;
        for (size_t i = 0; i < layers[level].size(); ++i) {
            DIVFTreeVertex* vertex =
                static_cast<DIVFTreeVertex*>(bufferMgr->
                    ReadAndPinVertex(layers[level][i].id, layers[level][i].version));
            CHECK_NOT_NULLPTR(vertex, LOG_TAG_DIVFTREE);
            vertex->Search(query, span, layers[level - 1], seen);
            vertex->Unpin();
        }
        CLOG(LOG_LEVEL_PANIC, LOG_TAG_NOT_IMPLEMENTED, "not implemented yet");
    }

    RetStatus ANNSearch(const VTYPE* query, size_t k, uint8_t internal_node_search_span, uint8_t leaf_node_search_span,
                        uint8_t start_level, uint8_t end_level, SortType sort_type,
                        std::vector<std::vector<ANNVectorInfo>&>& layers, DIVFTreeVertex* pinned_root_version) {
        BufferManager* bufferMgr = BufferManager::GetInstance();
        RetStatus rs = RetStatus::Success();
        FatalAssert(bufferMgr != nullptr, LOG_TAG_DIVFTREE, "BufferManager is not initialized.");
        CHECK_NOT_NULLPTR(pinned_root_version, LOG_TAG_DIVFTREE);
        /*
         * Since a version of the root is pinned, we basicaly have an MVCC snapshop of the
         * database based on that root version and it's children are not deleted unless they are unpinned or empty
         */
        FatalAssert(pinned_root_version->attr.centroid_id._level >= start_level, LOG_TAG_DIVFTREE,
                    "start_level should be lower than or equal to root level!");
        FatalAssert(end_level < start_level, LOG_TAG_DIVFTREE, "last level cannot be higher than start level!");

        uint8_t current_level = start_level;
        uint8_t span;
        while (current_level > end_level) {
            uint8_t next_level = current_level - 1;
            if (next_level == end_level) {
                span = k;
            } else if ((uint64_t)next_level > VectorID::LEAF_LEVEL) {
                span = internal_node_search_span;
            } else {
                FatalAssert((uint64_t)next_level == VectorID::LEAF_LEVEL, LOG_TAG_DIVFTREE,
                            "since current cannot be lower than end level, "
                            "if it is at vector level, the first condition will handle it.");
                span = leaf_node_search_span;
            }
            FatalAssert(!layers[current_level].empty(), LOG_TAG_DIVFTREE,
                        "current level cannot be empty!");
            FatalAssert(layers[next_level].empty(), LOG_TAG_DIVFTREE,
                        "next level should be empty!");
            FatalAssert(span > 0, LOG_TAG_DIVFTREE,
                        "span should be at least 1");
            if (pinned_root_version->attr.centroid_id._level == current_level) {
                FatalAssert(layers[current_level].size() == 1 &&
                            layers[current_level][0].id == pinned_root_version->attr.centroid_id &&
                            layers[current_level][0].version == pinned_root_version->attr.version, LOG_TAG_DIVFTREE,
                            "the highest level should only contain the pinned version of the root");
                SearchRoot(query, span, layers, pinned_root_version);
            } else {
                SearchLayer(query, span, layers, current_level);
            }

            if (layers[next_level].empty()) {
                layers[current_level].clear();
                if (current_level == pinned_root_version->attr.centroid_id._level) {
                    return RetStatus{.stat=RetStatus::FAIL, .message=nullptr};
                }
                ++current_level;
                continue;
            }
            --current_level;
        }

        if (sort_type != SortType::Unsorted) {
            std::sort_heap(layers[end_level].begin(), layers[end_level].end(), attr.reverseSimilarityComparator);
            if (sort_type == SortType::DecreasingSimilarity) {
                std::reverse(layers[end_level].begin(), layers[end_level].end());
            }
        }
        return RetStatus::Success();
    }

TESTABLE;
};

};

#endif