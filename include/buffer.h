#ifndef DIVFTREE_BUFFER_H_
#define DIVFTREE_BUFFER_H_

#include "interface/buffer.h"
#include "interface/divftree.h"

#include <memory>
#include <atomic>

namespace divftree {

// enum VersionUpdateType {
//     UPDATE_COMPACT, // compaction
//     UPDATE_MERGE, // merge and may also have compaction. parent was node ? with version
//     UPDATE_SPLIT, // split into nodes[] with versions[]

//     // Todo: should we even create new entries for these cases or should we just use pin to delete them?
//     UPDATE_RELOCATE, // migration to cluster id with version to cluster id with version
//     UPDATE_DELETE,

//     NUM_VERSION_UPDATE_TYPES
// }

// struct VersionUpdateInfo {
//     VersionUpdateType type;
//     union {

//         struct {
//             std::shared_ptr<BufferVectorEntry> parent; // new entry that is created after split/merge
//         } merge;

//         struct {
//             std::vector<std::shared_ptr<BufferVectorEntry>> vertices;
//         } split;

//         struct {
//             VectorID from_cluster_id; // cluster id from which the vector was migrated
//             VectorID to_cluster_id; // cluster id to which the vector was migrated
//         } relocate;
//     };
// }

union alignas(16) VectorLocation {
    struct {
        std::atomic<VectorID> containerId;
        std::atomic<uint32_t> containerVersion;
        std::atomic<uint16_t> entryOffset;
    };
    atomic_data128 raw;
};

struct BufferVectorEntry {
    VectorLocation location;

    BufferVectorEntry() : location{.containerId = INVALID_VECTOR_ID,
                                   .containerVersion = 0,
                                   .entryOffset = INVALID_OFFSET} {}
};

  /* 1) BufferEntry:
  *     1.1) SXLock
  *     1.2) CondVar
  *     1.3) Container: VectorID -> 64bit
  *     1.4) EntryOffset: 16bit
  *     1.5) State: 16bit? -> same as the state in the cluster
  *     1.5) ContainerVersion 32bit
  *     1.6) SXSpinLock
  *     1.7) ReaderPin: 64bit
  *     1.8) Ptr: Cluster* -> 64bit
  *     1.9) Log: fixed size list:<OldVersion, NewVersion, UpdateEntry>:
  *             has a 32bit head and a 32bit tail. when a new element is added,
  *             if tail+1 == head(list is full), both head and tail are advanced by 1
  *             and oldest version is overwritten. else tail is advanced by 1.
  *
  */

// constexpr uint8_t BUFFER_UPDATE_LOG_SIZE = 10;
// constexpr uint8_t BUFFER_OLD_VERSION_LIST_SIZE = 3;

enum BufferVertexEntryState : uint8_t {
    CLUSTER_INVALID = 0,
    CLUSTER_STABLE = 1,
    CLUSTER_FULL = 2,
    CLUSTER_DELETE_IN_PROGRESS = 3
};

/*
 * This needs to be prtected byt the header lock because we do not have 16Bytes FAA. As a result, if we
 * if we want to do this without locking, we have to use 16Byte CAS which causes a lot of contention on the
 * pin.
 */
struct ClusterPtr {
    std::atomic<uint64_t> pin;
    DIVFTreeVertexInterface* clusterPtr;
};
struct VersionedClusterPtr {
    std::atomic<uint64_t> versionPin;
    ClusterPtr clusterPtr;
};

struct BufferVertexEntry {
    BufferVectorEntry centroidMeta;
    std::atomic<BufferVertexEntryState> state;
    SXSpinLock headerLock;
    SXLock clusterLock;
    CondVar condVar;
    Version currentVersion;
    std::unordered_map<Version, VersionedClusterPtr> oldVersions;

    BufferVertexEntry(DIVFTreeVertexInterface* cluster) : centroidMeta(), state(CLUSTER_INVALID), currentVersion(0) {
        oldVersions[0] = VersionedClusterPtr{1, ClusterPtr{0, cluster}};
        /*
         * set version pin to 1 as BufferVertexEntry is referencing it with currentVersion and
         * set pin to 0 as no one is using it yet
         */
    }
};

class BufferManager : public BufferManagerInterface {
// TODO: reuse deleted IDs
public:
    ~BufferManager() override {
        if (!clusterDirectory.empty()) {
            Shutdown();
        }
    }

    DIVFTreeVertexInterface *AllocateMemoryForVertex(uint8_t level) {
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "AllocateMemoryForVertex not implemented");
        return nullptr;
    }

    RetStatus Init() override {
        FatalAssert(clusterDirectory.empty(), LOG_TAG_BUFFER, "Buffer already initialized");
        clusterDirectory.emplace_back();
        return RetStatus::Success();
    }

    RetStatus Shutdown() override {
        FatalAssert(!clusterDirectory.empty(), LOG_TAG_BUFFER, "Buffer not initialized");
        // TODO: nobody should be accessing the buffer or any clusters/pages at this point!!!
        for (size_t level = clusterDirectory.size() - 1; level > 0; --level) {
            for (BufferVertexEntry& entry : directory[level]) {
                entry.clusterLock.Lock(SX_EXCLUSIVE);
            }
            directory[level].clear();
        }
        directory[0].clear();
        directory.clear();
        return RetStatus::Success();
    }

    DIVFTreeVertexInterface* GetVertex(VectorID vertex_id) override {
        CHECK_VECTORID_IS_VALID(vertex_id, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_CENTROID(vertex_id, LOG_TAG_BUFFER);
        FatalAssert(directory.size() > vertex_id._level, LOG_TAG_BUFFER, "Level is out of bounds. VertexID="
                    VECTORID_LOG_FMT ", max_level:%lu", VECTORID_LOG(vertex_id), directory.size());
        FatalAssert(directory[vertex_id._level].size() > vertex_id._val, LOG_TAG_BUFFER, "VertexID val is out of bounds. "
                    VECTORID_LOG_FMT ", max_val:%lu", VECTORID_LOG(vertex_id), directory[vertex_id._level].size());
        FatalAssert(directory[vertex_id._level][vertex_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER,
                    "Vertex not found in the buffer. VertexID=" VECTORID_LOG_FMT, VECTORID_LOG(vertex_id));

        DIVFTreeVertexInterface* vertex = static_cast<DIVFTreeVertexInterface*>
                                        (directory[vertex_id._level][vertex_id._val].cluster_address);
        FatalAssert(vertex->CentroidID() == vertex_id, LOG_TAG_BUFFER, "Mismatch in ID. BaseID=" VECTORID_LOG_FMT
                    ", Found ID=" VECTORID_LOG_FMT, VECTORID_LOG(vertex_id), VECTORID_LOG(vertex->CentroidID()));

        return vertex;
    }

    DIVFTreeVertexInterface* GetContainerLeaf(VectorID vec_id) {
        CHECK_VECTORID_IS_VALID(vec_id, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_VECTOR(vec_id, LOG_TAG_BUFFER);
        FatalAssert(directory.size() > vec_id._level, LOG_TAG_BUFFER, "Vector ID:%lu level is out of bounds."
                    " max_level:%lu", vec_id._id, directory.size());
        FatalAssert(directory[vec_id._level].size() > vec_id._val, LOG_TAG_BUFFER, "Vector ID:%lu val is"
                    " out of bounds. max_val:%lu", vec_id._id, directory[vec_id._level].size());
        FatalAssert(directory[vec_id._level][vec_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER,
                    "Leaf not found in the buffer. Vector ID:%lu", vec_id._id);

        DIVFTreeVertexInterface* leaf = static_cast<DIVFTreeVertexInterface*>
                                        (directory[vec_id._level][vec_id._val].cluster_address);

        CHECK_NOT_NULLPTR(leaf, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_VALID(leaf->CentroidID(), LOG_TAG_BUFFER);
        CHECK_VERTEX_IS_LEAF(leaf, LOG_TAG_BUFFER);
        FatalAssert(leaf->Contains(vec_id), LOG_TAG_BUFFER, "Parent leaf " VECTORID_LOG_FMT
                    " dose not contain the vector" VECTORID_LOG_FMT,
                    VECTORID_LOG(leaf->CentroidID()), VECTORID_LOG(vec_id));

        return leaf;
    }

    Vector GetVector(VectorID id) {
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_BUFFER);
        FatalAssert(directory.size() > id._level, LOG_TAG_BUFFER, "Level is out of bounds. " VECTORID_LOG_FMT
                    ", max_level=%lu", VECTORID_LOG(id), directory.size());
        FatalAssert(directory[id._level].size() > id._val, LOG_TAG_BUFFER, "Val is out of bounds. " VECTORID_LOG_FMT
                    ", max_val:%lu", VECTORID_LOG(id), directory[id._level].size());
        FatalAssert(directory[id._level][id._val].vector_address != INVALID_ADDRESS , LOG_TAG_BUFFER,
                    "Vector not found in the buffer. VectorID=" VECTORID_LOG_FMT, VECTORID_LOG(id));

        return Vector(directory[id._level][id._val].vector_address);
    }

    bool InBuffer(VectorID id) {
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_BUFFER);
        if (directory.size() <= id._level || directory[id._level].size() <= id._val) {
            return false;
        }

        return true;
    }

    VectorID RecordRoot() {
        FatalAssert(directory.size() > 0, LOG_TAG_BUFFER, "tree should be initialized");
        FatalAssert(((directory.size() == 1) || !(directory.back().empty())), LOG_TAG_BUFFER,
                    "last level cannot be empty");
        VectorID _id = NextID(directory.size());
        directory.emplace_back();
        directory[_id._level].emplace_back(nullptr);
        return _id;
    }

    // todo batch record
    VectorID RecordVector(uint8_t level) {
        FatalAssert(directory.size() >= 2, LOG_TAG_BUFFER, "Tree should have a height of at least 2");
        FatalAssert(directory.size() - 1 >= level, LOG_TAG_BUFFER, "Level is out of bounds. level=%hhu, max_level:%lu",
                    level, directory.size());
        FatalAssert(((level == 0) || (!directory[level - 1].empty())), LOG_TAG_BUFFER, "last level cannot be empty");
        VectorID _id = NextID(level);
        directory[level].emplace_back(nullptr);
        return _id;
    }

    RetStatus UpdateVectorInfo(VectorID id, Address cont_addr, Address entry_addr, Address clust_addr) {
        RetStatus rs = RetStatus::Success();
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_BUFFER);
        directory[id._level][id._val].compare_exchange_strong()
        return rs;
    }

    RetStatus UpdateClusterAddress(VectorID id, Address cluster_address) {
        RetStatus rs = RetStatus::Success();
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_BUFFER);
        FatalAssert(cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER,
                    "Invalidating cluster address. VectorID=" VECTORID_LOG_FMT, VECTORID_LOG(id));
        FatalAssert(InBuffer(id), LOG_TAG_BUFFER, "Vector does not exist in the buffer. VectorID="
                    VECTORID_LOG_FMT, VECTORID_LOG(id));
        directory[id._level][id._val].cluster_address = cluster_address;
        return rs;
    }

    // // todo batch update
    // RetStatus Update(VectorID id, Address vector_address, Address cluster_address) {
    //     RetStatus rs = RetStatus::Success();
    //     rs = UpdateVectorAddress(id, vector_address);
    //     if (!rs.IsOK()) {
    //         return rs;
    //     }
    //     rs = UpdateClusterAddress(id, cluster_address); // todo: revert change of vector address if rs fails here
    //     return rs;
    // }

    // void Evict(VectorID id) {
    //     // todo
    //     FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Eviction is not implemented");
    // }

    // void Erase(VectorID id) {
    //     // todo
    //     FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Erase is not implemented");
    // }

    uint64_t GetHeight() {
        return directory.size();
    }

    String ToString() {
        String str = "<Height: " + std::to_string(directory.size()) + ", ";
        str += "Directory:[";
        for (size_t i = 0; i < directory.size(); ++i) {
            str += "Level" + std::to_string(i) + ":[";
            for (size_t j = 0; j < directory[i].size(); ++j) {
                str += "{VectorID:(" + std::to_string(i) + ", ?, " + std::to_string(j) + "), Info:";
                VectorInfo& vec_info = directory[i][j];
                str += String("(%p, %p)}", vec_info.vector_address, vec_info.cluster_address);
                if (j != directory[i].size() - 1) {
                    str += ", ";
                }
            }
            if (i != directory.size() - 1) {
                str += "], ";
            }
            else {
                str += "]";
            }
            str += ">";
        }
        return str;
    }

protected:
    /* bufferM */
    VectorID NextID(uint8_t level) {
        FatalAssert(directory.size() > 0, LOG_TAG_BUFFER, "tree should be initialized");
        FatalAssert(!directory.back().empty() || (level == 1 && directory.size() == 1), LOG_TAG_BUFFER,
                    "last level cannot be empty");
        FatalAssert(level <= directory.size(),
                    LOG_TAG_BUFFER, "Input level(%hhu) should be non-zero and less than or equal to "
                    "the height of the tree(%hhu).",level, directory.size());

        VectorID _id = 0;
        _id._creator_vertex_id = 0; // todo for disaggregated
        _id._level = level;
        if (level == directory.size()) {
            _id._val = 0;
        }
        else {
            if (directory[level].size() >= VectorID::MAX_ID_PER_LEVEL - 1) {
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_BUFFER, "level %hhu is full.", level);
            }
            _id._val = directory[level].size();
        }

        CHECK_VECTORID_IS_VALID(_id, LOG_TAG_BUFFER);
        return _id;
    }

    SXSpinLock bufferMgrLock;
    std::vector<BufferVectorEntry> vectorDirectory;
    std::vector<std::vector<BufferVertexEntry>> clusterDirectory;

TESTABLE;
};

};

#endif