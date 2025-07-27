#ifndef DIVFTREE_BUFFER_H_
#define DIVFTREE_BUFFER_H_

#include "interface/buffer.h"
#include "interface/divftree.h"

#include <memory>
#include <atomic>

namespace divftree {

enum VersionUpdateType {
    UPDATE_COMPACT, // compaction
    UPDATE_MERGE, // merge and may also have compaction. parent was node ? with version
    UPDATE_SPLIT, // split into nodes[] with versions[]

    // Todo: should we even create new entries for these cases or should we just use pin to delete them?
    UPDATE_RELOCATE, // migration to cluster id with version to cluster id with version
    UPDATE_DELETE,

    NUM_VERSION_UPDATE_TYPES
}

struct VersionUpdateInfo {
    VersionUpdateType type;
    std::experimental::atomic_shared_ptr<BufferVectorEntry> a;
    union {

        struct {
            std::shared_ptr<BufferVectorEntry> parent; // new entry that is created after split/merge
        } merge;

        struct {
            std::vector<std::shared_ptr<BufferVectorEntry>> vertices;
        } split;

        struct {
            VectorID from_cluster_id; // cluster id from which the vector was migrated
            VectorID to_cluster_id; // cluster id to which the vector was migrated
        } relocate;
    };
}

struct BufferVectorEntry {
    const uint64_t version; // version of the vector
    const std::atomic<Address> container_address; // valid if not root: the address of the cluster containing the vector
    const std::atomic<uint16_t> entry_offset; // valid if not root: offset of the cluster entry in the vertex containing the vector
    const Address cluster_address; // If it is a centroid -> is the address of the cluster with that id/ if a vector, address of the container leaf

    /* We also need to know what happend in the previous version i.e if splitted to which nodes with which versions? if merged who was the parent and version */
    std::atomic<BufferVectorEntry*> previous_version;
    /* todo: should I use shared_ptr instead and do not have a list of previous versions? */
    std::atomic<uint64_t> pin; // as long as this entry is the head of the list, it will have at least 1 pin
    std::atomic<bool> deleted;

    BufferVectorEntry(uint64_t ver, Address cont_addr, uint16_t entry_offs, Address clust_addr,
                      BufferVectorEntry* prev_version = nullptr, bool empty = false)
        : version(ver), container_address(cont_addr), entry_offset(entry_offs), cluster_address(clust_addr),
          previous_version(prev_version), pin(1), deleted(empty) {}
};
class BufferManager : public BufferManagerInterface {
// TODO: reuse deleted IDs
public:
    ~BufferManager() override {
        if (!directory.empty()) {
            Shutdown();
        }
    }

    RetStatus Init() override {
        FatalAssert(directory.empty(), LOG_TAG_BUFFER, "Buffer already initialized");

        /* Create the vector level -> level 0 */
        directory.emplace_back();
        return RetStatus::Success();
    }

    RetStatus Shutdown() override {
        FatalAssert(!directory.empty(), LOG_TAG_BUFFER, "Buffer not initialized");
        for (size_t level = directory.size() - 1; level > 0; --level) {
            for (VectorInfo& vec : directory[level]) {
                if (vec.cluster_address != INVALID_ADDRESS) {
                    static_cast<DIVFTreeVertexInterface*>(vec.cluster_address)->~DIVFTreeVertexInterface();
                    free(vec.cluster_address);
                    vec.cluster_address = INVALID_ADDRESS;
                }
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

    std::vector<std::vector<std::atomic<BufferVectorEntry*>>> directory;

TESTABLE;
};

};

#endif