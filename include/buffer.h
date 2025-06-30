#ifndef COPPER_BUFFER_H_
#define COPPER_BUFFER_H_

#include "interface/buffer.h"
#include "interface/copper.h"

namespace copper {

struct VectorInfo {
    Address vector_address; // valid if not root: points to the vector data
    Address cluster_address; // If it is a centroid -> is the address of the cluster with that id/ if a vector, address of the container leaf

    VectorInfo() : vector_address(INVALID_ADDRESS), cluster_address(INVALID_ADDRESS) {}
    VectorInfo(Address data, Address cluster) : vector_address(data), cluster_address(cluster) {}
};
class BufferManager : public BufferManagerInterface {
// TODO: reuse deleted IDs
public:
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
                    static_cast<CopperNodeInterface*>(vec.cluster_address)->~CopperNodeInterface();
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

    CopperNodeInterface* GetNode(VectorID node_id) override {
        CHECK_VECTORID_IS_VALID(node_id, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_CENTROID(node_id, LOG_TAG_BUFFER);
        FatalAssert(directory.size() > node_id._level, LOG_TAG_BUFFER, "Level is out of bounds. NodeID="
                    VECTORID_LOG_FMT ", max_level:%lu", VECTORID_LOG(node_id), directory.size());
        FatalAssert(directory[node_id._level].size() > node_id._val, LOG_TAG_BUFFER, "NodeID val is out of bounds. "
                    VECTORID_LOG_FMT ", max_val:%lu", VECTORID_LOG(node_id), directory[node_id._level].size());
        FatalAssert(directory[node_id._level][node_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER,
                    "Node not found in the buffer. NodeID=" VECTORID_LOG_FMT, VECTORID_LOG(node_id));

        CopperNodeInterface* node = static_cast<CopperNodeInterface*>
                                        (directory[node_id._level][node_id._val].cluster_address);
        FatalAssert(node->CentroidID() == node_id, LOG_TAG_BUFFER, "Mismatch in ID. BaseID=" VECTORID_LOG_FMT
                    ", Found ID=" VECTORID_LOG_FMT, VECTORID_LOG(node_id), VECTORID_LOG(node->CentroidID()));

        return node;
    }

    CopperNodeInterface* GetContainerLeaf(VectorID vec_id) {
        CHECK_VECTORID_IS_VALID(vec_id, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_VECTOR(vec_id, LOG_TAG_BUFFER);
        FatalAssert(directory.size() > vec_id._level, LOG_TAG_BUFFER, "Vector ID:%lu level is out of bounds."
                    " max_level:%lu", vec_id._id, directory.size());
        FatalAssert(directory[vec_id._level].size() > vec_id._val, LOG_TAG_BUFFER, "Vector ID:%lu val is"
                    " out of bounds. max_val:%lu", vec_id._id, directory[vec_id._level].size());
        FatalAssert(directory[vec_id._level][vec_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER,
                    "Leaf not found in the buffer. Vector ID:%lu", vec_id._id);

        CopperNodeInterface* leaf = static_cast<CopperNodeInterface*>
                                        (directory[vec_id._level][vec_id._val].cluster_address);

        CHECK_NOT_NULLPTR(leaf, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_VALID(leaf->CentroidID(), LOG_TAG_BUFFER);
        CHECK_NODE_IS_LEAF(leaf, LOG_TAG_BUFFER);
        FatalAssert(leaf->Contains(vec_id), LOG_TAG_BUFFER, "Parent leaf " VECTORID_LOG_FMT
                    " dose not contain the vector" VECTORID_LOG_FMT,
                    VECTORID_LOG(leaf->CentroidID()), VECTORID_LOG(vec_id));

        return leaf;
    }

    Vector GetVector(VectorID id) {
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_VECTOR(id, LOG_TAG_BUFFER);
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
        directory[_id._level].emplace_back(INVALID_ADDRESS, INVALID_ADDRESS);
        return _id;
    }

    // todo batch record
    VectorID RecordVector(uint8_t level) {
        FatalAssert(directory.size() >= 2, LOG_TAG_BUFFER, "Tree should have a height of at least 2");
        FatalAssert(directory.size() - 1 > level, LOG_TAG_BUFFER, "Level is out of bounds. level=%hhu, max_level:%lu",
                    level, directory.size());
        FatalAssert(((level == 0) || (!directory[level - 1].empty())), LOG_TAG_BUFFER, "last level cannot be empty");
        VectorID _id = NextID(level);
        directory[level].emplace_back(INVALID_ADDRESS, INVALID_ADDRESS);
        return _id;
    }

    RetStatus UpdateVectorAddress(VectorID id, Address vector_address) {
        RetStatus rs = RetStatus::Success();
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_BUFFER);
        // TODO: reconsider when adding merge logic
        FatalAssert(vector_address != INVALID_ADDRESS, LOG_TAG_BUFFER,
                    "Invalidating vector address. VectorID=" VECTORID_LOG_FMT, VECTORID_LOG(id));
        FatalAssert(InBuffer(id), LOG_TAG_BUFFER, "Vector does not exist in the buffer. VectorID="
                    VECTORID_LOG_FMT, VECTORID_LOG(id));
        directory[id._level][id._val].vector_address = vector_address;
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
    //     if (!rs.Is_OK()) {
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
                str += "(" + std::to_string((uint64_t)(vec_info.vector_address)) + ", " + std::to_string((uint64_t)(vec_info.cluster_address)) + ")}";
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
        _id._creator_node_id = 0; // todo for disaggregated
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

    std::vector<std::vector<VectorInfo>> directory;

TESTABLE;
};

};

#endif