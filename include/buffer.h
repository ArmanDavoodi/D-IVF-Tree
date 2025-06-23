#ifndef COPPER_BUFFER_H_
#define COPPER_BUFFER_H_

#include "common.h"
#include "vector_utils.h"

namespace copper {

struct VectorInfo {
    Address vector_address; // valid if not root: points to the vector data
    Address cluster_address; // If it is a centroid -> is the address of the cluster with that id/ if a vector, address of the container leaf

    VectorInfo() : vector_address(INVALID_ADDRESS), cluster_address(INVALID_ADDRESS) {}
    VectorInfo(Address data, Address cluster) : vector_address(data), cluster_address(cluster) {}
};

template <typename T, uint16_t _DIM, uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX,
          typename DIST_TYPE, template<typename, uint16_t, typename> class _CORE>
class Buffer_Manager {
// TODO: reuse deleted IDs
public:
    typedef Copper_Node<T, _DIM, KI_MIN, KI_MAX, DIST_TYPE, _CORE> Internal_Node;
    typedef Copper_Node<T, _DIM, KL_MIN, KL_MAX, DIST_TYPE, _CORE> Leaf_Node;

    inline RetStatus Init() {
        FatalAssert(directory.empty(), LOG_TAG_BUFFER, "Buffer already initialized");

        /* Create the vector level -> level 0 */
        directory.emplace_back();
        return RetStatus::Success();
    }

    inline RetStatus Shutdown() {
        FatalAssert(!directory.empty(), LOG_TAG_BUFFER, "Buffer not initialized");
        for (size_t level = directory.size() - 1; level > 0; --level) {
            for (VectorInfo& vec : directory[level]) {
                if (vec.cluster_address != INVALID_ADDRESS) {
                    if (level > 1) {
                        delete (Internal_Node*)(vec.cluster_address);
                    }
                    else {
                        delete (Leaf_Node*)(vec.cluster_address);
                    }
                    vec.cluster_address = INVALID_ADDRESS;
                }
            }
            directory[level].clear();
        }
        directory[0].clear();
        directory.clear();
        return RetStatus::Success();
    }

    template<typename NodeType>
    inline NodeType* Get_Node(VectorID node_id) {
        static_assert(std::is_same<NodeType, Internal_Node>::value || std::is_same<NodeType, Leaf_Node>::value,
                  "NodeType must be either Internal_Node or Leaf_Node");

        FatalAssert(node_id.Is_Valid(), LOG_TAG_BUFFER, "Invalid Node Id: " VECTORID_LOG_FMT, VECTORID_LOG(node_id));
        FatalAssert(node_id.Is_Centroid(), LOG_TAG_BUFFER, "ID:%lu is a vector ID", node_id._id);
        FatalAssert(directory.size() > node_id._level, LOG_TAG_BUFFER, "Node ID:%lu level is out of bounds. max_level:%lu", node_id._id, directory.size());
        FatalAssert(directory[node_id._level].size() > node_id._val, LOG_TAG_BUFFER, "Node ID:%lu val is out of bounds. max_val:%lu", node_id._id, directory[node_id._level].size());
        FatalAssert(directory[node_id._level][node_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Node not found in the buffer. Node ID:%lu", node_id._id);

        NodeType* node = (NodeType*)(directory[node_id._level][node_id._val].cluster_address);
        FatalAssert(node->CentroidID() == node_id, LOG_TAG_BUFFER, "Mismatch in ID. Base ID:%lu, Found ID:%lu",
            node_id._id, node->CentroidID()._id);

        return node;
    }

    /* inline Leaf_Node* Get_Leaf(VectorID leaf_id) {
        FatalAssert(leaf_id.Is_Valid(), LOG_TAG_BUFFER, "Invalid Leaf Id: " VECTORID_LOG_FMT, VECTORID_LOG(leaf_id));
        FatalAssert(leaf_id.Is_Leaf(), LOG_TAG_BUFFER, "Leaf ID:%lu is not a leaf", leaf_id._id);
        FatalAssert(directory.size() > leaf_id._level, LOG_TAG_BUFFER, "Leaf ID:%lu level is out of bounds. max_level:%lu", leaf_id._id, directory.size());
        FatalAssert(directory[leaf_id._level].size() > leaf_id._val, LOG_TAG_BUFFER, "Leaf ID:%lu val is out of bounds. max_val:%lu", leaf_id._id, directory[leaf_id._level].size());
        FatalAssert(directory[leaf_id._level][leaf_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Leaf not found in the buffer. Leaf ID:%lu", leaf_id._id);

        Leaf_Node* leaf = (Leaf_Node*)(directory[leaf_id._level][leaf_id._val].cluster_address);
        FatalAssert(leaf->_centroid_id == leaf_id, LOG_TAG_BUFFER, "Mismatch in ID. Base ID:%lu, Found ID:%lu", leaf_id._id, leaf->_centroid_id._id);

        return leaf;
    } */

    inline Leaf_Node* Get_Container_Leaf(VectorID vec_id) {
        FatalAssert(vec_id.Is_Valid(), LOG_TAG_BUFFER, "Invalid Vector Id: " VECTORID_LOG_FMT, VECTORID_LOG(vec_id));
        FatalAssert(vec_id.Is_Vector(), LOG_TAG_BUFFER, "Vector ID:%lu is not a vector", vec_id._id);
        FatalAssert(directory.size() > vec_id._level, LOG_TAG_BUFFER, "Vector ID:%lu level is out of bounds. max_level:%lu", vec_id._id, directory.size());
        FatalAssert(directory[vec_id._level].size() > vec_id._val, LOG_TAG_BUFFER, "Vector ID:%lu val is out of bounds. max_val:%lu", vec_id._id, directory[vec_id._level].size());
        FatalAssert(directory[vec_id._level][vec_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Leaf not found in the buffer. Vector ID:%lu", vec_id._id);

        Leaf_Node* leaf = (Leaf_Node*)(directory[vec_id._level][vec_id._val].cluster_address);
        FatalAssert(leaf->_centroid_id.Is_Valid(), LOG_TAG_BUFFER,  "Invalid Leaf ID. Vector ID:%lu", vec_id._id);
        FatalAssert(leaf->_centroid_id.Is_Leaf(), LOG_TAG_BUFFER,  "Cluster %lu is not a leaf. Vector ID:%lu", leaf->_centroid_id, vec_id._id);
        FatalAssert(leaf->Contains(vec_id), LOG_TAG_BUFFER, "Parent leaf:%lu dose not contain the vector:%lu", leaf->_centroid_id, vec_id._id);

        return leaf;
    }

    inline Vector<T, _DIM> Get_Vector(VectorID id) {
        FatalAssert(id.Is_Valid(), LOG_TAG_BUFFER, "Invalid Vector Id: " VECTORID_LOG_FMT, VECTORID_LOG(id));
        FatalAssert(directory.size() > id._level, LOG_TAG_BUFFER, "Vector ID:%lu level is out of bounds. max_level:%lu", id._id, directory.size());
        FatalAssert(directory[id._level].size() > id._val, LOG_TAG_BUFFER, "Vector ID:%lu val is out of bounds. max_val:%lu", id._id, directory[id._level].size());
        FatalAssert(directory[id._level][id._val].vector_address != INVALID_ADDRESS , LOG_TAG_BUFFER, "Vector not found in the buffer. Vector ID:%lu", id._id);

        return Vector<T, _DIM>(directory[id._level][id._val].vector_address, false);
    }

    inline bool In_Buffer(VectorID id) {
        FatalAssert(id.Is_Valid(), LOG_TAG_BUFFER, "Invalid Vector Id: " VECTORID_LOG_FMT, VECTORID_LOG(id));
        if (directory.size() <= id._level || directory[id._level].size() <= id._val) {
            return false;
        }

        return true;
    }

    inline VectorID Record_Root() {
        FatalAssert(directory.size() > 0, LOG_TAG_BUFFER, "tree should be initialized");
        FatalAssert(((directory.size() == 1) || !(directory.back().empty())), LOG_TAG_BUFFER, "last level cannot be empty");
        VectorID _id = Next_ID(directory.size());
        directory.emplace_back();
        directory[_id._level].emplace_back(INVALID_ADDRESS, INVALID_ADDRESS);
        return _id;
    }

    // todo batch record
    inline VectorID Record_Vector(uint8_t level) {
        FatalAssert(directory.size() >= 2, LOG_TAG_BUFFER, "Tree should have a height of at least 2");
        FatalAssert(directory.size() - 1 > level, LOG_TAG_BUFFER, "Vector ID:%lu level is out of bounds. max_level:%lu",
                    level, directory.size());
        FatalAssert(((level == 0) || (!directory[level - 1].empty())), LOG_TAG_BUFFER, "last level cannot be empty");
        VectorID _id = Next_ID(level);
        directory[level].emplace_back(INVALID_ADDRESS, INVALID_ADDRESS);
        return _id;
    }

    inline RetStatus UpdateVectorAddress(VectorID id, Address vector_address) {
        RetStatus rs = RetStatus::Success();
        FatalAssert(id.Is_Valid(), LOG_TAG_BUFFER, "Invalid Vector Id: " VECTORID_LOG_FMT, VECTORID_LOG(id));
        FatalAssert(vector_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Invalidating vector address. Vector ID:%lu", id._id); // TODO: reconsider when adding merge logic
        FatalAssert(In_Buffer(id), LOG_TAG_BUFFER, "Vector does not exist in the buffer. Vector ID:%lu", id._id);
        directory[id._level][id._val].vector_address = vector_address;
        return rs;
    }

    inline RetStatus UpdateClusterAddress(VectorID id, Address cluster_address) {
        RetStatus rs = RetStatus::Success();
        FatalAssert(id.Is_Valid(), LOG_TAG_BUFFER, "Invalid Vector Id: " VECTORID_LOG_FMT, VECTORID_LOG(id));
        FatalAssert(cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Invalidating cluster address. Vector ID:%lu", id._id);
        FatalAssert(In_Buffer(id), LOG_TAG_BUFFER, "Vector does not exist in the buffer. Vector ID:%lu", id._id);
        directory[id._level][id._val].cluster_address = cluster_address;
        return rs;
    }

    // todo batch update
    inline RetStatus Update(VectorID id, Address vector_address, Address cluster_address) {
        RetStatus rs = RetStatus::Success();
        rs = UpdateVectorAddress(id, vector_address);
        if (!rs.Is_OK()) {
            return rs;
        }
        rs = UpdateClusterAddress(id, cluster_address); // todo: revert change of vector address if rs fails here
        return rs;
    }

    inline void Evict(VectorID id) {
        // todo
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Eviction is not implemented");
    }

    inline void Erase(VectorID id) {
        // todo
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Erase is not implemented");
    }

    inline uint64_t Get_Height() {
        return directory.size();
    }

    inline std::string to_string() {
        std::string str = "<Height: " + std::to_string(directory.size()) + ", ";
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
    inline VectorID Next_ID(uint8_t level) {
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

        FatalAssert(_id.Is_Valid(), LOG_TAG_BUFFER, "generated invalid id: " VECTORID_LOG_FMT, VECTORID_LOG(_id));
        return _id;
    }

    std::vector<std::vector<VectorInfo>> directory;

TESTABLE;
};

};

#endif