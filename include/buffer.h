#ifndef COPPER_BUFFER_H_
#define COPPER_BUFFER_H_

#include "common.h"
#include "vector_utils.h"

namespace copper {

template <typename T, uint16_t _DIM, uint16_t _MIN_SIZE, uint16_t _MAX_SIZE,
            typename DIST_TYPE, typename _DIST> class Copper_Node;

struct VectorInfo {
    Address vector_address; // valid if not root: points to the vector data
    Address cluster_address; // If it is a centroid -> is the address of the cluster with that id/ if a vector, address of the container leaf
    /*
    root -> parent = null, cluster != null
    vector -> parent != null, cluster = null
    anyother vector -> parent != null, cluster != null
    */

    VectorInfo() : vector_address(INVALID_ADDRESS), cluster_address(INVALID_ADDRESS) {}
    VectorInfo(Address data, Address cluster) : vector_address(data), cluster_address(cluster) {}
};

// todo implement buffer
template <typename T, uint16_t _DIM, uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX,
        typename DIST_TYPE, typename _DIST>
class Buffer_Manager {
// TODO: reuse deleted IDs
public:
    typedef Copper_Node<T, _DIM, KI_MIN, KI_MAX, DIST_TYPE, _DIST> Internal_Node;
    typedef Copper_Node<T, _DIM, KL_MIN, KL_MAX, DIST_TYPE, _DIST> Leaf_Node;

    inline Internal_Node* Get_Node(VectorID node_id) {
        AssertFatal(node_id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Node Id");
        AssertFatal(node_id.Is_Internal_Node(), LOG_TAG_BASIC, "Node ID:%lu is not an internal node", node_id._id);
        AssertFatal(directory.size() > node_id._level, LOG_TAG_BASIC, "Node ID:%lu level is out of bounds. max_level:%lu", node_id._id, directory.size());
        AssertFatal(directory[node_id._level].size() > node_id.val, LOG_TAG_BASIC, "Node ID:%lu val is out of bounds. max_val:%lu", node_id._id, directory[node_id._level].size());
        AssertFatal(directory[node_id._level][node_id.val].cluster_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Node not found in the buffer. Node ID:%lu", node_id._id);
        
        Internal_Node* node = (Internal_Node*)(directory[node_id._level][node_id.val].cluster_address);
        AssertFatal(node->_centroid_id == node_id, LOG_TAG_BASIC, "Mismatch in ID. Base ID:%lu, Found ID:%lu", node_id._id, directory[node_id._level][node_id.val]->_centroid_id._id);
        
        return node;
    }

    inline Leaf_Node* Get_Leaf(VectorID leaf_id) {
        AssertFatal(leaf_id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Leaf Id");
        AssertFatal(leaf_id.Is_Leaf(), LOG_TAG_BASIC, "Leaf ID:%lu is not a leaf", leaf_id._id);
        AssertFatal(directory.size() > leaf_id._level, LOG_TAG_BASIC, "Leaf ID:%lu level is out of bounds. max_level:%lu", leaf_id._id, directory.size());
        AssertFatal(directory[leaf_id._level].size() > leaf_id.val, LOG_TAG_BASIC, "Leaf ID:%lu val is out of bounds. max_val:%lu", leaf_id._id, directory[leaf_id._level].size());
        AssertFatal(directory[leaf_id._level][leaf_id.val].cluster_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Leaf not found in the buffer. Leaf ID:%lu", leaf_id._id);
        
        Leaf_Node* leaf = (Leaf_Node*)(directory[leaf_id._level][leaf_id.val].cluster_address);
        AssertFatal(leaf->_centroid_id == leaf_id, LOG_TAG_BASIC, "Mismatch in ID. Base ID:%lu, Found ID:%lu", leaf_id._id, directory[leaf_id._level][leaf_id.val]->_centroid_id._id);
        
        return leaf;
    }

    inline Leaf_Node* Get_Container_Leaf(VectorID vec_id) {
        AssertFatal(vec_id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(vec_id.Is_Vector(), LOG_TAG_BASIC, "Vector ID:%lu is not a vector", vec_id._id);
        AssertFatal(directory.size() > vec_id._level, LOG_TAG_BASIC, "Vector ID:%lu level is out of bounds. max_level:%lu", vec_id._id, directory.size());
        AssertFatal(directory[vec_id._level].size() > vec_id.val, LOG_TAG_BASIC, "Vector ID:%lu val is out of bounds. max_val:%lu", vec_id._id, directory[vec_id._level].size());
        AssertFatal(directory[vec_id._level][vec_id.val].cluster_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Leaf not found in the buffer. Vector ID:%lu", vec_id._id);

        Leaf_Node* leaf = (Leaf_Node*)(directory[vec_id._level][vec_id.val].cluster_address);
        AssertFatal(leaf->_centroid_id != INVALID_VECTOR_ID, LOG_TAG_BASIC,  "Invalid Leaf ID. Vector ID:%lu", vec_id._id);
        AssertFatal(leaf->_centroid_id.Is_Leaf(), LOG_TAG_BASIC,  "Cluster %lu is not a leaf. Vector ID:%lu", leaf->_centroid_id, vec_id._id);
        AssertFatal(leaf->Contains(vec_id), LOG_TAG_BASIC, "Parent leaf:%lu dose not contain the vector:%lu", leaf->_centroid_id, vec_id._id);
        
        return leaf;
    }

    inline Vector<T, _DIM> Get_Vector(VectorID id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(directory.size() > id._level, LOG_TAG_BASIC, "Vector ID:%lu level is out of bounds. max_level:%lu", id._id, directory.size());
        AssertFatal(directory[id._level].size() > id.val, LOG_TAG_BASIC, "Vector ID:%lu val is out of bounds. max_val:%lu", id._id, directory[id._level].size());
        AssertFatal(directory[id._level][id.val].vector_address != INVALID_ADDRESS , LOG_TAG_BASIC, "Vector not found in the buffer. Vector ID:%lu", id._id);

        return Vector<T, _DIM>(directory[vec_id._level][vec_id.val].vector_address, false);
    }

    inline bool In_Buffer(VectorID id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        if (directory.size() <= id._level || directory[id._level].size() <= id.val) {
            return false;
        }

        return true;
    }

    inline VectorID Record_Root(Address node_address) {
        AssertFatal(node_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Invalid node address"); 
        VectorID _id = Next_ID(directory.size());       
        directory.emplace_back();
        directory[id._level].emplace_back(INVALID_ADDRESS, node_address);
        return _id;
    }

    // todo batch record
    inline VectorID Record_Vector(uint8_t level) {
        VectorID _id = Next_ID(level);
        vectors[level].emplace_back(INVALID_ADDRESS, INVALID_ADDRESS);
        return _id;
    }

    inline RetStatus UpdateVectorAddress(VectorID id, Address vector_address) {
        RetStatus rs = RetStatus::Success();
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(vector_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Invalidating vector address. Vector ID:%lu", id._id); // TODO: reconsider when adding merge logic
        AssertFatal(In_Buffer(id), LOG_TAG_BASIC, "Vector does not exist in the buffer. Vector ID:%lu", id._id);
        vectors[id._level][id.val].vector_address = vector_address;
        return rs;
    }

    inline RetStatus UpdateClusterAddress(VectorID id, Address cluster_address) {
        RetStatus rs = RetStatus::Success();
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(cluster_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Invalidating cluster address. Vector ID:%lu", id._id);
        AssertFatal(In_Buffer(id), LOG_TAG_BASIC, "Vector does not exist in the buffer. Vector ID:%lu", id._id);
        vectors[id._level][id.val].cluster_address = cluster_address;
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
    }

    inline void Erase(VectorID id) {
        // todo
    }

protected:
    inline VectorID Next_ID(uint8_t level) {
        AssertFatal(directory.size() > 1, LOG_TAG_DEFAULT, "Height of the tree should be at least two but is %hhu.", directory.size());
        AssertFatal((level < directory.size()) || (level == directory.size() && !directory.back().empty()), LOG_TAG_DEFAULT, "Input level(%hhu) should be less than height of the tree(%hhu)."
                , level, directory.size());

        VectorID _id;
        _id._creator_node_id = 0; // todo for disaggregated
        _id._level = level;
        if (level == directory.size()) {
            _id.val = 0;
        }
        else {
            if (directory[level].size() > VectorID::MAX_ID_PER_LEVEL) {
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_BASIC, "level %hhu is full.", level);
            }
            _id.val = directory[level].size();
        }

        if (_id == INVALID_VECTOR_ID) {
            _id.val++;
        }
        return _id;
    }

    std::vector<std::vector<VectorInfo>> directory;
};

};

#endif