#ifndef COPPER_BUFFER_H_
#define COPPER_BUFFER_H_

#include "common.h"
#include "vector_utils.h"

#ifdef TESTING
namespace UT {
class Test;
};
#endif

namespace copper {

template <typename T, uint16_t _DIM, uint16_t _MIN_SIZE, uint16_t _MAX_SIZE,
            typename DIST_TYPE, typename _DIST> class Copper_Node;

struct VectorInfo {
    Address vector_address; // valid if not root: points to the vector data
    Address cluster_address; // If it is a centroid -> is the address of the cluster with that id/ if a vector, address of the container leaf

    VectorInfo() : vector_address(INVALID_ADDRESS), cluster_address(INVALID_ADDRESS) {}
    VectorInfo(Address data, Address cluster) : vector_address(data), cluster_address(cluster) {}
};


template <typename T, uint16_t _DIM, uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX,
        typename DIST_TYPE, typename _DIST>
class Buffer_Manager {
// TODO: reuse deleted IDs
public:
    typedef Copper_Node<T, _DIM, KI_MIN, KI_MAX, DIST_TYPE, _DIST> Internal_Node;
    typedef Copper_Node<T, _DIM, KL_MIN, KL_MAX, DIST_TYPE, _DIST> Leaf_Node;

    inline Internal_Node* Get_Node(VectorID node_id) {
        FatalAssert(node_id != INVALID_VECTOR_ID, LOG_TAG_BUFFER, "Invalid Node Id");
        FatalAssert(node_id.Is_Internal_Node(), LOG_TAG_BUFFER, "Node ID:%lu is not an internal node", node_id._id);
        FatalAssert(directory.size() > node_id._level, LOG_TAG_BUFFER, "Node ID:%lu level is out of bounds. max_level:%lu", node_id._id, directory.size());
        FatalAssert(directory[node_id._level].size() > node_id._val, LOG_TAG_BUFFER, "Node ID:%lu val is out of bounds. max_val:%lu", node_id._id, directory[node_id._level].size());
        FatalAssert(directory[node_id._level][node_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Node not found in the buffer. Node ID:%lu", node_id._id);
        
        Internal_Node* node = (Internal_Node*)(directory[node_id._level][node_id._val].cluster_address);
        FatalAssert(node->_centroid_id == node_id, LOG_TAG_BUFFER, "Mismatch in ID. Base ID:%lu, Found ID:%lu", node_id._id, directory[node_id._level][node_id._val]->_centroid_id._id);
        
        return node;
    }

    inline Leaf_Node* Get_Leaf(VectorID leaf_id) {
        FatalAssert(leaf_id != INVALID_VECTOR_ID, LOG_TAG_BUFFER, "Invalid Leaf Id");
        FatalAssert(leaf_id.Is_Leaf(), LOG_TAG_BUFFER, "Leaf ID:%lu is not a leaf", leaf_id._id);
        FatalAssert(directory.size() > leaf_id._level, LOG_TAG_BUFFER, "Leaf ID:%lu level is out of bounds. max_level:%lu", leaf_id._id, directory.size());
        FatalAssert(directory[leaf_id._level].size() > leaf_id._val, LOG_TAG_BUFFER, "Leaf ID:%lu val is out of bounds. max_val:%lu", leaf_id._id, directory[leaf_id._level].size());
        FatalAssert(directory[leaf_id._level][leaf_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Leaf not found in the buffer. Leaf ID:%lu", leaf_id._id);
        
        Leaf_Node* leaf = (Leaf_Node*)(directory[leaf_id._level][leaf_id._val].cluster_address);
        FatalAssert(leaf->_centroid_id == leaf_id, LOG_TAG_BUFFER, "Mismatch in ID. Base ID:%lu, Found ID:%lu", leaf_id._id, directory[leaf_id._level][leaf_id._val]->_centroid_id._id);
        
        return leaf;
    }

    inline Leaf_Node* Get_Container_Leaf(VectorID vec_id) {
        FatalAssert(vec_id != INVALID_VECTOR_ID, LOG_TAG_BUFFER, "Invalid Vector Id");
        FatalAssert(vec_id.Is_Vector(), LOG_TAG_BUFFER, "Vector ID:%lu is not a vector", vec_id._id);
        FatalAssert(directory.size() > vec_id._level, LOG_TAG_BUFFER, "Vector ID:%lu level is out of bounds. max_level:%lu", vec_id._id, directory.size());
        FatalAssert(directory[vec_id._level].size() > vec_id._val, LOG_TAG_BUFFER, "Vector ID:%lu val is out of bounds. max_val:%lu", vec_id._id, directory[vec_id._level].size());
        FatalAssert(directory[vec_id._level][vec_id._val].cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Leaf not found in the buffer. Vector ID:%lu", vec_id._id);

        Leaf_Node* leaf = (Leaf_Node*)(directory[vec_id._level][vec_id._val].cluster_address);
        FatalAssert(leaf->_centroid_id != INVALID_VECTOR_ID, LOG_TAG_BUFFER,  "Invalid Leaf ID. Vector ID:%lu", vec_id._id);
        FatalAssert(leaf->_centroid_id.Is_Leaf(), LOG_TAG_BUFFER,  "Cluster %lu is not a leaf. Vector ID:%lu", leaf->_centroid_id, vec_id._id);
        FatalAssert(leaf->Contains(vec_id), LOG_TAG_BUFFER, "Parent leaf:%lu dose not contain the vector:%lu", leaf->_centroid_id, vec_id._id);
        
        return leaf;
    }

    inline Vector<T, _DIM> Get_Vector(VectorID id) {
        FatalAssert(id != INVALID_VECTOR_ID, LOG_TAG_BUFFER, "Invalid Vector Id");
        FatalAssert(directory.size() > id._level, LOG_TAG_BUFFER, "Vector ID:%lu level is out of bounds. max_level:%lu", id._id, directory.size());
        FatalAssert(directory[id._level].size() > id._val, LOG_TAG_BUFFER, "Vector ID:%lu val is out of bounds. max_val:%lu", id._id, directory[id._level].size());
        FatalAssert(directory[id._level][id._val].vector_address != INVALID_ADDRESS , LOG_TAG_BUFFER, "Vector not found in the buffer. Vector ID:%lu", id._id);

        return Vector<T, _DIM>(directory[vec_id._level][vec_id._val].vector_address, false);
    }

    inline bool In_Buffer(VectorID id) {
        FatalAssert(id != INVALID_VECTOR_ID, LOG_TAG_BUFFER, "Invalid Vector Id");
        if (directory.size() <= id._level || directory[id._level].size() <= id._val) {
            return false;
        }

        return true;
    }

    inline VectorID Record_Root() {
        VectorID _id = Next_ID(directory.size());       
        directory.emplace_back();
        directory[id._level].emplace_back(INVALID_ADDRESS, INVALID_ADDRESS);
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
        FatalAssert(id != INVALID_VECTOR_ID, LOG_TAG_BUFFER, "Invalid Vector Id");
        FatalAssert(vector_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Invalidating vector address. Vector ID:%lu", id._id); // TODO: reconsider when adding merge logic
        FatalAssert(In_Buffer(id), LOG_TAG_BUFFER, "Vector does not exist in the buffer. Vector ID:%lu", id._id);
        vectors[id._level][id._val].vector_address = vector_address;
        return rs;
    }

    inline RetStatus UpdateClusterAddress(VectorID id, Address cluster_address) {
        RetStatus rs = RetStatus::Success();
        FatalAssert(id != INVALID_VECTOR_ID, LOG_TAG_BUFFER, "Invalid Vector Id");
        FatalAssert(cluster_address != INVALID_ADDRESS, LOG_TAG_BUFFER, "Invalidating cluster address. Vector ID:%lu", id._id);
        FatalAssert(In_Buffer(id), LOG_TAG_BUFFER, "Vector does not exist in the buffer. Vector ID:%lu", id._id);
        vectors[id._level][id._val].cluster_address = cluster_address;
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

protected:
    inline VectorID Next_ID(uint8_t level) {
        FatalAssert(directory.size() > 1, LOG_TAG_BUFFER, "Height of the tree should be at least two but is %hhu.", directory.size());
        FatalAssert((level < directory.size()) || (level == directory.size() && !directory.back().empty()), LOG_TAG_BUFFER, "Input level(%hhu) should be less than height of the tree(%hhu)."
                , level, directory.size());

        VectorID _id;
        _id._creator_node_id = 0; // todo for disaggregated
        _id._level = level;
        if (level == directory.size()) {
            _id._val = 0;
        }
        else {
            if (directory[level].size() > VectorID::MAX_ID_PER_LEVEL) {
                CLOG(LOG_LEVEL_PANIC, LOG_TAG_BUFFER, "level %hhu is full.", level);
            }
            _id._val = directory[level].size();
        }

        if (_id == INVALID_VECTOR_ID) {
            _id._val++;
        }
        return _id;
    }

    std::vector<std::vector<VectorInfo>> directory;
    
friend class UT::Test;
};

};

#endif