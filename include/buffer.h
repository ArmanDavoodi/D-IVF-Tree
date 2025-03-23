#ifndef COPPER_BUFFER_H_
#define COPPER_BUFFER_H_

#include "common.h"
#include "vector_utils.h"

namespace copper {

template <typename T, uint16_t _DIM, uint16_t _MIN_SIZE, uint16_t _MAX_SIZE,
            typename DIST_TYPE, typename _DIST> class Copper_Node;

struct VectorInfo {
    VectorID parent_id; // valid if not root
    Address vector_address; // valid if not root: points to the vector data
    Address cluster_address; // Valid if it is a centroid
    /*
    root -> parent = null, cluster != null
    vector -> parent != null, cluster = null
    anyother vector -> parent != null, cluster != null
    */

    VectorInfo() : parent_id(INVALID_VECTOR_ID), vector_address(INVALID_ADDRESS), cluster_address(INVALID_ADDRESS) {}
    VectorInfo(VectorID parent, Address data, Address cluster) : parent_id(parent), vector_address(data), cluster_address(cluster) {}
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
        AssertFatal(vectors.size() > node_id._level, LOG_TAG_BASIC, "Node ID:%lu level is out of bounds. max_level:%lu", node_id._id, vectors.size());
        AssertFatal(vectors[node_id._level].size() > node_id.val, LOG_TAG_BASIC, "Node ID:%lu val is out of bounds. max_val:%lu", node_id._id, vectors[node_id._level].size());
        AssertFatal(vectors[node_id._level][node_id.val].cluster_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Node not found in the buffer. Node ID:%lu", node_id._id);
        
        Internal_Node* node = (Internal_Node*)vectors[node_id._level][node_id.val].cluster_address;
        AssertFatal(node->_centroid_id == node_id, LOG_TAG_BASIC, "Mismatch in ID. Base ID:%lu, Found ID:%lu", node_id._id, vectors[node_id._level][node_id.val]->_centroid_id._id);
        
        return node;
    }

    inline Internal_Node* Get_Parent(VectorID node_id) {
        // todo
    }

    inline Leaf_Node* Get_Leaf(VectorID leaf_id) {
        AssertFatal(leaf_id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Leaf Id");
        AssertFatal(leaf_id.Is_Leaf(), LOG_TAG_BASIC, "Leaf ID:%lu is not a leaf", leaf_id._id);
        AssertFatal(vectors.size() > leaf_id._level, LOG_TAG_BASIC, "Leaf ID:%lu level is out of bounds. max_level:%lu", leaf_id._id, vectors.size());
        AssertFatal(vectors[leaf_id._level].size() > leaf_id.val, LOG_TAG_BASIC, "Leaf ID:%lu val is out of bounds. max_val:%lu", leaf_id._id, vectors[leaf_id._level].size());
        AssertFatal(vectors[leaf_id._level][leaf_id.val].cluster_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Leaf not found in the buffer. Leaf ID:%lu", leaf_id._id);
        
        Leaf_Node* leaf = (Leaf_Node*)vectors[leaf_id._level][leaf_id.val].cluster_address;
        AssertFatal(leaf->_centroid_id == leaf_id, LOG_TAG_BASIC, "Mismatch in ID. Base ID:%lu, Found ID:%lu", leaf_id._id, vectors[leaf_id._level][leaf_id.val]->_centroid_id._id);
        
        return leaf;
    }

    inline Leaf_Node* Get_Container_Leaf(VectorID vec_id) {
        AssertFatal(vec_id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(vec_id.Is_Vector(), LOG_TAG_BASIC, "Vector ID:%lu is not a vector", vec_id._id);
        AssertFatal(vectors.size() > vec_id._level, LOG_TAG_BASIC, "Vector ID:%lu level is out of bounds. max_level:%lu", vec_id._id, vectors.size());
        AssertFatal(vectors[vec_id._level].size() > vec_id.val, LOG_TAG_BASIC, "Vector ID:%lu val is out of bounds. max_val:%lu", vec_id._id, vectors[vec_id._level].size());
        AssertFatal(vectors[vec_id._level][vec_id.val].cluster_address == INVALID_ADDRESS, LOG_TAG_BASIC, "Vectors cannot have a cluster address. Vector ID:%lu", vec_id._id);
        AssertFatal(vectors[vec_id._level][vec_id.val].parent_id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Vectors should have a valid parent ID. Vector ID:%lu", vec_id._id);
        AssertFatal(vectors[vec_id._level][vec_id.val].parent_id.Is_Leaf() , LOG_TAG_BASIC, "Vector parent should be a leaf. Vector ID:%lu, Parent ID:%lu", vec_id._id, vectors[vec_id._level][vec_id.val].parent_id);

        Leaf_Node* leaf = Get_Leaf(vectors[vec_id._level][vec_id.val].parent_id);
        AssertFatal(leaf->Contains(vec_id), LOG_TAG_BASIC, "Parent leaf:%lu dose not contain the vector:%lu", leaf->_centroid_id, vec_id._id);
        
        return leaf;
    }

    inline Vector<T, _DIM> Get_Vector(VectorID id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(vectors.size() > id._level, LOG_TAG_BASIC, "Vector ID:%lu level is out of bounds. max_level:%lu", id._id, vectors.size());
        AssertFatal(vectors[id._level].size() > id.val, LOG_TAG_BASIC, "Vector ID:%lu val is out of bounds. max_val:%lu", id._id, vectors[id._level].size());
        AssertFatal(vectors[id._level][id.val].vector_address != INVALID_ADDRESS , LOG_TAG_BASIC, "Vector not found in the buffer. Vector ID:%lu", id._id);

        return Vector<T, _DIM>(vectors[vec_id._level][vec_id.val].vector_address, false);
    }

    inline bool In_Buffer(VectorID id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        if (vectors.size() <= id._level || vectors[id._level].size() <= id.val) {
            return false;
        }

        return vectors[vec_id._level][vec_id.val].vector_address == INVALID_ADDRESS && vectors[vec_id._level][vec_id.val].cluster_address == INVALID_ADDRESS;
    }

    inline Internal_Node* New_Root(VectorID id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(vectors.size() == id._level, LOG_TAG_BASIC, "Vector ID:%lu Invalid level for root. id level:%lu, excpected level:%lu", id._id, id._level, vectors.size());
        AssertFatal(id.val == 0, LOG_TAG_BASIC, "Vector ID:%lu Invalid id for root. id val:%lu, excpected level:%lu", id._id, id.val, 0);

        vectors.emplace_back();
        vectors[id._level].emplace_back(INVALID_VECTOR_ID, INVALID_ADDRESS, new Internal_Node()); // todo: constructor args
        return vectors[id._level][0].cluster_address;
    }

    inline Internal_Node* New_Internal_Node(VectorID id, Address vector_address, VectorID parent_id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(id.Is_Internal_Node(), LOG_TAG_BASIC, "Vector Id:%lu is not an internal node.", id._id);
        AssertFatal(vectors.size() - 1 > id._level, LOG_TAG_BASIC, "Vector ID:%lu Invalid level. id level:%lu, max level:%lu", id._id, id._level, vectors.size() - 2); // it should not be at root level
        AssertFatal(parent_id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid  parent Id");
        AssertFatal(vectors.size() > parent_id._level, LOG_TAG_BASIC, "Parent ID:%lu Invalid level. parent level:%lu, max level:%lu", parent_id._id, parent_id._level, vectors.size() - 1);
        AssertFatal(parent_id._level > id._level, LOG_TAG_BASIC, "Parent level should be larger than child level. parent level:%lu, child level:%lu", parent_id._level, id._level);
        AssertFatal(In_Buffer(parent_id), LOG_TAG_BASIC, "Parent should be in the buffer.");
        AssertFatal(vectors[id._level].size() == id.val, LOG_TAG_BASIC, "Invalid id for new node. ID val:%lu, expected val:%lu", id.val, vectors[id._level].size());
        Internal_Node* parent =  Get_Node(parent_id);
        AssertFatal(parent->Contains(id), LOG_TAG_BASIC, "Parent:%lu should already contain id:%lu", parent_id._id, id._id);
        AssertFatal(parent->_bucket._beg + , LOG_TAG_BASIC, "Parent:%lu should already contain id:%lu", parent_id._id, id._id);
        return Allocate_Node_For_VectorID(id, vector_address, parent_id);
    }

    inline Leaf* New_Leaf(VectorID id, Address vector_address, VectorID parent_id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(id.Is_Leaf(), LOG_TAG_BASIC, "Vector Id:%lu is not a leaf node.", id._id);
        return Allocate_Node_For_VectorID(id, vector_address, parent_id);
    }

    inline Address Insert(VectorID id, VectorID parent_id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");

        AssertFatal(vectors.size() >= id._level, LOG_TAG_BASIC, "Vector ID:%lu level is out of bounds. max_level:%lu", id._id, vectors.size());

        if (vectors.size() == vec_id._level) {
            vectors.emplace_back();
        }

        AssertFatal(vectors[id._level].size() == id.val, LOG_TAG_BASIC, "Vector ID:%lu has invalid id:%lu", id._id, vectors[id._level].size());

        vectors[id._level].emplace_back(parent_id, new Copper_Node, cluster_address);
    }

    inline void Update(VectorID id, Address vector_address, VectorID parent_id, Address cluster_address) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(vector_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Invalidating vector address. Vector ID:%lu", id._id);
        AssertFatal(id.Is_Centroid() && cluster_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Invalidating cluster address. Vector ID:%lu", id._id);
        AssertFatal(parent_id != INVALID_VECTOR_ID || cluster_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Both parent ID and cluster address cannot be invalid. Vector ID:%lu", id._id);

        AssertFatal(vectors.size() >= id._level, LOG_TAG_BASIC, "Vector ID:%lu level is out of bounds. max_level:%lu", id._id, vectors.size());

        if (vectors.size() == vec_id._level) {
            vectors.emplace_back();
        }

        AssertFatal(vectors[id._level].size() >= id.val, LOG_TAG_BASIC, "Vector ID:%lu val is out of bounds. max_val:%lu", id._id, vectors[id._level].size());

        if (vectors[id._level].size() == id.val) {
            vectors[id._level].emplace_back(parent_id, vector_address, cluster_address);
        }
        else {
            vectors[id._level][id.val].parent_id = parent_id;
            vectors[id._level][id.val].vector_address = vector_address;
            vectors[id._level][id.val].cluster_address = cluster_address;
        }
    }

    inline void Evict(VectorID id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(vector_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Invalidating vector address. Vector ID:%lu", id._id);
        AssertFatal(id.Is_Centroid() && cluster_address != INVALID_ADDRESS, LOG_TAG_BASIC, "Invalidating cluster address. Vector ID:%lu", id._id);

        AssertFatal(vectors.size() >= id._level, LOG_TAG_BASIC, "Vector ID:%lu level is out of bounds. max_level:%lu", id._id, vectors.size());

        if (vectors.size() == vec_id._level) {
            vectors.emplace_back();
        }

        AssertFatal(vectors[id._level].size() >= id.val, LOG_TAG_BASIC, "Vector ID:%lu val is out of bounds. max_val:%lu", id._id, vectors[id._level].size());

        if (vectors[id._level].size() == id.val) {
            vectors[id._level].emplace_back(parent_id, vector_address, cluster_address);
        }
    }

    inline void Erase(VectorID id) {
        AssertFatal(id != INVALID_VECTOR_ID, LOG_TAG_BASIC, "Invalid Vector Id");
        AssertFatal(vectors.size() > id._level, LOG_TAG_BASIC, "Vector ID:%lu level is out of bounds. max_level:%lu", id._id, vectors.size());
        AssertFatal(vectors[id._level].size() > id.val, LOG_TAG_BASIC, "Vector ID:%lu val is out of bounds. max_val:%lu", id._id, vectors[id._level].size());

        vectors[id._level][id.val]
    }

protected:
    std::vector<std::vector<VectorInfo>> vectors;

};

};

#endif