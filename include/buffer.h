#ifndef COPPER_BUFFER_H_
#define COPPER_BUFFER_H_

#include "common.h"
#include "vector_utils.h"

namespace copper {

template <typename T, uint16_t _DIM, uint16_t _MIN_SIZE, uint16_t _MAX_SIZE,
            typename DIST_TYPE, typename _DIST> class Copper_Node;

// todo implement buffer
template <typename T, uint16_t _DIM, uint16_t KI_MIN, uint16_t KI_MAX, uint16_t KL_MIN, uint16_t KL_MAX,
        typename DIST_TYPE, typename _DIST>
class Buffer_Manager {
public:
    typedef Copper_Node<T, _DIM, KI_MIN, KI_MAX, DIST_TYPE, _DIST> Internal_Node;
    typedef Copper_Node<T, _DIM, KL_MIN, KL_MAX, DIST_TYPE, _DIST> Leaf_Node;

    inline Internal_Node* Get_Node(VectorID& node_id);
    inline Leaf_Node* Get_Leaf(VectorID& leaf_id);
    inline Leaf_Node* Get_Container_Leaf(VectorID& vec_id);
    inline Vector<T, _DIM> Get_Vector(VectorID id);

};

};

#endif