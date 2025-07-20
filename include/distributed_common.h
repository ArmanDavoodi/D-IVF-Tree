#ifndef DISTRIBUTED_COMMON_H_
#define DISTRIBUTED_COMMON_H_

#include <cstdint>

namespace divftree {

inline constexpr uint8_t NUM_COMPUTE_NODES = 1;
inline constexpr uint8_t NUM_MEMORY_NODES = 0;
inline constexpr uint8_t NUM_NODES = NUM_COMPUTE_NODES + NUM_MEMORY_NODES;
inline constexpr uint8_t MAX_COMPUTE_NODE_ID = (NUM_COMPUTE_NODES * 2) - 1;
inline constexpr uint8_t MAX_MEMORY_NODE_ID = ((NUM_MEMORY_NODES - 1) * 2);

inline constexpr uint8_t INVALID_NODE_ID = UINT8_MAX;

static_assert(NUM_NODES > 0, "Number of nodes must be greater than 0.");
static_assert(NUM_NODES < INVALID_NODE_ID, "Number of nodes must be less than or equal to 254.");
static_assert(NUM_COMPUTE_NODES > 0, "Number of compute nodes must be greater than 0.");
static_assert((NUM_COMPUTE_NODES == 1) || (NUM_MEMORY_NODES > 0),
              "If there is more than one compute node, there must be at least one memory node.");
static_assert(MAX_COMPUTE_NODE_ID < INVALID_NODE_ID, "MAX_COMPUTE_NODE_ID cannot exceed 255.");
static_assert(MAX_MEMORY_NODE_ID < INVALID_NODE_ID, "MAX_MEMORY_NODE_ID cannot exceed 255.");

inline constexpr bool IsValidNodeID(uint8_t node_id) {
    return (node_id < NUM_NODES);
}

inline constexpr uint8_t IsComputeNode(uint8_t node_id) {
    return IsValidNodeID(node_id) && (node_id % 2 != 0);
}

inline constexpr uint8_t IsMemoryNode(uint8_t node_id) {
    return IsValidNodeID(node_id) && (node_id % 2 == 0);
}

inline uint8_t self_node_id = 1;

};

#endif