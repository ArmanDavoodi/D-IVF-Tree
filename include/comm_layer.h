#ifndef COMM_LAYER_H_
#define COMM_LAYER_H_

#include "common.h"
#include "debug.h"

#include "utils/rdma_manager.h"

namespace divftree {

enum class MessageType : uint8_t {
    REGISTER_MEMORY = 0,
    SHUTDOWN_REQUEST,
    SHUTDOWN_RESPONSE, /* it may be accept or decline */

    CN_TO_MN_INSERT_REQUEST,
    CN_TO_MN_DELETE_REQUEST,
    CN_TO_MN_MIGRATION_REQUEST,
    CN_TO_MN_MERGE_REQUEST,
    CN_TO_MN_UNPIN_NOTIFICATION,
    CN_TO_MN_CLUSTER_ADDRESS_REQUEST,

    MN_TO_CN_INSERT_FAILED_RESPONSE,
    MN_TO_CN_DELETE_FAILED_RESPONSE,
    MN_TO_CN_CLUSTER_ADDRESS_RESPONSE,

    MN_TO_CN_VECTOR_INSERT_NOTIFICATION,
    MN_TO_CN_CLUSTER_INSERT_NOTIFICATION,
    MN_TO_CN_COMPACTION_NOTIFICATION,
    MN_TO_CN_SPLIT_NOTIFICATION,
    MN_TO_CN_EXPANSION_NOTIFICATION,
    MN_TO_CN_DELETE_NOTIFICATION,
    MN_TO_CN_MIGRATION_NOTIFICATION,
    MN_TO_CN_MERGE_NOTIFICATION,

    // MN_TO_CN_MIGRATION_CHECK_REQUEST,
    MN_TO_CN_MERGE_CHECK_REQUEST,

    URGENT_MN_TO_CN_SPLIT_REQUEST,

    MESSAGE_TYPE_COUNT
};

struct  __attribute__((packed)) RegisterMemoryMessage {
    const MessageType type = MessageType::REGISTER_MEMORY;
    uintptr_t addr;
    size_t length;
    uint32_t rkey;
};

struct  __attribute__((packed)) ShutdownRequestMessage {
    const MessageType type = MessageType::SHUTDOWN_REQUEST;
    /* todo: need more? */
};

struct  __attribute__((packed)) ShutdownResponseMessage {
    const MessageType type = MessageType::SHUTDOWN_RESPONSE;
    bool accepted;
    /* todo: need more? */
};

struct  __attribute__((packed)) InsertRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_INSERT_REQUEST;
    VectorID target_leaf; /* should be leaf */
    Version target_version;
    VectorID vector; /* cannot be cluster */
    VTYPE data[]; /* todo: does it have paddings? */
};

struct  __attribute__((packed)) DeleteRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_DELETE_REQUEST;
    VectorID target_vector; /* cannot be cluster */
};

/* todo: currently this is basically a full message as it is more than 3KB. I should add statistics to check the send success rate */
/* todo: if most of the times we need to migrate more than half, we can use a black list instead of white list approach -> should not happen if the clustering algorithm is good though */
struct  __attribute__((packed)) MigrationRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_MIGRATION_REQUEST;
    VectorID first_cluster;
    VectorID second_cluster;
    Version first_version;
    Version second_version;
    uint64_t num_first_to_second;
    uint64_t num_second_to_first;
    uint16_t offsets[]; /* first num_first_to_second are offsets in first cluster, and the rest are offsets in second cluster */
};

struct  __attribute__((packed)) MergeRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_MERGE_REQUEST;
    VectorID src_cluster;
    Version src_version;
    VectorID dest_cluster;
    Version dest_version;
};

struct  __attribute__((packed)) UnpinNotificationMessage {
    const MessageType type = MessageType::CN_TO_MN_UNPIN_NOTIFICATION;
    VectorID old_root_id;
    Version old_root_version;
};

struct  __attribute__((packed)) ClusterAddressRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_CLUSTER_ADDRESS_REQUEST;
    VectorID cluster_id;
    Version cluster_version;
};

struct __attribute__((packed)) InsertFailedMessage {
    const MessageType type = MessageType::MN_TO_CN_INSERT_FAILED_RESPONSE;
    VectorID target_leaf; /* should be leaf */
    Version target_version;
    VectorID vector; /* cannot be cluster */
    VTYPE data[]; /* todo: does it have paddings? */
};

struct  __attribute__((packed)) DeleteFailedMessage {
    const MessageType type = MessageType::MN_TO_CN_DELETE_FAILED_RESPONSE;
    VectorID target_vector; /* cannot be cluster */
};

struct  __attribute__((packed)) ClusterAddressResponseMessage {
    const MessageType type = MessageType::MN_TO_CN_CLUSTER_ADDRESS_RESPONSE;
    VectorID cluster_id;
    Version cluster_version;
    uintptr_t cluster_addr;
};

struct __attribute__((packed)) VectorInsertNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_VECTOR_INSERT_NOTIFICATION;
    VectorID container_id;
    Version container_version;
    uint16_t offset;
    VectorID new_vector_id;
    VTYPE vector_data[];
};

struct __attribute__((packed)) ClusterInsertNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_CLUSTER_INSERT_NOTIFICATION;
    VectorID container_id;
    Version container_version;
    uint16_t num_inserted;
    uint16_t offset;
    uint16_t outdated_offset;
    char data[]; /* <VectorID, Version, VectorData>[] */
};

struct __attribute__((packed)) DeleteNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_DELETE_NOTIFICATION;
    VectorID container_id;
    Version container_version;
    uint16_t offset;
};

struct __attribute__((packed)) CompactionNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_COMPACTION_NOTIFICATION;
    VectorID target_cluster;
    Version new_version;
    uintptr_t cluster_addr;
};

struct __attribute__((packed)) SplitNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_SPLIT_NOTIFICATION;
    // VectorID clusters[2]; /* the first one is the target cluster */
    // Version versions[2]; /* the version of the first id is the new version of the cluster */
    // Version cluster_addrs[2];
    char data[];
};

struct __attribute__((packed)) ExpansionNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_EXPANSION_NOTIFICATION;
    VectorID new_root_id;
    Version new_root_version;
    uintptr_t new_root_addr;
    // VectorID clusters[2]; /* the first one is the target cluster(which should be old root) */
    // Version versions[2]; /* the version of the first id is the new version of the cluster */
    // Version cluster_addrs[2];
    char data[];
};

/* todo: this message might be too large so we may need to send multiple of them instead of one for one migration */
struct __attribute__((packed)) MigrationNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_MIGRATION_NOTIFICATION;
    VectorID src_cluster;
    VectorID dest_cluster;
    Version src_version;
    Version dest_version;
    uint64_t num_migrated;
    char data[]; /* <offset, vectorID, vectorData, Version(if applicable)> */
};

struct  __attribute__((packed)) MergeNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_MERGE_NOTIFICATION;
    VectorID src_cluster;
    Version src_version;
    VectorID dest_cluster;
    Version dest_version;
};

// struct  __attribute__((packed)) MigrationCheckRequestMessage {
//     const MessageType type = MessageType::MN_TO_CN_MIGRATION_CHECK_REQUEST;
//     VectorID first_cluster_id;
//     VectorID second_cluster_id;
//     Version first_cluster_version; /* we do not fail if we have a newer version at CN we just use the newer version */
//     Version second_cluster_version;
//     uintptr_t first_cluster_addr;
//     uintptr_t second_cluster_addr;
// };

struct __attribute__((packed)) MergeCheckRequestMessage {
    const MessageType type = MessageType::MN_TO_CN_MERGE_CHECK_REQUEST;
    VectorID src_cluster_id;
    VectorID parent_cluster_id;
    Version parent_cluster_version; /* we do not fail if we have a newer version at CN we just use the newer version */
    uintptr_t parent_cluster_addr;
};

struct __attribute__((packed)) UrgentSplitRequestMessage {
    const MessageType type = MessageType::URGENT_MN_TO_CN_SPLIT_REQUEST;
    VectorID target_vector;
    Version target_version;
    uintptr_t target_vector_addr;

    uintptr_t new_cluster_addr[]; /* the CN will use these for the new clusters and puts the VectorData+ID of the cluster itself as the last vector in it */
};

};

#endif