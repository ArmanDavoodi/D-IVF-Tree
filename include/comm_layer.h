#ifndef COMM_LAYER_H_
#define COMM_LAYER_H_

#include "common.h"
#include "debug.h"

#include "utils/rdma_manager.h"

namespace divftree {

constexpr uint8_t BROADCAST_NODE_ID = MEMORY_NODE_ID;

enum class MessageType : uint8_t {
    BASE_MESSAGE_START = 0,
        REGISTER_MEMORY = BASE_MESSAGE_START,
        SHUTDOWN_REQUEST,
        SHUTDOWN_RESPONSE, /* it may be accept or decline */
    BASE_MESSAGE_END = SHUTDOWN_RESPONSE,

    CN_MESSAGE_START,
        CN_TO_MN_INSERT_REQUEST = CN_MESSAGE_START,
        CN_TO_MN_DELETE_REQUEST,
        CN_TO_MN_MIGRATION_REQUEST,
        CN_TO_MN_MERGE_REQUEST,
        CN_TO_MN_UNPIN_NOTIFICATION,
        CN_TO_MN_CLUSTER_ADDRESS_REQUEST,
    CN_MESSAGE_END = CN_TO_MN_CLUSTER_ADDRESS_REQUEST,

    MN_MESSAGE_START,
        MN_RESPONSE_START = MN_MESSAGE_START,
            MN_TO_CN_INSERT_FAILED_RESPONSE = MN_RESPONSE_START,
            MN_TO_CN_DELETE_FAILED_RESPONSE,
            MN_TO_CN_CLUSTER_ADDRESS_RESPONSE,
        MN_RESPONSE_END = MN_TO_CN_CLUSTER_ADDRESS_RESPONSE,

        MN_NOTIF_START,
            MN_TO_CN_VECTOR_INSERT_NOTIFICATION = MN_NOTIF_START,
            MN_TO_CN_CLUSTER_INSERT_NOTIFICATION,
            MN_TO_CN_COMPACTION_NOTIFICATION,
            MN_TO_CN_SPLIT_NOTIFICATION,
            MN_TO_CN_EXPANSION_NOTIFICATION,
            MN_TO_CN_DELETE_NOTIFICATION,
            MN_TO_CN_ROOT_PRUNE_NOTIFICATION,
            MN_TO_CN_MIGRATION_NOTIFICATION,
            MN_TO_CN_MERGE_NOTIFICATION,
        MN_NOTIF_END = MN_TO_CN_MERGE_NOTIFICATION,

        MN_REQUEST_START,
            MN_TO_CN_MERGE_CHECK_REQUEST = MN_REQUEST_START,
            // MN_TO_CN_MIGRATION_CHECK_REQUEST,
        MN_REQUEST_END = MN_TO_CN_MERGE_CHECK_REQUEST,

        MN_URGENT_START,
            URGENT_MN_TO_CN_SPLIT_REQUEST = MN_URGENT_START,
        MN_URGENT_END = URGENT_MN_TO_CN_SPLIT_REQUEST,

    MN_MESSAGE_END = MN_URGENT_END,
    MESSAGE_TYPE_COUNT
};

struct  __attribute__((packed)) RegisterMemoryMessage {
    const MessageType type = MessageType::REGISTER_MEMORY;
    uintptr_t addr;
    size_t length;
    uint32_t rkey;

    constexpr static size_t Size() {
        return sizeof(RegisterMemoryMessage);
    }
};

struct  __attribute__((packed)) ShutdownRequestMessage {
    const MessageType type = MessageType::SHUTDOWN_REQUEST;
    /* todo: need more? */

    constexpr static size_t Size() {
        return sizeof(ShutdownRequestMessage);
    }
};

struct  __attribute__((packed)) ShutdownResponseMessage {
    const MessageType type = MessageType::SHUTDOWN_RESPONSE;
    bool accepted;
    /* todo: need more? */
    constexpr static size_t Size() {
        return sizeof(ShutdownResponseMessage);
    }
};

struct  __attribute__((packed)) InsertRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_INSERT_REQUEST;
    VectorID target_leaf; /* should be leaf */
    Version target_version;
    VectorID vector; /* cannot be cluster */
    VTYPE data[]; /* todo: does it have paddings? */

    constexpr static size_t Size(uint16_t dim) {
        return sizeof(InsertRequestMessage) + sizeof(VTYPE) * dim;
    }
};

struct  __attribute__((packed)) DeleteRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_DELETE_REQUEST;
    VectorID target_vector; /* cannot be cluster */
    bool need_response;

    constexpr static size_t Size() {
        return sizeof(DeleteRequestMessage);
    }
};

/* todo: currently this is basically a full message as it is more than 3KB. I should add statistics to check the send success rate */
/* todo: if most of the times we need to migrate more than half, we can use a black list instead of white list approach -> should not happen if the clustering algorithm is good though */
/* since this needs to lock a lot of clusters it is considered a general request */
struct  __attribute__((packed)) MigrationRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_MIGRATION_REQUEST;
    VectorID first_cluster;
    VectorID second_cluster;
    Version first_version;
    Version second_version;
    uint64_t num_first_to_second;
    uint64_t num_second_to_first;
    uint16_t offsets[]; /* first num_first_to_second are offsets in first cluster, and the rest are offsets in second cluster */

    constexpr static size_t Size(uint64_t num_first_to_second, uint64_t num_second_to_first) {
        return sizeof(MigrationRequestMessage) +
               sizeof(uint16_t) * (num_first_to_second + num_second_to_first);
    }
};

/* although we can put this in the cluster for the cluster with lower id, it makes things more complicated
because src is locked in X mode and dest in S. As a result if srcId < destId and we lock src and then try to insert into
it too much in a way to cause a major update, we already have it locked in X mode and have to change the main code
to handle this case. todo: maybe do this later but now set it as a general task  */
struct  __attribute__((packed)) MergeRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_MERGE_REQUEST;
    VectorID src_cluster;
    Version src_version;
    VectorID dest_cluster;
    Version dest_version;

    constexpr static size_t Size() {
        return sizeof(MergeRequestMessage);
    }
};

/* a general task as we do not need to lock the whole page we just lock the header I guess */
struct  __attribute__((packed)) UnpinNotificationMessage {
    const MessageType type = MessageType::CN_TO_MN_UNPIN_NOTIFICATION;
    VectorID old_root_id;
    Version old_root_version;

    constexpr static size_t Size() {
        return sizeof(UnpinNotificationMessage);
    }
};

/* a general task as we do not need to lock the whole page we just lock the header I guess */
struct  __attribute__((packed)) ClusterAddressRequestMessage {
    const MessageType type = MessageType::CN_TO_MN_CLUSTER_ADDRESS_REQUEST;
    VectorID cluster_id;
    Version cluster_version;

    constexpr static size_t Size() {
        return sizeof(ClusterAddressRequestMessage);
    }
};

/* a general task as we just need to repeat */
struct __attribute__((packed)) InsertFailedMessage {
    const MessageType type = MessageType::MN_TO_CN_INSERT_FAILED_RESPONSE;
    VectorID target_leaf; /* should be leaf */
    Version target_version;
    VectorID vector; /* cannot be cluster */
    VTYPE data[]; /* todo: does it have paddings? */

    constexpr static size_t Size(uint16_t dim) {
        return sizeof(InsertFailedMessage) + sizeof(VTYPE) * dim;
    }
};

/* a general task */
struct  __attribute__((packed)) DeleteFailedMessage {
    const MessageType type = MessageType::MN_TO_CN_DELETE_FAILED_RESPONSE;
    VectorID target_vector; /* cannot be cluster */

    constexpr static size_t Size() {
        return sizeof(DeleteFailedMessage);
    }
};

/* a general task */
struct  __attribute__((packed)) ClusterAddressResponseMessage {
    const MessageType type = MessageType::MN_TO_CN_CLUSTER_ADDRESS_RESPONSE;
    VectorID cluster_id;
    Version cluster_version;
    uintptr_t cluster_addr;

    constexpr static size_t Size() {
        return sizeof(ClusterAddressResponseMessage);
    }
};

struct __attribute__((packed)) VectorInsertNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_VECTOR_INSERT_NOTIFICATION;
    VectorID container_id;
    Version container_version;
    uint16_t offset;
    VectorID new_vector_id;
    VTYPE vector_data[];

    constexpr static size_t Size(uint16_t dim) {
        return sizeof(VectorInsertNotificationMessage) + sizeof(VTYPE) * dim;
    }
};

struct __attribute__((packed)) ClusterInsertNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_CLUSTER_INSERT_NOTIFICATION;
    VectorID container_id;
    Version container_version;
    uint16_t num_inserted;
    uint16_t offset;
    uint16_t outdated_offset;
    char data[]; /* <VectorID, Version, VectorData>[] */

    constexpr static size_t Size(uint16_t dim, uint16_t num_inserted) {
        return sizeof(ClusterInsertNotificationMessage) +
               num_inserted * (sizeof(VectorID) + sizeof(Version) + sizeof(VTYPE) * dim);
    }
};


struct __attribute__((packed)) CompactionNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_COMPACTION_NOTIFICATION;
    VectorID target_cluster;
    Version new_version;
    uintptr_t cluster_addr;

    constexpr static size_t Size() {
        return sizeof(CompactionNotificationMessage);
    }
};

struct __attribute__((packed)) SplitNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_SPLIT_NOTIFICATION;
    // VectorID clusters[2]; /* the first one is the target cluster */
    // Version versions[2]; /* the version of the first id is the new version of the cluster */
    // uintptr_t cluster_addrs[2];
    char data[];

    constexpr static size_t Size(uint16_t split_factor) {
        return sizeof(SplitNotificationMessage) +
               split_factor * (sizeof(VectorID) + sizeof(Version) + sizeof(uintptr_t));
    }
};

struct __attribute__((packed)) ExpansionNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_EXPANSION_NOTIFICATION;
    VectorID new_root_id;
    Version new_root_version;
    uintptr_t new_root_addr;
    // VectorID clusters[2]; /* the first one is the target cluster(which should be old root) */
    // Version versions[2]; /* the version of the first id is the new version of the cluster */
    // uintptr_t cluster_addrs[2];
    char data[];

    constexpr static size_t Size(uint16_t split_factor) {
        return sizeof(ExpansionNotificationMessage) +
               split_factor * (sizeof(VectorID) + sizeof(Version) + sizeof(uintptr_t));
    }
};

struct __attribute__((packed)) DeleteNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_DELETE_NOTIFICATION;
    VectorID container_id;
    Version container_version;
    uint16_t offset;

    constexpr static size_t Size() {
        return sizeof(DeleteNotificationMessage);
    }
};

struct __attribute__((packed)) RootPruneNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_ROOT_PRUNE_NOTIFICATION;
    VectorID old_root_id;
    Version old_root_version;
    VectorID new_root_id;
    Version new_root_version;
    uintptr_t new_root_addr;

    constexpr static size_t Size() {
        return sizeof(RootPruneNotificationMessage);
    }
};

/* todo: this message might be too large so we may need to send multiple of them instead of one for one migration */
/* since in this case, we are only the cache, we do not need to lock the children and we can just lock the parents
   but similar to merge, for now we consider this general. todo: later consider it cluster based */
struct __attribute__((packed)) MigrationNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_MIGRATION_NOTIFICATION;
    VectorID src_cluster;
    VectorID dest_cluster;
    Version src_version;
    Version dest_version;
    uint64_t num_migrated;
    uint16_t offset; /* start offset where they were inserted at dest */
    uint16_t old_offsets[]; /* <old offsets> */

    constexpr static size_t Size(uint64_t num_migrated) {
        return sizeof(MigrationNotificationMessage) +
               num_migrated * (sizeof(uint16_t));
    }
};

struct  __attribute__((packed)) MergeNotificationMessage {
    const MessageType type = MessageType::MN_TO_CN_MERGE_NOTIFICATION;
    VectorID src_cluster;
    Version src_version;
    VectorID dest_cluster;
    Version dest_version;

    constexpr static size_t Size() {
        return sizeof(MergeNotificationMessage);
    }
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

    constexpr static size_t Size() {
        return sizeof(MergeCheckRequestMessage);
    }
};

struct __attribute__((packed)) UrgentSplitRequestMessage {
    const MessageType type = MessageType::URGENT_MN_TO_CN_SPLIT_REQUEST;
    VectorID target_vector;
    Version target_version;
    uintptr_t target_vector_addr;

    uintptr_t new_cluster_addr[]; /* the CN will use these for the new clusters and puts the VectorData+ID of the cluster itself as the last vector in it */

    constexpr static size_t Size(uint16_t split_factor) {
        return sizeof(UrgentSplitRequestMessage) +
               sizeof(uintptr_t) * split_factor;
    }
};

struct CommLayerMessage {
protected:
    BufferInfo info;
public:
    MessageType GetMessageType() const {
        FatalAssert(info.length >= sizeof(MessageType), LOG_TAG_COMM_LAYER,
                    "Buffer length %zu is smaller than MessageType size %zu",
                    info.length, sizeof(MessageType));
        CHECK_NOT_NULLPTR(info.buffer, LOG_TAG_COMM_LAYER);
        return *reinterpret_cast<MessageType*>(info.buffer);
    }

    void* GetMessageBuffer() const {
        CHECK_NOT_NULLPTR(info.buffer, LOG_TAG_COMM_LAYER);
        return info.buffer;
    }

    size_t GetMessageLength() const {
        return info.length;
    }

friend class CommLayer;
};

struct ClusterRemoteAccessInfo {
    VectorID cluster_id;
    Version cluster_version;
    void* local_addr;
    uintptr_t remote_addr;
};

class CommLayer {
protected:
    uint16_t dim;
    uint16_t split_factor;
    CommLayer() = default;
    ~CommLayer() = default;
public:
    inline uint64_t GetMessageSize(const void* message) const {
        CHECK_NOT_NULLPTR(message, LOG_TAG_COMM_LAYER);
        MessageType type = *reinterpret_cast<const MessageType*>(message);
        switch (type) {
            case MessageType::REGISTER_MEMORY:
            {
                return RegisterMemoryMessage::Size();
            }
            case MessageType::SHUTDOWN_REQUEST:
            {
                return ShutdownRequestMessage::Size();
            }
            case MessageType::SHUTDOWN_RESPONSE:
            {
                return ShutdownResponseMessage::Size();
            }
            case MessageType::CN_TO_MN_INSERT_REQUEST:
            {
                return InsertRequestMessage::Size(dim);
            }
            case MessageType::CN_TO_MN_DELETE_REQUEST:
            {
                return DeleteRequestMessage::Size();
            }
            case MessageType::CN_TO_MN_MIGRATION_REQUEST:
            {
                const MigrationRequestMessage* req =
                        reinterpret_cast<const MigrationRequestMessage*>(message);
                return MigrationRequestMessage::Size(req->num_first_to_second,
                                                     req->num_second_to_first);
            }
            case MessageType::CN_TO_MN_MERGE_REQUEST:
            {
                return MergeRequestMessage::Size();
            }
            case MessageType::CN_TO_MN_UNPIN_NOTIFICATION:
            {
                return UnpinNotificationMessage::Size();
            }
            case MessageType::CN_TO_MN_CLUSTER_ADDRESS_REQUEST:
            {
                return ClusterAddressRequestMessage::Size();
            }
            case MessageType::MN_TO_CN_INSERT_FAILED_RESPONSE:
            {
                return InsertFailedMessage::Size(dim);
            }
            case MessageType::MN_TO_CN_DELETE_FAILED_RESPONSE:
            {
                return DeleteFailedMessage::Size();
            }
            case MessageType::MN_TO_CN_CLUSTER_ADDRESS_RESPONSE:
            {
                return ClusterAddressResponseMessage::Size();
            }
            case MessageType::MN_TO_CN_VECTOR_INSERT_NOTIFICATION:
            {
                return VectorInsertNotificationMessage::Size(dim);
            }
            case MessageType::MN_TO_CN_CLUSTER_INSERT_NOTIFICATION:
            {
                const ClusterInsertNotificationMessage* notif =
                        reinterpret_cast<const ClusterInsertNotificationMessage*>(message);
                return ClusterInsertNotificationMessage::Size(dim, notif->num_inserted);
            }
            case MessageType::MN_TO_CN_COMPACTION_NOTIFICATION:
            {
                return CompactionNotificationMessage::Size();
            }
            case MessageType::MN_TO_CN_SPLIT_NOTIFICATION:
            {
                return SplitNotificationMessage::Size(split_factor);
            }
            case MessageType::MN_TO_CN_EXPANSION_NOTIFICATION:
            {
                return ExpansionNotificationMessage::Size(split_factor);
            }
            case MessageType::MN_TO_CN_DELETE_NOTIFICATION:
            {
                return DeleteNotificationMessage::Size();
            }
            case MessageType::MN_TO_CN_ROOT_PRUNE_NOTIFICATION:
            {
                return RootPruneNotificationMessage::Size();
            }
            case MessageType::MN_TO_CN_MIGRATION_NOTIFICATION:
            {
                const MigrationNotificationMessage* notif =
                        reinterpret_cast<const MigrationNotificationMessage*>(message);
                return MigrationNotificationMessage::Size(notif->num_migrated);
            }
            case MessageType::MN_TO_CN_MERGE_NOTIFICATION:
            {
                return MergeNotificationMessage::Size();
            }
            case MessageType::MN_TO_CN_MERGE_CHECK_REQUEST:
            {
                return MergeCheckRequestMessage::Size();
            }
            case MessageType::URGENT_MN_TO_CN_SPLIT_REQUEST:
            {
                const UrgentSplitRequestMessage* urgent_msg =
                        reinterpret_cast<const UrgentSplitRequestMessage*>(message);
                return UrgentSplitRequestMessage::Size(split_factor);
            }
            default:
            {
                FatalAssert(false, LOG_TAG_COMM_LAYER,
                            "Cannot get message size for unknown or variable-size message type %u",
                            static_cast<uint8_t>(type));
                return 0;
            }
        }
    }

    RetStatus BuildRequestMessage(uint8_t target_node_id, MessageType type, CommLayerMessage& message,
                                  size_t message_size) {
#ifdef MEMORY_NODE
        FatalAssert((type >= MessageType::BASE_MESSAGE_START &&
                    type <= MessageType::BASE_MESSAGE_END) ||
                    (type >= MessageType::MN_MESSAGE_START &&
                    type <= MessageType::MN_MESSAGE_END),
                    LOG_TAG_COMM_LAYER,
                    "Memory Node can only send base or MN messages. Invalid message type: %u",
                    static_cast<uint8_t>(type));
#else
        FatalAssert((type >= MessageType::BASE_MESSAGE_START &&
                    type <= MessageType::BASE_MESSAGE_END) ||
                    (type >= MessageType::CN_MESSAGE_START &&
                    type <= MessageType::CN_MESSAGE_END),
                    LOG_TAG_COMM_LAYER,
                    "Compute Node can only send base or CN messages. Invalid message type: %u",
                    static_cast<uint8_t>(type));
#endif

#ifdef MEMORY_NODE
        if (type == MessageType::URGENT_MN_TO_CN_SPLIT_REQUEST) {
            FatalAssert(target_node_id != BROADCAST_NODE_ID,
                        LOG_TAG_COMM_LAYER,
                        "Urgent messages cannot be sent to broadcast node");
            FatalAssert(message_size <= sizeof(UrgentMessageData),
                        LOG_TAG_COMM_LAYER,
                        "Urgent message size %zu exceeds the maximum allowed size %u",
                        message_size,
                        sizeof(UrgentMessageData));
            UrgentMessage* raw_message = reinterpret_cast<UrgentMessage*>(threadSelf->GetUrgentMessageBuffer());
            raw_message->Clear();
            raw_message->meta.detail.length = static_cast<uint8_t>(message_size);
            *(reinterpret_cast<MessageType*>(raw_message->data.data)) = type;
            message.info.max_length = sizeof(UrgentMessageData);
            message.info.length = message_size;
            message.info.buffer = raw_message->data.data;
            message.info.target_node_id = target_node_id;
            message.info.buffer_idx = 0;
            FatalAssert(message.GetMessageType() == type,
                        LOG_TAG_COMM_LAYER,
                        "Failed to set message type %u in urgent message",
                        static_cast<uint8_t>(type));
            return RetStatus::Success();
        } else if (target_node_id == BROADCAST_NODE_ID) {
            message.info.buffer = threadSelf->GetBroadcastBuffer(message.info.max_length);
            FatalAssert(message_size <= message.info.max_length,
                        LOG_TAG_COMM_LAYER,
                        "Broadcast message size %zu exceeds the maximum allowed size %zu",
                        message_size,
                        message.info.max_length);
            message.info.length = message_size;
            message.info.target_node_id = target_node_id;
            *(reinterpret_cast<MessageType*>(message.info.buffer)) = type;
            FatalAssert(message.GetMessageType() == type,
                        LOG_TAG_COMM_LAYER,
                        "Failed to set message type %u in broadcast message",
                        static_cast<uint8_t>(type));
            return RetStatus::Success();
        }
#endif
        RDMA_Manager* rdma_manager = RDMA_Manager::GetInstance();
        CHECK_NOT_NULLPTR(rdma_manager, LOG_TAG_COMM_LAYER);
        RetStatus rs = rdma_manager->GrabCommBuffer(target_node_id, message_size, message.info);
        if (!rs.IsOK()) {
            FatalAssert(false, LOG_TAG_COMM_LAYER,
                        "Failed to grab communication buffer for target node %u, message size %zu: %s",
                        target_node_id, message_size, rs.Msg());
            return rs;
        }
        CHECK_NOT_NULLPTR(message.info.buffer, LOG_TAG_COMM_LAYER);
        *(reinterpret_cast<MessageType*>(message.info.buffer)) = type;
        FatalAssert(message.GetMessageType() == type,
                    LOG_TAG_COMM_LAYER,
                    "Failed to set message type %u in message",
                    static_cast<uint8_t>(type));
        return rs;
    }

    RetStatus SendMessage(CommLayerMessage& message, bool flush) {
        FatalAssert(message.info.buffer != nullptr,
                    LOG_TAG_COMM_LAYER,
                    "Cannot send message with null buffer");
        MessageType type = message.GetMessageType();
        uint8_t target_node_id = message.info.target_node_id;
#ifdef MEMORY_NODE
        FatalAssert((type >= MessageType::BASE_MESSAGE_START &&
                    type <= MessageType::BASE_MESSAGE_END) ||
                    (type >= MessageType::MN_MESSAGE_START &&
                    type <= MessageType::MN_MESSAGE_END),
                    LOG_TAG_COMM_LAYER,
                    "Memory Node can only send base or MN messages. Invalid message type: %u",
                    static_cast<uint8_t>(type));
#else
        FatalAssert((type >= MessageType::BASE_MESSAGE_START &&
                    type <= MessageType::BASE_MESSAGE_END) ||
                    (type >= MessageType::CN_MESSAGE_START &&
                    type <= MessageType::CN_MESSAGE_END),
                    LOG_TAG_COMM_LAYER,
                    "Compute Node can only send base or CN messages. Invalid message type: %u",
                    static_cast<uint8_t>(type));
#endif
        RDMA_Manager* rdma_manager = RDMA_Manager::GetInstance();
        CHECK_NOT_NULLPTR(rdma_manager, LOG_TAG_COMM_LAYER);
#ifdef MEMORY_NODE
        if (type == MessageType::URGENT_MN_TO_CN_SPLIT_REQUEST) {
            FatalAssert(target_node_id != BROADCAST_NODE_ID,
                        LOG_TAG_COMM_LAYER,
                        "Urgent messages cannot be sent to broadcast node");
            UrgentMessage* raw_message = reinterpret_cast<UrgentMessage*>(message.info.buffer);
            return rdma_manager->SendUrgentMessage(raw_message, target_node_id);
        } else if (target_node_id == BROADCAST_NODE_ID) {
            return rdma_manager->BroadCastCommRequest(message.info.buffer, message.info.length, flush);
        }
#endif

        return rdma_manager->ReleaseCommBuffer(message.info, flush);
    }

    RetStatus CheckReceivedMessagesFromNode(uint8_t target_node_id, std::vector<CommLayerMessage>& general_messages,
                                            std::unordered_map<VectorID, std::vector<CommLayerMessage>, VectorIDHash>&
                                                cluster_based_messages) {
        RDMA_Manager* rdma_manager = RDMA_Manager::GetInstance();
        CHECK_NOT_NULLPTR(rdma_manager, LOG_TAG_COMM_LAYER);
        uint8_t self_node_id = rdma_manager->GetSelfNodeId();
        uint8_t num_nodes = rdma_manager->GetNumNodes();
        FatalAssert(self_node_id < num_nodes, LOG_TAG_COMM_LAYER,
                    "Invalid self_node_id %u in CheckReceivedMessagesFromNode", self_node_id);
        FatalAssert(target_node_id < num_nodes, LOG_TAG_COMM_LAYER,
                    "Invalid target_node_id %u in CheckReceivedMessagesFromNode", target_node_id);
        FatalAssert(target_node_id != self_node_id,
                    LOG_TAG_COMM_LAYER,
                    "Cannot check received messages from broadcast node");
#ifdef MEMORY_NODE
            FatalAssert(self_node_id == MEMORY_NODE_ID, LOG_TAG_COMM_LAYER,
                        "Self node id %u is not memory node in CheckReceivedMessagesFromNode",
                        self_node_id);
#else
            FatalAssert(self_node_id != MEMORY_NODE_ID, LOG_TAG_COMM_LAYER,
                        "Self node id %u is memory node in CheckReceivedMessagesFromNode",
                        self_node_id);
#endif
        std::vector<BufferInfo> receive_buffers;
        RetStatus rs = rdma_manager->PollCommRequests(target_node_id, receive_buffers);
        if (!rs.IsOK()) {
            FatalAssert(false, LOG_TAG_COMM_LAYER,
                        "Failed to poll communication requests from node %u: %s",
                        target_node_id, rs.Msg());
            return rs;
        }

        for (const BufferInfo& buffer_info : receive_buffers) {
            CommLayerMessage message;
            message.info = buffer_info;
            uint64_t num_messages = message.info.num_requests;
            FatalAssert(num_messages > 0, LOG_TAG_COMM_LAYER,
                        "Received buffer with zero messages from node %u",
                        target_node_id);
            void* current_msg_ptr = message.GetMessageBuffer();
            for (uint64_t i = 0; i < num_messages; ++i) {
                FatalAssert(current_msg_ptr != nullptr,
                            LOG_TAG_COMM_LAYER,
                            "Null message pointer for message %zu from node %u",
                            i, target_node_id);
                FatalAssert(current_msg_ptr <= static_cast<char*>(message.GetMessageBuffer()) + message.info.max_length,
                            LOG_TAG_COMM_LAYER,
                            "Message pointer out of bounds for message %zu from node %u",
                            i, target_node_id);
                MessageType type = message.GetMessageType();
                uint64_t message_size = GetMessageSize(current_msg_ptr);
#ifdef MEMORY_NODE
                FatalAssert((type >= MessageType::BASE_MESSAGE_START &&
                            type <= MessageType::BASE_MESSAGE_END) ||
                            (type >= MessageType::CN_MESSAGE_START &&
                            type <= MessageType::CN_MESSAGE_END),
                            LOG_TAG_COMM_LAYER,
                            "Memory Node can only receive base or CN messages. Invalid message type: %u",
                            static_cast<uint8_t>(type));
#else
                FatalAssert((type >= MessageType::BASE_MESSAGE_START &&
                            type <= MessageType::BASE_MESSAGE_END) ||
                            (type >= MessageType::MN_MESSAGE_START &&
                            type <= MessageType::MN_MESSAGE_END),
                            LOG_TAG_COMM_LAYER,
                            "Compute Node can only receive base or MN messages. Invalid message type: %u",
                            static_cast<uint8_t>(type));
#endif
                CommLayerMessage current_msg;
                current_msg.info.buffer = new char[message_size];
                memcpy(current_msg.info.buffer, current_msg_ptr, message_size);
                current_msg.info.length = message_size;
                current_msg.info.max_length = message_size;
                current_msg.info.target_node_id = target_node_id;

                VectorID cluster_id = INVALID_VECTOR_ID;
                switch (type) {
                    case MessageType::CN_TO_MN_INSERT_REQUEST:
                    {
                        InsertRequestMessage* insert_msg =
                            reinterpret_cast<InsertRequestMessage*>(current_msg.GetMessageBuffer());
                        CHECK_NOT_NULLPTR(insert_msg, LOG_TAG_COMM_LAYER);
                        FatalAssert(insert_msg->type == type,
                                    LOG_TAG_COMM_LAYER,
                                    "Message type mismatch in InsertRequestMessage: expected %u, got %u",
                                    static_cast<uint8_t>(type),
                                    static_cast<uint8_t>(insert_msg->type));
                        cluster_id = insert_msg->target_leaf;
                        FatalAssert(cluster_id.IsValid() && cluster_id.IsLeaf(),
                                    LOG_TAG_COMM_LAYER,
                                    "Invalid target leaf " VECTORID_LOG_FMT " in InsertRequestMessage",
                                    VECTORID_LOG(cluster_id));
                        break;
                    }
                    case MessageType::MN_TO_CN_VECTOR_INSERT_NOTIFICATION:
                    {
                        VectorInsertNotificationMessage* vector_insert_msg =
                            reinterpret_cast<VectorInsertNotificationMessage*>(current_msg.GetMessageBuffer());
                        CHECK_NOT_NULLPTR(vector_insert_msg, LOG_TAG_COMM_LAYER);
                        FatalAssert(vector_insert_msg->type == type,
                                    LOG_TAG_COMM_LAYER,
                                    "Message type mismatch in VectorInsertNotificationMessage: expected %u, got %u",
                                    static_cast<uint8_t>(type),
                                    static_cast<uint8_t>(vector_insert_msg->type));
                        cluster_id = vector_insert_msg->container_id;
                        FatalAssert(cluster_id.IsValid() && cluster_id.IsLeaf(),
                                    LOG_TAG_COMM_LAYER,
                                    "Invalid container id " VECTORID_LOG_FMT " in VectorInsertNotificationMessage",
                                    VECTORID_LOG(cluster_id));
                        break;
                    }
                    case MessageType::MN_TO_CN_CLUSTER_INSERT_NOTIFICATION:
                    {
                        ClusterInsertNotificationMessage* cluster_insert_msg =
                            reinterpret_cast<ClusterInsertNotificationMessage*>(current_msg.GetMessageBuffer());
                        CHECK_NOT_NULLPTR(cluster_insert_msg, LOG_TAG_COMM_LAYER);
                        FatalAssert(cluster_insert_msg->type == type,
                                    LOG_TAG_COMM_LAYER,
                                    "Message type mismatch in ClusterInsertNotificationMessage: expected %u, got %u",
                                    static_cast<uint8_t>(type),
                                    static_cast<uint8_t>(cluster_insert_msg->type));
                        cluster_id = cluster_insert_msg->container_id;
                        FatalAssert(cluster_id.IsValid() && cluster_id.IsInternalVertex(),
                                    LOG_TAG_COMM_LAYER,
                                    "Invalid container id " VECTORID_LOG_FMT " in ClusterInsertNotificationMessage",
                                    VECTORID_LOG(cluster_id));
                        break;
                    }
                    case MessageType::MN_TO_CN_DELETE_NOTIFICATION:
                    {
                        DeleteNotificationMessage* delete_notif_msg =
                            reinterpret_cast<DeleteNotificationMessage*>(current_msg.GetMessageBuffer());
                        CHECK_NOT_NULLPTR(delete_notif_msg, LOG_TAG_COMM_LAYER);
                        FatalAssert(delete_notif_msg->type == type,
                                    LOG_TAG_COMM_LAYER,
                                    "Message type mismatch in DeleteNotificationMessage: expected %u, got %u",
                                    static_cast<uint8_t>(type),
                                    static_cast<uint8_t>(delete_notif_msg->type));
                        cluster_id = delete_notif_msg->container_id;
                        FatalAssert(cluster_id.IsValid() && cluster_id.IsCentroid(), LOG_TAG_COMM_LAYER,
                                    "Invalid container id " VECTORID_LOG_FMT " in DeleteNotificationMessage",
                                    VECTORID_LOG(cluster_id));
                        break;
                    }
                    case MessageType::MN_TO_CN_ROOT_PRUNE_NOTIFICATION:
                    {
                        RootPruneNotificationMessage* root_prune_msg =
                            reinterpret_cast<RootPruneNotificationMessage*>(current_msg.GetMessageBuffer());
                        CHECK_NOT_NULLPTR(root_prune_msg, LOG_TAG_COMM_LAYER);
                        FatalAssert(root_prune_msg->type == type,
                                    LOG_TAG_COMM_LAYER,
                                    "Message type mismatch in RootPruneNotificationMessage: expected %u, got %u",
                                    static_cast<uint8_t>(type),
                                    static_cast<uint8_t>(root_prune_msg->type));
                        cluster_id = root_prune_msg->old_root_id;
                        FatalAssert(cluster_id.IsValid() && cluster_id.IsInternalVertex(), LOG_TAG_COMM_LAYER,
                                    "Invalid old root id " VECTORID_LOG_FMT " in RootPruneNotificationMessage",
                                    VECTORID_LOG(cluster_id));
                        break;
                    }
                    default:
                    {
                        cluster_id = INVALID_VECTOR_ID;
                        break;
                    }
                }

                if (cluster_id.IsValid()) {
                    auto it = cluster_based_messages.find(cluster_id);
                    if (it == cluster_based_messages.end()) {
                        it = cluster_based_messages.emplace(cluster_id,
                                                            std::vector<CommLayerMessage>()).first;
                    }
                    it->second.push_back(current_msg);
                } else {
                    general_messages.push_back(current_msg);
                }
            }
        }

        rs = rdma_manager->ReleaseCommReciveBuffers(target_node_id, receive_buffers);
        if (!rs.IsOK()) {
            FatalAssert(false, LOG_TAG_COMM_LAYER,
                        "Failed to release communication receive buffers from node %u: %s",
                        target_node_id, rs.Msg());
        }
        return rs;
    }

    /* general messages may also have vectorID and version but cluster based ones are messages that explicitly
       require locking a cluster. some general messages(e.g. deleteVector) may also need locks so we may need to
       analyze the general messages first in the upper layer to filter such messages*/
    RetStatus CheckReceivedMessages(std::vector<CommLayerMessage>& general_messages,
                                    std::unordered_map<VectorID, std::vector<CommLayerMessage>, VectorIDHash>&
                                        cluster_based_messages) {


        FatalAssert(general_messages.empty(), LOG_TAG_COMM_LAYER,
                    "General messages vector is not empty in CheckReceivedMessages");
        FatalAssert(cluster_based_messages.empty(), LOG_TAG_COMM_LAYER,
                    "Cluster-based messages map is not empty in CheckReceivedMessages");
#ifndef MEMORY_NODE
        return CheckReceivedMessagesFromNode(MEMORY_NODE_ID, general_messages, cluster_based_messages);
#endif
        RDMA_Manager* rdma_manager = RDMA_Manager::GetInstance();
        CHECK_NOT_NULLPTR(rdma_manager, LOG_TAG_COMM_LAYER);
        uint8_t self_node_id = rdma_manager->GetSelfNodeId();
        uint8_t num_nodes = rdma_manager->GetNumNodes();
        FatalAssert(self_node_id < num_nodes, LOG_TAG_COMM_LAYER,
                    "Invalid self_node_id %u in CheckReceivedMessages", self_node_id);
        for (uint8_t node_id = 0; node_id < num_nodes; ++node_id) {
            uint8_t target_node_id = (uint8_t)(((uint64_t)node_id + threadSelf->ID()) % num_nodes);
            if (target_node_id == self_node_id) {
                continue;
            }
            RetStatus rs = CheckReceivedMessagesFromNode(target_node_id, general_messages, cluster_based_messages);
            if (!rs.IsOK()) {
                FatalAssert(false, LOG_TAG_COMM_LAYER,
                            "Failed to check received messages from node %u: %s",
                            target_node_id, rs.Msg());
                return rs;
            }
        }
        return RetStatus::Success();
    }

    RetStatus FreeMessages(std::vector<CommLayerMessage>& general_messages,
                           std::unordered_map<VectorID, std::vector<CommLayerMessage>, VectorIDHash>&
                                cluster_based_messages) {
        for (CommLayerMessage& message : general_messages) {
            delete[] message.info.buffer;
            message.info.buffer = nullptr;
        }
        general_messages.clear();
        for (auto& pair : cluster_based_messages) {
            for (CommLayerMessage& message : pair.second) {
                delete[] message.info.buffer;
                message.info.buffer = nullptr;
            }
            pair.second.clear();
        }
        cluster_based_messages.clear();
        return RetStatus::Success();
    }

#ifndef MEMORY_NODE
    RetStatus PostClusterReadRequests(const std::vector<ClusterRemoteAccessInfo>& clusters_to_read);
    RetStatus PostClusterWriteRequests(const std::vector<ClusterRemoteAccessInfo>& clusters_to_write);
    RetStatus CheckClusterReadCompletions(std::vector<std::pair<VectorID, Version>>& completed_reads);
    /* todo: since currently the only urgenmessage is split request we do not generalize */
    RetStatus CheckUrgentMessages(std::vector<UrgentSplitRequestMessage>& messages);
#endif
};

};

#endif