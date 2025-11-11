#ifndef DIVFTREE_BUFFER_H_
#define DIVFTREE_BUFFER_H_

#include "interface/divftree.h"

#include <memory>
#include <atomic>
#include <unordered_map>

namespace divftree {

struct BufferVertexEntry;
class BufferManager;

/*
 * This needs to be prtected byt the header lock because we do not have 16Bytes FAA. As a result, if we
 * if we want to do this without locking, we have to use 16Byte CAS which causes a lot of contention on the
 * pin.
 */
/* todo: use CAS16 for this */
struct ClusterPtr {
    std::atomic<uint64_t> pin;
    DIVFTreeVertexInterface* clusterPtr;

    ClusterPtr(uint64_t p, DIVFTreeVertexInterface* ptr) : pin{p}, clusterPtr{ptr} {}
    ClusterPtr() : pin{0}, clusterPtr{nullptr} {}
    ~ClusterPtr() = default;

    /* has to be protected by a lock! */
    ClusterPtr(const ClusterPtr& other) {
        pin.store(other.pin.load(std::memory_order_relaxed), std::memory_order_relaxed);
        clusterPtr = other.clusterPtr;
    }

    /* has to be protected by a lock! */
    ClusterPtr& operator=(const ClusterPtr& other) {
        pin.store(other.pin.load(std::memory_order_relaxed), std::memory_order_relaxed);
        clusterPtr = other.clusterPtr;
        return *this;
    }
};

struct BufferVertexEntry {
    const VectorID selfId;
    // std::atomic<BufferVertexEntryState> state;
    SXSpinLock clusterLock;
    SXSpinLock headerLock;
    Version currentVersion;

    std::unordered_map<Version, ClusterPtr> liveVersions;

    std::atomic<bool> hasCompactionTask;
    std::atomic<bool> hasMergeTask;
    std::unordered_set<VectorID, VectorIDHash> migrationTasks;

    BufferVertexEntry(DIVFTreeVertexInterface* cluster, VectorID id, uint64_t initialPin = 1);
    ~BufferVertexEntry();

    static void* operator new(std::size_t size);
    static void operator delete(void* ptr) noexcept;

    DIVFTreeVertexInterface* ReadLatestVersion(bool pinCluster = true, bool needsHeaderLock = false);

    void UpdateClusterPtr(DIVFTreeVertexInterface* newCluster);
    void UpdateClusterPtr(DIVFTreeVertexInterface* newCluster, Version newVersion);

    String ToString();
};

struct RootEntry {
    BufferVertexEntry* rootEntry;
    Version rootVersion;
    std::atomic<bool> send_unpin;
};

/* todo: add tostring for buffer entries */
class BufferManager {
// TODO: reuse deleted IDs
public:
    BufferManager(uint64_t internalSize, uint64_t leafSize);
    ~BufferManager();

    /*
     * vertexMetaDataSize should be sizeof(Vertex without the Cluster Header -> maybe we should use a pointer?)
     *
     * will return the rootEntry in the INVALID state and locked in exclusive mode!
     */
    inline static BufferVertexEntry* Init(uint64_t vertexMetaDataSize,
                                          uint16_t leaf_blk_size, uint16_t internal_blk_size,
                                          uint16_t leaf_cap, uint16_t internal_cap, uint16_t dim);

    /*
     * Note: No thread should be calling any functions from the bufferMgr the moment
     * shutdown is called or some resources may not be cleaned properly
     */
    inline static void Shutdown();

    inline static BufferManager* GetInstance();

    DIVFTreeVertexInterface* AllocateMemoryForVertex(uint8_t level);

    // void UpdateRoot(VectorID newRootId, Version newRootVersion, BufferVertexEntry* oldRootEntry);
    // BufferVertexEntry* CreateNewRootEntry(VectorID expRootId);
    void BatchCreateBufferEntry(uint16_t num_entries, uint8_t level, BufferVertexEntry** entries,
                                DIVFTreeVertexInterface** clusters = nullptr, VectorID* ids = nullptr,
                                Version* versions = nullptr);

    VectorID GetCurrentRootId() const;
    VectorID GetCurrentRootIdAndVersion(Version& version);

    RetStatus ReadAndPinVertex(VectorID vertexId, Version version, DIVFTreeVertexInterface*& vertex,
                               bool* outdated = nullptr);
    DIVFTreeVertexInterface* ReadAndPinRoot();

    /*
     * If we need to lock multiple clusters, then lower level clusters always take priority and
     * are locked first(i.e root is locked the last). If we want to lock to clusters in the same level,
     * the cluster with lower id.val should be locked first.
     */
    BufferVertexEntry* ReadBufferEntry(VectorID vertexId, LockMode mode, bool* blocked = nullptr);
    BufferVertexEntry* TryReadBufferEntry(VectorID vertexId, LockMode mode);
    void ReleaseBufferEntry(BufferVertexEntry* entry);

    uint64_t GetHeight() const;

    String ToString();

    RetStatus CreateUpdateHandle(VectorID target);
    void SignalUpdateHandleIfNeeded(VectorID target);
    RetStatus WaitForUpdateToGoThrough(VectorID target);
    RetStatus CheckIfUpdateHasGoneThrough(VectorID target, bool& updated);

    bool AddMigrationTaskIfNotExists(VectorID first, VectorID second, BufferVertexEntry* firstEntry = nullptr);
    void RemoveMigrationTask(VectorID first, VectorID second, BufferVertexEntry* firstEntry = nullptr);
    bool AddCompactionTaskIfNotExists(VectorID id, BufferVertexEntry* entry = nullptr);
    void RemoveCompactionTask(VectorID id, BufferVertexEntry* entry = nullptr);
    bool AddMergeTaskIfNotExists(VectorID id, BufferVertexEntry* entry = nullptr);
    void RemoveMergeTask(VectorID id, BufferVertexEntry* entry = nullptr);

    VectorID GetRandomCentroidIdAtLayer(uint8_t level, VectorID exclude = INVALID_VECTOR_ID,
                                        bool need_lock = true, uint64_t* num_retries = nullptr);
    std::pair<VectorID, VectorID> GetTwoRandomCentroidIdAtLayer(uint8_t level, bool need_lock = true);
    VectorID GetRandomCentroidIdAtNonRootLayer(VectorID exclude = INVALID_VECTOR_ID);
    std::pair<VectorID, VectorID> GetTwoRandomCentroidIdAtNonRootLayer();

protected:
    const uint64_t internalVertexSize;
    const uint64_t leafVertexSize;
    SXSpinLock bufferMgrLock;
    RootEntry root;
    std::vector<BufferVertexEntry*> clusterDirectory[MAX_TREE_HIGHT];
    SXSpinLock handleLock;
    std::unordered_map<VectorID, std::atomic<bool>*, VectorIDHash> handles;

    inline static BufferManager *bufferMgrInstance = nullptr;

    inline BufferVertexEntry* GetVertexEntry(VectorID id, bool needLock = true);
    inline BufferVertexEntry* GetRootEntry(bool needLock = true);

    DIVFTreeVertexInterface* AllocateMemoryFromTheMemoryNode(uint8_t level);

TESTABLE;
};

};

#include "buffer_impl.h"

#endif