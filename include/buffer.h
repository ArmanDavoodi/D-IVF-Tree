#ifndef DIVFTREE_BUFFER_H_
#define DIVFTREE_BUFFER_H_

#include "interface/buffer.h"
#include "interface/divftree.h"

#include <memory>
#include <atomic>

namespace divftree {

struct ReleaseBufferEntryFlags {
    uint8_t notifyAll : 1;
    uint8_t stablize : 1;
    uint8_t unused : 6;
};

union alignas(16) VectorLocation {
    struct {
        VectorID containerId;
        Version containerVersion;
        uint16_t entryOffset;
    };
    __int128_t raw;

    inline bool operator==(const VectorLocation& other) {
        return raw == other.raw;
    }

    inline bool operator!=(const VectorLocation& other) {
        return raw != other.raw;
    }
};

constexpr VectorLocation INVALID_VECTOR_LOCATION{.containerId = INVALID_VECTOR_ID,
                                                 .containerVersion = 0, .entryOffset = INVALID_OFFSET};

union alignas(16) AtomicVectorLocation {
    struct {
        std::atomic<VectorID> containerId;
        std::atomic<Version> containerVersion;
        std::atomic<uint16_t> entryOffset;
    };
    atomic_data128 raw;

    AtomicVectorLocation() : raw{INVALID_VECTOR_LOCATION.raw} {}

    AtomicVectorLocation(const VectorLocation& other) : raw{other.raw} {}

    AtomicVectorLocation(const AtomicVectorLocation& other, bool needAtomic = true,
                         bool relaxed = false) : raw{needAtomic ? 0 : other.raw.raw} {
        if (needAtomic) {
            atomic_load128(&other.raw, &raw, relaxed);
        }
    }

    void Store(VectorLocation location, bool needAtomic = true, bool relaxed = false) {
        if (!needAtomic) {
            return raw.raw = location.raw;
        }
        atomic_data128 data(location.raw);
        atomic_store128(&raw, &data, relaxed)
    }

    VectorLocation Load(bool needAtomic = true, bool relaxed = false) {
        if (!needAtomic) {
            return VectorLocation{.raw=raw.raw};
        }
        atomic_data128 data;
        atomic_load128(&raw, &data, relaxed);
        return VectorLocation{.raw=data.raw};
    }
};

struct BufferVectorEntry {
    const VectorID selfId;
    AtomicVectorLocation location;

    /*
     * The caller has to handle pinVersion and UnpinVersion for the container clusters.
     * Can only use location.Store if:
     *    1) if is vertex: parent is locked in X mode | parent is locked in S mode and this entry is locked in any mode
     *    2) if is vector: parent is locked in X mode |
     *                     parent is locked in S mode and this thread has changed this vector's state to Migrated
     * The atomicity is only meant to synchronize reads with writes and not writes with writes!!
     */

    BufferVectorEntry(VectorID id) : selfId(id), location(INVALID_VECTOR_LOCATION) {}

    /*
     * If self is a centroid, it is recomended to use the BufferVertex function!
     *  Also, if this is a centroid, it should be locked in X mode.
     */
    BufferVertexEntry* ReadParent(VectorLocation& currentLocation) {
        BufferManager* bufferMgr = BufferMgr::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_BUFFER);
        currentLocation = INVALID_VECTOR_LOCATION;
        if (selfId == bufferMgr->GetCurrentRootId()) {
            return nullptr;
        }

        while (true) {
            currentLocation = INVALID_VECTOR_LOCATION;
            VectorLocation location = location.Load();
            if (location == INVALID_VECTOR_LOCATION) {
                /* this means that we should have become the new root! */
                FatalAssert(selfId == bufferMgr->GetCurrentRootId());
                return nullptr;
            }

            BufferVertexEntry* parent = bufferMgr->ReadBufferEntry(location.containerId, SX_SHARED);
            currentLocation = location.Load();
            if (currentLocation == INVALID_VECTOR_LOCATION) {
                /* this means that we should have become the new root! */
                if (parent != nullptr) {
                    bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                    parent = nullptr;
                }
                FatalAssert(selfId == bufferMgr->GetCurrentRootId());
                return nullptr;
            }

            if (currentLocation != location) {
                if ((parent != nullptr) && (currentLocation.containerId == location.containerId) &&
                    (currentLocation.containerVersion == location.containerVersion)) {
                    /* There was a compaction and we are good to go! */
                    return parent;
                }

                if (parent != nullptr) {
                    bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                    parent = nullptr;
                }
                continue;
            }

            CHECK_NOT_NULLPTR(parent, LOG_TAG_BUFFER);
            return parent;
        }
    }
};

enum BufferVertexEntryState : uint8_t {
    CLUSTER_INVALID = 0,
    CLUSTER_STABLE = 1,
    CLUSTER_FULL = 2,
    CLUSTER_DELETE_IN_PROGRESS = 3,
    CLUSTER_DELETED = 4
};

/*
 * This needs to be prtected byt the header lock because we do not have 16Bytes FAA. As a result, if we
 * if we want to do this without locking, we have to use 16Byte CAS which causes a lot of contention on the
 * pin.
 */
struct ClusterPtr {
    std::atomic<uint64_t> pin;
    DIVFTreeVertexInterface* clusterPtr;
};

struct VersionedClusterPtr {
    std::atomic<uint64_t> versionPin;
    ClusterPtr clusterPtr;
};

struct BufferVertexEntry {
    BufferVectorEntry centroidMeta;
    std::atomic<BufferVertexEntryState> state;
    SXSpinLock headerLock;
    SXLock clusterLock;
    CondVar condVar;
    Version currentVersion;
    /*
     * liveVersions can only be updated if parent is locked(any mode),
     * self is locked in X mode, and headerLock is locked in X mode
     */
    std::unordered_map<Version, VersionedClusterPtr> liveVersions;

    BufferVertexEntry(DIVFTreeVertexInterface* cluster, VectorID id) : centroidMeta(id), state(CLUSTER_INVALID),
                                                                       currentVersion(0) {
        oldVersions[0] = VersionedClusterPtr{1, ClusterPtr{0, cluster}};
        /*
         * set version pin to 1 as BufferVertexEntry is referencing it with currentVersion and
         * set pin to 0 as no one is using it yet
         */
    }

    inline BufferVertexEntry* ReadParent(VectorLocation& currentLocation) {
        threadSelf->SanityCheckLockHeldInModeByMe(&clusterLock, SX_EXCLUSIVE);
        return centroidMeta.ReadParent(currentLocation);
    }

    DIVFTreeVertexInterface* ReadLatestVersion(bool pinCluster = true, bool needsHeaderLock = false) {
        if (!needsHeaderLock) {
            FatalAssert(threadSelf->LockHeldByMe(&clusterLock) != LockHeld::UNLOCKED ||
                        threadSelf->LockHeldByMe(&headerLock) != LockHeld::UNLOCKED);
        } else {
            headerLock.Lock(SX_SHARED);
        }

        DIVFTreeVertexInterface* cluster = nullptr;
        auto& it = liveVersions.find(currentVersion);
        if (it != liveVersions.end()) {
            FatalAssert(it->second.versionPin.load() != 0, LOG_TAG_BUFFER,
                        "Found current version but its version pin is 0!");
            cluster = it->second.clusterPtr.clusterPtr;
            if (pinCluster) {
                it->second.clusterPtr.pin.fetch_add(1);
            }
        }

        if (needsHeaderLock) {
            headerLock.Unlock();
        }

        return cluster;
    }

    DIVFTreeVertexInterface* MVCCReadAndPinCluster(Version version, bool headerLocked=false) {
        if (!headerLocked) {
            headerLock.Lock(SX_SHARED);
        }

        FatalAssert(version <= currentVersion, LOG_TAG_BUFFER, "Version is out of bounds: VertexID="
                    VECTORID_LOG_FMT ", latest version = %u, input version = %u",
                    VECTORID_LOG(vertexId), currentVersion, version);
        DIVFTreeVertexInterface* cluster = nullptr;
        auto& it = liveVersions.find(version);
        if (it != liveVersions.end() && it->second.versionPin.load() != 0) {
            cluster = it->second.clusterPtr.clusterPtr;
            it->second.clusterPtr.pin.fetch_add(1);
        }

        if (!headerLocked) {
            headerLock.Unlock();
        }

        return cluster;
    }

    RetStatus UpgradeAccessToExclusive(BufferVertexEntryState& targetState, bool unlockOnFail = false) {
        FatalAssert(state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(centroidMeta.selfId));
        threadSelf->SanityCheckLockHeldInModeByMe(&clusterLock, SX_SHARED);
        BufferVertexEntryState expected = CLUSTER_STABLE;
        if (state.compare_exchange_strong(expected, targetState)) {
            clusterLock.Unlock();
            clusterLock.Lock(SX_EXCLUSIVE);
            return RetStatus::Success();
        } else {
            condVar.Wait(LockWrapper<SX_SHARED>{clusterLock});
            if (unlockOnFail) {
                clusterLock.Unlock();
            }
            targetState = expected;
            return RetStatus{.stat=RetStatus::FAILED_TO_CAS_ENTRY_STATE, .message=""};
        }
    }

    void DowngradeAccessToShared() {
        FatalAssert(state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(centroidMeta.selfId));
        FatalAssert(state.load() != CLUSTER_STABLE, LOG_TAG_BUFFER, "BufferEntry state is Stable! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(centroidMeta.selfId));
        threadSelf->SanityCheckLockHeldInModeByMe(&clusterLock, SX_EXCLUSIVE);
        clusterLock.Unlock();
        clusterLock.Lock(SX_SHARED);
        state.store(CLUSTER_STABLE);
        condVar.NotifyAll();
        state.notify_all();
    }

    /* parentNode should also be locked in shared or exclusive mode and self should be locked in X mode */
    Version UpdateClusterPtr(DIVFTreeVertexInterface* newCluster, bool updateVersion = true) {
        threadSelf->SanityCheckLockHeldInModeByMe(&clusterLock, SX_EXCLUSIVE);
        headerLock.Lock(SX_EXCLUSIVE);
        FatalAssert(liveVersions.find(currentVersion) != liveVersions.end(), LOG_TAG_BUFFER,
                    "new version already exists!");
        if (updateVersion) {
            currentVersion++;
            FatalAssert(liveVersions.find(currentVersion) == liveVersions.end(), LOG_TAG_BUFFER,
                        "new version already exists!");
            FatalAssert(newCluster != nullptr, LOG_TAG_BUFFER, "Invalid cluster ptr!");
            liveVersions.emplace(currentVersion, 1, 0, newCluster);
            UnpinVersion(currentVersion - 1, true);
        } else {
            std::unordered_map<Version, VersionedClusterPtr>::iterator& it = liveVersions.find(currentVersion);
            uint64_t pinCount = it->second.clusterPtr.pin.load(std::memory_order_relaxed);
            DIVFTreeVertexInterface* cluster = it->second.clusterPtr.clusterPtr;
            it->second.clusterPtr.pin.store(0, std::memory_order_relaxed);
            it->second.clusterPtr.clusterPtr = newCluster;
            if (newCluster == nullptr) {
                liveVersions.erase(it);
            }
            cluster->MarkForRecycle(pinCount);
        }
        headerLock.Unlock();
        return currentVersion;
    }

    void UnpinVersion(Version version, bool headerLocked=false) {
        if (!headerLocked) {
            headerLock.Lock(SX_SHARED);
        }
        threadSelf->SanityCheckLockHeldByMe(&headerLock);
        FatalAssert(version <= currentVersion, LOG_TAG_BUFFER, "Version is out of bounds: VertexID="
                    VECTORID_LOG_FMT ", latest version = %u, input version = %u",
                    VECTORID_LOG(vertexId), currentVersion, version);
        auto& it = liveVersions.find(version);
        FatalAssert(it != liveVersions.end(), LOG_TAG_BUFFER, "version is not live!");

        /*
         * If versionPins gets to 0, it means that it is an old version so it should no longer be pinned or unpinned
         * therefore, unlocking the lock and acquiring it in exclusive mode should not cause any concurrency issues
         * for this specific version and we only need to get it to handle removing this version from the versionList.
         */
        uint64_t versionPin = it->second.versionPin.fetch_sub(1, std::memory_order_relaxed) - 1;
        FatalAssert(versionPin != UINT64_MAX, LOG_TAG_BUFFER, "Possible unpin underflow!");
        if (versionPin == 0) {
            headerLock.Unlock();
            headerLock.Lock(SX_EXCLUSIVE);
            uint64_t pinCount = it->second.clusterPtr.pin.load(std::memory_order_relaxed);
            DIVFTreeVertexInterface* cluster = it->second.clusterPtr.clusterPtr;
            it->second.clusterPtr.pin.store(0, std::memory_order_relaxed);
            it->second.clusterPtr.clusterPtr = nullptr;
            liveVersions.erase(it);
            cluster->MarkForRecycle(pinCount);
            headerLock.Unlock();
        } else if (!headerLocked) {
            headerLock.Unlock();
        }
    }

    void PinVersion(Version version, bool headerLocked=false) {
        if (!headerLocked) {
            headerLock.Lock(SX_SHARED);
        }
        threadSelf->SanityCheckLockHeldByMe(&headerLock);
        FatalAssert(version <= currentVersion, LOG_TAG_BUFFER, "Version is out of bounds: VertexID="
                    VECTORID_LOG_FMT ", latest version = %u, input version = %u",
                    VECTORID_LOG(vertexId), currentVersion, version);
        auto& it = liveVersions.find(version);
        FatalAssert(it != liveVersions.end(), LOG_TAG_BUFFER, "version is not live!");
        uint64_t oldVersionPin = it->second.versionPin.fetch_add(1);
        UNUSED_VARIABLE(oldVersionPin);
        FatalAssert(oldVersionPin != 0, LOG_TAG_BUFFER, "oldVersionPin was zero!");

        if (!headerLocked) {
            headerLock.Unlock();
        }
    }
};

class BufferManager : public BufferManagerInterface {
// TODO: reuse deleted IDs
public:
    BufferManager(uint64_t internalSize, uint64_t leafSize) : internalVertexSize(internalSize),
                                                              leafVertexSize(leafSize),
                                                              currentRootId(INVALID_VECTOR_ID) override {
        FatalAssert(bufferMgrInstance == nullptr, LOG_TAG_BUFFER, "Buffer already initialized");
    }

    ~BufferManager() override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");

        for (size_t level = MAX_TREE_HIGHT - 1; level > 0; --level) {
            for (BufferVertexEntry* entry : clusterDirectory[level]) {
                if (entry == nullptr) {
                    continue;
                }
                entry->clusterLock.Lock(SX_EXCLUSIVE);
                entry->headerLock.Lock(SX_EXCLUSIVE);
                for (auto& mvccClus : entry->liveVersions) {
                    mvccClus.second.versionPin = 0;
                    uint64_t readerPins = mvccClus.second.clusterPtr.pin.load(std::memory_order_relaxed);
                    DIVFTreeVertexInterface* clusterPtr = mvccClus.second.clusterPtr.clusterPtr;
                    mvccClus.second.clusterPtr.pin.store(0, std::memory_order_relaxed);
                    mvccClus.second.clusterPtr.clusterPtr = nullptr;
                    clusterPtr->MarkForRecycle(readerPins);
                }
                entry->liveVersions.clear();
                entry->currentVersion = 0;
                entry->state.store(CLUSTER_INVALID);
                entry->centroidMeta.location.Store(INVALID_VECTOR_LOCATION, true);
                entry->headerLock.Unlock();
                entry->clusterLock.Unlock();
            }
        }

        for (BufferVertexEntry* entry : clusterDirectory[0]) {
            if (entry == nullptr) {
                continue;
            }
            entry->clusterLock.Lock(SX_EXCLUSIVE);
            entry->headerLock.Lock(SX_EXCLUSIVE);
            for (auto& mvccClus : entry->liveVersions) {
                mvccClus.second.versionPin = 0;
                uint64_t readerPins = mvccClus.second.clusterPtr.pin.load(std::memory_order_relaxed);
                DIVFTreeVertexInterface* clusterPtr = mvccClus.second.clusterPtr.clusterPtr;
                mvccClus.second.clusterPtr.pin.store(0, std::memory_order_relaxed);
                mvccClus.second.clusterPtr.clusterPtr = nullptr;
                clusterPtr->MarkForRecycle(readerPins);
            }
            entry->liveVersions.clear();
            entry->currentVersion = 0;
            entry->state.store(CLUSTER_INVALID);
            entry->centroidMeta.location.Store(INVALID_VECTOR_LOCATION, true);
            entry->headerLock.Unlock();
            entry->clusterLock.Unlock();
        }

        for (BufferVectorEntry* entry : vectorDirectory) {
            if (entry == nullptr) {
                continue;
            }
            entry->location.Store(INVALID_VECTOR_LOCATION, true);
        }

        sleep(10);

        for (size_t level = MAX_TREE_HIGHT - 1; level > 0; --level) {
            for (BufferVertexEntry* entry : clusterDirectory[level]) {
                if (entry != nullptr) {
                    delete entry;
                    entry = nullptr;
                }
            }
            clusterDirectory[level].clear();
        }

        for (BufferVertexEntry* entry : clusterDirectory[0]) {
            if (entry != nullptr) {
                delete entry;
                entry = nullptr;
            }
        }
        clusterDirectory[0].clear();

        for (BufferVectorEntry* entry : vectorDirectory) {
            if (entry != nullptr) {
                delete entry;
                entry = nullptr;
            }
        }
        vectorDirectory.clear();
    }

    /*
     * vertexMetaDataSize should be sizeof(Vertex without the Cluster Header -> maybe we should use a pointer?)
     *
     * will return the rootEntry in the INVALID state and locked in exclusive mode!
     */
    inline static RetStatus Init(uint64_t vertexMetaDataSize, uint16_t cap, uint16_t dim, BufferVertexEntry*& rootEntry) {
        FatalAssert(bufferMgrInstance == nullptr, LOG_TAG_BUFFER, "Buffer already initialized");
        uint64_t internalVertexSize = ALLIGNED_SIZE(ALLIGNED_SIZE(vertexMetaDataSize) +
                                      ALLIGNED_SIZE(sizeof(ClusterHeader) + Cluster::DataBytes(false, cap, dim)));
        uint64_t leafVertexSize = ALLIGNED_SIZE(ALLIGNED_SIZE(vertexMetaDataSize) +
                                                ALLIGNED_SIZE(sizeof(ClusterHeader) +
                                                              Cluster::DataBytes(true, cap, dim)));
        bufferMgrInstance = new BufferManager(internalVertexSize, leafVertexSize);
        rootEntry = nullptr;
        bufferMgrInstance->BatchCreateBufferEntry(1, VectorID::LEAF_LEVEL, &rootEntry);
        return RetStatus::Success();
    }

    /*
     * Note: No thread should be calling any functions from the bufferMgr the moment
     * shutdown is called or some resources may not be cleaned properly
     */
    inline static RetStatus Shutdown() {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        delete bufferMgrInstance;
        bufferMgrInstance = nullptr;
        return RetStatus::Success();
    }

    inline static BufferManager* GetInstance() {
        return bufferMgrInstance;
    }

    DIVFTreeVertexInterface* AllocateMemoryForVertex(uint8_t level) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        FatalAssert(MAX_TREE_HIGHT > VectorID::VECTOR_LEVEL && (uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_BUFFER,
                    "Level is out of bounds.");
        return aligned_alloc(CACHE_LINE_SIZE,
                             ((uint64_t)level == VectorID::LEAF_LEVEL ? leafVertexSize : internalVertexSize));
    }

    void BatchCreateBufferEntry(size_t num_entries, uint8_t level, BufferVertexEntry** entries) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_NOT_NULLPTR(entries, LOG_TAG_BUFFER);
        FatalAssert(num_entries > 0, LOG_TAG_BUFFER, "number of entries cannot be 0!");
        FatalAssert(MAX_TREE_HIGHT > VectorID::VECTOR_LEVEL && (uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_BUFFER,
                    "Level is out of bounds.");

        bufferMgrLock.Lock(SX_EXCLUSIVE);
        const uiont64_t nextVal = clusterDirectory[level].size();
        for (size_t i = 0; i < num_entries; ++i) {
            VectorID id = 0;
            id._val = nextVal + i;
            id._level = level;
            id._creator_node_id = 0;

            DIVFTreeVertexInterface* memLoc = AllocateMemoryForVertex(level)
            CHECK_NOT_NULLPTR(memLoc, LOG_TAG_BUFFER);
            entries[i] = new BufferVertexEntry(memLoc, id);
            entries[i]->clusterLock.Lock(SX_EXCLUSIVE);

            clusterDirectory[level].emplace_back(entries[i]);
        }
        bufferMgrLock.Unlock();
    }

    void BatchCreateVectorEntry(size_t num_entries, BufferVectorEntry** entries) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_NOT_NULLPTR(entries, LOG_TAG_BUFFER);
        FatalAssert(num_entries > 0, LOG_TAG_BUFFER, "number of entries cannot be 0!");

        bufferMgrLock.Lock(SX_EXCLUSIVE);
        const uiont64_t nextVal = vectorDirectory.size();
        for (size_t i = 0; i < num_entries; ++i) {
            VectorID id = 0;
            id._val = nextVal + i;
            id._level = VectorID::LEAF_LEVEL;
            id._creator_node_id = 0;

            entries[i] = new BufferVectorEntry(id);
            vectorDirectory.emplace_back(entries[i]);
        }
        bufferMgrLock.Unlock();
    }

    void RecordRoot(VectorID newRootId) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        SANITY_CHECK(
            bufferMgrLock.Lock(SX_SHARED);
            BufferVertexEntry* newRootEntry = GetVertexEntry(newRootId, false);
            UNUSED_VARIABLE(newRootEntry);
            CHECK_NOT_NULLPTR(newRootEntry, LOG_TAG_BUFFER);
            VectorID oldRootId = currentRootId.load();
            if (oldRootId.IsValid()) {
                BufferVertexEntry* oldRootEntry = GetVertexEntry(oldRootId, false);
                CHECK_NOT_NULLPTR(oldRootEntry, LOG_TAG_BUFFER);
                threadSelf->SanityCheckLockHeldInModeByMe(&oldRootEntry->clusterLock, SX_EXCLUSIVE);
            }
            /*
            * the new root state can be deleted if it is deleted right now but has not been able
            * to get the lock on the current root yet
            */
            FatalAssert(newRootEntry->state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER,
                        "newRootState cannot be Invalid!");
            bufferMgrLock.Unlock();
        )
        currentRootId.store(newRootId);
    }

    VectorID GetCurrentRootId() const {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        return currentRootId.load();
    }

    inline BufferVectorEntry* GetVectorEntry(VectorID id) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_VECTOR(id, LOG_TAG_BUFFER);
        bufferMgrLock.Lock(SX_SHARED);
        FatalAssert(vectorDirectory.size() > id._val, LOG_TAG_BUFFER, "VertexID val is out of bounds. "
                    VECTORID_LOG_FMT ", max_val:%lu", VECTORID_LOG(id), vectorDirectory.size());
        BufferVectorEntry* entry = vectorDirectory[id._val];
        bufferMgrLock.Unlock();
        return entry;
    }

    inline void PinVertexVersion(VectorID vertexId, Version version) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        BufferVertexEntry* entry = GetVertexEntry(vertexId);
        FatalAssert(entry != nullptr, LOG_TAG_BUFFER, "VertexID not found in buffer. VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));

        FatalAssert(entry->state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
        FatalAssert(entry->state.load() != CLUSTER_DELETED, LOG_TAG_BUFFER, "BufferEntry state is Deleted! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
        FatalAssert(entry->centroidMeta.selfId == vertexId, LOG_TAG_BUFFER, "BufferEntry id mismatch! VertexID="
                    VECTORID_LOG_FMT "EntryID = " VECTORID_LOG_FMT, VECTORID_LOG(vertexId),
                    VECTORID_LOG(entry->centroidMeta.selfId));
        entry->PinVersion(version);
    }

    inline void UnpinVertexVersion(VectorID vertexId, Version version) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        BufferVertexEntry* entry = GetVertexEntry(vertexId);
        FatalAssert(entry != nullptr, LOG_TAG_BUFFER, "VertexID not found in buffer. VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));

        FatalAssert(entry->state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
        FatalAssert(entry->state.load() != CLUSTER_DELETED, LOG_TAG_BUFFER, "BufferEntry state is Deleted! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
        FatalAssert(entry->centroidMeta.selfId == vertexId, LOG_TAG_BUFFER, "BufferEntry id mismatch! VertexID="
                    VECTORID_LOG_FMT "EntryID = " VECTORID_LOG_FMT, VECTORID_LOG(vertexId),
                    VECTORID_LOG(entry->centroidMeta.selfId));
        entry->UnpinVersion(version);
    }

    inline VectorLocation LoadCurrentVectorLocation(VectorID vectorId) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_VECTORID_IS_VALID(vectorId, LOG_TAG_BUFFER);

        if (vectorId.IsCentroid()) {
            BufferVertexEntry* entry = GetVertexEntry(vectorId);
            if (entry == nullptr || entry->state.load() == CLUSTER_DELETED || entry->state.load() == CLUSTER_INVALID) {
                return INVALID_VECTOR_LOCATION;
            }
            FatalAssert(entry->centroidMeta.selfId == vectorId, LOG_TAG_BUFFER, "BufferEntry id mismatch! VertexID="
                        VECTORID_LOG_FMT "EntryID = " VECTORID_LOG_FMT, VECTORID_LOG(vectorId),
                        VECTORID_LOG(entry->centroidMeta.selfId));
            return entry->centroidMeta.location.Load();
        } else {
            BufferVectorEntry* entry = GetVectorEntry(vectorId);
            if (entry == nullptr) {
                return INVALID_VECTOR_LOCATION;
            }
            return entry->location.Load();
        }
    }

    /*
     * The caller has to handle pinVersion and UnpinVersion for the container clusters.
     * Can only be called if vectorId:
     *    1) if is vertex: parent is locked in X mode | parent is locked in S mode and this entry is locked in any mode
     *    2) if is vector: parent is locked in X mode |
     *                     parent is locked in S mode and this thread has changed this vector's state to Migrated
     * The atomicity is only meant to synchronize reads with writes and not writes with writes!!
     */
    inline void UpdateVectorLocation(VectorID vectorId, VectorLocation newLocation) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_VECTORID_IS_VALID(vectorId, LOG_TAG_BUFFER);
        VectorLocation oldLocation = INVALID_VECTOR_LOCATION;
        if (newLocation != INVALID_VECTOR_LOCATION) {
            CHECK_VECTORID_IS_VALID(newLocation.containerId, LOG_TAG_BUFFER);
            CHECK_VECTORID_IS_CENTROID(newLocation.containerId, LOG_TAG_BUFFER);
            FatalAssert(newLocation.entryOffset != INVALID_OFFSET, LOG_TAG_BUFFER,
                        "Invalid entry offset.");
            PinVertexVersion(newLocation.containerId,
                             newLocation.containerVersion);
        }

        if (vectorId.IsCentroid()) {
            BufferVertexEntry* entry = GetVertexEntry(vectorId);
            FatalAssert(entry != nullptr, LOG_TAG_BUFFER, "VertexID not found in buffer. VertexID="
                        VECTORID_LOG_FMT, VECTORID_LOG(vectorId));
            FatalAssert(entry->state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                        VECTORID_LOG_FMT, VECTORID_LOG(vectorId));
            FatalAssert(entry->state.load() != CLUSTER_DELETED, LOG_TAG_BUFFER, "BufferEntry state is Deleted! VertexID="
                        VECTORID_LOG_FMT, VECTORID_LOG(vectorId));
            FatalAssert(entry->centroidMeta.selfId == vectorId, LOG_TAG_BUFFER, "BufferEntry id mismatch! VertexID="
                        VECTORID_LOG_FMT "EntryID = " VECTORID_LOG_FMT, VECTORID_LOG(vectorId),
                        VECTORID_LOG(entry->centroidMeta.selfId));
            oldLocation = entry->centroidMeta.location.Load();
            entry->centroidMeta.location.Store(newLocation);
        } else {
            BufferVectorEntry* entry = GetVectorEntry(vectorId);
            FatalAssert(entry != nullptr, LOG_TAG_BUFFER, "VectorID not found in buffer. VectorID="
                        VECTORID_LOG_FMT, VECTORID_LOG(vectorId));
            oldLocation = entry->location.Load();
            entry->location.Store(newLocation);
        }

        if (oldLocation != INVALID_VECTOR_LOCATION) {
            UnpinVertexVersion(oldLocation.containerId,
                               oldLocation.containerVersion);
        }
    }

    inline void UpdateVectorLocationOffset(VectorID vectorId, uint16_t newOffset) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_VECTORID_IS_VALID(vectorId, LOG_TAG_BUFFER);

        if (vectorId.IsCentroid()) {
            BufferVertexEntry* entry = GetVertexEntry(vectorId);
            FatalAssert(entry != nullptr, LOG_TAG_BUFFER, "VertexID not found in buffer. VertexID="
                        VECTORID_LOG_FMT, VECTORID_LOG(vectorId));
            FatalAssert(entry->state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                        VECTORID_LOG_FMT, VECTORID_LOG(vectorId));
            FatalAssert(entry->state.load() != CLUSTER_DELETED, LOG_TAG_BUFFER, "BufferEntry state is Deleted! VertexID="
                        VECTORID_LOG_FMT, VECTORID_LOG(vectorId));
            FatalAssert(entry->centroidMeta.selfId == vectorId, LOG_TAG_BUFFER, "BufferEntry id mismatch! VertexID="
                        VECTORID_LOG_FMT "EntryID = " VECTORID_LOG_FMT, VECTORID_LOG(vectorId),
                        VECTORID_LOG(entry->centroidMeta.selfId));
            entry->centroidMeta.location.entryOffset.store(newOffset);
        } else {
            BufferVectorEntry* entry = GetVectorEntry(vectorId);
            FatalAssert(entry != nullptr, LOG_TAG_BUFFER, "VectorID not found in buffer. VectorID="
                        VECTORID_LOG_FMT, VECTORID_LOG(vectorId));
            entry->location.entryOffset.store(newOffset);
        }
    }

    DIVFTreeVertexInterface* ReadAndPinVertex(VectorID vertexId, Version version) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        BufferVertexEntry* entry = GetVertexEntry(vertexId);
        if (entry == nullptr) {
            return nullptr;
        }

        FatalAssert(entry->state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
        FatalAssert(entry->centroidMeta.selfId == vertexId, LOG_TAG_BUFFER, "BufferEntry id mismatch! VertexID="
                    VECTORID_LOG_FMT "EntryID = " VECTORID_LOG_FMT, VECTORID_LOG(vertexId),
                    VECTORID_LOG(entry->centroidMeta.selfId));
        if (entry->state.load() == CLUSTER_DELETED) {
            return nullptr;
        }

        entry->headerLock.Lock(SX_SHARED);
        FatalAssert(version <= entry->currentVersion, LOG_TAG_BUFFER, "Version is out of bounds: VertexID="
                    VECTORID_LOG_FMT ", latest version = %u, input version = %u",
                    VECTORID_LOG(vertexId), entry->currentVersion, version);
        auto& it = entry->liveVersions.find(version);
        if (it == entry->liveVersions.end() || it->second.versionPin.load() == 0) {
            entry->headerLock.Unlock();
            return nullptr;
        }

        it->second.clusterPtr.pin.fetch_add(1);
        DIVFTreeVertexInterface* vertex = it->second.clusterPtr.clusterPtr;
        entry->headerLock.Unlock();
        if (vertex == nullptr) {
            return vertex;
        }

        FatalAssert(vertex->CentroidID() == vertexId, LOG_TAG_BUFFER, "Mismatch in ID. BaseID=" VECTORID_LOG_FMT
                    ", Found ID=" VECTORID_LOG_FMT, VECTORID_LOG(vertexId), VECTORID_LOG(vertex->CentroidID()));
        return vertex;
    }

    /*
     * If we need to lock multiple clusters, then lower level clusters always take priority and
     * are locked first(i.e root is locked the last). If we want to lock to clusters in the same level,
     * the cluster with lower id.val should be locked first.
     */
    BufferVertexEntry* ReadBufferEntry(VectorID vertexId, LockMode mode, bool* blocked = nullptr) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        BufferVertexEntry* entry = GetVertexEntry(vertexId);
        if (entry == nullptr) {
            return nullptr;
        }

        FatalAssert(entry->state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
        FatalAssert(entry->centroidMeta.selfId == vertexId, LOG_TAG_BUFFER, "BufferEntry id mismatch! VertexID="
                    VECTORID_LOG_FMT "EntryID = " VECTORID_LOG_FMT, VECTORID_LOG(vertexId),
                    VECTORID_LOG(entry->centroidMeta.selfId));
        BufferVertexEntryState state = entry->state.load();
        while (state != CLUSTER_STABLE) {
            if (state == CLUSTER_DELETED) {
                return nullptr;
            }
            entry->state.wait(state);
            state = entry->state.load();
            if (blocked != nullptr) {
                *blocked = true;
            }
        }

        if (blocked == nullptr || (*blocked)) {
            entry->clusterLock.Lock(mode);
        } else {
            if (!entry->clusterLock.TryLock(mode)) {
                *blocked = true;
                entry->clusterLock.Lock(mode);
            } else {
                *blocked = false;
            }
        }

        state = entry->state.load();
        if (state == CLUSTER_DELETED) {
            entry->clusterLock.Unlock();
            return nullptr;
        }

        if (state != CLUSTER_STABLE) {
            if (blocked != nullptr) {
                *blocked = true;
            }
            entry->condVar.Wait(LockWrapper<mode>{entry->clusterLock})
        }

        state = entry->state.load();
        if (state == CLUSTER_DELETED) {
            entry->clusterLock.Unlock();
            return nullptr;
        }

        FatalAssert(state == CLUSTER_STABLE, LOG_TAG_BUFFER,
                    "BufferEntry state is not stable! VertexID=" VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
        return entry;
    }

    BufferVertexEntry* TryReadBufferEntry(VectorID vertexId, LockMode mode) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        BufferVertexEntry* entry = GetVertexEntry(vertexId);
        if (entry == nullptr) {
            return nullptr;
        }

        FatalAssert(entry->state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
        FatalAssert(entry->centroidMeta.selfId == vertexId, LOG_TAG_BUFFER, "BufferEntry id mismatch! VertexID="
                    VECTORID_LOG_FMT "EntryID = " VECTORID_LOG_FMT, VECTORID_LOG(vertexId),
                    VECTORID_LOG(entry->centroidMeta.selfId));
        if (entry->state.load() != CLUSTER_STABLE || !entry->clusterLock.TryLock(mode)) {
            return nullptr;
        }

        if (entry->state.load() != CLUSTER_STABLE) {
            entry->clusterLock.Unlock();
            return nullptr;
        }

        FatalAssert(entry->state.load() == CLUSTER_STABLE, LOG_TAG_BUFFER, "BufferEntry state is not stable! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
        return &entry;
    }

    void ReleaseBufferEntry(BufferVertexEntry* entry, ReleaseBufferEntryFlags flags) override {
        CHECK_NOT_NULLPTR(entry, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_VALID(entry->centroidMeta.selfId, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_CENTROID(entry->centroidMeta.selfId, LOG_TAG_BUFFER);
        FatalAssert(entry->state.load() != CLUSTER_INVALID, LOG_TAG_BUFFER, "BufferEntry state is Invalid! VertexID="
                    VECTORID_LOG_FMT, VECTORID_LOG(entry->centroidMeta.selfId));
        if ((bool)(flags.notifyAll)) {
            FatalAssert(entry->state.load() != CLUSTER_STABLE, LOG_TAG_BUFFER, "BufferEntry state is Stable! VertexID="
                        VECTORID_LOG_FMT, VECTORID_LOG(entry->centroidMeta.selfId));
            threadSelf->SanityCheckLockHeldInModeByMe(&entry->clusterLock, SX_EXCLUSIVE);
            if ((bool)(flags.stablize)) {
                entry->state.store(CLUSTER_STABLE);
            } else {
                FatalAssert(entry->state.load() == CLUSTER_DELETED, LOG_TAG_BUFFER, "BufferEntry state is not deleted! "
                            "VertexID=" VECTORID_LOG_FMT, VECTORID_LOG(entry->centroidMeta.selfId));
            }
            entry->condVar.NotifyAll();
            entry->state.notify_all();
        }
        entry->clusterLock.Unlock();
    }

    uint64_t GetHeight() const {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        return currentRootId.load()._level + 1;
    }

    String ToString() {
        FatalAssert(false, LOG_TAG_NOT_IMPLEMENTED, "Not Implemented!");
        /*
         * Todo: we cannot get any other locks when we get the bufferMgr lock but if we do not get the
         * BufferMgr lock the directory may change! Moreover, we may have multiple versions and printing all versions
         * seem to be too much! Can we do anything about this?
         */
        if (bufferMgrInstance == nullptr) {
            return String("NULL");
        }

        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        bufferMgrLock.Lock(SX_SHARED);
        uint64_t height = GetHeight();
        String str = "<Height: " + std::to_string(height) + ", ";
        str += "Directory:[";
        for (size_t i = 0; i < height; ++i) {
            str += "Level: " + std::to_string(i) + ":[";
            for (BufferVertexEntry *entry : clusterDirectory[i]) {
                if (entry == nullptr) {
                    str += "{NULL}";
                } else {
                    str += String("{VectorID:" VECTORID_LOG_FMT ", Info:", VECTORID_LOG(entry->centroidMeta.selfId));
                    str += entry->
                }
                if (j != clusterDirectory[i].size() - 1) {
                    str += ", ";
                }
            }
            if (i != height - 1) {
                str += "], ";
            }
            else {
                str += "]";
            }
            str += ">";
        }
        bufferMgrLock.Unlock();
        return str;
    }

protected:
    inline BufferVertexEntry* GetVertexEntry(VectorID id, bool needLock = true) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_CENTROID(id, LOG_TAG_BUFFER);
        uint64_t levelIdx = id._level - 1;
        FatalAssert(MAX_TREE_HIGHT > levelIdx, LOG_TAG_BUFFER, "Level is out of bounds. VertexID=" VECTORID_LOG_FMT,
                    VECTORID_LOG(id));
        if (needLock) {
            bufferMgrLock.Lock(SX_SHARED);
        }
        threadSelf->SanityCheckLockHeldByMe(&bufferMgrLock);
        FatalAssert(clusterDirectory[levelIdx].size() > id._val, LOG_TAG_BUFFER, "VertexID val is out of bounds. "
                    VECTORID_LOG_FMT ", max_val:%lu", VECTORID_LOG(id), clusterDirectory[levelIdx].size());
        BufferVertexEntry* entry = clusterDirectory[levelIdx][id._val];
        if (needLock) {
            bufferMgrLock.Unlock();
        }
        return entry;
    }

    const uint64_t internalVertexSize;
    const uint64_t leafVertexSize;
    SXSpinLock bufferMgrLock;
    std::atomic<VectorID> currentRootId;
    std::vector<BufferVectorEntry*> vectorDirectory;
    std::vector<BufferVertexEntry*> clusterDirectory[MAX_TREE_HIGHT];

    inline static BufferManager *bufferMgrInstance = nullptr;

TESTABLE;
};

};

#endif