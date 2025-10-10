#ifndef DIVFTREE_BUFFER_H_
#define DIVFTREE_BUFFER_H_

#include "interface/divftree.h"

#include <memory>
#include <atomic>
#include <unordered_map>

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

    inline VectorLocation& operator=(const VectorLocation& other) {
        raw = other.raw;
        return *this;
    }

    inline bool operator==(const VectorLocation& other) {
        return raw == other.raw;
    }

    inline bool operator!=(const VectorLocation& other) {
        return raw != other.raw;
    }

    VectorLocation() : raw(0) {}
    VectorLocation(const VectorLocation& other) : raw(other.raw) {}
    VectorLocation(VectorLocation&& other) : raw(other.raw) {}

    VectorLocation(VectorID cid, Version cv, uint16_t eo) : containerId(cid), containerVersion(cv), entryOffset(eo) {}
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

    BufferVectorEntry(VectorID id) : selfId(id), location(INVALID_VECTOR_LOCATION) {
        /* since we have an atomicVectorLocation and it has to be 16byte aligned, the vector entry should be
        16 byte aligned as well */
        FatalAssert(ALLIGNED(this, 16), LOG_TAG_BUFFER, "BufferVectorEntry is not alligned!");

    }

    /*
     * If self is a centroid, it is recomended to use the BufferVertex function!
     *  Also, if this is a centroid, it should be locked in X mode.
     */
    BufferVertexEntry* ReadParentEntry(VectorLocation& currentLocation) {
        BufferManager* bufferMgr = BufferMgr::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_BUFFER);
        currentLocation = INVALID_VECTOR_LOCATION;
        if (selfId == bufferMgr->GetCurrentRootId()) {
            return nullptr;
        }

        BufferVertexEntry* parent = nullptr;
        while (true) {
            currentLocation = INVALID_VECTOR_LOCATION;
            VectorLocation expLocation = location.Load();
            if (expLocation == INVALID_VECTOR_LOCATION) {
                /* this means that we should have become the new root! */
                FatalAssert(selfId.IsVector() || (selfId == bufferMgr->GetCurrentRootId()), LOG_TAG_BUFFER,
                            "if location is invalid and we are a centroid we have to be the root!");
                return nullptr;
            }

            parent = bufferMgr->ReadBufferEntry(expLocation.containerId, SX_SHARED);
            currentLocation = location.Load();
            if (currentLocation == INVALID_VECTOR_LOCATION) {
                /* this means that we should have become the new root! */
                if (parent != nullptr) {
                    bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                    parent = nullptr;
                }
                FatalAssert(selfId.IsVector() || (selfId == bufferMgr->GetCurrentRootId()), LOG_TAG_BUFFER,
                            "if location is invalid and we are a centroid we have to be the root!");
                return nullptr;
            }

            if (currentLocation != expLocation) {
                if ((parent != nullptr) && (currentLocation.containerId == expLocation.containerId) &&
                    (currentLocation.containerVersion == expLocation.containerVersion)) {
                    /* This just means that there was a compaction and out offset has changed */
                    break
                }

                if (parent != nullptr) {
                    bufferMgr->ReleaseBufferEntry(parent, ReleaseBufferEntryFlags{.notifyAll=0, .stablize=0});
                    parent = nullptr;
                }
                continue;
            }

            break;
        }

        CHECK_NOT_NULLPTR(parent, LOG_TAG_BUFFER);
        SANITY_CHECK(
            FatalAssert(parent->centroidMeta.selfId._level = selfId._level + 1, LOG_TAG_BUFFER,
                        "level mismatch between parent and child!");
            DIVFTreeVertexInterface* parent_vertex = parent->ReadLatestVersion(false);
            bool is_leaf = parent->centroidMeta.selfId.IsLeaf();
            void* meta = parent_vertex->GetCluster().MetaData(currentLocation.entryOffset, false,
                                                              parent_vertex->GetAttributes().block_size,
                                                              parent_vertex->GetAttributes().cap,
                                                              parent_vertex->GetAttributes().index->
                                                              GetAttributes().dimension);
            if (is_leaf) {
                VectorMetaData* vmt = reinterpret_cast<VectorMetaData*>(meta);
                FatalAssert(meta->id == selfId, LOG_TAG_BUFFER, "Corrupted location data!");
                FatalAssert(meta->state == VECTOR_STATE_VALID, LOG_TAG_BUFFER, "Corrupted location data!");
            } else {
                CentroidMetaData* vmt = reinterpret_cast<CentroidMetaData*>(meta);
                FatalAssert(meta->id == selfId, LOG_TAG_BUFFER, "Corrupted location data!");
                FatalAssert(meta->state == VECTOR_STATE_VALID, LOG_TAG_BUFFER, "Corrupted location data!");
            }
        )
        return parent;
    }

    DIVFTreeVertexInterface* ReadAndPinParent(VectorLocation& currentLocation) {
        BufferManager* bufferMgr = BufferMgr::GetInstance();
        CHECK_NOT_NULLPTR(bufferMgr, LOG_TAG_BUFFER);
        currentLocation = INVALID_VECTOR_LOCATION;
        if (selfId == bufferMgr->GetCurrentRootId()) {
            return nullptr;
        }

        DIVFTreeVertexInterface* parent = nullptr;
        while (true) {
            VectorLocation excpectedLocation = location.Load();
            if (excpectedLocation == INVALID_VECTOR_LOCATION) {
                /* this means that we should have become the new root! */
                FatalAssert(selfId.IsVector() || (selfId == bufferMgr->GetCurrentRootId()), LOG_TAG_BUFFER,
                            "if location is invalid and we are a centroid we have to be the root!");
                return nullptr;
            }

            parent = bufferMgr->ReadAndPin(excpectedLocation.containerId, excpectedLocation.containerVersion);
            if (parent == nullptr) {
                continue;
            }
            currentLocation = location.Load();
            if (currentLocation.containerId != excpectedLocation.containerId ||
                currentLocation.containerVersion != excpectedLocation.containerVersion) {
                parent->Unpin();
                parent = nullptr;
                continue;
            }

            /* todo: stat collect how many times we have to retry */
            return parent;
        }
    }
};

enum BufferVertexEntryState : uint8_t {
    CLUSTER_CREATED = 0,
    CLUSTER_STABLE = 1,
    CLUSTER_FULL = 2,
    CLUSTER_DELETE_IN_PROGRESS = 3,
    /* this means that currentVersion is deleted but it doesn't mean that we cannot reador unpin older other versions */
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
    /*
     * 1) If currentVersion of a cluster is this version then it is pinned once
     * 2) If this cluster is the root it is pinned once(since root should only have one
     *    version the previous condition also applies so it is pinned twice in total)
     * 3) If a vertex contains this version of this cluster, this cluster is pinned
     * 4) For every vector in this cluster that their immidiate VectorLocation in the directory is this version
     *    of the cluster, this version is pinned.
     */
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

    BufferVertexEntry(DIVFTreeVertexInterface* cluster, VectorID id, uint64_t initialPin = 1) :
        centroidMeta(id), state(CLUSTER_CREATED), currentVersion(0) {
        /* since we have an atomicVectorLocation and it has to be 16byte aligned, the vertex entry should be
           16 byte aligned as well */
        FatalAssert(ALLIGNED(this, 16), LOG_TAG_BUFFER, "BufferVertexEntry is not alligned!");
        oldVersions[0] = VersionedClusterPtr{initialPin, ClusterPtr{0, cluster}};
        /*
         * set version pin to 1 as BufferVertexEntry is referencing it with currentVersion and
         * set pin to 0 as no one is using it yet
         */
    }

    inline BufferVertexEntry* ReadParentEntry(VectorLocation& currentLocation) {
        threadSelf->SanityCheckLockHeldInModeByMe(&clusterLock, SX_EXCLUSIVE);
        return centroidMeta.ReadParentEntry(currentLocation);
    }

    DIVFTreeVertexInterface* ReadLatestVersion(bool pinCluster = true, bool needsHeaderLock = false) {
        if (!needsHeaderLock) {
            FatalAssert(threadSelf->LockHeldByMe(&clusterLock) != LockHeld::UNLOCKED ||
                        threadSelf->LockHeldByMe(&headerLock) != LockHeld::UNLOCKED, LOG_TAG_BUFFER,
                        "No lock is held!");
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
    void UpdateClusterPtr(DIVFTreeVertexInterface* newCluster) {
        threadSelf->SanityCheckLockHeldInModeByMe(&clusterLock, SX_EXCLUSIVE);
        headerLock.Lock(SX_EXCLUSIVE);
        FatalAssert(liveVersions.find(currentVersion) != liveVersions.end(), LOG_TAG_BUFFER,
                    "new version already exists!");
        std::unordered_map<Version, VersionedClusterPtr>::iterator& it = liveVersions.find(currentVersion);
        uint64_t pinCount = it->second.clusterPtr.pin.load(std::memory_order_relaxed);
        DIVFTreeVertexInterface* cluster = it->second.clusterPtr.clusterPtr;
        it->second.clusterPtr.pin.store(0, std::memory_order_relaxed);
        it->second.clusterPtr.clusterPtr = newCluster;
        if (newCluster == nullptr) {
            liveVersions.erase(it);
        }
        cluster->MarkForRecycle(pinCount);
        headerLock.Unlock();
    }

    /* parentNode should also be locked in shared or exclusive mode and self should be locked in X mode */
    void UpdateClusterPtr(DIVFTreeVertexInterface* newCluster, Version newVersion) {
        threadSelf->SanityCheckLockHeldInModeByMe(&clusterLock, SX_EXCLUSIVE);
        headerLock.Lock(SX_EXCLUSIVE);
        FatalAssert(liveVersions.find(currentVersion) != liveVersions.end(), LOG_TAG_BUFFER,
                    "new version already exists!");
        FatalAssert(newVersion == currentVersion + 1, LOG_TAG_BUFFER, "Version jump detected!");
        currentVersion++;
        FatalAssert(liveVersions.find(currentVersion) == liveVersions.end(), LOG_TAG_BUFFER,
                    "new version already exists!");
        FatalAssert(newCluster != nullptr, LOG_TAG_BUFFER, "Invalid cluster ptr!");
        liveVersions.emplace(currentVersion, 1, 0, newCluster);
        UnpinVersion(currentVersion - 1, true);
        headerLock.Unlock();
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
            FatalAssert(it->second.versionPin.load() == 0, LOG_TAG_BUFFER,
                        "version pin incremented during access mode upgrade!");
            uint64_t pinCount = it->second.clusterPtr.pin.load(std::memory_order_relaxed);
            DIVFTreeVertexInterface* cluster = it->second.clusterPtr.clusterPtr;
            it->second.clusterPtr.pin.store(0, std::memory_order_relaxed);
            it->second.clusterPtr.clusterPtr = nullptr;
            if (version == currentVersion) {
                FatalAssert(state.load() == CLUSTER_DELETE_IN_PROGRESS, LOG_TAG_BUFFER,
                            "cluster should be in pruning process");
                state.store(CLUSTER_DELETED, std::memory_order_release);
            }
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

/* todo: add tostring for buffer entries */
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

        for (BufferVectorEntry* entry : vectorDirectory) {
            if (entry == nullptr) {
                continue;
            }
            entry->location.Store(INVALID_VECTOR_LOCATION, true);
        }

        for (size_t level = MAX_TREE_HIGHT - 1; level != (size_t)(-1); --level) {
            for (BufferVertexEntry* entry : clusterDirectory[level]) {
                if (entry == nullptr) {
                    continue;
                }
                entry->clusterLock.Lock(SX_EXCLUSIVE);
                entry->headerLock.Lock(SX_EXCLUSIVE);
                for (auto& mvccClus : entry->liveVersions) {
                    /* todo: need to rethink */
                    mvccClus.second.versionPin = 0;
                    uint64_t readerPins = mvccClus.second.clusterPtr.pin.load(std::memory_order_relaxed);
                    DIVFTreeVertexInterface* clusterPtr = mvccClus.second.clusterPtr.clusterPtr;
                    mvccClus.second.clusterPtr.pin.store(0, std::memory_order_relaxed);
                    mvccClus.second.clusterPtr.clusterPtr = nullptr;
                    clusterPtr->MarkForRecycle(readerPins);
                }
                entry->liveVersions.clear();
                entry->currentVersion = 0;
                entry->state.store(CLUSTER_DELETED);
                entry->centroidMeta.location.Store(INVALID_VECTOR_LOCATION, true);
                entry->headerLock.Unlock();
                entry->clusterLock.Unlock();
            }
        }

        sleep(10);

        for (BufferVectorEntry* entry : vectorDirectory) {
            if (entry != nullptr) {
                delete entry;
                entry = nullptr;
            }
        }
        vectorDirectory.clear();

        for (size_t level = MAX_TREE_HIGHT - 1; level != (size_t)(-1); --level) {
            for (BufferVertexEntry* entry : clusterDirectory[level]) {
                if (entry != nullptr) {
                    delete entry;
                    entry = nullptr;
                }
            }
            clusterDirectory[level].clear();
        }
    }

    /*
     * vertexMetaDataSize should be sizeof(Vertex without the Cluster Header -> maybe we should use a pointer?)
     *
     * will return the rootEntry in the INVALID state and locked in exclusive mode!
     */
    inline static BufferVertexEntry* Init(uint64_t vertexMetaDataSize,
                                          uint16_t leaf_cap, uint16_t internal_cap, uint16_t dim) {
        FatalAssert(bufferMgrInstance == nullptr, LOG_TAG_BUFFER, "Buffer already initialized");
        uint64_t internalVertexSize = vertexMetaDataSize + Cluster::TotalBytes(false, internal_cap, dim);
        uint64_t leafVertexSize = vertexMetaDataSize + Cluster::TotalBytes(true, leaf_cap, dim);
        bufferMgrInstance = new BufferManager(internalVertexSize, leafVertexSize);
        BufferVertexEntry* root = bufferMgrInstance->CreateNewRootEntry();
        return root;
    }

    /*
     * Note: No thread should be calling any functions from the bufferMgr the moment
     * shutdown is called or some resources may not be cleaned properly
     */
    inline static void Shutdown() {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        delete bufferMgrInstance;
        bufferMgrInstance = nullptr;
    }

    inline static BufferManager* GetInstance() {
        return bufferMgrInstance;
    }

    DIVFTreeVertexInterface* AllocateMemoryForVertex(uint8_t level) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        FatalAssert(MAX_TREE_HIGHT > (uint64_t)level && (uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_BUFFER,
                    "Level is out of bounds.");
        return std::aligned_alloc(CACHE_LINE_SIZE,
                                  ((uint64_t)level == VectorID::LEAF_LEVEL ? leafVertexSize : internalVertexSize));
    }

    void Recycle(DIVFTreeVertexInterface* memory) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        if (memory == nullptr) {
            return;
        }
        std::free(memory);
    }

    void UpdateRoot(VectorID newRootId, Version newRootVersion, BufferVertexEntry* oldRootEntry) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_VECTORID_IS_VALID(newRootId, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_CENTROID(newRootId, LOG_TAG_BUFFER);
        CHECK_NOT_NULLPTR(oldRootEntry, LOG_TAG_BUFFER);
        threadSelf->SanityCheckLockHeldInModeByMe(&oldRootEntry->clusterLock, SX_EXCLUSIVE);
        bufferMgrLock.Lock(SX_EXCLUSIVE);
        PinVertexVersion(newRootId, newRootVersion, false);
        FatalAssert(currentRootId.load(std::memory_order_relaxed) == oldRootEntry->centroidMeta.selfId,
                    LOG_TAG_BUFFER, "entry is not the root!");
        currentRootId.store(newRootId, std::memory_order_release);
        bufferMgrLock.Unlock(); /* unlock buffermanger before unpin to avoid recycling while its lock is held */
        oldRootEntry->UnpinVersion(oldRootEntry->currentVersion);
    }

    BufferVertexEntry* CreateNewRootEntry() {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        bufferMgrLock.Lock(SX_EXCLUSIVE);
        VectorID currentId = currentRootId.load(std::memory_order_relaxed);
        VectorID newId = INVALID_VECTOR_ID;
        newId._creator_node_id = 0;
        BufferVertexEntry* oldRootEntry = nullptr;
        if (currentId == INVALID_VECTOR_ID) {
            /* this is called during init */
            newId._level = VectorID::LEAF_LEVEL;
        } else {
            oldRootEntry = GetVertexEntry(currentId, false);
            CHECK_NOT_NULLPTR(oldRootEntry, LOG_TAG_BUFFER);
            threadSelf->SanityCheckLockHeldInModeByMe(&oldRootEntry->clusterLock, SX_EXCLUSIVE);
            FatalAssert(oldRootEntry->centroidMeta.selfId == currentId, LOG_TAG_BUFFER, "id mismatch!");
            newId._level = currentId._level + 1;
        }
        FatalAssert(MAX_TREE_HIGHT > newId._level && newId._level > VectorID::VECTOR_LEVEL, LOG_TAG_BUFFER,
                    "Level is out of bounds.");
        newId._val = clusterDirectory[newId._level].size();
        DIVFTreeVertexInterface* memLoc = AllocateMemoryForVertex(newId._level);
        CHECK_NOT_NULLPTR(memLoc, LOG_TAG_BUFFER);
        /* because of the currentVersion + currentRootId pin should be 2 */
        BufferVertexEntry* newRoot = new (std::align_val_t(16)) BufferVertexEntry(memLoc, id, 2);
        newRoot->clusterLock.Lock(SX_EXCLUSIVE);
        clusterDirectory[newId._level].emplace_back(newRoot);
        newRoot->PinVersion(newRoot->currentVersion);
        currentRootId.store(newId, std::memory_order_release);
        if (oldRootEntry != nullptr) {
            /* Unpin once due to not being root anymore */
            oldRootEntry->UnpinVersion(oldRootEntry->currentVersion);
        }
        bufferMgrLock.Unlock();
    }

    void BatchCreateBufferEntry(uint16_t num_entries, uint8_t level, BufferVertexEntry** entries,
                                DIVFTreeVertexInterface** clusters = nullptr, VectorID* ids = nullptr,
                                Version* versions = nullptr) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_NOT_NULLPTR(entries, LOG_TAG_BUFFER);
        FatalAssert(num_entries > 0, LOG_TAG_BUFFER, "number of entries cannot be 0!");
        FatalAssert(MAX_TREE_HIGHT > (uint64_t)level && (uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_BUFFER,
                    "Level is out of bounds.");

        bufferMgrLock.Lock(SX_EXCLUSIVE);
        FatalAssert(level <= currentRootId._level, LOG_TAG_BUFFER,
                    "Level is out of bounds.");
        const uiont64_t nextVal = clusterDirectory[level].size();
        for (size_t i = 0; i < num_entries; ++i) {
            VectorID id = 0;
            id._val = nextVal + i;
            id._level = level;
            id._creator_node_id = 0;

            DIVFTreeVertexInterface* cluster = AllocateMemoryForVertex(level);
            if (clusters != nullptr) {
                clusters[i] = cluster;
            }
            if (ids != nullptr) {
                ids[i] = id;
            }
            CHECK_NOT_NULLPTR(cluster, LOG_TAG_BUFFER);

            entries[i] = new (std::align_val_t(16)) BufferVertexEntry(cluster, id);
            entries[i]->clusterLock.Lock(SX_EXCLUSIVE);
            if (versions != nullptr) {
                versions[i] = entries[i]->currentVersion;
            }

            clusterDirectory[level].emplace_back(entries[i]);
        }
        bufferMgrLock.Unlock();
    }

    void BatchCreateVectorEntry(size_t num_entries, BufferVectorEntry** entries, bool create_update_handle = false) {
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

            entries[i] = new (std::align_val_t(16)) BufferVectorEntry(id);
            vectorDirectory.emplace_back(entries[i]);

            if (create_update_handle) {
                CreateUpdateHandle(id);
            }
        }
        bufferMgrLock.Unlock();
    }

    VectorID GetCurrentRootId() const {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        return currentRootId.load();
    }

    VectorID GetCurrentRootIdAndVersion(Version& version) const {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        VectorID rootId;
        while (true) {
            BufferVertexEntry* entry = GetRootEntry();
            CHECK_NOT_NULL(entry, LOG_TAG_BUFFER);

            entry->headerLock.Lock(SX_SHARED);
            if (currentRootId.load(std::memory_order_acquire) != entry->centroidMeta.selfId) {
                entry->headerLock.Unlock();
                continue;
            }

            auto& it = entry->liveVersions.find(entry->currentVersion);
            if (it == entry->liveVersions.end() || it->second.versionPin.load() == 0) {
                FatalAssert((entry->state.load(std::memory_order_relaxed) == CLUSTER_DELETED) &&
                            currentRootId.load(std::memory_order_acquire) != entry->centroidMeta.selfId,
                            LOG_TAG_BUFFER, "could not find version but it is still root or not deleted!");
                entry->headerLock.Unlock();
                continue;
            }

            version = entry->currentVersion;
            rootId = entry->centroidMeta.selfId;
            entry->headerLock.Unlock();
            CHECK_NOT_NULL(vertex, LOG_TAG_BUFFER);
            return rootId;
        }
    }

    inline BufferVectorEntry* GetVectorEntry(VectorID id) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        CHECK_VECTORID_IS_VALID(id, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_VECTOR(id, LOG_TAG_BUFFER);
        bufferMgrLock.Lock(SX_SHARED);
        if (vectorDirectory.size() <= id._val) {
            bufferMgrLock.Unlock();
            return nullptr;
        }
        BufferVectorEntry* entry = vectorDirectory[id._val];
        bufferMgrLock.Unlock();
        return entry;
    }

    inline void PinVertexVersion(VectorID vertexId, Version version, bool needLock = true) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        BufferVertexEntry* entry = GetVertexEntry(vertexId, needLock);
        FatalAssert(entry != nullptr, LOG_TAG_BUFFER, "VertexID not found in buffer. VertexID="
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

        /* todo: is it possible to unpin an older version after deletion? */
        // FatalAssert(entry->state.load() != CLUSTER_DELETED, LOG_TAG_BUFFER, "BufferEntry state is Deleted! VertexID="
        //             VECTORID_LOG_FMT, VECTORID_LOG(vertexId));
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
            if (entry == nullptr || entry->state.load() == CLUSTER_DELETED || entry->state.load() == CLUSTER_CREATED) {
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

    DIVFTreeVertexInterface* ReadAndPinRoot() override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        while (true) {
            BufferVertexEntry* entry = GetRootEntry();
            CHECK_NOT_NULL(entry, LOG_TAG_BUFFER);

            entry->headerLock.Lock(SX_SHARED);
            if (currentRootId.load(std::memory_order_acquire) != entry->centroidMeta.selfId) {
                entry->headerLock.Unlock();
                continue;
            }
            auto& it = entry->liveVersions.find(entry->currentVersion);
            if (it == entry->liveVersions.end() || it->second.versionPin.load() == 0) {
                FatalAssert((entry->state.load(std::memory_order_relaxed) == CLUSTER_DELETED) &&
                            currentRootId.load(std::memory_order_acquire) != entry->centroidMeta.selfId,
                            LOG_TAG_BUFFER, "could not find version but it is still root or not deleted!");
                entry->headerLock.Unlock();
                continue;
            }

            it->second.clusterPtr.pin.fetch_add(1);
            DIVFTreeVertexInterface* vertex = it->second.clusterPtr.clusterPtr;
            entry->headerLock.Unlock();
            CHECK_NOT_NULL(vertex, LOG_TAG_BUFFER);
            return vertex;
        }
    }

    DIVFTreeVertexInterface* ReadAndPinVertex(VectorID vertexId, Version version, bool* outdated = nullptr) override {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        BufferVertexEntry* entry = GetVertexEntry(vertexId);
        if (entry == nullptr) {
            return nullptr;
        }

        FatalAssert(entry->centroidMeta.selfId == vertexId, LOG_TAG_BUFFER, "BufferEntry id mismatch! VertexID="
                    VECTORID_LOG_FMT "EntryID = " VECTORID_LOG_FMT, VECTORID_LOG(vertexId),
                    VECTORID_LOG(entry->centroidMeta.selfId));
        // if (entry->state.load() == CLUSTER_DELETED) {
        //     return nullptr;
        // }

        entry->headerLock.Lock(SX_SHARED);
        FatalAssert(version <= entry->currentVersion, LOG_TAG_BUFFER, "Version is out of bounds: VertexID="
                    VECTORID_LOG_FMT ", latest version = %u, input version = %u",
                    VECTORID_LOG(vertexId), entry->currentVersion, version);
        if (outdated != nullptr) {
            *outdated = (version == entry->currentVersion);
        }
        auto& it = entry->liveVersions.find(version);
        if (it == entry->liveVersions.end() || it->second.versionPin.load() == 0) {
            entry->headerLock.Unlock();
            return nullptr;
        }

        it->second.clusterPtr.pin.fetch_add(1);
        DIVFTreeVertexInterface* vertex = it->second.clusterPtr.clusterPtr;
        entry->headerLock.Unlock();
        CHECK_NOT_NULLPTR(vertex, LOG_TAG_BUFFER);
        // if (vertex == nullptr) {
        //     return vertex;
        // }

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

    inline void ReleaseEntriesIfNotNull(BufferVertexEntry** entries, uint8_t num_entries,
                                        ReleaseBufferEntryFlags flags) override {
        CHECK_NOT_NULLPTR(entries, LOG_TAG_BUFFER);
        for (uint8_t i = 0; i < num_entries; ++i) {
            if (entries[i] != nullptr) {
                ReleaseBufferEntry(entries[i], flags);
                entries[i] = nullptr;
            }
        }
    }

    inline void ReleaseBufferEntryIfNotNull(BufferVertexEntry* entry, ReleaseBufferEntryFlags flags) override {
        if (entry != nullptr) {
            ReleaseBufferEntry(entry, flags);
        }
    }

    void ReleaseBufferEntry(BufferVertexEntry* entry, ReleaseBufferEntryFlags flags) override {
        CHECK_NOT_NULLPTR(entry, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_VALID(entry->centroidMeta.selfId, LOG_TAG_BUFFER);
        CHECK_VECTORID_IS_CENTROID(entry->centroidMeta.selfId, LOG_TAG_BUFFER);
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

    RetStatus CreateUpdateHandle(VectorID target) {
        handleLock.Lock(SX_EXCLUSIVE);
        auto it = handles.find(target);
        if (it != handles.end()) {
            handleLock.Unlock();
            return RetStatus{.stat=RetStatus::ALREADY_HAS_A_HANDLE,
                             .message="Cannot have more than one handle per id at a time"};
        }

        handles[target] = new std::atomic<bool>{false};
        handleLock.Unlock();
        return RetStatus::Succsess();
    }

    void SignalUpdateHandleIfNeeded(VectorID target) {
        handleLock.Lock(SX_SHARED);
        auto it = handles.find(target);
        if (it == handles.end()) {
            handleLock.Unlock();
            return;
        }

        std::atomic<bool>* handle = it->second;
        CHECK_NOT_NULLPTR(handle, LOG_TAG_BUFFER);

        handle->store(true, std::memory_order_release);
        handle->notify_all();
        handleLock.Unlock();

    }

    RetStatus WaitForUpdateToGoThrough(VectorID target) {
        handleLock.Lock(SX_SHARED);
        auto it = handles.find(target);
        if (it == handles.end()) {
            handleLock.Unlock();
            return RetStatus{.stat=RetStatus::NO_HANDLE_FOUND_FOR_TARGET,
                             .message="no handle found for the target vectorId"};
        }
        std::atomic<bool>* handle = it->second;
        CHECK_NOT_NULLPTR(handle, LOG_TAG_BUFFER);
        handleLock.Unlock();

        handle->wait(false, std::memory_order_acquire);
        FatalAssert(*handle, LOG_TAG_BUFFER, "at this point, the operation must have gone through!");
        handleLock.Lock(SX_EXCLUSIVE);
        handles.erase(target);
        delete handle;
        handleLock.Unlock();
        return RetStatus::Succsess();
    }

    RetStatus CheckIfUpdateHasGoneThrough(VectorID target, bool& updated) {
        handleLock.Lock(SX_SHARED);
        auto it = handles.find(target);
        if (it == handles.end()) {
            handleLock.Unlock();
            return RetStatus{.stat=RetStatus::NO_HANDLE_FOUND_FOR_TARGET,
                             .message="no handle found for the target vectorId"};
        }
        std::atomic<bool>* handle = it->second;
        CHECK_NOT_NULLPTR(handle, LOG_TAG_BUFFER);
        handleLock.Unlock();

        if (handle->load(std::memory_order_acquire)) {
            handleLock.Lock(SX_EXCLUSIVE);
            handles.erase(target);
            delete handle;
            handleLock.Unlock();
            updated = true;
        } else {
            updated = false;
        }
        return RetStatus::Succsess();
    }

    VectorID GetRandomCentroidIdAtLayer(uint8_t level, VectorID exclude = INVALID_VECTOR_ID,
                                        bool need_lock = true, uint64_t* num_retries = nullptr) const {
        FatalAssert(MAX_TREE_HIGHT > (uint64_t)level && (uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_BUFFER,
                    "Level is out of bounds.");
        if (need_lock) {
            bufferMgrLock.Lock(SX_SHARED);
        }
        threadSelf->SanityCheckLockHeldByMe(&bufferMgrLocK);

        if ((uint64_t)level >= currentRootId.load(std::memory_order_relaxed)._level) {
            if (need_lock) {
                bufferMgrLock.Unlock();
            }
            return INVALID_VECTOR_ID;
        }

        VectorID ret = exclude;
        uint64_t num_retry = 0;
        if (num_retries == nullptr) {
            num_retries = &num_retry;
        }
        /* todo: add a stat collection code here to see how many times this fails and rethink if it is a bottelneck */
        while (ret == exclude) {
            ret._level = level;
            ret._creator_node_id = 0; /* todo: rething in multi-node */
            ret._val = threadSelf->UniformRange64(0, clusterDirectory[level - 1].size());
            BufferVertexEntry* vertex = GetVertexEntry(ret, false);
            if (vertex == nullptr || vertex->state.load(std::memory_order_acquire) != CLUSTER_STABLE) {
                ret = exclude;
            }
            ++(*num_retries);
            if ((*num_retries) >= Thread::MAX_RETRY) {
                ret = INVALID_VECTOR_ID;
                break;
            }
        }

        if (need_lock) {
            bufferMgrLock.Unlock();
        }

        return ret;
    }

    std::pair<VectorID, VectorID> GetTwoRandomCentroidIdAtLayer(uint8_t level, bool need_lock = true) const {
        FatalAssert(MAX_TREE_HIGHT > (uint64_t)level && (uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_BUFFER,
                    "Level is out of bounds.");
        if (need_lock) {
            bufferMgrLock.Lock(SX_SHARED);
        }
        threadSelf->SanityCheckLockHeldByMe(&bufferMgrLocK);

        if ((uint64_t)level >= currentRootId.load(std::memory_order_relaxed)._level) {
            if (need_lock) {
                bufferMgrLock.Unlock();
            }
            return INVALID_VECTOR_ID;
        }

        uint64_t num_retries = 0;

        VectorID first = INVALID_VECTOR_ID;
        VectorID second = INVALID_VECTOR_ID;

        first._level = level;
        first._creator_node_id = 0; /* todo: rething in multi-node */
        second._level = level;
        second._creator_node_id = 0; /* todo: rething in multi-node */

        /* todo: add a stat collection code here to see how many times this fails and rethink if it is a bottelneck */
        while (true) {
            auto& index = threadSelf->UniformRangeTwo64(0, clusterDirectory[level - 1].size());
            first._val = index.first;

            if (index.first == index.second) {
                BufferVertexEntry* vertex = GetVertexEntry(first, false);
                if (vertex == nullptr || vertex->state.load(std::memory_order_acquire) != CLUSTER_STABLE) {
                    ++num_retries;
                    if (num_retries >= Thread::MAX_RETRY) {
                        first = INVALID_VECTOR_ID;
                        second = INVALID_VECTOR_ID;
                        break;
                    }
                    continue;
                }

                second = GetRandomCentroidIdAtLayer(level, first, false, &num_retries);
                if (second == INVALID_VECTOR_ID) {
                    first = INVALID_VECTOR_ID;
                }
                break;
            }
            second._val = index.second;

            BufferVertexEntry* first_vertex = GetVertexEntry(first, false);
            BufferVertexEntry* second_vertex = GetVertexEntry(second, false);
            bool first_unusable =
                (first_vertex == nullptr || first_vertex->state.load(std::memory_order_acquire) != CLUSTER_STABLE);
            bool second_unusable =
                (second_vertex == nullptr || second_vertex->state.load(std::memory_order_acquire) != CLUSTER_STABLE);
            if (first_unusable && second_unusable) {
                ++num_retries;
                if (num_retries >= Thread::MAX_RETRY) {
                    first = INVALID_VECTOR_ID;
                    second = INVALID_VECTOR_ID;
                    break;
                }
                continue;
            }

            if (first_unusable) {
                std::swap(first, second);
                second_unusable = true;
            }

            if (second_unusable) {
                second = GetRandomCentroidIdAtLayer(level, first, false, &num_retries);
            }

            if ((num_retries >= Thread::MAX_RETRY) || (first == INVALID_VECTOR_ID) || (second == INVALID_VECTOR_ID)) {
                first = INVALID_VECTOR_ID;
                second = INVALID_VECTOR_ID;
            }
            break;
        }

        if (need_lock) {
            bufferMgrLock.Unlock();
        }
        return std::make_pair(first, second);
    }

    VectorID GetRandomCentroidIdAtNonRootLayer(VectorID exclude = INVALID_VECTOR_ID) const {
        FatalAssert(MAX_TREE_HIGHT > (uint64_t)level && (uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_BUFFER,
                    "Level is out of bounds.");
        bufferMgrLock.Lock(SX_SHARED);
        uint8_t level = threadSelf->UniformRange32(1, currentRootId.load(std::memory_order_relaxed)._level - 1);
        VectorID ret = GetRandomCentroidIdAtLayer(level, exclude, false);
        bufferMgrLock.Unlock();
        return ret;
    }

    std::pair<VectorID, VectorID> GetTwoRandomCentroidIdAtNonRootLayer() const {
        FatalAssert(MAX_TREE_HIGHT > (uint64_t)level && (uint64_t)level > VectorID::VECTOR_LEVEL, LOG_TAG_BUFFER,
                    "Level is out of bounds.");
        bufferMgrLock.Lock(SX_SHARED);
        uint8_t level = threadSelf->UniformRange32(1, currentRootId.load(std::memory_order_relaxed)._level - 1);
        std::pair<VectorID, VectorID> ret = GetTwoRandomCentroidIdAtLayer(level, exclude, false);
        bufferMgrLock.Unlock();
        return ret;
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

    inline BufferVertexEntry* GetRootEntry(bool needLock = true) {
        FatalAssert(bufferMgrInstance == this, LOG_TAG_BUFFER, "Buffer not initialized");
        if (needLock) {
            bufferMgrLock.Lock(SX_SHARED);
        }
        threadSelf->SanityCheckLockHeldByMe(&bufferMgrLock);
        BufferVertexEntry* entry = GetVertexEntry(currentRootId.load(std::memory_order_relaxed), false);
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
    SXSpinLock handleLock;
    std::unordered_map<VectorID, std::atomic<bool>*, VectorIDHash> handles;

    inline static BufferManager *bufferMgrInstance = nullptr;

TESTABLE;
};

};

#endif