#include "common/pdl_art/pmem.h"
#include "common/numa.h"

constexpr int DEFAULT_POOL_ID = 2;
constexpr int POOL_SIZE_MOD = 2;
constexpr mode_t PMEM_MODE = 0666;
constexpr int POOL_ID_OFFSET = 48;
constexpr int ERRNO_TYPE = 13;

struct PMemPoolManager {
    static const int maxPoolId = 1024;
    static const size_t defaultPoolSize = 512 * 1024 * 1024;
    static const size_t defaultMemsetSize = 1024 * 1024 * 1024;

    int grpNum{};
    std::vector<int> grpCurrentPoolId;
    volatile void *baseAddresses[maxPoolId]{};  // dram
    volatile void *logVaddr{};
    std::mutex mtx;

    int GetGrpByPoolId(int poolId) const {
        return (poolId - DEFAULT_POOL_ID) % grpNum;
    }

    std::string GetPmemFilename(int poolId) const {
        const auto& path = NVMDB::g_dir_config->getDirPaths()[GetGrpByPoolId(poolId)];
        return (path + "/index_data_" + std::to_string(poolId));
    }

    void Init(int *isCreate, root_obj **root, root_obj **sl_root);
    void Bind(int poolId, const char *nvmPath, size_t size, void **rootP, int *isCreated);
    void BindLog(int poolId, const char *nvmPath, size_t size);
    bool Unbind(int poolId);
    void UnbindAll();

    void *GetBaseOf(int poolId);
    void Alloc(size_t size, void **p, PMEMoid *oid);
    void Free(void *pptr);
    static void FreeVaddr(void *vaddr);
};

void PMemPoolManager::FreeVaddr(void *vaddr) {
    PMEMoid ptr = pmemobj_oid(vaddr);
    pmemobj_free(&ptr);
}

void PMemPoolManager::Free(void *pptr) {
    // p -> pool_id and offset
    // then perform free
    int poolId = (int)((((uintptr_t)pptr) & MASK_POOL) >> 48);
    void *rawPtr = (void *)((((uintptr_t)pptr) & MASK) + (uintptr_t)baseAddresses[poolId]);
    PMEMoid ptr = pmemobj_oid(rawPtr);
    pmemobj_free(&ptr);
}

void *PMemPoolManager::GetBaseOf(int poolId) {
    if (baseAddresses[poolId] == nullptr) {
        std::lock_guard<std::mutex> lockGuard(mtx);
        if (baseAddresses[poolId] == nullptr) {
            int grpId = GetGrpByPoolId(poolId);
            if (grpCurrentPoolId[grpId] <= poolId) {
                grpCurrentPoolId[grpId] = poolId;
            }
            int isCreate;
            Bind(poolId, GetPmemFilename(poolId).c_str(), defaultPoolSize, nullptr, &isCreate);
            CHECK(!isCreate);
        }
    }
    return const_cast<void *>(baseAddresses[poolId]);
}
/*
 * 0: log
 * 1: search layer;
 * from 2, numa-aware memory pool
 */
void PMemPoolManager::Init(int *isCreate, root_obj **root, root_obj **sl_root) {
    memset_s(baseAddresses, sizeof(baseAddresses), 0, sizeof(baseAddresses)); // init dram space

    grpNum = (int)NVMDB::g_dir_config->size();

    std::string slPath = NVMDB::g_dir_config->getDirPaths()[0] + "/index_sl";
    std::string logPath = NVMDB::g_dir_config->getDirPaths()[0] + "/index_log";

    BindLog(0, logPath.c_str(), PMem::DEFAULT_POOL_SIZE);
    Bind(1, slPath.c_str(), PMem::DEFAULT_POOL_SIZE, (void **)sl_root, isCreate);
    Bind(DEFAULT_POOL_ID, GetPmemFilename(DEFAULT_POOL_ID).c_str(), PMem::DEFAULT_POOL_SIZE, (void **)root, isCreate);
    grpCurrentPoolId.push_back(DEFAULT_POOL_ID);
    for (int i = 1; i < grpNum; i++) {
        int poolId = i + 2;
        Bind(poolId, GetPmemFilename(poolId).c_str(), PMem::DEFAULT_DATA_POOL_SIZE, nullptr, nullptr);
        grpCurrentPoolId.push_back(poolId);
    }
}

void PMemPoolManager::Alloc(size_t size, void **p, PMEMoid *oid) {
restart:
    int grpId = NVMDB::NumaBinding::getThreadLocalGroupId();
    int poolId = grpCurrentPoolId[grpId];
    auto pop = (PMEMobjpool *)baseAddresses[poolId];
    int ret = pmemobj_alloc(pop, oid, size, 0, nullptr, nullptr);
    if (ret != 0) {
        if (errno != ENOMEM) {
            printf("unknown errno %d\n", errno);
        }
        mtx.lock();
        if (grpCurrentPoolId[grpId] != poolId) {
            mtx.unlock();
            goto restart;
        }
        int nextPoolId = poolId + grpNum;
        Bind(nextPoolId, GetPmemFilename(nextPoolId).c_str(), defaultPoolSize, nullptr, nullptr);
        grpCurrentPoolId[grpId] = nextPoolId;
        mtx.unlock();
        goto restart;
    }
    *p = reinterpret_cast<void *>(((unsigned long)poolId) << POOL_ID_OFFSET | oid->off);
}

void PMemPoolManager::BindLog(int poolId, const char *nvmPath, size_t size) {
    size_t realSize = size / POOL_SIZE_MOD;
    int isCreated;
    Bind(poolId, nvmPath, size, nullptr, &isCreated);
    PMEMoid g_root = pmemobj_root((PMEMobjpool *)baseAddresses[poolId], realSize);
    logVaddr = pmemobj_direct(g_root);
    if (isCreated) {
        auto *addr = reinterpret_cast<uint8_t *>(const_cast<void *>(logVaddr));
        size_t remainSize = realSize;
        size_t offset = 0;
        while (remainSize > 0) {
            size_t cpySize = (remainSize < defaultMemsetSize ? remainSize : defaultMemsetSize);
            errno_t ret = memset_s(addr + offset, cpySize, 0, cpySize);
            SecureRetCheck(ret);
            offset += cpySize;
            remainSize -= cpySize;
        }
    }
}

void PMemPoolManager::Bind(int poolId, const char *nvmPath, size_t size, void **rootP, int *isCreated) {
    PMEMobjpool *pop;
    const char *layout = "phydra";
    if (access(nvmPath, F_OK) != 0) {
        pop = pmemobj_create(nvmPath, layout, size, PMEM_MODE);
        if (pop == nullptr) {
            LOG(ERROR) << "bind create error " << "path : " << nvmPath << std::endl;
            abort();
        }
        baseAddresses[poolId] = reinterpret_cast<void *>(pop);
        if (isCreated) {
            *isCreated = 1;
        }
    } else {
        pop = pmemobj_open(nvmPath, layout);
        if (pop == nullptr) {
            if (errno == ERRNO_TYPE) {
                if (remove(nvmPath) != 0) {
                    abort();
                }
                pop = pmemobj_create(nvmPath, layout, size, PMEM_MODE);
                if (pop == nullptr) {
                    abort();
                }
            } else {
                abort();
            }
        }
        baseAddresses[poolId] = reinterpret_cast<void *>(pop);
        if (isCreated) {
            *isCreated = 0;
        }
    }
    if (rootP) {
        PMEMoid gRoot = pmemobj_root(pop, sizeof(root_obj));
        *rootP = (root_obj *)pmemobj_direct(gRoot);
    }
    DCHECK(baseAddresses[poolId] != nullptr);
}

bool PMemPoolManager::Unbind(int poolId) {
    auto pop = reinterpret_cast<PMEMobjpool *>(const_cast<void *>(baseAddresses[poolId]));
    pmemobj_close(pop);
    baseAddresses[poolId] = nullptr;
    return true;
}

void PMemPoolManager::UnbindAll() {
    for (int i = 0; i < maxPoolId; i++) {
        if (baseAddresses[i] != nullptr) {
            Unbind(i);
        }
    }
}

PMemPoolManager *g_pmemMgr = nullptr;

void PMem::CreatePMemPool(int *is_create, root_obj **root, root_obj **sl_root) {
    g_pmemMgr = new PMemPoolManager;
    g_pmemMgr->Init(is_create, root, sl_root);
}

void PMem::UnmountPMEMPool() {
    g_pmemMgr->UnbindAll();
    delete g_pmemMgr;
    g_pmemMgr = nullptr;
}

int PMem::GetPoolNum() {
    return g_pmemMgr->grpNum;
}

void PMem::alloc(size_t size, void **p, PMEMoid *oid) {
    DCHECK(g_pmemMgr != nullptr);
    g_pmemMgr->Alloc(size, p, oid);
}

void *PMem::getBaseOf(int poolId) {
    DCHECK(g_pmemMgr != nullptr);
    return g_pmemMgr->GetBaseOf(poolId);
}

void PMem::free(void *pptr) {
    DCHECK(g_pmemMgr != nullptr);
    return g_pmemMgr->Free(pptr);
}

void PMem::freeVaddr(void *vaddr) {
    DCHECK(g_pmemMgr != nullptr);
    return PMemPoolManager::FreeVaddr(vaddr);
}

void *PMem::getOpLog(int i) {
    DCHECK(g_pmemMgr != nullptr);
    auto vaddr = (unsigned long)g_pmemMgr->logVaddr;
    return (void *)(vaddr + (sizeof(OpStruct) * i));
}