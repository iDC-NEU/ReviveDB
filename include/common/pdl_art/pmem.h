#ifndef PACTREE_PMEM_H
#define PACTREE_PMEM_H

#include "common/pdl_art/arch.h"
#include "common/pdl_art/op_log.h"
#include "common/nvm_utils.h"
#include <mutex>

#define MASK 0x8000FFFFFFFFFFFF
#define MASK_DIRTY 0xDFFFFFFFFFFFFFFF  // DIRTY_BIT
#define MASK_POOL 0x7FFFFFFFFFFFFFFF

struct root_obj {
    PMEMoid ptr[2];
};

class PMem {
public:
    static const size_t DEFAULT_POOL_SIZE = NVMDB::CompileValue(4 * 1024 * 1024 * 1024UL, 1 * 1024 * 1024 * 1024UL);
    static const size_t DEFAULT_DATA_POOL_SIZE = NVMDB::CompileValue(16 * 1024 * 1024 * 1024UL, 4 * 1024 * 1024 * 1024UL);
    static_assert(NVMDB::NVMDB_NUM_LOGS_PER_THREAD * NVMDB::NVMDB_MAX_THREAD_NUM * sizeof(OpStruct) <= PMem::DEFAULT_POOL_SIZE / 2, "");
    static void CreatePMemPool(int *is_create, root_obj **root, root_obj **sl_root);
    static int GetPoolNum();
    static void UnmountPMEMPool();
    static void *getBaseOf(int poolId);

    static void alloc(size_t size, void **p, PMEMoid *oid);
    static void free(void *pptr);
    static void freeVaddr(void *vaddr);

    static void *getOpLog(int i);
};

static inline void flushToNVM(char *data, size_t sz) {}

#endif
