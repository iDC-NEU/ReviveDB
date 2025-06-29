#ifndef NVMDB_THREAD_H
#define NVMDB_THREAD_H

#include "common/nvm_types.h"
#include <vector>

namespace NVMDB {

void InitGlobalThreadStorageMgr();
void InitThreadLocalStorage();
void DestroyThreadLocalStorage();

void InitGlobalVariables();
void DestroyGlobalVariables();
void InitThreadLocalVariables();
void DestroyThreadLocalVariables();

}  // namespace NVMDB

#endif // NVMDB_THREAD_H