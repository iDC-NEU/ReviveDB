#ifndef NVMDB_HEAP_SPACE_H
#define NVMDB_HEAP_SPACE_H

#include "table_space/nvm_table_space.h"

namespace NVMDB {

void HeapCreate(const std::shared_ptr<DirectoryConfig>& dirConfig);

void HeapBootStrap(const std::shared_ptr<DirectoryConfig>& dirConfig);

void HeapExitProcess();

extern TableSpace *g_heapSpace;

}  // namespace NVMDB

#endif  // NVMDB_HEAP_SPACE_H