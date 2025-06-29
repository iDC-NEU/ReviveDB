#include "common/numa.h"

namespace NVMDB {
std::map<int, std::vector<int>> NumaBinding::nodeCpuMap_;
int NumaBinding::nodeCount_ = -1;
std::mutex NumaBinding::mutex_;
thread_local int NumaBinding::localGroupId_ = 0;
}