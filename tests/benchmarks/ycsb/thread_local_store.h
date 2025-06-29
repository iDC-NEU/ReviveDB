#pragma once
namespace NVMDB {
namespace YCSB {
template <typename T>
class ThreadLocalStore {
public:
    static inline T *Get()
    {
        static thread_local T inst;
        return &inst;
    }

    ~ThreadLocalStore() = default;

private:
    ThreadLocalStore() = default;
};
}  // namespace YCSB
}  // namespace NVMDB