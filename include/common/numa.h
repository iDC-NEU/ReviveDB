#pragma once

#include <sched.h>
#include <unistd.h>
#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <mutex>
#include <stdexcept>

namespace  NVMDB {
class NumaBinding {
public:
    // 绑定当前线程到指定NUMA节点
    static bool bindThreadToNode(int nodeId) {
        try {
            cpu_set_t cpuset;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                const auto& cpus = getNodeCpus(nodeId);
                if (cpus.empty()) {
                    return false;
                }
                CPU_ZERO(&cpuset);
                for (int cpu : cpus) {
                    CPU_SET(cpu, &cpuset);
                }
            }
            if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) == -1) {
                return false;
            }
            localGroupId_ = nodeId;
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    static int getThreadLocalGroupId() { return localGroupId_; }

private:
    static std::map<int, std::vector<int>> nodeCpuMap_;  // 缓存每个节点的CPU列表
    static int nodeCount_;                                // 缓存节点数量
    static std::mutex mutex_;                            // 保护并发访问
    static thread_local int localGroupId_;               // 缓存当前节点的numa组Id

    // 初始化节点数量
    static void initNodeCount() {
        int count = 0;
        while (true) {
            std::string filepath = "/sys/devices/system/node/node" +
                                   std::to_string(count) + "/cpulist";
            if (!std::ifstream(filepath).good()) {
                break;
            }
            ++count;
        }
        nodeCount_ = count;
    }


    // 获取系统中的NUMA节点数量
    static int getNumaNodeCount() {
        if (nodeCount_ == -1) {
            initNodeCount();
        }
        return nodeCount_;
    }

    // 获取指定节点的CPU列表
    static const std::vector<int>& getNodeCpus(int nodeId) {
        // 检查节点ID是否有效
        if (getNumaNodeCount() <= nodeId) {
            return nodeCpuMap_[nodeId]; // 返回空vector
        }

        // 如果已经缓存，直接返回
        auto it = nodeCpuMap_.find(nodeId);
        if (it != nodeCpuMap_.end()) {
            return it->second;
        }

        // 读取并解析CPU列表
        std::vector<int>& cpus = nodeCpuMap_[nodeId];
        std::string filepath = "/sys/devices/system/node/node" +
                               std::to_string(nodeId) + "/cpulist";
        std::ifstream cpulist(filepath);
        if (!cpulist.is_open()) {
            return cpus;
        }

        std::string line;
        std::getline(cpulist, line);
        cpulist.close();

        // 解析CPU列表字符串 (格式: "0-3,7,9-11")
        size_t pos = 0;
        while (pos < line.length()) {
            size_t next = line.find_first_of(",", pos);
            if (next == std::string::npos) {
                next = line.length();
            }

            std::string range = line.substr(pos, next - pos);
            size_t dashPos = range.find('-');

            if (dashPos == std::string::npos) {
                cpus.push_back(std::stoi(range));
            } else {
                int start = std::stoi(range.substr(0, dashPos));
                int end = std::stoi(range.substr(dashPos + 1));
                for (int cpu = start; cpu <= end; ++cpu) {
                    cpus.push_back(cpu);
                }
            }

            pos = next + 1;
        }

        return cpus;
    }
};
}