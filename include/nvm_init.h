#ifndef NVMDB_DBCORE_H
#define NVMDB_DBCORE_H

#include <string>

namespace NVMDB {

/* 创建数据库初始环境 */
void InitDB(const std::string& dir);

/* 数据库启动，进行必要的初始化。 */
void BootStrap(const std::string& dir);

/* 进程退出时调用，清理内存变量 */
void ExitDBProcess();

}  // namespace NVMDB

#endif
