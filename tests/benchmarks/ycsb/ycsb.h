#pragma once
#include "heap/nvm_tuple.h"
#include "nvm_table.h"
#include "nvmdb_thread.h"
#include "transaction/nvm_transaction.h"
#include "ycsb_statisitic.h"
#include "scrambled_zipfian_generator.h"
#include "ycsb_def.h"
#include "ycsb_request_rowid.h"
#include "ycsb_result.h"
#include <boost/noncopyable.hpp>
#include <cstdint>
#include <gtest/gtest.h>
#include <string>
namespace NVMDB {
namespace YCSB {
template <typename Database, typename RowIdGenerator>
class YcsbThreadLocalVariable : boost::noncopyable {
public:
    YcsbThreadLocalVariable(size_t threadId, Table *table, const YcsbTableParam &tableParam,
                            const YcsbRunParam &runParam, RowIdGenerator &rowIdGen, YcsbStat::Stat &threadLocalStat,
                            const size_t seed = std::random_device{}())
        : m_threadId(threadId),
          m_table(table),
          m_tableParam(tableParam),
          m_runParam(runParam),

          m_rowIdGen(rowIdGen),
          m_randomColumnIdx(0, 9),
          m_randomPercent(1, 100),
          m_stat(threadLocalStat)
    {
        rowTupleData = static_cast<RAMTuple *>(malloc(OpPerTxn * sizeof(RAMTuple)));
        for (int i = 0; i < OpPerTxn; i++) {
            new (&rowTupleData[i])
                RAMTuple(m_table->GetColDesc(), m_table->GetRowLen(), m_tupleBuf.at(i).data(), m_undoBuf.at(i).data());
        }
        GetThreadLocalRandomGenerator()->seed(seed);
        InitThreadLocalVariables();
        m_txn = GetCurrentTxContext();
    }

    ~YcsbThreadLocalVariable()
    {
        free(rowTupleData);
        DestroyThreadLocalVariables();
    }

    void genRequests()
    {
        std::array<Operation, OpPerTxn> &requests = m_requestBuf.m_operations;
        char *const columnValueBuf = m_requestBuf.m_columnValueBuf.data();
        std::vector<std::pair<RowId, bool>> rowIds = m_rowIdGen(m_threadId);

        // 生成读/写请求
        size_t rowBufOffset = 0;
        for (size_t requestId = 0; requestId < OpPerTxn; requestId++) {
            Operation &op = requests[requestId];
            op.m_pKey = rowIds[requestId].first;
            op.m_genBySkew = rowIds[requestId].second;
            if (m_randomPercent(*GetThreadLocalRandomGenerator()) <= m_runParam.ReadPercent) {
                op.m_opType = Operation::OpType::Read;
            } else {
                op.m_opType = Operation::OpType::Write;
                op.m_colIdx = m_randomColumnIdx(*GetThreadLocalRandomGenerator());

                op.m_colValue = columnValueBuf + rowBufOffset;
                rowBufOffset += m_tableParam.BytePerColumn;
            }
        }
        // 统一随机生成列的值
        fillColumnValueBuf(columnValueBuf, rowBufOffset);
    }

    bool execRequests()
    {
        const auto &requests = m_requestBuf.m_operations;
        int readCnt = 0, writeCnt = 0;
        TestTimer timer;
        m_txn->Begin();
        for (int i = 0; i < requests.size(); i++) {
            const Operation &op = requests[i];
            bool ok = true;
            switch (op.m_opType) {
                case Operation::OpType::Read: {
                    ++readCnt;
                    db.read(m_txn, m_table, op.m_pKey, &rowTupleData[i]);
                    // CHECK(HeapRead(m_txn, m_table, op.m_pKey, &rowTupleData[i]) == HamStatus::OK);
                } break;
                case Operation::OpType::Write: {
                    ++writeCnt;
                    ok = db.write(m_txn, m_table, op.m_pKey, &rowTupleData[i], op.m_colIdx, op.m_colValue);
                    // rowTupleData[i].UpdateCol(op.m_colIdx, op.m_colValue);
                    // ok = HeapUpdate2(m_txn, m_table, op.m_pKey, &rowTupleData[i]) == HamStatus::OK;
                } break;
                default:
                    CHECK(false) << "Shouldn't Access";
            }
            if (!ok) {
                m_txn->Abort();
                m_stat.abort();
                return false;
            }
        }
        m_txn->Commit();
        m_stat.commit(readCnt, writeCnt, timer.getDurationUs());
        return true;
    }

private:
    void fillColumnValueBuf(char *columnValueBuf, const size_t n) const
    {
        using RandomDefaultResultType = decltype((*GetThreadLocalRandomGenerator())());
        const size_t flatN = (n + sizeof(RandomDefaultResultType) - 1) / sizeof(RandomDefaultResultType);
        CHECK(flatN * sizeof(RandomDefaultResultType) < OpPerTxn * MaxColumnSize);
        auto columnValueBufInt = reinterpret_cast<RandomDefaultResultType *>(columnValueBuf);
        // >1B为粒度生成，理论上比一个char一个char生成快
        for (size_t i = 0; i < flatN; i++) {
            columnValueBufInt[i] = (*GetThreadLocalRandomGenerator())();
        }
    }
    const size_t m_threadId;

    Table *m_table;
    const YcsbTableParam m_tableParam;
    const YcsbRunParam m_runParam;
    Transaction *m_txn{nullptr};

    RowIdGenerator &m_rowIdGen;
    std::uniform_int_distribution<ColumnIdx> m_randomColumnIdx;
    std::uniform_int_distribution<short> m_randomPercent;

    Database db{};
    struct RequestBuffer {
        std::array<Operation, OpPerTxn> m_operations;
        std::array<char, OpPerTxn * MaxColumnSize> m_columnValueBuf;
    } m_requestBuf{};
    RAMTuple *rowTupleData;
    static_assert(ColumnCount * MaxColumnSize % 64 == 0, "");
    alignas(64) std::array<std::array<char, ColumnCount * MaxColumnSize>, OpPerTxn> m_tupleBuf{};
    alignas(64) std::array<std::array<UndoColumnDesc, ColumnCount * MaxColumnSize>, OpPerTxn> m_undoBuf{};

    YcsbStat::Stat &m_stat;
};
template <typename Database>
class YcsbTable {
public:
    explicit YcsbTable(const YcsbTableParam &tableParam, const bool isInit) : m_tableParam(tableParam)
    {
        TableDesc tableDesc;
        CHECK(TableDescInit(&tableDesc, ColumnCount));
        for (size_t colIndex = 0; colIndex < ColumnCount; colIndex++) {
            ColumnDesc &colDesc = tableDesc.col_desc[colIndex];
            colDesc.m_colLen = tableParam.BytePerColumn;
            colDesc.m_colOffset = tableDesc.row_len;
            colDesc.m_isNotNull = true;
            std::string colName = "ycsb_col" + std::to_string(colIndex);
            const errno_t rc = strcpy_s(colDesc.m_colName, NVM_MAX_COLUMN_NAME_LEN, colName.data());
            SecureRetCheck(rc);
            tableDesc.row_len += tableParam.BytePerColumn;
        }
        CHECK(tableDesc.row_len == tableParam.BytePerColumn * ColumnCount);
        tableDesc.row_len = BestTupleLenCalculator::g_btc.getBestAlignment(tableDesc.row_len + NVMTupleHeadSize) - NVMTupleHeadSize;
        m_table = std::make_unique<Table>(YcsbTableId, tableDesc);
        if (isInit) {
            const uint32 tableSegHead = m_table->CreateSegment();
            CHECK(tableSegHead == YcsbTableSegHead) << tableSegHead;
            LOG(INFO) << "Table Init Begin.";
            prepareTableMultiThread();
            LOG(INFO) << "Table Init Finish: " << tableParam.Items << " Rows.";
        } else {
            m_table->Mount(YcsbTableSegHead);
        }
        LOG(INFO) << "Table Check Begin.";
        checkTableMultiThread();
        LOG(INFO) << "Table Check Finish.";
    }

    BenchResult *runBench(const YcsbRunParam &runParam)
    {
        if (runParam.SkewOpPerTxn == 0 || runParam.Theta == 0) {
            // uniform use uncached generator
            return runBenchTemplate<UnCachedRowIdGenerator>(runParam);
        } else {
            // zipfian use cached generator
            return runBenchTemplate<CachedRowIdGenerator>(runParam);
        }
    }

private:
    template <typename RowIdGenerator>
    BenchResult *runBenchTemplate(const YcsbRunParam &runParam)
    {
        std::unique_ptr<ScrambledZipfianGenerator> scrambledZipfianGenerator{nullptr};
        if (runParam.SkewOpPerTxn != 0) {  // 偏斜分布
            scrambledZipfianGenerator =
                std::make_unique<ScrambledZipfianGenerator>(0, m_tableParam.Items - 1, runParam.Theta);
        }

        RowIdGenerator rowIdGen(scrambledZipfianGenerator.get(), m_tableParam, runParam);
        m_stats.resize(runParam.Terminal);
        // 启动ycsb测试线程
        std::vector<std::thread> workers;
        workers.reserve(runParam.Terminal);
        int64_t stopSig = 0;
        for (size_t threadId = 0; threadId < runParam.Terminal; threadId++) {
            workers.emplace_back([this, runParam, threadId, &stopSig, &rowIdGen]() {
                auto* localVar = new YcsbThreadLocalVariable<Database, RowIdGenerator>(
                    threadId, m_table.get(), m_tableParam, runParam, rowIdGen, m_stats.getStat(threadId));
                while (!stopSig) {
                    localVar->genRequests();
                    localVar->execRequests();
                    // while (!stopSig && !localVar.execRequests()); // TODO: 可以考虑添加个失败重试，这样不需要重新生成
                }
            });
        }
        SetForceWriteBackCSN(false);
        // Warm Up
        LOG(INFO) << "Start Warm Up.";
        YcsbStat::Snapshot baseSnapshot;
        for (int sec = 0; sec < runParam.WarmUpSec; sec++) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            baseSnapshot = m_stats.getGlobalSnapshot();
            baseSnapshot.print();
        }
        LOG(INFO) << "Start Run Bench.";
        // 正式运行
        int64_t maxTps = 0;
        YcsbStat::Snapshot snapshot;
        for (int sec = 0; sec < runParam.RunSec; sec++) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            snapshot = m_stats.getGlobalSnapshot();
            snapshot -= baseSnapshot;
            snapshot.print();
            maxTps = std::max(maxTps, snapshot.getTps());
        }
        // 停止运行
        stopSig = 1;
        for (std::thread &worker : workers) {
            worker.join();
        }
        LOG(INFO) << "Max Tps: " << maxTps;
        // 返回运行结果
        BenchResult *result = static_cast<BenchResult *>(_mm_malloc(sizeof(BenchResult), 64));
        result->m_tableParam = m_tableParam;
        result->m_runParam = runParam;
        result->m_statistic = snapshot;
        return result;
    }

    // 多线程插入，直到各线程rowId都大于等于items
    void prepareTableMultiThread() const
    {
        std::vector<std::thread> prepareThreads;
        prepareThreads.reserve(IngestThreadNum);
        for (size_t threadId = 0; threadId < IngestThreadNum; threadId++) {
            prepareThreads.emplace_back([this] {
                Database db{};
                InitThreadLocalVariables();
                Transaction &txn = *GetCurrentTxContext();
                RowId currentRowId{0};
                RAMTuple defaultTuple(m_table->GetColDesc(), m_table->GetRowLen());  // 没必要初始化
                while (currentRowId < m_tableParam.Items) {
                    txn.Begin();
                    for (size_t i = 0; i < IngestBatchSize && currentRowId < m_tableParam.Items; i++) {
                        currentRowId = db.insert(&txn, m_table.get(), &defaultTuple);
                        // currentRowId = HeapInsert(&txn, m_table.get(), &defaultTuple);
                    }
                    txn.Commit();
                }
                DestroyThreadLocalVariables();
            });
        }
        for (std::thread &prepareThread : prepareThreads) {
            prepareThread.join();
        }
    }

    // 检查表完整性
    void checkTableMultiThread() const
    {
        // 每个thread检测[beginRowIds[threadId], beginRowIds[threadId+1])的行
        std::vector<RowId> beginRowIds(IngestThreadNum + 1);
        for (size_t threadId = 0; threadId < IngestThreadNum; threadId++) {
            beginRowIds[threadId] = m_tableParam.Items / IngestThreadNum * threadId;
        }
        beginRowIds[IngestThreadNum] = m_tableParam.Items;
        // 用于检查的线程
        std::vector<std::thread> checkThreads;
        checkThreads.reserve(IngestThreadNum);
        for (size_t threadId = 0; threadId < IngestThreadNum; threadId++) {
            checkThreads.emplace_back(
                [this](const RowId beginRowId, const RowId endRowId) {
                    Database db{};
                    InitThreadLocalVariables();
                    Transaction &txn = *GetCurrentTxContext();
                    txn.Begin();
                    RAMTuple tuple(m_table->GetColDesc(), m_table->GetRowLen());
                    for (RowId rowId = beginRowId; rowId < endRowId; rowId++) {
                        db.read(&txn, m_table.get(), rowId, &tuple);
                        // CHECK(HeapRead(&txn, m_table.get(), rowId, &tuple) == HamStatus::OK);
                        if (beginRowId == 0 && rowId % std::max(1U, endRowId / 100) == 0) {
                            LOG(INFO) << "Thread 0 Checked: " << rowId * 100UL / endRowId << "%";
                        }
                    }
                    txn.Abort();
                    DestroyThreadLocalVariables();
                },
                beginRowIds[threadId], beginRowIds[threadId + 1]);
        }
        for (std::thread &prepareThread : checkThreads) {
            prepareThread.join();
        }
    }

    const YcsbTableParam m_tableParam;
    std::unique_ptr<Table> m_table{nullptr};
    YcsbStat m_stats{};
};
template <size_t NodeCnt>
inline std::string genTestDir(const std::string &suffix)
{
    return genTestDir<NodeCnt - 1>(suffix) + ";/mnt/pmem" + std::to_string(NodeCnt - 1) + "/" + suffix;
}
template <>
inline std::string genTestDir<1>(const std::string &suffix)
{
    return "/mnt/pmem0/" + suffix;
}
}  // namespace YCSB
}  // namespace NVMDB