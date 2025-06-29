#include "transaction/nvm_transaction.h"
#include "undo/nvm_undo.h"
#include "transaction/nvm_snapshot.h"
#include <unistd.h>

namespace NVMDB {

thread_local std::unique_ptr<Transaction> t_txContext = nullptr;
thread_local char* t_undoRecordCache = (char*)_mm_malloc(MAX_UNDO_RECORD_CACHE_SIZE, 64);

void InitTransactionContext() {
    t_txContext = std::make_unique<Transaction>();
}

void DestroyTransactionContext() {
    t_txContext = nullptr;
}

Transaction *GetCurrentTxContext() {
    return t_txContext.get();
}

Transaction::Transaction()
    : m_undoTxContext(nullptr), m_snapshotCSN(0), m_commitCSN(0), m_txStatus(TxStatus::EMPTY) {
    m_processArray = ProcessArray::GetGlobalProcArray();
    m_procArrayTID = m_processArray->addProcess();
    undoRecordCache = t_undoRecordCache;
}

Transaction::~Transaction() {
    m_processArray->removeProcess(m_procArrayTID);
    m_procArrayTID = INVALID_PROC_ARRAY_INDEX;
}

/* no need for read-only Tx to prepare undo. */
void Transaction::PrepareUndo() {
    if (m_undoTxContext == nullptr) {
        // 为事务在 undo segment 中分配一个槽
        m_undoTxContext = AllocUndoContext();
        m_txSlotPtr = m_undoTxContext->GetTxSlotLocation();
    }
}

void Transaction::Begin() {
    DCHECK(m_txStatus == TxStatus::EMPTY || m_txStatus == TxStatus::ABORTED || m_txStatus == TxStatus::COMMITTED);
    // DCHECK(m_writeSet.empty());
    // 全局最新的CSN, 线程基于这个版本进行读取
    m_snapshotCSN = m_processArray->getAndUpdateProcessLocalCSN(m_procArrayTID);
    DCHECK(IsCSNValid(m_snapshotCSN));
    // 全局最小的snapshot CSN, 低于这个CSN的交易一定已经完成执行
    m_minSnapshot = m_processArray->getGlobalMinCSN();
    m_txStatus = TxStatus::IN_PROGRESS;
}

void Transaction::Commit() {
    DCHECK(m_txStatus == TxStatus::IN_PROGRESS);
    m_txStatus = TxStatus::COMMITTING;
    if (m_undoTxContext != nullptr) {
        m_commitCSN = m_processArray->advanceGlobalCSN();
         for (auto& it: m_writeSet) {
             if (!it->needCache()) {
                 continue;
             }
             it->Lock();
             auto* dramCache = it->loadDRAMCache<NVMTuple>(NVMTupleHeadSize);
             if (dramCache->m_txInfo == GetTxSlotLocation()) {
                 dramCache->m_txInfo = m_commitCSN;
             }
             std::atomic_thread_fence(std::memory_order_release);
             it->Unlock();
         }
        // 需要同时解锁写集
        m_undoTxContext->UpdateTxSlotCSN(m_commitCSN);
        m_undoTxContext->UpdateTxSlotStatus(TxSlotStatus::COMMITTED);
        std::atomic_thread_fence(std::memory_order_release);
        m_undoTxContext = nullptr;
        m_writeSet.clear();
    }
    m_txStatus = TxStatus::COMMITTED;
    DCHECK(m_snapshotCSN == m_processArray->getProcessLocalCSN(m_procArrayTID));
    DCHECK(m_snapshotCSN >= m_processArray->getGlobalMinCSN());
}

void Transaction::Abort() {
    if (m_undoTxContext != nullptr) {
        // 事务开始 rollback，此时事务状态仍然是 IN_PROGRESS, 对于已经 rollback 的tuple
        m_undoTxContext->RollBack(reinterpret_cast<UndoRecord *>(undoRecordCache));
        // 事务完成 undo， 此时 heap 上没有undo的数据
        m_undoTxContext->UpdateTxSlotStatus(TxSlotStatus::ROLL_BACKED);
        m_undoTxContext = nullptr;
        m_writeSet.clear();
    }
    m_txStatus = TxStatus::ABORTED;
    DCHECK(m_snapshotCSN == m_processArray->getProcessLocalCSN(m_procArrayTID));
    DCHECK(m_snapshotCSN >= m_processArray->getGlobalMinCSN());
}

TMResult Transaction::VersionIsVisible(const uint64& csnOrTxPtr) const {
    bool committed = false;
    uint64 version_csn;
    if (TxInfoIsCSN(csnOrTxPtr)) {
        committed = true;
        version_csn = csnOrTxPtr;
    } else {
        TransactionInfo txInfo{};   // 返回m_txInfo 对应的事务的提交状态和CSN
        bool recycled = !GetTransactionInfo((TxSlotPtr)csnOrTxPtr, &txInfo);
        if (recycled) {
            /* fill MIN_SNAPSHOT back to txInfo as upper commit CSN. */
            return TMResult::OK;
        }
        switch (txInfo.status) {
            case TxSlotStatus::ROLL_BACKED:
            case TxSlotStatus::ABORTED:
                return TMResult::ABORTED;
            case TxSlotStatus::COMMITTED:
                committed = true;
                version_csn = txInfo.csn;
                break;
            case TxSlotStatus::IN_PROGRESS:
                committed = false;
                break;
            case TxSlotStatus::EMPTY:
            default:
                CHECK(false);
        }
    }
    // 对应事务已经提交
    if (committed) {
        if (version_csn < m_snapshotCSN) {
            return TMResult::OK;
        }
        return TMResult::INVISIBLE; // 版本不可见
    }
    // 对应事务没有提交
    if (m_undoTxContext != nullptr && csnOrTxPtr == m_txSlotPtr) {
        return TMResult::SELF_UPDATED;  // 自己更新自己
    }
    return TMResult::BEING_MODIFIED;    // 冲突
}

TMResult Transaction::SatisfiedUpdate(const NVMTuple& tuple) const {
    TMResult result = VersionIsVisible(tuple.m_txInfo);
    switch (result) {
        case TMResult::OK:
        case TMResult::ABORTED:
        case TMResult::SELF_UPDATED:
            return TMResult::OK;
        case TMResult::INVISIBLE:
        case TMResult::BEING_MODIFIED:
            return TMResult::BEING_MODIFIED;
        default:
            CHECK(false);
    }
    return TMResult::ABORTED;
}

/*
 * Insert 比较特殊，undo 的时候不能直接删除，因为可能存在这一的场景，
 *      tx 1  删除 IndexTuple,  CSN 为 c1
 *      tx 2  插入 IndexTuple, 把对应value 改成了 InvalidCSN
 *      tx 2  回滚，如果直接删除该 KV， 则在 tx 1 之前的事务是看不到该索引的，所以不能直接删除，而是
 *              回填一个大于等于的CSN，这样确保原来能看到这个IndexTuple的事务，依然能看到。
 *              这里有一个 trick， tx 1提交之后，tx 2才能插入，否则会有并发更新的问题，
 *              所以 tx2 的 snapshot 必然 大于 tx1 的CSN，所以直接用 tx2 的snapshot 回填即可。
 * undo 的格式
 * 因为UndoRecord 的head对索引undo来说没用，所以复用了下存储空间。segHead 和 rowid 两个 uint32 拼成了一个 uint64 的CSN
 */
void Transaction::PrepareIndexInsertUndo(const Key_t &key) {
    auto *undo = reinterpret_cast<UndoRecord *>(this->undoRecordCache);
    undo->m_undoType = IndexInsertUndo;
    undo->m_rowLen = 0;
    undo->m_segHead = m_commitCSN >> BIS_PER_U32;
    undo->m_rowId = m_commitCSN & 0xFFFFFFFF;
    undo->m_payload = sizeof(key);
    undo->m_pre = 0;
#ifndef NDEBUG
    undo->m_txSlot = this->GetTxSlotLocation();
#endif
    int ret = memcpy_s(undo->data, MAX_UNDO_RECORD_CACHE_SIZE, &key, sizeof(key));
    SecureRetCheck(ret);
    this->insertUndoRecord(undo);
}

void Transaction::PrepareIndexDeleteUndo(Key_t &key) {
    auto *undo = reinterpret_cast<UndoRecord *>(this->undoRecordCache);
    undo->m_undoType = IndexDeleteUndo;
    undo->m_rowLen = 0;
    undo->m_segHead = NVMInvalidPageId;
    undo->m_rowId = InvalidRowId;
    undo->m_payload = sizeof(key);
    undo->m_pre = 0;
    int ret = memcpy_s(undo->data, MAX_UNDO_RECORD_CACHE_SIZE, &key, sizeof(key));
    SecureRetCheck(ret);
    this->insertUndoRecord(undo);
}

}  // namespace NVMDB
