
## Undo 表空间

UNDO 基于 table space编写

有固定N个segment，每个segment里面有一定的数量的transaction slot。
一个事务会绑定一个segment，并从segment中分配若干个page作为自己的undo。

数据库初始化： 创建N个segment
数据库启动： 会把这N个segment信息加载到内存中
事务启动： 绑定一个 TxSlot 到事务中，绑定一个page
事务执行： 不断分配 undo 记录。
事务提交或回滚： 更新 TxSlot
后台线程： 定期扫描segment中的 TxSlot，找到可以回收的transaction，回收整个undo log和 TxSlot

正确性依赖：
1. 事务执行过程中，TxSlot 的状态一定是IN progress，事务提交之后，变成 committed 或者 aborted。
2. tuple 头如果存了 TxSlot，事务信息可以是正在进行、已提交或者已回滚。
如果存了 CSN，那么一定是一个已提交的事务，产生的tuple。
3. 重启之后，回滚被打断的事务，然后就可以清除掉所有的 TxSlot 和 undo page，因为 heap 是干净的。
被打断的事务，分成两种情况：
   3.1 还没到拿CSN阶段，此时只需要回滚索引，因为heap还没有往下写。
   3.2 拿到了CSN，正在往heap上写的。 这时候需要回滚heap和索引。

## Undo Record

struct UndoRecord {
    uint16 undo_type; // undo record 的大类
    uint16 row_len; // undo record 的小类
    uint32 segHead; // tuple 对应的 segment head
    RowId rowid;
    uint32 payload; // undo 数据长度
};

undo_type 是大类，回滚的时候会根据UndoType指定对应的处理函数。
