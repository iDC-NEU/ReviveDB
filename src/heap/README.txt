
heap对最外层的访问接口为增删查改，用户输入输出都是临时的 tuple 变量。需要带上 Transaction。有几个设计原则
    - 任何修改操作（增删改），都需要先记Undo。Insert也需要，因为它会分配一个RowID。
    - 增删改在事务执行期间就已经持久化，且有一个TxSlotPtr指针，指向事务信息。
    在提交的时候，改一下 TxSlot 状态即可，是事务是否提交的切换点。在此之前，其它人都能通过查TxSlot知道该事务未提交。
    - 事务提交结束之后，有一个回填操作，把CSN填到tuple头上。这一步操作是为了避免后续事务，每次都查询TxSlot。以后的优化，可以
    让回填操作，变成异步来做。
    - 读写 tuple 都需要上 spinlock，保证整个tuple的原子性。以后可以用OCC来提高速度。

可见性判断：
    如果是CSN，直接根据 csn 大小判断
    如果是 TxSlotPtr，找到对应的Transaction slot，看事务状态

事务提交流程：
    1. 先将 Transaction slot 中的状态，更改为已提交。这个时间点事务算是成功提交。
    2. 将全局的 CSN 原子 + 1。
    3. 更新 Transaction slot 中的 csn。
    如果 1-3 期间，有其它事务访问 Transaction slot的状态，需要忙等 csn 被更新到一个合法值。
    这样确保，事务在拿snapshot的时候，< snapshot 的事务，都已经提交，且能被自己观察到。

正确性保证：
    - 任何时候，如果一个tuple上有csn，那么一定是一个已经提交的事务
    - 如果tuple上的为 TxSlotPtr， 那么对应的 Transaction slot 一定存在，且状态是正确的
    - 任何异常退出，导致的 tuple 半写，一定会有Undo日志可以恢复。
    - 更新操作，一定没有并发的事务跟自己抢，也就是说，tuple的上一个版本，一定在当前事务开始之前就已经提交。
    - 更新操作，在释放自旋锁之前，一定会设置好版本链。
    - 读一个tuple，一定可以按照版本链找到一个合适自己的版本。

HeapInsert
    1. 分配一个 RowID，这一步是在内存中的。
    2. 插入一个 UndoRecord,
    3. 把当前 Item 加入到 write set 中。 新的tuple的旧版本指针为空。
    4. 把tuple复制到NVM上。
    提交时，需要把对应的 Row 落盘，注意需要把 NVMTUPLE_USED 标志位置上。
    回滚时，需要把对应的RowID释放掉。注意，Undo操作需要是可重入的，所以需要检查对应的Row是否还是自己插入的那个。

HeapUpdate
    1. 给 tuple 上锁，如果有人已经有锁了，就直接 abort
    2. 可见性检查，如果失败，就直接退出
    3. 把旧值copy到 UndoRecord 里面。整条记录都copy，作为 undo 的data。
    4. 更新当前 tuple 的 prev 指针，指向 undo 的位置，更新TxSlot信息。
    5. 加入到 write set中。
    提交时，需要 CSN 回填
    回滚时，需要回填数据信息。

HeapDelete
    1. 给 tuple 上锁，如果有人已经有锁了，就直接 abort
    2. 可见性检查，如果失败，就直接退出
    3. 如果可见，插入 undo
    4. 标记删除，更新 prev 指针和 TxSlot 信息
    5. 加入到 write set中
    提交时把对应的Row落盘，
    回滚时回填数据信息。

