# ToDo List

## Buffer Base BTree

### Buffer Pool

- [x] buffer pool的命中次数、命中率
- [ ] buffer pool 的mapping table使用了`STL unordered_map`性能很低,参考perf 图
- [ ] buffer pool的并发竞争问题
- [ ] buffer pool的刷回读取问题：**异步**、顺序预取、提前刷回、且在zns上无法并发刷回

### Zone BTree

- [x] 将中间结点都放置于DRAM之中
- [ ] 修改叶子结点`page_id`为 `zone_id+offset`
  - [ ] 分配叶子结点id需要一个分配算法
- [ ] 添加 `batch insert/delete/update`接口
- [x] `Optimistic Lock Coupling`

### Buffer Tree

- [ ] `Write-ahead-log`的实现
- [x] `Memtable`的实现: ~~`skiplist`~~ or Btree-OLC total in memory`
  - [ ] `buffer_btree` 分为 `insert`,`evict`,`flush`三个阶段:
    - [ ]  **[内存占用]** 目前`flush`阶段之后，淘汰的结点只是执行`count=0`,仍未从`buffer tree`中删除，导致占用大量内存,平均buffer_tree的叶子结点数量正比于负载。
    - [ ]  **[并发问题]** 目前`evict\flush`两阶段会将所有前台工作线程阻塞，以获取该被淘汰的结点和刷新
    - [ ]  [淘汰策略] 目前淘汰策略通过优先队列选择`max_leaf_count - keep_leaf_count`个淘汰，这两个参数的选择强烈依赖负载，改为按照内存使用量设置 `MAX_BUFFER_LEAF_MB` 和 `MAX_KEEP_LEAF_MB`

### ZNS Storage

- [ ] `Zone`的元数据、以及基于元数据的`Zone`的分配策略
- [ ] 基于元数据的`Zone`回收策略



