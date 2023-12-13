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
- [x ] `Memtable`的实现: ~~`skiplist`~~ or Btree-OLC total in memory`

### ZNS Storage

- [ ] `Zone`的元数据、以及基于元数据的`Zone`的分配策略
- [ ] 基于元数据的`Zone`回收策略



