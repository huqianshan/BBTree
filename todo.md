# ToDo List

## Buffer Base BTree

### Buffer Pool

- [x] buffer pool的命中次数、命中率
- [ ] buffer pool 的mapping table使用了`STL unordered_map`性能很低,参考perf 图
- [ ] buffer pool的并发竞争问题
- [ ] BatchFIFOWriteBuffer 单线程版本
- [ ] BatchFIFOWriteBuffer 多线程版本
- [ ] 与batch flush结合

### Zone BTree

- [x] 将中间结点都放置于DRAM之中
- [x] 修改叶子结点`page_id`为 `zone_id+offset`
  - [ ] `page_id=[zone_id + offset_in_zone]`,修改read时的offset以及写时分配的offset
  - [ ] 分配叶子结点id需要一个分配算法
- [ ] 添加 `batch insert/delete/update`接口
- [x] `Optimistic Lock Coupling`

### Buffer Tree

- [ ] `Write-ahead-log`的实现
- [x ] `Memtable`的实现: ~~`skiplist`~~ or Btree-OLC total in memory`

### ZNS Storage
- [ ] Cicle Buffer的Push/Pop 并发支持



