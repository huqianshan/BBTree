#include "buffer.h"

BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     DiskManager* disk_manager)
    : BufferPoolManager(pool_size, 1, 0, disk_manager) {
  count_ = hit_ = miss_ = 0;
}

BufferPoolManager::BufferPoolManager(size_t pool_size, uint32_t num_instances,
                                     uint32_t instance_index,
                                     DiskManager* disk_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager) {
  assert(num_instances > 0 &&
         "If BPI is not part of a pool, then the pool size should just be 1");
  assert(instance_index < num_instances &&
         "BPI index cannot be greater than the number of BPIs in the "
         "pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  page_data_ = (char*)aligned_alloc(PAGE_SIZE, pool_size_ * PAGE_SIZE);
  assert(((size_t)page_data_ & (PAGE_SIZE - 1)) == 0);
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    pages_[i].SetData(page_data_ + i * PAGE_SIZE);
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }

  int ret = pthread_mutex_init(&mutex_, nullptr);
  VERIFY(!ret);
  ret = pthread_cond_init(&cond_, nullptr);
  VERIFY(!ret);
  count_ = hit_ = miss_ = 0;
  // page_id_counts_.reserve(2000 * 1000);
}

BufferPoolManager::~BufferPoolManager() {
  // Print();

  delete[] pages_;
  free(page_data_);
  delete replacer_;

  int ret = pthread_mutex_destroy(&mutex_);
  VERIFY(!ret);
  ret = pthread_cond_destroy(&cond_);
  VERIFY(!ret);
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  GrabLock();
  if (page_id == INVALID_PAGE_ID ||
      page_table_.find(page_id) == page_table_.end()) {
    ReleaseLock();
    return false;
  }

  frame_id_t cur_frame = page_table_[page_id];
  Page* cur_page = pages_ + cur_frame;
  if (cur_page->is_dirty_) {
    disk_manager_->write_page(page_id, cur_page->GetData());
    cur_page->is_dirty_ = false;
  }
  ReleaseLock();
  return true;
}

// 1. FlushPage内部又有lock guard
void BufferPoolManager::FlushAllPages() {
  // Lock_guard lk(latch_);
  for (const auto& key : page_table_) {
    // INFO_PRINT("%p flush page:%4u :%4u\n", this, key.first, key.second);
    FlushPage(key.first);
  }
}

Page* BufferPoolManager::NewPage(page_id_t* page_id) {
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always
  // pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  // ValidatePageId(*page_id);

  GrabLock();
  page_id_t tmp_page_id = AllocatePage();

  // 3 pick a victim frame from free_lists or replacer
  Page* ret_page = GrabPageFrame();
  VERIFY(ret_page != nullptr);
  frame_id_t cur_frame_id = static_cast<frame_id_t>(ret_page - pages_);
  page_table_[tmp_page_id] = cur_frame_id;
  ret_page->pin_count_ = 1;
  ret_page->page_id_ = tmp_page_id;
  ret_page->is_dirty_ = true;
  replacer_->Pin(cur_frame_id);
  ret_page->ResetMemory();
  *page_id = tmp_page_id;

  // DEBUG_PRINT("new page_id:%4u frame_id:%4u\n", tmp_page_id, cur_frame_id);
  // new page is not hit, add miss
  page_id_counts_[tmp_page_id] = 1;
  miss_++;
  count_++;

  ReleaseLock();
  return ret_page;
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the
  // free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then
  // return a pointer to P.
  GrabLock();
  count_++;
  Page* ret_page = nullptr;

  if (page_table_.find(page_id) != page_table_.end()) {
    // 1.1
    frame_id_t cur_frame_id = page_table_[page_id];
    ret_page = pages_ + cur_frame_id;
    // need update pin_count and notify the lru replacer;
    ret_page->pin_count_++;
    replacer_->Pin(cur_frame_id);
    // DEBUG_PRINT("fetch page_id:%4u frame_id:%4u\n", page_id, cur_frame_id);
    page_id_counts_[page_id]++;
    hit_++;

    ReleaseLock();
    return ret_page;
  }
  miss_++;
  // 1.2 2 3
  ret_page = GrabPageFrame();
  if (ret_page == nullptr) {
    ReleaseLock();
    return nullptr;  // no frames found neither in free_list nor replacer for
                     // target page(page_id)
  }

  // 4 update some data
  disk_manager_->read_page(page_id, ret_page->data_);
  page_table_[page_id] = static_cast<frame_id_t>(ret_page - pages_);
  ret_page->page_id_ = page_id;  // update page_id
  ret_page->pin_count_ = 1;
  ret_page->is_dirty_ = false;
  // NOTE: 此时frame ID是否可能出现在replacer中
  // 有可能。deletePage把frame加入free list时，没有将它从replacer移除
  // 但获取frame时，会先从free list里取，然后需要pin，这样就把它从replacer移除了
  // 但是从replacer Victim中取出的页面，自然是删除的页面，所以不会出现这种情况
  replacer_->Pin(ret_page - pages_);
  ReleaseLock();
  return ret_page;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is
  // using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its
  // metadata and return it to the free list.

  // 0 ? only delete page in BufferPool, not in disk
  GrabLock();

  if (page_table_.find(page_id) == page_table_.end()) {
    ReleaseLock();
    return false;
  }

  // 1 2
  frame_id_t cur_frame_id = page_table_[page_id];
  Page* cur_page = pages_ + cur_frame_id;
  // if (cur_page->GetPinCount() != 0) {
  //   ReleaseLock();
  //   return false;
  // }

  // 3
  page_table_.erase(page_id);
  replacer_->Pin(cur_frame_id);
  cur_page->page_id_ = INVALID_PAGE_ID;
  cur_page->pin_count_ = 0;
  cur_page->is_dirty_ = false;
  cur_page->ResetMemory();
  free_list_.push_back(cur_frame_id);

  SignalForFreeFrame();
  ReleaseLock();
  return true;
}

/***
 * @attention unpin don't delete maping in page_table, so it can fetch the
 * unpinned page in page_table. And the unpin page will be both in page_table
 * and replacer.
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  GrabLock();

  // 0 Make sure you can unpin page_id
  if (page_table_.find(page_id) == page_table_.end()) {
    ReleaseLock();
    return true;
  }
  // 1 get page object
  frame_id_t cur_frame_id = page_table_[page_id];
  Page* cur_page = pages_ + cur_frame_id;

  // DEBUG_PRINT("unpin page_id:%4u frame_id:%4u\n", page_id, cur_frame_id);

  if (is_dirty) {
    // if page is dirty but is_dirty indicates non-dirty,
    // it also should be dirty
    cur_page->is_dirty_ |= is_dirty;
  }

  if (cur_page->GetPinCount() > 0) {
    cur_page->pin_count_--;
  }
  if (cur_page->GetPinCount() == 0) {
    replacer_->Unpin(cur_frame_id);
    SignalForFreeFrame();
  }
  ReleaseLock();
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManager::ValidatePageId(const page_id_t page_id) const {
  // allocated pages mod back to this BPI
  //   CHECK(page_id % num_instances_ ==
  // instance_index_);
  VERIFY(page_id % num_instances_ == instance_index_);
}

Page* BufferPoolManager::GrabPageFrame() {
  Page* ret_page = nullptr;
  frame_id_t cur_frame_id = -1;
  while (true) {
    if (!free_list_.empty()) {
      // 1. first find in free-lists
      cur_frame_id = free_list_.front();
      free_list_.pop_front();
      ret_page = pages_ + cur_frame_id;
      // DEBUG_PRINT("grab in free lists page_id:%4u frame_id:%4u\n",
      // ret_page->GetPageId(), cur_frame_id);
    } else if (replacer_->Victim(&cur_frame_id)) {
      // 2. then find in replacer
      ret_page = pages_ + cur_frame_id;
      if (ret_page->is_dirty_) {
        // flush raw page
        disk_manager_->write_page(ret_page->GetPageId(), ret_page->GetData());
      }
      // delete old page_id -> frame_id map
      page_table_.erase(ret_page->GetPageId());
      // DEBUG_PRINT("grab in replacer page_id:%4u frame_id:%4u\n",
      //             ret_page->GetPageId(), cur_frame_id);
    }
    if (ret_page != nullptr) {
      break;
    }
    // XXX: timed wait?
    // reset the next page id?
    WaitForFreeFrame();
  }
  return ret_page;
}

void BufferPoolManager::GrabLock() {
  int ret = pthread_mutex_lock(&mutex_);
  VERIFY(!ret);
}

void BufferPoolManager::ReleaseLock() {
  int ret = pthread_mutex_unlock(&mutex_);
  VERIFY(!ret);
}

void BufferPoolManager::SignalForFreeFrame() {
  int ret = pthread_cond_signal(&cond_);
  VERIFY(!ret);
}

void BufferPoolManager::WaitForFreeFrame() {
  int ret = pthread_cond_wait(&cond_, &mutex_);
  VERIFY(!ret);
}

void BufferPoolManager::Print() {
  INFO_PRINT(
      "Instance ID:%2u Page table size:%2lu Replacer size: %2lu"
      " Free list size: %2lu \n",
      instance_index_, page_table_.size(), replacer_->Size(),
      free_list_.size());
}

ParallelBufferPoolManager::ParallelBufferPoolManager(
    size_t num_instances, size_t pool_size, DiskManager* disk_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  bpmis_.resize(num_instances);
  num_instances_ = num_instances;
  index_ = 0;
  for (size_t i = 0; i < num_instances_; i++) {
    bpmis_[i] =
        new BufferPoolManager(pool_size, num_instances_, i, disk_manager);
  }
  disk_manager_ = disk_manager;
}

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances,
                                                     size_t pool_size,
                                                     std::string file_name,
                                                     bool is_one_file) {
  // Allocate and create individual BufferPoolManagerInstances
  bpmis_.resize(num_instances);
  num_instances_ = num_instances;
  index_ = 0;
  if (is_one_file) {
    auto tmp_file_name = file_name;
    auto disk_manager = new DiskManager(tmp_file_name.c_str(), 1);
    for (size_t i = 0; i < num_instances_; i++) {
      bpmis_[i] =
          new BufferPoolManager(pool_size, num_instances_, i, disk_manager);
    }
    disk_manager_ = disk_manager;
  } else {
    for (size_t i = 0; i < num_instances_; i++) {
      auto tmp_file_name = file_name + "_" + std::to_string(i);
      auto disk_manager = new DiskManager(tmp_file_name.c_str(), num_instances);
      bpmis_[i] =
          new BufferPoolManager(pool_size, num_instances_, i, disk_manager);
    }
    disk_manager_ = nullptr;
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and
// deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  Print();
  FlushAllPages();

  if (disk_manager_) {
    delete disk_manager_;
  } else {
    for (auto& buffer : bpmis_) {
      if (buffer->disk_manager_) {
        delete buffer->disk_manager_;
      }
      buffer->disk_manager_ = nullptr;
    }
  }

  for (auto& buffer : bpmis_) {
    delete buffer;
  }
}

u64 ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  size_t total = 0;
  for (auto& buffer : bpmis_) {
    total += buffer->GetPoolSize();
  }
  return total;
}

u64 ParallelBufferPoolManager::GetFileSize() {
  u64 total = 0;
  for (auto& buffer : bpmis_) {
    total += buffer->disk_manager_->get_file_size();
  }
  return total;
};
u64 ParallelBufferPoolManager::GetReadCount() {
  u64 total = 0;
  for (auto& buffer : bpmis_) {
    total += buffer->disk_manager_->get_read_count();
  }
  return total;
};
u64 ParallelBufferPoolManager::GetWriteCount() {
  u64 total = 0;
  for (auto& buffer : bpmis_) {
    total += buffer->disk_manager_->get_write_count();
  }
  return total;
};

BufferPoolManager* ParallelBufferPoolManager::GetBufferPoolManager(
    page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use
  // this method in your other methods.
  return bpmis_[page_id % num_instances_];
}

DiskManager* ParallelBufferPoolManager::GetDiskManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use
  // this method in your other methods.
  return bpmis_[page_id % num_instances_]->disk_manager_;
}

Page* ParallelBufferPoolManager::FetchPage(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManager
  return dynamic_cast<BufferPoolManager*>(GetBufferPoolManager(page_id))
      ->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManager
  return dynamic_cast<BufferPoolManager*>(GetBufferPoolManager(page_id))
      ->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPage(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManager
  return dynamic_cast<BufferPoolManager*>(GetBufferPoolManager(page_id))
      ->FlushPage(page_id);
}

Page* ParallelBufferPoolManager::NewPage(page_id_t* page_id) {
  // create new page. We will request page allocation in a round robin manner
  // from the underlying BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1)
  // success and return 2) looped around to starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a
  // different BPMI each time this function is called

  // fetch_add() returns old value
  // We don't care about unsigned int overflow
  size_t start_index = index_;
  size_t loop_index = index_;
  index_ = (index_ + 1) % num_instances_;
  Page* ret_page = nullptr;
  while (true) {
    ret_page = bpmis_[loop_index]->NewPage(page_id);
    if (ret_page != nullptr) {
      return ret_page;
    }
    loop_index = (loop_index + 1) % num_instances_;
    if (loop_index == start_index) {
      return nullptr;
    }
  }
  return ret_page;
}

DiskManager* ParallelBufferPoolManager::GetDiskManager(page_id_t* page_id) {
  size_t start_index = index_;
  size_t loop_index = index_;
  index_ = (index_ + 1) % num_instances_;
  DiskManager* ret = nullptr;
  while (true) {
    ret = bpmis_[loop_index]->disk_manager_;
    *page_id = bpmis_[loop_index]->AllocatePage();
    if (ret != nullptr) {
      return ret;
    }
    loop_index = (loop_index + 1) % num_instances_;
    if (loop_index == start_index) {
      return nullptr;
    }
  }
  return ret;
}

bool ParallelBufferPoolManager::DeletePage(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManager
  return dynamic_cast<BufferPoolManager*>(GetBufferPoolManager(page_id))
      ->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPages() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto& buffer : bpmis_) {
    auto instance = dynamic_cast<BufferPoolManager*>(buffer);
    instance->FlushAllPages();
  }
}

void ParallelBufferPoolManager::Print() {
  u64 page_table_size = 0;
  u64 replacer_size = 0;
  u64 free_list_size = 0;

  u64 count = 0;
  u64 miss = 0;
  u64 hit = 0;

  u64 total = 0;
  for (auto& buffer : bpmis_) {
    auto instance = dynamic_cast<BufferPoolManager*>(buffer);
    // instance->Print();
    page_table_size += instance->page_table_.size();
    replacer_size += instance->replacer_->Size();
    free_list_size += instance->free_list_.size();

    count += instance->count_;
    miss += instance->miss_;
    hit += instance->hit_;
    total += instance->page_id_counts_.size();
  }

  std::vector<page_id_t> page_ids;
  page_ids.reserve(total);
  for (auto& buffer : bpmis_) {
    auto instance = dynamic_cast<BufferPoolManager*>(buffer);
    for (auto& count : instance->page_id_counts_) {
      page_ids.push_back(count.second);
    }
  }
  std::sort(page_ids.begin(), page_ids.end());
  size_t avg = 0;
  for (size_t i = 0; i < total; i++) {
    avg += page_ids[i];
  }
  avg /= total;
  auto sz = total;
  INFO_PRINT("[ParaBufferPool]  Avg Count For a BufferPage: %3lu hits:", avg);
  std::vector<float> percentiles = {0.0, 0.1, 0.2, 0.3, 0.4,   0.5,
                                    0.6, 0.7, 0.8, 0.9, 0.999, 0.9999999};
  INFO_PRINT("  Percentile:")
  for (auto& p : percentiles) {
    INFO_PRINT(" {%2.3lf%%->%4lu}, ", p * 100, page_ids[size_t(p * sz)]);
  }
  INFO_PRINT("\n");

  INFO_PRINT("[ParaBufferPool] Total size: " KMAG "%4.2f" KRESET
             " MB Instance Nums:%2lu Page table "
             "size:%4lu Replacer size: %4lu Free list size: %4lu \n",
             page_table_size * 4.0 / 1024, num_instances_, page_table_size,
             replacer_size, free_list_size);

  double miss_ratio = miss * 100.0 / count;
  double hit_ratio = hit * 100.0 / count;
  printf("[ParaBufferPool] count:%4lu miss: %4lu %2.2lf%% hit: %4lu " KCYN
         "%2.2lf%%" KRESET "\n",
         count, miss, miss_ratio, hit, hit_ratio);
}

/*
 * ===================================================================
 * ===================================================================
 * ======================== FIFO Batch Buffer ========================
 * ===================================================================
 * ===================================================================
 */

FIFOBacthReplacer::FIFOBacthReplacer(size_t num_pages) {
  // fifo_list_ = new std::vector<frame_id_t>(num_pages);
  head_ = new Item(-1, -1, nullptr);
  tail_ = new Item(-1, -1, nullptr);

  head_->next_ = tail_;
  tail_->prev_ = head_;

  max_size_ = num_pages;
  cur_size_ = 0;
}

FIFOBacthReplacer::~FIFOBacthReplacer() {
  while (head_) {
    Item* temp = head_;
    head_ = head_->next_;
    delete temp;
  }
}

bool FIFOBacthReplacer::Add(frame_id_t frame_id, u32 length, char* data) {
  if (cur_size_ == max_size_) {
    return false;
  }
  Item* item = new Item(frame_id, length, data);
  item->prev_ = head_;

  item->next_ = head_->next_;
  head_->next_->prev_ = item;
  head_->next_ = item;

  cur_size_++;
  return true;
};

bool FIFOBacthReplacer::IsFull() { return cur_size_ == max_size_; }

bool FIFOBacthReplacer::Victim(Item** item) {
  if (cur_size_ == 0) {
    return false;
  }

  Item* temp = tail_->prev_;
  temp->prev_->next_ = tail_;
  tail_->prev_ = temp->prev_;
  *item = temp;

  cur_size_--;
  return true;
}

u64 FIFOBacthReplacer::Size() { return cur_size_; }

void FIFOBacthReplacer::Print() {
  Item* temp = head_;
  while (temp) {
    std::cout << temp->frame_id_ << " ";
    temp = temp->next_;
  }
  std::cout << std::endl;
}

/*
 * ===================================================================
 * ===================================================================
 * ======================== ZoneManager   ============================
 * ===================================================================
 * ===================================================================
 */

ZoneManager::ZoneManager(Zone* zone, u64 max_size) : zone_(zone) {
  zone_id_ = zone->GetZoneNr();
  wp_ = zone->wp_ / PAGE_SIZE;
  cap_ = zone->GetCapacityLeft() / PAGE_SIZE;
  end_ = zone->start_ / PAGE_SIZE + zone->GetMaxCapacity() / PAGE_SIZE;

  replacer_ = new FIFOBacthReplacer(max_size);
  flusher_ = new CircleBuffer<Slot>(max_size);
  rw_lock_ = new ReaderWriterLatch();
  read_cache_ = nullptr;
}
ZoneManager::~ZoneManager() {
  SAFE_DELETE(rw_lock_);
  // SAFE_DELETE(read_cache_);
  SAFE_DELETE(zone_);
  SAFE_DELETE(replacer_);
  SAFE_DELETE(flusher_);
}

/**
 * every time add a page to flusher,
 * check to flush the existed page in flusher to zns
 */
void ZoneManager::AddPage(Slot slot) {
  // 1.1 add the current evicted pages to flusher
  flusher_->Push(slot);
  // 1.2 if no exited page in flusher, just return
  Slot tmp;
  if (!flusher_->Back(tmp)) {
    return;
  }

  // 1.3 flushed the existed page
  Page* page = tmp.first;
  u64 length = tmp.second;
  // if (length > 0) {
  // length indicates the page not flushed
  zone_->Append(page->GetData(), length * PAGE_SIZE);
  // 2. after flushed to zns, it will be safe to umap the items
  //
  for (u64 i = 0; i < length; i++) {
    // DEBUG_PRINT("%lu pin count %d \n", i, page[i].GetPinCount());
    // buffer_pool_manager_->page_table_.erase(page[i].GetPageId());
    page_table_.erase(page[i].GetPageId());
    // ATOMIC_SPIN_UNTIL(page[i].GetReadCount(), 0);
    // page[i].SetStatus(FLUSHED);
  }
  // flusher_->Set(flusher_->GetTail(), {page, 0});
  // }
  flusher_->Pop(tmp);

  // flushing to zns when the length of consecutive page
  // is greater than the Threshold
  /*   u64 tail = flusher_->GetTail();
    u64 head = flusher_->GetHead();
    u64 cursor = tail;
    bool flag = false;
    u64 length = 0;
    while (cursor <= head) {
      Slot tmp = flusher_->At(cursor);
      length += tmp.second;
      if (length > MIN_SEQ_PAGES_TO_BE_FLUSHED) {
        flag = true;
        break;
      }
    }
    if (flag) {
      char* seq_area = (char*)malloc(length * PAGE_SIZE);
      u64 offset_bytes = 0;

      for (u64 i = tail; i <= cursor; i++) {
        Slot tmp = flusher_->At(tail);
        offset_bytes += tmp.second * PAGE_SIZE;
        memcpy(seq_area + offset_bytes, tmp.first->GetData(),
               PAGE_SIZE * tmp.second);
      }
      zone_->Append(seq_area, length * PAGE_SIZE);
      free(seq_area);
    } */

  // INFO_PRINT("[Flusher] write page %lu length %lu\n", page->GetPageId(),
  //  length);

  // :NOTE: :hjl:  may be need return and sleep
  /*   for (u64 cursor = 0; cursor < length; cursor++) {
      // DEBUG_PRINT("%lu pin count %d \n", i, page[i].GetPinCount());
      // ATOMIC_SPIN_UNTIL(page[i].GetPinCount(), 0);
      if (page[cursor].GetReadCount() != 0) {
        return;
      }
    } */

  // DESTROY_PAGE(page);
}

Page* ZoneManager::AllocateSeqPage(u64 length) {
  Page* tmp_page = new Page[length];
  char* page_data = (char*)aligned_alloc(PAGE_SIZE, length * PAGE_SIZE);
  memset(page_data, 0, length * PAGE_SIZE);
  if (page_data == nullptr || tmp_page == nullptr) {
    return nullptr;
  }
  for (u64 i = 0; i < length; i++) {
    tmp_page[i].SetData(page_data + i * PAGE_SIZE);
  }
  return tmp_page;
}

Page* ZoneManager::GrabPageFrameImp(u64 length) {
  Page* ret_page = nullptr;
  Item* cur_item = nullptr;
  if (!replacer_->IsFull()) {
    return AllocateSeqPage(length);
  } else if (replacer_->Victim(&cur_item)) {
    // 2. then find in replacer
    ret_page = reinterpret_cast<Page*>(cur_item->data_);
    u64 length = cur_item->length;
    // 3. set the page_id to INVALID_PAGE_ID
    for (size_t i = 0; i < length; i++) {
      ret_page[i].SetStatus(EVICTED);
    }
    // 4. allocate new page
    Page* tmp_page = AllocateSeqPage(length);

    // 5. if hot add to lru read buffer
    /* for (size_t i = 0; i < length; i++) {
      Page* cur_page = tmp_page + i;
      page_id_t cur_page_id = cur_page->GetPageId();
      // if (cur_page->IsHot()) {
      if (page_id_counts_[cur_page_id] > 1) {
        Page* page = read_buffer_->AddReadOnlyPage(cur_page_id);
        memcpy(page->GetData(), cur_page->GetData(), PAGE_SIZE);
        page->page_id_ = cur_page_id;
      }
    } */
    AddPage({ret_page, length});
    return tmp_page;
  }
  return nullptr;
}

Page* ZoneManager::GetPageImp(page_id_t page_id) {
  Page* ret_page = nullptr;
  if (page_table_.find(page_id) != page_table_.end()) {
    // 1.1 first find in fifo write buffer
    ret_page = page_table_[page_id];
    // need update pin_count in outer function;
    // DEBUG_PRINT("fetch *page_id:%4u frame_id:%4u\n", *page_id, cur_frame_id);
    page_id_counts_[page_id]++;
    hit_++;
  }
  return ret_page;
}

Page* ZoneManager::NewPageImp(page_id_t* page_id, u64 length) {
  Page* ret_page = GrabPageFrameImp(length);
  if (ret_page == nullptr) {
    return nullptr;
  }

  for (u64 i = 0; i < length; i++) {
    if (!AllocatePageId(page_id + i)) {
      return nullptr;
    }
    ret_page[i].page_id_ = page_id[i];
    ret_page[i].read_count_ = 1;
    ret_page[i].is_dirty_ = true;
    page_table_[page_id[i]] = ret_page + i;

    page_id_counts_[page_id[i]] = 1;
    miss_++;
    count_++;
  }
  return ret_page;
}

Page* ZoneManager::NewPage(page_id_t* page_id, u64 length) {
  WriteLockGuard guard(rw_lock_);
  return NewPageImp(page_id, length);
}

Page* ZoneManager::UpdatePage(page_id_t* page_id) {
  WriteLockGuard guard(rw_lock_);
  count_++;
  // 1. find in RingBuffer first
  Page* ret_page = nullptr;
  // 1.1 find in fifo buffer first
  ret_page = GetPageImp(*page_id);
  if (ret_page != nullptr) {
    ret_page->read_count_++;
    return ret_page;
  }

  // 2 find in lru cache
  miss_++;
  // 3.1 allocate a new page_id in zns
  page_id_t tmp_page_id = INVALID_PAGE_ID;
  Page* new_page = NewPageImp(&tmp_page_id);
  // 3.2 read the page from raw id on zns,
  ReadPageFromZNSImp((bytes_t*)(new_page->GetData()), *page_id);
  *page_id = tmp_page_id;

  return new_page;
}

Page* ZoneManager::FetchPage(page_id_t page_id) {
  ReadLockGuard guard(rw_lock_);
  // 1. find in RingBuffer first
  Page* page = nullptr;
  if (page = GetPageImp(page_id)) {
    return page;
  }
  // 2. find in LRU cache,
  // page = read_cache_->FetchPage(page_id);
  // 3. if not exist in LRU cache, then fetch from disk
  if (ReadPageFromZNSImp((bytes_t*)page->GetData(), page_id)) {
    page->page_id_ = page_id;
    page->pin_count_ = 0;
    page->read_count_ = 1;
    page->is_dirty_ = false;
    page->ResetMemory();
    page_table_[page_id] = page;
    page_id_counts_[page_id] = 1;
    miss_++;
    count_++;
  }
  return page;
}

bool ZoneManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  WriteLockGuard guard(rw_lock_);
  // 1. find in RingBuffer first
  // 0 Make sure you can unpin page_id
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  // 1 get page object
  Page* cur_page = page_table_[page_id];
  if (is_dirty) {
    // if page is dirty but is_dirty indicates non-dirty, it also should be
    // dirty
    cur_page->is_dirty_ |= is_dirty;
  }
  if (cur_page->GetPinCount() > 0) {
    cur_page->pin_count_--;
  }
  return true;

  // 2. find in LRU cache, if not exist in LRU cache, then fetch from disk
  // automatically
  // page = read_cache_->UnpinPage(page_id, is_dirty);
}

bool ZoneManager::AllocatePageId(page_id_t* page_id) {
  if (wp_ < end_) {
    *page_id = MAKE_PAGE_ID(zone_id_, wp_);
    wp_++;
    return true;
  }
  *page_id = INVALID_PAGE_ID;
  return false;
}

bool ZoneManager::ReadPageFromZNSImp(bytes_t* data, zns_id_t zid,
                                     u32 page_nums) {
  offset_t zone_offset = zone_->GetMaxCapacity() * GET_ZONE_OFFSET(zid);
  offset_t offset = zone_offset + GET_ZONE_OFFSET(zid) * PAGE_SIZE;
  return zone_->Read((char*)data, page_nums, offset);
}

/*
 * ===================================================================
 * ===================================================================
 * ========================  ZoneBufferPool ==========================
 * ===================================================================
 * ===================================================================
 */
ZoneManagerPool::ZoneManagerPool(u32 pool_size, u32 num_instances_,
                                 const char* db_file)
    : pool_size_(pool_size), num_instances_(num_instances_), index_(0) {
  zns_ = new ZnsManager(db_file);

  CHECK_OR_EXIT(zns_, "ZnsManager is nullptr\n");

  for (size_t i = 0; i < num_instances_; i++) {
    auto zone = zns_->GetUsableZone();
    auto zbf = new ZoneManager(zone);
    zone_buffers_.push_back(zbf);
    zone_table_[zone->GetZoneNr()] = i;
  }

  INFO_PRINT("ZoneManagerPool is created\n");
}

ZoneManagerPool::~ZoneManagerPool() {
  for (auto& zbf : zone_buffers_) {
    SAFE_DELETE(zbf);
  }
  SAFE_DELETE(zns_);
  INFO_PRINT("ZoneManagerPool is deleted\n");
}

// :TODO: :hjl: the condition which zone is full is not implemented
Page* ZoneManagerPool::NewPage(page_id_t* page_id, u64 length) {
  // 1. robin-round to get zone buffer
  size_t start_index = index_;
  size_t loop_index = index_;
  index_ = (index_ + 1) % num_instances_;
  Page* ret_page = nullptr;
  while (true) {
    // 2. get page from zone buffer
    ret_page = zone_buffers_[loop_index]->NewPage(page_id, length);
    if (ret_page != nullptr) {
      // 3. record the zid to zone_buffers_
      zone_table_[GET_ZONE_ID(*page_id)] = loop_index;
      return ret_page;
    }
    loop_index = (loop_index + 1) % num_instances_;
    if (loop_index == start_index) {
      return nullptr;
    }
  }
  return ret_page;
}

Page* ZoneManagerPool::FetchPage(page_id_t page_id) {
  // 1. get zone buffer from pageid
  zns_id_t zid = GET_ZONE_ID(page_id);
  // 2. return the page from zone buffer
  return zone_buffers_[zone_table_[zid]]->FetchPage(page_id);
}

Page* ZoneManagerPool::UpdatePage(page_id_t* page_id) {
  zns_id_t zid = GET_ZONE_ID(*page_id);
  return zone_buffers_[zone_table_[zid]]->UpdatePage(page_id);
}

bool ZoneManagerPool::UnpinPage(page_id_t page_id, bool is_dirty) {
  zns_id_t zid = GET_ZONE_ID(page_id);
  return zone_buffers_[zone_table_[zid]]->UnpinPage(page_id, is_dirty);
}

void ZoneManagerPool::FlushAllPages() {
  // for (auto& buffer : zone_buffers_) {
  // buffer->FlushAllPages();
  // }
}

u64 ZoneManagerPool::GetPoolSize() {
  u64 total = 0;
  for (auto& buffer : zone_buffers_) {
    total += buffer->replacer_->Size();
  }
  return total;
}

u64 ZoneManagerPool::GetFileSize() {
  auto zbd_ = zns_->zbd_;
  return zbd_->GetTotalBytesWritten();
}

u64 ZoneManagerPool::GetReadCount() {
  auto zbd_ = zns_->zbd_;
  return zbd_->GetReadCount();
}

u64 ZoneManagerPool::GetWriteCount() {
  auto zbd_ = zns_->zbd_;
  return zbd_->GetWriteCount();
}

void ZoneManagerPool::Print() {
  u64 page_table_size = 0;
  u64 replacer_size = 0;
  u64 free_list_size = 0;

  u64 count = 0;
  u64 miss = 0;
  u64 hit = 0;

  u64 total = 0;
  for (auto& buffer : zone_buffers_) {
    auto instance = buffer;
    // instance->Print();
    page_table_size += instance->page_table_.size();
    replacer_size += instance->replacer_->Size();

    count += instance->count_;
    miss += instance->miss_;
    hit += instance->hit_;
    total += instance->page_id_counts_.size();
  }

  std::vector<page_id_t> page_ids;
  page_ids.reserve(total);
  for (auto& buffer : zone_buffers_) {
    auto instance = buffer;
    for (auto& count : instance->page_id_counts_) {
      page_ids.push_back(count.second);
    }
  }
  std::sort(page_ids.begin(), page_ids.end());
  size_t avg = 0;
  for (size_t i = 0; i < total; i++) {
    avg += page_ids[i];
  }
  avg /= total;
  auto sz = total;
  INFO_PRINT("[ZoneBufferPool]  Avg Count For a BufferPage: %3lu hits:", avg);
  std::vector<float> percentiles = {0.0, 0.1, 0.2, 0.3, 0.4,   0.5,
                                    0.6, 0.7, 0.8, 0.9, 0.999, 0.9999999};
  INFO_PRINT("  Percentile:")
  for (auto& p : percentiles) {
    INFO_PRINT(" {%2.3lf%%->%4lu}, ", p * 100, page_ids[size_t(p * sz)]);
  }
  INFO_PRINT("\n");

  INFO_PRINT("[ZoneBufferPool] Total size: " KMAG "%4.2f" KRESET
             " MB Instance Nums:%2u Page table "
             "size:%4lu Replacer size: %4lu Free list size: %4lu \n",
             page_table_size * 4.0 / 1024, num_instances_, page_table_size,
             replacer_size, free_list_size);

  double miss_ratio = miss * 100.0 / count;
  double hit_ratio = hit * 100.0 / count;
  printf("[ZoneBufferPool] count:%4lu miss: %4lu %2.2lf%% hit: %4lu " KCYN
         "%2.2lf%%" KRESET "\n",
         count, miss, miss_ratio, hit, hit_ratio);
}
