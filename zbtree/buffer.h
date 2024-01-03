#pragma once
#include <algorithm>
#include <list>
#include <mutex>
#include <mutex>  // NOLINT
#include <thread>
#include <unordered_map>
#include <vector>

#include "page.h"
#include "replacer.h"
#include "storage.h"

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManager {
 public:
  using Mutex = std::mutex;
  using Lock_guard = std::lock_guard<Mutex>;
  friend class ParallelBufferPoolManager;

  /**
   * Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable
   * logging)
   */
  BufferPoolManager(size_t pool_size, DiskManager *disk_manager);
  /**
   * Creates a new BufferPoolManager.
   * @param pool_size the size of the buffer pool
   * @param num_instances total number of BPIs in parallel BPM
   * @param instance_index index of this BPI in the parallel BPM
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable
   * logging)
   */
  BufferPoolManager(size_t pool_size, uint32_t num_instances,
                    uint32_t instance_index, DiskManager *disk_manager);

  /**
   * Destroys an existing BufferPoolManager.
   */
  ~BufferPoolManager();

  /** @return size of the buffer pool */
  size_t GetPoolSize() { return pool_size_; }

  /** @return pointer to all the pages in the buffer pool */
  Page *GetPages() { return pages_; }

 protected:
  /**
   * Fetch the requested page from the buffer pool.
   * @param page_id id of page to be fetched
   * @return the requested page
   */
  Page *FetchPage(page_id_t page_id);

  /**
   * Unpin the target page from the buffer pool. Add the frame of pageid to
   * replacer.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page pin count is <= 0 before this call, true
   * otherwise
   */
  bool UnpinPage(page_id_t page_id, bool is_dirty);

  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true
   * otherwise
   */
  bool FlushPage(page_id_t page_id);

  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new
   * page
   */
  Page *NewPage(page_id_t *page_id);

  /**
   * Deletes a page from the buffer pool. caller after Unpin. add frame of
   * pageid to free_list
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page
   * didn't exist or deletion succeeded
   */
  bool DeletePage(page_id_t page_id);

  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  void FlushAllPages();

  /**
   * Allocate a page on disk.∂
   * @return the id of the allocated page
   */
  page_id_t AllocatePage();

  /**
   * Deallocate a page on disk.
   * @param page_id id of the page to deallocate
   */
  // void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  //   // This is a no-nop right now without a more complex data structure to
  //   track
  //   // deallocated pages
  // }

  /**
   * Validate that the page_id being used is accessible to this BPI. This can be
   * used in all of the functions to validate input data and ensure that a
   * parallel BPM is routing requests to the correct BPI
   * @param page_id
   */
  void ValidatePageId(page_id_t page_id) const;

  Page *GrabPageFrame();

  void GrabLock();
  void ReleaseLock();

  void WaitForFreeFrame();
  void SignalForFreeFrame();

  void PrintBufferPool();

  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** How many instances are in the parallel BPM (if present, otherwise just 1
   * BPI) */
  const uint32_t num_instances_ = 1;
  /** Index of this BPI in the parallel BPM (if present, otherwise just 0) */
  const uint32_t instance_index_ = 0;
  /** Each BPI maintains its own counter for page_ids to hand out, must ensure
   * they mod back to its instance_index_ */
  std::atomic<page_id_t> next_page_id_;

  /** Array of buffer pool pages in memory. */
  Page *pages_;
  /** Page-aligned data */
  char *page_data_;
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_;
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  std::unordered_map<page_id_t, frame_id_t> page_id_counts_;
  /** Replacer to find unpinned pages for replacement. */
  // LRUReplacer *replacer_;
  FIFOReplacer *replacer_;
  /** List of free pages. */
  std::list<frame_id_t> free_list_;
  /** This latch protects shared data structures. We recommend updating this
   * comment to describe what it protects. */
  // std::mutex latch_;

  // big lock for buffer pool manager instance
  pthread_mutex_t mutex_;
  pthread_cond_t cond_;
  // count
  u64 count_;
  u64 hit_;
  u64 miss_;
};

class ParallelBufferPoolManager {
 public:
  /**
   * Creates a new ParallelBufferPoolManager.
   * @param num_instances the number of individual BufferPoolManagerInstances to
   * store
   * @param pool_size the pool size of each BufferPoolManager
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable
   * logging)
   */
  ParallelBufferPoolManager(size_t num_instances, size_t pool_size,
                            DiskManager *disk_manager);

  ParallelBufferPoolManager(size_t num_instances, size_t pool_size,
                            std::string file_name, bool is_one_file = true);
  /**
   * Destroys an existing ParallelBufferPoolManager.
   */
  ~ParallelBufferPoolManager();

  /** @return size of the buffer pool */
  size_t GetPoolSize();

  //  protected:
  /**
   * @param page_id id of page
   * @return pointer to the BufferPoolManager responsible for handling given
   * page id
   */
  BufferPoolManager *GetBufferPoolManager(page_id_t page_id);

  /**
   * Fetch the requested page from the buffer pool.
   * @param page_id id of page to be fetched
   * @return the requested page
   */
  Page *FetchPage(page_id_t page_id);

  /**
   * Unpin the target page from the buffer pool.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page pin count is <= 0 before this call, true
   * otherwise
   */
  bool UnpinPage(page_id_t page_id, bool is_dirty);

  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true
   * otherwise
   */
  bool FlushPage(page_id_t page_id);

  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new
   * page
   */
  Page *NewPage(page_id_t *page_id);

  /**
   * Deletes a page from the buffer pool.
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page
   * didn't exist or deletion succeeded
   */
  bool DeletePage(page_id_t page_id);

  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  void FlushAllPages();

  u64 GetFileSize();
  u64 GetReadCount();
  u64 GetWriteCount();

  void Print();
  std::vector<BufferPoolManager *> bpmis_;
  size_t num_instances_;
  std::atomic<size_t> index_;
  DiskManager *disk_manager_;
};

typedef std::pair<Page *, u64> Slot;

#include <condition_variable>

class CircleFlusher {
 public:
  CircleFlusher(DiskManager *disk_manager, u64 flush_size)
      : disk_manager_(disk_manager), flush_size_(flush_size), stop_(false) {
    flush_pages_ = new CircleBuffer<Slot>(flush_size_);
    flush_thread_ = std::thread([this] { this->FlushThread(); });
  }

  ~CircleFlusher() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stop_ = true;
      cv_.notify_all();
    }
    flush_thread_.join();
    delete flush_pages_;
  }

  void AddPage(Slot slot) {
    while (!flush_pages_->Push(slot)) {
      // _mm_pause();
      // std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    // cv_.notify_one();
  }

  void FlushThread() {
    while (true) {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return stop_ || !flush_pages_->IsEmpty(); });
      if (stop_ && flush_pages_->IsEmpty()) {
        return;
      }
      FlushPage();
    }
  }

  void FlushPage() {
    Slot slot;
    bool ret = flush_pages_->Pop(slot);
    // no need to check ret.
    if (!ret) {
      return;
    }
    Page *page = slot.first;
    u64 length = slot.second;
    while (page->GetPinCount() != 0) {
      // _mm_pause();
      // std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    disk_manager_->write_n_pages(page->GetPageId(), length, page->GetData());
    free(page->GetData());
    delete page;
  }

 private:
  u64 flush_size_;
  u64 begin_ = 0;
  CircleBuffer<Slot> *flush_pages_;
  DiskManager *disk_manager_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread flush_thread_;
  bool stop_;
};

class FIFOBatchBufferPool {
 public:
  explicit FIFOBatchBufferPool(size_t pool_size, DiskManager *disk_manager);

  ~FIFOBatchBufferPool();

  /**
   * @brief if exists in batchbuffer  pool no need to modify the page_id.
   * if not then need update the page_id
   */
  Page *FetchPage(page_id_t *page_id);
  /**
   * @brief add lock
   */
  Page *NewPage(page_id_t *page_id, u64 length = 1);
  /**
   * @brief no lock
   */
  Page *FIFOBatchBufferPool::NewPageImp(page_id_t *page_id, u64 length = 1);
  bool UnpinPage(page_id_t page_id, u64 length, bool is_dirty);
  bool DeletePage(page_id_t page_id, u64 length);
  bool FlushPage(page_id_t page_id, u64 length);

  Page *GrabPageFrame(u64 length = 1);

  u64 AllocatePageId() {
    if (cur_wp_ < max_wp_) {
      return cur_wp_++;
    } else {
      return INVALID_PAGE_ID;
    }
  }

  bool EnoughPageId(u64 length) {
    if (cur_wp_ + length <= max_wp_) {
      return true;
    } else {
      return false;
    }
  }

  void Pin(frame_id_t frame_id);

  void Unpin(frame_id_t frame_id);

  void GrabLock() {
    int ret = pthread_mutex_lock(&mutex_);
    VERIFY(!ret);
  }

  void ReleaseLock() {
    int ret = pthread_mutex_unlock(&mutex_);
    VERIFY(!ret);
  }

  void SignalForFreeFrame() {
    int ret = pthread_cond_signal(&cond_);
    VERIFY(!ret);
  }

  void WaitForFreeFrame() {
    int ret = pthread_cond_wait(&cond_, &mutex_);
    VERIFY(!ret);
  }
  size_t Size();
  void Print();

 private:
  FIFOBacthReplacer *replacer_;
  std::unordered_map<page_id_t, Page *> page_table_;
  std::unordered_map<page_id_t, frame_id_t> page_id_counts_;
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_;
  CircleFlusher *flusher_;
  u64 pool_size_;
  u64 cur_wp_;
  u64 max_wp_;

  // big lock for buffer pool manager instance
  pthread_mutex_t mutex_;
  pthread_cond_t cond_;

  // count
  u64 count_;
  u64 hit_;
  u64 miss_;
  // page frame exists in the replacer
  bool IsValid(frame_id_t frame_id) const;
  // remove node
  void Invalidate(frame_id_t frame_id);
};

class NodeRAII {
 public:
  NodeRAII(ParallelBufferPoolManager *buffer_pool_manager, page_id_t page_id)
      : buffer_pool_manager_(buffer_pool_manager), page_id_(page_id) {
    page_ = buffer_pool_manager_->FetchPage(page_id_);
    CheckAndInitPage();
    // DEBUG_PRINT("raii fetch\n");
  }

  NodeRAII(ParallelBufferPoolManager *buffer_pool_manager, page_id_t *page_id)
      : buffer_pool_manager_(buffer_pool_manager) {
    page_ = buffer_pool_manager_->NewPage(&page_id_);
    CheckAndInitPage();
    *page_id = page_id_;
  }

  void CheckAndInitPage() {
    if (page_ != nullptr) {
      node_ = reinterpret_cast<void *>(page_->GetData());
    } else {
      std::cout << "allocte error, out of memory for buffer pool" << std::endl;
      exit(-1);
    }
    dirty_ = false;
  }

  ~NodeRAII() {
    if (page_ != nullptr) {
      buffer_pool_manager_->UnpinPage(page_id_, dirty_);
      // DEBUG_PRINT("raii unpin\n");
    }
  }

  void *GetNode() { return node_; }
  Page *GetPage() { return page_; }
  page_id_t GetPageId() { return page_id_; }
  void SetDirty(bool is_dirty) { dirty_ = is_dirty; }
  bool SetLeafPtr(void *leaf_ptr) {
    if (page_ != nullptr) {
      page_->SetLeafPtr(leaf_ptr);
      return true;
    }
    return false;
  }

 private:
  ParallelBufferPoolManager *buffer_pool_manager_;
  page_id_t page_id_;
  Page *page_ = nullptr;
  void *node_ = nullptr;
  bool dirty_;
};
