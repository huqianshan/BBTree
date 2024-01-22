#pragma once
#include <algorithm>
#include <list>
#include <mutex>
#include <mutex>  // NOLINT
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
  LRUReplacer *replacer_;
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
  DiskManager *GetDiskManager(page_id_t page_id);
  // allocate a new page_id
  DiskManager *GetDiskManager(page_id_t *page_id);
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

// #define NO_BUFFER_POOL

class NodeRAII {
 public:
  NodeRAII(ParallelBufferPoolManager *buffer_pool_manager, page_id_t page_id)
      : buffer_pool_manager_(buffer_pool_manager), page_id_(page_id) {
    page_ = nullptr;
    dirty_ = false;
#ifdef NO_BUFFER_POOL
    disk_manager_ = buffer_pool_manager_->GetDiskManager(page_id_);
    node_ = (char *)aligned_alloc(PAGE_SIZE, PAGE_SIZE);
    disk_manager_->read_page(page_id_, node_);
#else
    page_ = buffer_pool_manager_->FetchPage(page_id_);
    CheckAndInitPage();
#endif
    // DEBUG_PRINT("raii fetch\n");
  }

  NodeRAII(ParallelBufferPoolManager *buffer_pool_manager, page_id_t *page_id)
      : buffer_pool_manager_(buffer_pool_manager) {
    page_ = nullptr;
#ifdef NO_BUFFER_POOL
    disk_manager_ = buffer_pool_manager_->GetDiskManager(&page_id_);
    node_ = (char *)aligned_alloc(PAGE_SIZE, PAGE_SIZE);
    // disk_manager_->read_page(page_id_, (char *)node_);
    memset(node_, 0x00, PAGE_SIZE);
    dirty_ = true;
#else
    page_ = buffer_pool_manager_->NewPage(&page_id_);
    CheckAndInitPage();
#endif
    *page_id = page_id_;
  }

  void CheckAndInitPage() {
    if (page_ != nullptr) {
      node_ = reinterpret_cast<char *>(page_->GetData());
    } else {
      std::cout << "allocte error, out of memory for buffer pool" << std::endl;
      exit(-1);
    }
    dirty_ = false;
  }

  ~NodeRAII() {
#ifdef NO_BUFFER_POOL
    if (node_ != nullptr) {
      if (dirty_) {
        disk_manager_->write_page(page_id_, reinterpret_cast<char *>(node_));
      }
      free(node_);
    }
#else
    if (page_ != nullptr) {
      buffer_pool_manager_->UnpinPage(page_id_, dirty_);
      // DEBUG_PRINT("raii unpin\n");
    }
#endif
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
  DiskManager *disk_manager_;
  page_id_t page_id_;
  Page *page_ = nullptr;
  char *node_ = nullptr;
  bool dirty_;
};
