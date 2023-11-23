//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "config.h"
#include "page.h"
#include "storage.h"

namespace BTree {
typedef u32 frame_id_t;  // frame id type

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be
   * required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer();

  bool Victim(frame_id_t *frame_id);

  void Pin(frame_id_t frame_id);

  void Unpin(frame_id_t frame_id);

  size_t Size();

 private:
  // LRU list node
  struct Node {
    frame_id_t prev_id_;
    frame_id_t next_id_;
  };
  const frame_id_t dummy_;
  size_t size_;
  // LRU list has a dummy head node
  Node *lru_list_;
  // page frame exists in the replacer
  bool IsValid(frame_id_t frame_id) const;
  // remove node
  void Invalidate(frame_id_t frame_id);
  // insert node at MRU end
  void Add(frame_id_t frame_id);
};

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
  void Print();
  std::vector<BufferPoolManager *> bpmis_;
  size_t num_instances_;
  std::atomic<size_t> index_;
};

// extern BTree::ParallelBufferPoolManager *bpm;
}  // namespace BTree
