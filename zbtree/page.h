
#pragma once

#include <cstring>
#include <deque>
#include <iostream>
#include <unordered_set>

#include "config.h"
#include "rwlatch.h"

/**
 * Page is the basic unit of storage within the database system. Page provides a
 * wrapper for actual data pages being held in main memory. Page also contains
 * book-keeping information that is used by the buffer pool manager, e.g. pin
 * count, dirty flag, page id, etc.
 */
class Page {
  // There is book-keeping information inside the page that should only be
  // relevant to the buffer pool manager.
  friend class BufferPoolManager;

 public:
  /** Constructor. */
  Page() = default;

  /** Default destructor. */
  ~Page() = default;

  /** @return the actual data contained within this page */
  inline char *GetData() { return data_; }

  /** set the position of data */
  inline void SetData(char *buf) {
    data_ = buf;
    ResetMemory();
  }

  /** @return the page id of this page */
  inline page_id_t GetPageId() { return page_id_; }

  /** @return the leaf ptr of this page */
  inline void *GetLeafPtr() { return leaf_ptr_; }
  inline void SetLeafPtr(void *leaf_ptr) { leaf_ptr_ = leaf_ptr; }

  /** @return the pin count of this page */
  inline int GetPinCount() { return pin_count_.load(); }

  /** @return true if the page in memory has been modified from the page on
   * disk, false otherwise */
  inline bool IsDirty() { return is_dirty_; }

  /** Acquire the page write latch. */
  inline void WLatch() { rwlatch_.WLock(); }

  /** Release the page write latch. */
  inline void WUnlatch() { rwlatch_.WUnlock(); }

  /** Acquire the page read latch. */
  inline void RLatch() { rwlatch_.RLock(); }

  /** Release the page read latch. */
  inline void RUnlatch() { rwlatch_.RUnlock(); }
  /** helper function: Try to get the read lock */
  inline bool TryRLatch() { return rwlatch_.TryRLock(); }

  /** helper function Set dirty flag */
  inline void SetDirty(bool is_dirty) { is_dirty_ = is_dirty; }
  inline void SetReadOnly() { read_only = READ_ONLY; }
  inline bool IsReadOnly() { read_only == READ_ONLY; }

  //  protected:
  static const size_t SIZE_PAGE_HEADER = 8;
  static const size_t OFFSET_PAGE_START = 0;
  static const size_t OFFSET_LSN = 4;

  //  private:
  /** Zeroes out the data that is held within the page. */
  inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }

  /** The actual data that is stored within a page. */
  char *data_ = nullptr;
  /** The ID of this page. */
  page_id_t page_id_ = INVALID_PAGE_ID;
  void *leaf_ptr_ = nullptr;
  /** The pin count of this page. */
  std::atomic<int> pin_count_{0};
  /** True if the page is dirty, i.e. it is different from its corresponding
   * page on disk. */
  bool is_dirty_ = false;
  bool read_only = false;
  /** Page latch. */
  ReaderWriterLatch rwlatch_;
};

class ZPage {
  // There is book-keeping information inside the page that should only be
  // relevant to the buffer pool manager.
  friend class BufferPoolManager;

 public:
  /** Constructor. */
  ZPage() = default;

  /** Default destructor. */
  ~ZPage() = default;

  /** @return the actual data contained within this page */
  inline char *GetData() { return data_; }

  /** set the position of data */
  inline void SetData(char *buf) {
    data_ = buf;
    ResetMemory();
  }

  /** @return the page id of this page */
  inline page_id_t GetPageId() { return page_id_; }

  /** @return the leaf ptr of this page */
  inline void *GetLeafPtr() { return leaf_ptr_; }
  inline void SetLeafPtr(void *leaf_ptr) { leaf_ptr_ = leaf_ptr; }

  /** @return the pin count of this page */
  inline int GetPinCount() { return pin_count_.load(); }

  /** @return true if the page in memory has been modified from the page on
   * disk, false otherwise */
  inline bool IsDirty() { return is_dirty_; }

  /** helper function Set dirty flag */
  inline void SetDirty(bool is_dirty) { is_dirty_ = is_dirty; }

  //  private:
  /** Zeroes out the data that is held within the page. */
  inline void ResetMemory() { memset(data_, 0, PAGE_SIZE); }

  /** The actual data that is stored within a page. */
  char *data_ = nullptr;
  /** The ID of this page. */
  void *leaf_ptr_ = nullptr;
  /** The pin count of this page. */
  std::atomic<int> pin_count_{0};
  page_id_t page_id_ = INVALID_PAGE_ID;
  /** True if the page is dirty, i.e. it is different from its corresponding
   * page on disk. */
  bool is_dirty_ = false;
};