//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page.h
//
// Identification: src/include/storage/page/page.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>
#include <deque>
#include <iostream>
#include <unordered_set>

#include "config.h"
#include "rwlatch.h"

namespace BTree {

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

  /** @return the pin count of this page */
  inline int GetPinCount() { return pin_count_; }

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

 protected:
  static const size_t SIZE_PAGE_HEADER = 8;
  static const size_t OFFSET_PAGE_START = 0;
  static const size_t OFFSET_LSN = 4;

 private:
  /** Zeroes out the data that is held within the page. */
  inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }

  /** The actual data that is stored within a page. */
  char *data_ = nullptr;
  /** The ID of this page. */
  page_id_t page_id_ = INVALID_PAGE_ID;
  /** The pin count of this page. */
  int pin_count_ = 0;
  /** True if the page is dirty, i.e. it is different from its corresponding
   * page on disk. */
  bool is_dirty_ = false;
  /** Page latch. */
  ReaderWriterLatch rwlatch_;
};

/**
 * Transaction tracks information related to a transaction.
 */
class Transaction {
 public:
  explicit Transaction() {
    // Initialize the sets that will be tracked.
    page_set_ = std::make_shared<std::deque<Page *>>();
    deleted_page_set_ = std::make_shared<std::unordered_set<page_id_t>>();
  }

  ~Transaction() = default;

  DISALLOW_COPY(Transaction);

  /** @return the id of this transaction */
  // inline txn_id_t GetTransactionId() const { return txn_id_; }

  /** @return the page set */
  inline std::shared_ptr<std::deque<Page *>> GetPageSet() { return page_set_; }

  /**
   * Adds a page into the page set.
   * @param page page to be added
   */
  inline void AddIntoPageSet(Page *page) { page_set_->push_back(page); }

  /** @return the deleted page set */
  inline std::shared_ptr<std::unordered_set<page_id_t>> GetDeletedPageSet() {
    return deleted_page_set_;
  }

  /**
   * Adds a page to the deleted page set.
   * @param page_id id of the page to be marked as deleted
   */
  inline void AddIntoDeletedPageSet(page_id_t page_id) {
    deleted_page_set_->insert(page_id);
  }

 private:
  /** The ID of this transaction. */
  // txn_id_t txn_id_;
  /** The LSN of the last record written by the transaction. */
  /** Concurrent index: the pages that were latched during index operation. */
  std::shared_ptr<std::deque<Page *>> page_set_;
  /** Concurrent index: the page IDs that were deleted during index operation.*/
  std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set_;
};

}  // namespace BTree