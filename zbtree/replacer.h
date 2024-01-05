#pragma once

#include <vector>

#include "config.h"
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
  void Add(frame_id_t frame_id);
};

class FIFOReplacer {
 public:
  /**
   * Create a new FIFOReplacer.
   * @param num_pages the maximum number of pages the FIFOReplacer will be
   * required to store
   */
  explicit FIFOReplacer(size_t num_pages);

  /**
   * Destroys the FIFOReplacer.
   */
  ~FIFOReplacer();

  bool Add(frame_id_t frame_id);
  bool Victim(frame_id_t *frame_id);
  bool ToVictim(frame_id_t *frame_id);
  bool Remove(frame_id_t frame_id);

  void Pin(frame_id_t frame_id);

  void Unpin(frame_id_t frame_id);

  size_t Size();
  void Print();

 private:
  // LRU list node
  struct Node {
    frame_id_t prev_id_;
    frame_id_t next_id_;
  };
  // LRU list has a dummy head node
  // Node *lru_list_;
  std::vector<frame_id_t> *fifo_list_;
  u32 max_size_;
  u32 cur_size_;
  u32 fifo_head_;
  u32 fifo_tail_;
  // page frame exists in the replacer
  bool IsValid(frame_id_t frame_id) const;
  // remove node
  void Invalidate(frame_id_t frame_id);
};

struct Item {
  // frame_id_t prev_id_;
  // frame_id_t next_id_;
  frame_id_t frame_id_;
  u32 length;
  Item *next_;
  Item *prev_;
  char *data_;
  Item(frame_id_t frame_id, u32 length, char *addr)
      : frame_id_(frame_id),
        length(length),
        data_(addr),
        next_(nullptr),
        prev_(nullptr){};
};

class FIFOBacthReplacer {
 public:
  /**
   * Create a new FIFOReplacer.
   * @param num_nodes the maximum number of items the FIFOReplacer will be
   * required to store
   */
  explicit FIFOBacthReplacer(size_t num_nodes);

  /**
   * Destroys the FIFOReplacer.
   */
  ~FIFOBacthReplacer();

  bool Add(frame_id_t frame_id);
  bool Add(frame_id_t frame_id, Item *begin, u32 length, char *data);
  bool Victim(Item *item);
  bool ToVictim(frame_id_t *frame_id);
  bool Remove(frame_id_t frame_id);
  bool IsFull();

  void Pin(frame_id_t frame_id);
  void Unpin(frame_id_t frame_id);

  u64 Size();
  void Print();

 private:
  // LRU list node
  // std::vector<frame_id_t> *fifo_list_;
  Item *head_;
  Item *tail_;
  u32 max_size_;
  u32 cur_size_;
  // u32 fifo_head_;
  // u32 fifo_tail_;
  // page frame exists in the replacer
  bool IsValid(frame_id_t frame_id) const;
  // remove node
  void Invalidate(frame_id_t frame_id);
};
