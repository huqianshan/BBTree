#pragma once

#include <atomic>
#include <cassert>
#include <chrono>  // NOLINT
#include <cstdint>
#include <iostream>
#include <utility>  //std::pair

// namespace BTree {
using u64 = uint64_t;
using u32 = uint32_t;
using i32 = int32_t;

// typedef i32 txn_id_t;
typedef u32 page_id_t;
typedef u64 KeyType;
typedef u64 ValueType;
typedef std::pair<KeyType, ValueType> PairType;

static constexpr u32 PAGE_SIZE = 4096;  // size of a data page in byte
static constexpr u32 CIRCLE_FLUSHER_SIZE = 1024;
static constexpr page_id_t INVALID_PAGE_ID = -1;  // invalid page id

const KeyType MIN_KEY = std::numeric_limits<KeyType>::min();
const ValueType INVALID_VALUE = std::numeric_limits<ValueType>::max();

enum NodeType { INNERNODE = 0, LEAFNODE, ROOTNODE, INVALIDNODE };
enum TreeOpType {
  TREE_OP_FIND = 0,
  TREE_OP_INSERT,
  TREE_OP_REMOVE,
  TREE_OP_UPDATE,
  TREE_OP_SCAN,
};

enum LatchMode {
  LATCH_MODE_READ = 0,
  LATCH_MODE_WRITE,
  LATCH_MODE_DELETE,
  LATCH_MODE_UPDATE,  // not supported
  LATCH_MODE_SCAN,    // not supported
  LATCH_MODE_NOP,     // not supported
};

class Node;
class InnerNode;
class LeafNode;

template <typename T>
class CircleBuffer {
  // https://github.com/cameron314/concurrentqueue
 public:
  CircleBuffer(u64 max_size)
      : head_(0), tail_(0), size_(0), max_size_(max_size) {}
  ~CircleBuffer() {}

  bool Push(const T &item) {
    u64 current_size = size_.load();
    if (current_size == max_size_) {
      return false;
    }
    buffer_[head_] = item;
    u64 new_head = (head_ + 1) % max_size_;
    while (!head_.compare_exchange_strong(head_, new_head))
      ;
    size_.fetch_add(1);
    return true;
  }

  bool Pop(T &item) {
    u64 current_size = size_.load();
    if (current_size == 0) {
      return false;
    }
    item = buffer_[tail_];
    u64 new_tail = (tail_ + 1) % max_size_;
    while (!tail_.compare_exchange_strong(tail_, new_tail))
      ;
    size_.fetch_sub(1);
    return true;
  }

  bool IsEmpty() { return size_.load() == 0; }

  bool IsFull() { return size_.load() == max_size_; }

  u64 Size() { return size_.load(); }

 private:
  const u64 max_size_;
  T buffer_[max_size_];
  std::atomic<u64> head_;
  std::atomic<u64> tail_;
  std::atomic<u64> size_;
};

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)     \
  cname(const cname &) = delete; \
  cname &operator=(const cname &) = delete;

#define DISALLOW_MOVE(cname) \
  cname(cname &&) = delete;  \
  cname &operator=(cname &&) = delete;

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

// redefine assert()
#define BTREE_ASSERT(expr, message) assert((expr) && (message))

#ifdef NDEBUG
#define VERIFY(expression) ((void)(expression))
#else
#define VERIFY(expression) assert(expression)
#endif

#ifdef DEBUG

#define INFO_PRINT(fmt, args...)
#define DEBUG_PRINT(fmt, args...)
#define FATAL_PRINT(fmt, args...)

#else

#define KNRM "\x1B[0m"
#define KRED "\x1B[31m"
#define KGRN "\x1B[32m"
#define KYEL "\x1B[33m"
#define KBLU "\x1B[34m"
#define KMAG "\x1B[35m"
#define KCYN "\x1B[36m"
#define KWHT "\x1B[37m"
#define KRESET "\033[0m"
#define STR(X) #X

#define INFO_PRINT(fmt, args...) \
  fprintf(stdout, fmt, ##args);  \
  fprintf(stdout, KRESET);       \
  fflush(stdout);

#define DEBUG_PRINT(fmt, args...)                                           \
  fprintf(stdout, KNRM "[%s:%d @%s()]: " fmt, __FILE__, __LINE__, __func__, \
          ##args);                                                          \
  fprintf(stdout, KRESET);                                                  \
  fflush(stdout);

#define FATAL_PRINT(fmt, args...)                                          \
  fprintf(stderr, KNRM "[%s:%d@%s()]: " fmt, __FILE__, __LINE__, __func__, \
          ##args);                                                         \
  fprintf(stdout, KRESET);                                                 \
  fflush(stderr);                                                          \
  exit(-1);

#endif

// }  // namespace BTree