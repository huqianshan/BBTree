#pragma once
#ifndef CONFIG_H
#define CONFIG_H

#include <immintrin.h>

#include <atomic>
#include <cassert>
#include <chrono>  // NOLINT
#include <cstdint>
#include <iostream>
#include <mutex>
#include <utility>  //std::pair

#include "tsc.h"

#define UNITTEST_
#define READ_ONLY (true)
#define WRITE_FLAG (false)
// namespace BTree {
using u8 = uint8_t;
using u64 = uint64_t;
using u32 = uint32_t;
using i32 = int32_t;

typedef char *bytes_t;
typedef u64 page_id_t;
typedef u64 offset_t;
typedef u64 KeyType;
typedef u64 ValueType;
typedef u64 zns_id_t;  // 16 bit for zone id, 48 bit for zone offset
typedef std::pair<KeyType, ValueType> PairType;

static constexpr u32 PAGE_SIZE = 4096;  // size of a data page in byte
static constexpr u32 CIRCLE_FLUSHER_SIZE = 1024;
static constexpr page_id_t INVALID_PAGE_ID = -1;  // invalid page id
// config for buffer_btree
// how many leaf nodes can be kept in memory
constexpr int32_t MAX_BUFFER_LEAF_MB = 15;
constexpr int32_t MAX_KEEP_LEAF_MB = 14;
constexpr int32_t max_leaf_count = MAX_BUFFER_LEAF_MB * 1024 / 4;
constexpr int32_t keep_leaf_count = MAX_KEEP_LEAF_MB * 1024 / 4;
const u32 POOL_SIZE = 4;
// #define TRAVERSE_GREATER

const KeyType MIN_KEY = std::numeric_limits<KeyType>::min();
const ValueType INVALID_VALUE = std::numeric_limits<ValueType>::max();

#define GET_ZONE_ID(zid_) ((zid_) >> 48)
#define GET_ZONE_OFFSET(zid_) ((zid_) & 0x0000FFFFFFFFFFFF)
#define MAKE_PAGE_ID(zone_id, zone_offset) \
  (((zone_id) << 48) | ((zone_offset) & 0x0000FFFFFFFFFFFF))

const u32 SPIN_LIMIT = 6;
#define ATOMIC_SPIN_UNTIL(ptr, status)                    \
  u32 atomic_step_ = 0;                                   \
  while (ptr != status) {                                 \
    auto limit = 1 << std::min(atomic_step_, SPIN_LIMIT); \
    for (uint32_t v_ = 0; v_ < limit; v_++) {             \
      _mm_pause();                                        \
    };                                                    \
    atomic_step_++;                                       \
  }

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

enum PageStatus {
  ACTIVE = 0,
  EVICTED,
  FLUSHED,
};

class Node;
class InnerNode;
class LeafNode;


#define KNRM "\x1B[0m"
#define KBOLD "\e[1m"
#define KRED "\x1B[31m"
#define KGRN "\x1B[32m"
#define KYEL "\x1B[33m"
#define KBLU "\x1B[34m"
#define KMAG "\x1B[35m"
#define KCYN "\x1B[36m"
#define KWHT "\x1B[37m"
#define KBLK_GRN_ITA \
  "\033[3;30;42m"  // #"black green italic" = black text with green background,
                   // italic text
#define KBLK_RED_ITA \
  "\033[9;30;41m"  // #"black red strike" = black text with red background,
                   // strikethrough line through the text
#define KRESET "\033[0m"

// 1 = bold; 5 = slow blink; 31 = foreground color red
// 34 = foreground color blue
// See:
// https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_(Select_Graphic_Rendition)_parameters
#define COLOR_BOLD_SLOW_BLINKING "\e[1;5m"
#define COLOR_BOLD_SLOW_BLINKING_RED "\e[1;5;31m"
#define COLOR_BOLD_BLUE "\e[1;34m"
#endif

#define SAFE_DELETE(ptr) \
  do {                   \
    delete (ptr);        \
    (ptr) = nullptr;     \
  } while (0)

#define FATAL_PRINT(fmt, args...)                                          \
  fprintf(stderr, KRED "[%s:%d@%s()]: " fmt, __FILE__, __LINE__, __func__, \
          ##args);                                                         \
  fprintf(stderr, KRESET);                                                 \
  fflush(stderr);

#define CHECK_OR_EXIT(ptr, msg) \
  do {                          \
    if ((ptr) == nullptr) {     \
      FATAL_PRINT(msg);         \
      exit(-1);                 \
    }                           \
  } while (0)

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

#ifdef NDEBUG
#define VERIFY(expression) ((void)(expression))
#else
#define VERIFY(expression) assert(expression)
#endif

#ifdef DEBUG

#define INFO_PRINT(fmt, args...)
#define DEBUG_PRINT(fmt, args...)
// #define FATAL_PRINT(fmt, args...)

#else

#define INFO_PRINT(fmt, args...) \
  fprintf(stdout, fmt, ##args);  \
  fprintf(stdout, KRESET);       \
  fflush(stdout);

#define DEBUG_PRINT(fmt, args...)                                           \
  fprintf(stdout, KNRM "[%s:%d @%s()]: " fmt, __FILE__, __LINE__, __func__, \
          ##args);                                                          \
  fprintf(stdout, KRESET);                                                  \
  fflush(stdout);
#endif

// }  // namespace BTree
#endif