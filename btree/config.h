#pragma once
#include <atomic>
#include <cassert>
#include <chrono>  // NOLINT
#include <cstdint>
#include <utility>  //std::pair

namespace BTree {

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

#define BTREE_ASSERT(expr, message) assert((expr) && (message))
// redefine assert()
#ifndef NDEBUG
#define VERIFY(expression) assert(expression)
#else
#define VERIFY(expression) ((void)(expression))
#endif

static constexpr int HEADER_PAGE_ID = 0;     // the header page id
static constexpr int PAGE_SIZE = 4096;       // size of a data page in byte
static constexpr int BUFFER_POOL_SIZE = 10;  // size of buffer pool
static constexpr int INVALID_PAGE_ID = -1;   // invalid page id
static constexpr int INVALID_LSN = -1;       // invalid log sequnse number
using u64 = uint64_t;
using u32 = uint32_t;

typedef u32 lsn_t;  // log sequence number type
typedef u32 page_id_t;
typedef u64 KeyType;
typedef u64 ValueType;
typedef std::pair<KeyType, ValueType> PairType;

enum NodeType { INNERNODE = 0, LEAFNODE, ROOTNODE, INVALIDNODE };
enum TreeOpType {
  TREE_OP_FIND = 0,
  TREE_OP_INSERT,
  TREE_OP_REMOVE,
  TREE_OP_UPDATE,
  TREE_OP_SCAN,
};
const u32 INNER_MAX_SLOT = 6;
const u32 LEAF_MAX_SLOT = 6;
// the first KV slot is reserved for meta
const u64 PAGE_HEADER_SIZE = sizeof(KeyType) + sizeof(ValueType);
// Key的最小值
const KeyType MIN_KEY = std::numeric_limits<KeyType>::min();
// 最大的int64用作非法值
const ValueType INVALID_VALUE = std::numeric_limits<ValueType>::max();

}  // namespace BTree
