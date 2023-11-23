#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <vector>

#include "buffer.h"
#include "config.h"
#include "rwlatch.h"

namespace BTree {

class KVRecord {
 public:
  KeyType key;
  ValueType value;
  bool operator<(const KVRecord &o) const { return key < o.key; }
};

class Node {
 public:
  NodeType node_type;
  page_id_t page_id_;  // node标识
  page_id_t parent_page_id_;
  u32 size_;
  u32 max_size_;

  Node() = delete;
  virtual ~Node(){};
  virtual NodeType type() const = 0;

  bool IsLeaf() const;
  bool IsRoot() const;
  void SetNodeType(NodeType page_type);

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id_);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id_);

  void SetLSN(lsn_t lsn = INVALID_LSN);
};

/**
 * Store n indexed keys and n+1 child pointers (page_id_) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
class InnerNode : public Node {
 public:
  PairType array_[0];
  // KeyType keys[INNER_MAX_SLOT];  // Keys for values
  //   Node *values[INNER_MAX_SLOT];  // Pointers for child nodes

  InnerNode() = delete;
  virtual ~InnerNode(){};
  void Init(page_id_t page_id, page_id_t parent_id, int max_size);
  int InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                      const ValueType &new_value);
  void MoveHalfTo(InnerNode *recipient,
                  ParallelBufferPoolManager *buffer_pool_manager);
  void MoveAllTo(InnerNode *recipient, const KeyType &middle_key,
                 ParallelBufferPoolManager *buffer_pool_manager);

  void MoveFirstToEndOf(InnerNode *recipient, const KeyType &middle_key,
                        ParallelBufferPoolManager *buffer_pool_manager);

  void MoveLastToFrontOf(InnerNode *recipient, const KeyType &middle_key,
                         ParallelBufferPoolManager *buffer_pool_manager);

  void CopyNFrom(PairType *items, int size,
                 ParallelBufferPoolManager *buffer_pool_manager);
  void Remove(int index);
  ValueType RemoveAndReturnOnlyChild();

  int KeyIndex(const KeyType &key) const;
  int ValueIndex(const ValueType &value) const;
  ValueType Lookup(const KeyType &key) const;
  void InsertAt(int index, const KeyType &new_key, const ValueType &new_value);

  void ToGraph(std::ofstream &out, ParallelBufferPoolManager *bpm) const;
  void ToString(ParallelBufferPoolManager *bpm) const;
};

/**
 * Store indexed key and value together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + VAL(1) | KEY(2) + VAL(2| ... | KEY(n) + VAL(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 24 bytes in total):
 *  ---------------------------------------------------------------------
 * | NodeType (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentNodeId (4) | NodeId (4) | NextNodeId (4)
 *  -----------------------------------------------
 */
class LeafNode : public Node {
 public:
  DISALLOW_COPY_AND_MOVE(LeafNode);
  LeafNode() = delete;

  // Init an empty leaf node
  LeafNode(Node *parent, u32 level, page_id_t pid);
  virtual ~LeafNode(){};

  KeyType KeyAt(int index) const;

  void Init(page_id_t page_id, page_id_t parent_id, int max_size);
  int Insert(const KeyType &key, const ValueType &value);
  bool Lookup(const KeyType &key, ValueType *value) const;
  bool Update(const KeyType &key, const ValueType &value);
  void InsertAt(int index, const KeyType &key, const ValueType &value);
  void RemoveAt(int index);
  void MoveHalfTo(LeafNode *recipient);
  void MoveAllTo(LeafNode *recipient);
  void MoveFirstToEndOf(LeafNode *recipient);
  void MoveLastToFrontOf(LeafNode *recipient);
  void CopyLastFrom(const PairType &item);
  // helper function
  bool CheckDuplicated(const KeyType &key) const;
  int KeyIndex(const KeyType &key) const;

  page_id_t GetNextPageId() const;
  void SetNextPageId(page_id_t next_page_id);
  bool search_record_in_page(const KeyType key, const char *page_data,
                             ValueType *value);
  /**
   * check the order of keys page by page
   */
  KeyType check_key_order() const;

  void ToGraph(std::ofstream &out) const;
  void ToString() const;

 public:
  page_id_t next_page_id_;
  PairType array_[0];
};

// extern Transaction *transaction;
class BTree {
 public:
  explicit BTree(ParallelBufferPoolManager *buffer) {
    SetRootPageId(INVALID_PAGE_ID);
    buffer_pool_manager_ = buffer;
  };

  /* CRUD function  */
  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value);
  // Remove a key and its value from this B+ tree.
  bool Remove(const KeyType &key);
  bool Update(const KeyType &key, const ValueType &value);
  // return the value associated with a given key
  bool Get(const KeyType &key, ValueType *result);
  bool Scan(const KeyType &key_begin, const KeyType &key_end,
            std::vector<ValueType> result);
  Page *FindLeafPage(const KeyType &key, Transaction *transaction,
                     bool leftMost = false, LatchMode mode = LATCH_MODE_READ);
  bool InsertIntoLeaf(const KeyType &key, const ValueType &value,
                      LeafNode *leaf_ptr);
  void StartNewTree(const KeyType &key, const ValueType &value);
  template <typename N>
  N *Split(N *node);
  void InsertIntoParent(Node *old_node, const KeyType &key, Node *new_node);
  template <typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction);

  template <typename N>
  bool Coalesce(N **neighbor_node, N **node, InnerNode **parent, int index,
                Transaction *transaction);

  template <typename N>
  bool Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(Node *old_root_node, Transaction *transaction);

  // concurrent helper
  void ReleaseLatchQueue(Transaction *transaction, LatchMode mode);
  bool CheckSafe(Node *tree_ptr, LatchMode mode);
  void DeletePages(Transaction *transaction);

  // helper function
  void SetRootPageId(page_id_t root_page_id);
  Page *SafelyNewPage(page_id_t *page_id, const std::string &logout_string);
  Page *SafelyGetFrame(page_id_t page_id, const std::string &logout_string);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  /* Debug Routines for FREE!! */
  void ToGraph(std::ofstream &out) const;
  void ToString() const;
  void Draw(std::string path) const;

 private:
  page_id_t root_page_id_;
  ReaderWriterLatch root_id_latch_;
  ParallelBufferPoolManager *buffer_pool_manager_;
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

 private:
  ParallelBufferPoolManager *buffer_pool_manager_;
  page_id_t page_id_;
  Page *page_ = nullptr;
  void *node_ = nullptr;
  bool dirty_;
};

}  // namespace BTree
