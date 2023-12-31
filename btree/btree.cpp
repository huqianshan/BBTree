#include "btree.h"

#include <stack>
namespace BTree {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool Node::IsLeaf() const { return node_type == NodeType::LEAFNODE; }
bool Node::IsRoot() const { return parent_page_id_ == INVALID_PAGE_ID; }
void Node::SetNodeType(NodeType page_type) { node_type = page_type; }

int Node::GetSize() const { return size_; }
void Node::SetSize(int size) { size_ = size; }
void Node::IncreaseSize(int amount) { size_ += amount; }

/*
 * Helper methods to get/set max size (capacity) of the page
 */
int Node::GetMaxSize() const { return max_size_; }
void Node::SetMaxSize(int size) { max_size_ = size; }

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * 根据书籍p665(DataBase System
Concepts第7版)所说，如果规定阶数（也就是一个节点的最大指针分支数量）为n，则
+--------+--------------------------+--------------+------------------------+
|  type  |          最多key         | 最多pointers |      最少pointers      |
+--------+--------------------------+--------------+------------------------+
|  叶子  |            n-1           |       n      |    (n-1)/2 向上取整    |
+--------+--------------------------+--------------+------------------------+
| 非叶子 |            n-1           |       n      |      n/2 向上取整      |
+--------+--------------------------+--------------+------------------------+
|  root  |    n-1(非叶子)或n（叶子）） |       n      |  1（叶子）或2（非叶子）
|
+--------+--------------------------+--------------+------------------------+
root节点是特殊的，首先root不用满足节点数量低于一半时分裂，其次
root节点可能是叶子节点，也可能是非叶子节点。如果root节点作为叶子节点，
则key和value的数量一样，此时min size = 1；如果root节点作为非叶子节点，
则key数量=value数量-1，此时min size = 2；
 */
int Node::GetMinSize() const {
  // root page has special condition; it could either leaf_page or internal
  // page;
  if (IsRoot()) {
    return IsLeaf() ? 1 : 2;
  }
  return (IsLeaf() ? max_size_ : max_size_ + 1) / 2;
}
/*
 * Helper methods to get/set parent page id
 */
page_id_t Node::GetParentPageId() const { return parent_page_id_; }
void Node::SetParentPageId(page_id_t parent_page_id) {
  parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t Node::GetPageId() const { return page_id_; }
void Node::SetPageId(page_id_t page_id) { page_id_ = page_id; }

/*
 * helper function
 * binary search for a key and return the index (array_ offset)
 * return the index that is the last `i` that key >= array_[i].first
 * @attenion different from leafnode: the first key is not used
 */
int InnerNode::KeyIndex(const KeyType &key) const {
  int size = GetSize();
  if (size == 1 || key < array_[1].first) return 0;

  int right = size;
  // `right` is the first 'impossible' index
  int left = 1;
  while (left < right - 1) {
    int mid = (left + right) / 2;

    if (key < array_[mid].first) {
      right = mid;
    }
    if (key > array_[mid].first) {
      left = mid;
    }
    if (key == array_[mid].first) {
      return mid;
    }
  }
  return left;
};

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
int InnerNode::ValueIndex(const ValueType &value) const {
  int len = GetSize();
  for (int i = 0; i < len; i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
};

/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 * dont compare key equals
 */
ValueType InnerNode::Lookup(const KeyType &key) const {
  int index = KeyIndex(key);
  return array_[index].second;
};

void InnerNode::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetNodeType(NodeType::INNERNODE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPageId(page_id);
  SetParentPageId(parent_id);
};

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InnerNode::InsertNodeAfter(const ValueType &old_value,
                               const KeyType &new_key,
                               const ValueType &new_value) {
  // 1. first find the index for old_value,which is just after the old_value
  int index = ValueIndex(old_value) + 1;
  // 2.then migrate the slots to a latter position
  for (int i = GetSize() - 1; i >= index; --i) {
    array_[i + 1] = array_[i];
  }
  array_[index] = {new_key, new_value};
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * @attention only use this function in insert to parent
 */
void InnerNode::MoveHalfTo(InnerNode *recipient,
                           ParallelBufferPoolManager *buffer_pool_manager) {
  // the latter half
  int raw_size = GetSize();
  int half_size = raw_size / 2;

  PairType *items = array_;
  for (int i = half_size; i < raw_size; i++) {
    // 1. set the paraent of items'child  to me
    page_id_t child_page_id = items[i].second;
    Page *child_page_ptr = buffer_pool_manager->FetchPage(child_page_id);
    Node *node = reinterpret_cast<Node *>(child_page_ptr->GetData());
    node->SetParentPageId(recipient->GetPageId());
    // 2. flush to disk
    buffer_pool_manager->UnpinPage(child_page_id, true);
    // 3. copy n item
    recipient->InsertAt(recipient->GetSize(), items[i].first, items[i].second);
    // 4. delete the half pairs in raw nodes
    items[i] = {0, 0};
  }
  SetSize(half_size);
};

/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the
 * invariant. You also need to use BufferPoolManager to persist changes to the
 * parent page id for those pages that are moved to the recipient
 */
void InnerNode::MoveAllTo(InnerNode *recipient, const KeyType &middle_key,
                          ParallelBufferPoolManager *buffer_pool_manager) {
  int move_size = GetSize();
  int begin = recipient->GetSize();

  // 1. set the first from parents' first key(middle key) make ordered.
  recipient->array_[begin] = {middle_key, this->array_[0].second};
  NodeRAII child_node_raii(buffer_pool_manager, this->array_[0].second);
  InnerNode *child_node =
      reinterpret_cast<InnerNode *>(child_node_raii.GetNode());
  child_node->SetParentPageId(recipient->GetPageId());
  child_node_raii.SetDirty(true);

  // 2. move all the rest to the recipient and set parent id to new parent
  for (int i = 1; i < move_size; i++) {
    recipient->array_[begin + i] = this->array_[i];

    NodeRAII child_node_raii(buffer_pool_manager, this->array_[i].second);
    InnerNode *child_node =
        reinterpret_cast<InnerNode *>(child_node_raii.GetNode());
    child_node->SetParentPageId(recipient->GetPageId());
    child_node_raii.SetDirty(true);

    recipient->array_[begin + i] = this->array_[i];
    this->array_[i] = {0, 0};
  }
  this->SetSize(0);
  recipient->IncreaseSize(move_size);
};

/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the
 * invariant. You also need to use BufferPoolManager to persist changes to the
 * parent page id for those pages that are moved to the recipient
 */
void InnerNode::MoveFirstToEndOf(
    InnerNode *recipient, const KeyType &middle_key,
    ParallelBufferPoolManager *buffer_pool_manager) {
  // 1. copy the first item to the recipient
  page_id_t child_page_id = this->array_[0].second;
  recipient->array_[recipient->GetSize()] = {middle_key, child_page_id};
  // 2. set the parent id
  NodeRAII child_node_raii(buffer_pool_manager, child_page_id);
  InnerNode *child_node =
      reinterpret_cast<InnerNode *>(child_node_raii.GetNode());
  child_node->SetParentPageId(recipient->GetPageId());
  child_node_raii.SetDirty(true);
  // 3. update size
  Remove(0);
  recipient->IncreaseSize(1);
};

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s
 * array to position the middle_key at the right place. You also need to use
 * BufferPoolManager to persist changes to the parent page id for those pages
 * that are moved to the recipient
 */
void InnerNode::MoveLastToFrontOf(
    InnerNode *recipient, const KeyType &middle_key,
    ParallelBufferPoolManager *buffer_pool_manager) {
  int end = this->GetSize() - 1;
  page_id_t child_page_id = this->array_[end].second;

  // 1. copy the last item to the recipient
  recipient->InsertAt(0, middle_key, child_page_id);
  this->array_[end] = {0, 0};
  this->IncreaseSize(-1);
  // 2. set key at 1 to middle key in recipient
  recipient->array_[1].first = middle_key;
  // 3. update the child ptr
  NodeRAII child_node_raii(buffer_pool_manager, child_page_id);
  child_node_raii.SetDirty(true);
  InnerNode *child_node =
      reinterpret_cast<InnerNode *>(child_node_raii.GetNode());
  child_node->SetParentPageId(recipient->GetPageId());
};

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents
 * page now changes to me. So I need to 'adopt' them by changing their parent
 * page id, which needs to be persisted with BufferPoolManger
 * @attention only use this function in insert to parent
 */
void InnerNode::CopyNFrom(PairType *items, int size,
                          ParallelBufferPoolManager *buffer_pool_manager) {
  return;
};

void InnerNode::InsertAt(int index, const KeyType &new_key,
                         const ValueType &new_value) {
  int size = GetSize();

  for (int i = size - 1; i >= index; --i) {
    array_[i + 1] = array_[i];
  }
  array_[index] = {new_key, new_value};

  IncreaseSize(1);
}

void InnerNode::Remove(int index) {
  int size = GetSize();
  for (int i = index; i < size - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  if (index == size - 1) array_[index] = {0, 0};
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
ValueType InnerNode::RemoveAndReturnOnlyChild() {
  if (GetSize() != 1) {
    return INVALID_PAGE_ID;
  }

  page_id_t page_id = array_[0].second;
  Remove(0);
  return page_id;
}

KeyType LeafNode::KeyAt(int index) const { return array_[index].first; }
// KeyType LeafNode::SetKeyAt(int index) const { return array_[index].first; }

void LeafNode::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetNodeType(NodeType::LEAFNODE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
};

bool LeafNode::CheckDuplicated(const KeyType &key) const {
  int index = KeyIndex(key);
  return (index < GetSize()) && (array_[index].first == key);
}

void LeafNode::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

void LeafNode::InsertAt(int index, const KeyType &key, const ValueType &value) {
  int size = GetSize();
  for (int i = size - 1; i >= index; --i) {
    array_[i + 1] = array_[i];
  }
  array_[index] = {key, value};
  IncreaseSize(1);
}

int LeafNode::Insert(const KeyType &key, const ValueType &value) {
  int size = GetSize();
  // if (size == 0) {
  //   array_[0] = {key, value};
  //   IncreaseSize(1);
  //   return 1;
  // }

  int index = KeyIndex(key);
  if (index < GetSize() && array_[index].first == key) {
    return -1;
  }

  InsertAt(index, key, value);
  return size + 1;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */

int LeafNode::KeyIndex(const KeyType &key) const {
  int size = GetSize();

  // `right` is the first 'impossible' index
  int right = size;
  int left = 0;
  while (left < right) {
    int mid = (left + right) / 2;

    if (key < array_[mid].first) {
      right = mid;
    }
    if (key > array_[mid].first) {
      left = mid + 1;
    }
    if (key == array_[mid].first) {
      return mid;
    }
  }
  return left;
}

void LeafNode::RemoveAt(int index) {
  int size = GetSize();
  for (int i = index; i < size - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  if (index == size - 1) {
    array_[index] = {0, 0};
  }
  IncreaseSize(-1);
}

void LeafNode::MoveHalfTo(LeafNode *recipient) {
  int size = GetSize();
  int half = size / 2;
  int begin = size - half;
  for (int i = begin; i < size; ++i) {
    recipient->InsertAt(recipient->GetSize(), array_[i].first,
                        array_[i].second);
    this->array_[i] = {0, 0};
  }
  IncreaseSize(-half);
}
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't
 * forget to update the next_page id in the sibling page
 */
void LeafNode::MoveAllTo(LeafNode *recipient) {
  recipient->next_page_id_ = this->next_page_id_;
  int move_size = GetSize();
  int begin = recipient->GetSize();

  // cannot assume all the keys in this is smaller than the keys in recipient
  // moveAllTo can be called in both direction
  for (int i = 0; i < move_size; ++i) {
    recipient->InsertAt(begin + i, array_[i].first, array_[i].second);
    this->array_[i] = {0, 0};
  }
  SetSize(0);
}

/*****************************************************************************
 * Leaf Node LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafNode::Lookup(const KeyType &key, ValueType *value) const {
  int index = KeyIndex(key);

  if (index < GetSize() && array_[index].first == key) {
    *value = array_[index].second;
    return true;
  }
  return false;
};

/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then update its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafNode::Update(const KeyType &key, const ValueType &value) {
  int index = KeyIndex(key);

  if (index < GetSize() && array_[index].first == key) {
    array_[index].second = value;
    return true;
  }
  return false;
}

void LeafNode::MoveFirstToEndOf(LeafNode *recipient) {
  recipient->CopyLastFrom(array_[0]);
  RemoveAt(0);
}

void LeafNode::CopyLastFrom(const PairType &item) {
  int size = GetSize();
  InsertAt(size, item.first, item.second);
}

void LeafNode::MoveLastToFrontOf(LeafNode *recipient) {
  int size = GetSize();
  recipient->InsertAt(0, array_[size - 1].first, array_[size - 1].second);
  RemoveAt(size - 1);
};

page_id_t LeafNode::GetNextPageId() const { return next_page_id_; };

void InnerNode::ToGraph(std::ofstream &out,
                        ParallelBufferPoolManager *bpm) const {
  std::string internal_prefix("INT_");
  // Print node name
  out << internal_prefix << this->GetPageId();
  // Print node properties
  out << "[shape=plain color=pink ";
  // Print data of the node
  out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
         "CELLPADDING=\"4\">\n";
  // Print data
  out << "<TR><TD COLSPAN=\"" << this->GetSize() << "\">P=" << this->GetPageId()
      << "</TD></TR>\n";
  out << "<TR><TD COLSPAN=\"" << this->GetSize() << "\">"
      << "max_size=" << this->GetMaxSize() << ",min_size=" << this->GetMinSize()
      << ",size=" << this->GetSize() << "</TD></TR>\n";
  out << "<TR>";

  for (int i = 0; i < this->GetSize(); i++) {
    out << "<TD PORT=\"p" << this->array_[i].second << "\">";
    if (i > 0) {
      out << this->array_[i].first;
    } else {
      out << " ";
    }
    out << "</TD>\n";
  }
  out << "</TR>";
  // Print table end
  out << "</TABLE>>];\n";

  // Print Parent link
  if (this->GetParentPageId() != INVALID_PAGE_ID) {
    out << internal_prefix << this->GetParentPageId() << ":p"
        << this->GetPageId() << " -> " << internal_prefix << this->GetPageId()
        << ";\n";
  }

  // Print leaves
  for (int i = 0; i < this->GetSize(); i++) {
    auto child_page = reinterpret_cast<Node *>(
        bpm->FetchPage(this->array_[i].second)->GetData());

    if (child_page->IsLeaf()) {
      auto n = reinterpret_cast<LeafNode *>(child_page);
      n->ToGraph(out);
    } else {
      auto n = reinterpret_cast<InnerNode *>(child_page);
      n->ToGraph(out, bpm);
    }

    if (i > 0) {
      auto sibling_page = reinterpret_cast<Node *>(
          bpm->FetchPage(this->array_[i - 1].second)->GetData());
      if (!sibling_page->IsLeaf() && !child_page->IsLeaf()) {
        out << "{rank=same " << internal_prefix << sibling_page->GetPageId()
            << " " << internal_prefix << child_page->GetPageId() << "};\n";
      }
      bpm->UnpinPage(sibling_page->GetPageId(), false);
    }
    bpm->UnpinPage(child_page->GetPageId(), false);
  }
}

void InnerNode::ToString(ParallelBufferPoolManager *bpm) const {
  std::cout << "Internal Page: " << GetPageId()
            << " parent: " << GetParentPageId() << std::endl;
  for (int i = 0; i < GetSize(); i++) {
    std::cout << array_[i].first << ": " << array_[i].second << ",";
  }
  std::cout << std::endl;
  for (int i = 0; i < GetSize(); i++) {
    auto child_page = reinterpret_cast<Node *>(
        bpm->FetchPage(this->array_[i].second)->GetData());

    if (child_page->IsLeaf()) {
      auto n = reinterpret_cast<LeafNode *>(child_page);
      n->ToString();
    } else {
      auto n = reinterpret_cast<InnerNode *>(child_page);
      n->ToString(bpm);
    }
    bpm->UnpinPage(child_page->GetPageId(), false);
  }
}

void LeafNode::ToString() const {
  std::cout << "[LeafNode: " + std::to_string(GetPageId()) +
                   " parent: " + std::to_string(GetParentPageId()) +
                   " next: " + std::to_string(GetNextPageId());
  std::cout << std::endl;
  for (int i = 0; i < GetSize(); i++) {
    std::cout << " " + std::to_string(array_[i].first) + ":" +
                     std::to_string(array_[i].second);
  }
  std::cout << "]" << std::endl;
}

void LeafNode::ToGraph(std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  // Print node name
  out << leaf_prefix << this->GetPageId();
  // Print node properties
  out << "[shape=plain color=green ";
  // Print data of the node
  out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
         "CELLPADDING=\"4\">\n";
  // Print data
  out << "<TR><TD COLSPAN=\"" << this->GetSize() << "\">P=" << this->GetPageId()
      << "</TD></TR>\n";
  out << "<TR><TD COLSPAN=\"" << this->GetSize() << "\">"
      << "max_size=" << this->GetMaxSize() << ",min_size=" << this->GetMinSize()
      << ",size=" << this->GetSize() << "</TD></TR>\n";
  out << "<TR>";
  for (int i = 0; i < this->GetSize(); i++) {
    out << "<TD>" << this->KeyAt(i) << "</TD>\n";
  }
  out << "</TR>";
  // Print table end
  out << "</TABLE>>];\n";
  // Print Leaf node link if there is a next page
  if (this->GetNextPageId() != INVALID_PAGE_ID) {
    out << leaf_prefix << this->GetPageId() << " -> " << leaf_prefix
        << this->GetNextPageId() << ";\n";
    out << "{rank=same " << leaf_prefix << this->GetPageId() << " "
        << leaf_prefix << this->GetNextPageId() << "};\n";
  }

  // Print parent links if there is a parent
  if (this->GetParentPageId() != INVALID_PAGE_ID) {
    out << internal_prefix << this->GetParentPageId() << ":p"
        << this->GetPageId() << " -> " << leaf_prefix << this->GetPageId()
        << ";\n";
  }
}

BTree::~BTree() {
  GetNodeNums();
  if (buffer_pool_manager_) {
    delete buffer_pool_manager_;
  }
}

bool BTree::Insert(const KeyType &key, const ValueType &value) {
  Transaction *transaction = new Transaction();
  bool ret;

  root_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);  // nullptr means root_id_latch_

  if (IsEmpty()) {
    StartNewTree(key, value);
    ret = true;
  } else {
    Page *page_ptr = FindLeafPage(key, transaction, false, LATCH_MODE_WRITE);
    LeafNode *leaf_ptr = reinterpret_cast<LeafNode *>(page_ptr->GetData());
    if (page_ptr == nullptr) {
      return false;
    }
    ret = InsertIntoLeaf(key, value, leaf_ptr);
    page_ptr->SetDirty(true);
  }

  // root_id_latch_.WUnlock();
  ReleaseLatchQueue(transaction, LATCH_MODE_WRITE);
  delete transaction;
  return ret;
};

/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
bool BTree::Remove(const KeyType &key) {
  Transaction *transaction = new Transaction();

  root_id_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);  // nullptr means root_id_latch_

  if (IsEmpty()) {
    root_id_latch_.WUnlock();
    return false;
  }

  Page *leaf_page_ptr =
      FindLeafPage(key, transaction, false, LATCH_MODE_DELETE);

  LeafNode *leaf_ptr = reinterpret_cast<LeafNode *>(leaf_page_ptr->GetData());
  if (!leaf_ptr->CheckDuplicated(key)) {
    ReleaseLatchQueue(transaction, LATCH_MODE_DELETE);
    return false;
  }

  int index = leaf_ptr->KeyIndex(key);
  leaf_ptr->RemoveAt(index);

  if (leaf_ptr->GetSize() < leaf_ptr->GetMinSize()) {
    CoalesceOrRedistribute<LeafNode>(leaf_ptr, transaction);
  }
  leaf_page_ptr->SetDirty(true);

  ReleaseLatchQueue(transaction, LATCH_MODE_DELETE);
  DeletePages(transaction);
  delete transaction;
  return true;
};

bool BTree::Update(const KeyType &key, const ValueType &value) {
  Transaction *transaction = new Transaction();
  root_id_latch_.RLock();
  transaction->AddIntoPageSet(nullptr);  // nullptr means root_id_latch_

  if (IsEmpty()) {
    root_id_latch_.RUnlock();
    return false;
  }

  Page *page_ptr = FindLeafPage(key, transaction, false, LATCH_MODE_UPDATE);
  LeafNode *leaf_ptr = reinterpret_cast<LeafNode *>(page_ptr->GetData());
  bool ret = leaf_ptr->Update(key, value);

  if (ret) {
    page_ptr->SetDirty(true);
  }
  ReleaseLatchQueue(transaction, LATCH_MODE_UPDATE);
  delete transaction;
  return ret;
};

bool BTree::Get(const KeyType &key, ValueType *result) {
  Transaction *transaction = new Transaction();
  root_id_latch_.RLock();
  transaction->AddIntoPageSet(nullptr);  // nullptr means root_id_latch_

  if (IsEmpty()) {
    root_id_latch_.RUnlock();
    return false;
  }

  Page *page_ptr = FindLeafPage(key, transaction, false, LATCH_MODE_READ);
  LeafNode *leaf_ptr = reinterpret_cast<LeafNode *>(page_ptr->GetData());
  bool ret = leaf_ptr->Lookup(key, result);

  // release the latch first!

  ReleaseLatchQueue(transaction, LATCH_MODE_READ);
  delete transaction;
  return ret;
};

bool BTree::Scan(const KeyType &key_begin, const KeyType &key_end,
                 std::vector<ValueType> result) {
  return true;
};

/*
 * @brief leaf page containing particular key, if leftMost flag == 1, find
 * the left most leaf page
 * @param key: the key to find
 */
Page *BTree::FindLeafPage(const KeyType &key, Transaction *transaction,
                          bool leftMost, LatchMode mode) {
  // It's outer function's duty to lock `root_id_latch_`
  std::shared_ptr<std::deque<Page *>> deque_ptr = transaction->GetPageSet();
  page_id_t page_id = root_page_id_;
  Page *page_ptr = nullptr;
  Node *tree_ptr;

  while (true) {
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    // VERIFY(page_ptr != nullptr);
    tree_ptr = reinterpret_cast<Node *>(page_ptr->GetData());

    if (mode == LATCH_MODE_READ || mode == LATCH_MODE_UPDATE) {
      page_ptr->RLatch();
    } else {
      page_ptr->WLatch();
    }

    if (CheckSafe(tree_ptr, mode)) {
      ReleaseLatchQueue(transaction, mode);
    }
    deque_ptr->push_back(page_ptr);

    bool is_leaf = tree_ptr->IsLeaf();
    if (is_leaf) {
      break;
    }

    InnerNode *internal_ptr = reinterpret_cast<InnerNode *>(tree_ptr);
    if (leftMost) {
      page_id = internal_ptr->array_[0].first;
    } else {
      page_id = internal_ptr->Lookup(key);
    }
  }
  return page_ptr;
};

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BTree::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                           LeafNode *leaf_ptr) {
  int size = leaf_ptr->Insert(key, value);
  if (size == leaf_ptr->GetMaxSize()) {
    LeafNode *new_leaf_ptr = Split<LeafNode>(leaf_ptr);

    leaf_ptr->MoveHalfTo(new_leaf_ptr);
    new_leaf_ptr->SetNextPageId(leaf_ptr->GetNextPageId());
    leaf_ptr->SetNextPageId(new_leaf_ptr->GetPageId());
    // After `MoveHalfTo` middle key is kept in array[0]
    InsertIntoParent(leaf_ptr, new_leaf_ptr->KeyAt(0), new_leaf_ptr);

    buffer_pool_manager_->UnpinPage(new_leaf_ptr->GetPageId(), 1);
  } else if (size == -1) {
    // duplicated key
    return false;
  }
  return true;
};

void BTree::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id;

  Page *root_page_ptr =
      SafelyNewPage(&root_page_id, "Out of memory in `StartNewPage`");

  SetRootPageId(root_page_id);
  // UpdateRootPageId(1); for header page

  LeafNode *leaf_ptr = reinterpret_cast<LeafNode *>(root_page_ptr->GetData());
  leaf_ptr->Init(root_page_id, INVALID_PAGE_ID, LEAF_MAX_SLOT);
  leaf_ptr->Insert(key, value);

  buffer_pool_manager_->UnpinPage(root_page_id, true);
};

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
template <typename N>
N *BTree::Split(N *node) {
  page_id_t new_page_id;
  Page *new_page_ptr =
      SafelyNewPage(&new_page_id, "Buffer pool run out of memory in `Split`");

  N *type_n_page_ptr = reinterpret_cast<N *>(new_page_ptr->GetData());
  if (node->IsLeaf()) {
    type_n_page_ptr->Init(new_page_id, node->GetParentPageId(), LEAF_MAX_SLOT);
  } else {
    type_n_page_ptr->Init(new_page_id, node->GetParentPageId(), INNER_MAX_SLOT);
  }
  return type_n_page_ptr;
}

Page *BTree::SafelyGetFrame(page_id_t page_id,
                            const std::string &logout_string) {
  Page *page_ptr = buffer_pool_manager_->FetchPage(page_id);
  if (page_ptr == nullptr) {
    // throw Exception(ExceptionType::OUT_OF_MEMORY, logout_string);
    std::cout << "allocte error, out of memory for buffer pool" << logout_string
              << std::endl;
    exit(-1);
  }
  return page_ptr;
}

Page *BTree::SafelyNewPage(page_id_t *page_id,
                           const std::string &logout_string) {
  Page *new_page_ptr = buffer_pool_manager_->NewPage(page_id);
  if (new_page_ptr == nullptr) {
    // throw Exception(ExceptionType::OUT_OF_MEMORY, logout_string);
    // printf("error, out of memory for buffer pool\n");
    std::cout << "allocte error, out of memory for buffer pool" << logout_string
              << std::endl;
    exit(-1);
  }
  return new_page_ptr;
};

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BTree::InsertIntoParent(Node *old_node, const KeyType &key,
                             Node *new_node) {
  if (old_node->IsRoot()) {
    // Which means root has been split and need to construct a new root
    page_id_t new_root_page_id;
    Page *new_root_page_ptr;
    new_root_page_ptr =
        SafelyNewPage(&new_root_page_id, "Out of memory in `InsertIntoParent`");
    InnerNode *new_root_ptr =
        reinterpret_cast<InnerNode *>(new_root_page_ptr->GetData());

    new_root_ptr->Init(new_root_page_id, INVALID_PAGE_ID, INNER_MAX_SLOT);

    // populate new root
    old_node->SetParentPageId(new_root_page_id);
    new_node->SetParentPageId(new_root_page_id);
    new_root_ptr->SetSize(2);
    new_root_ptr->array_[1].first = key;
    new_root_ptr->array_[1].second = new_node->GetPageId();
    new_root_ptr->array_[0].second = old_node->GetPageId();

    SetRootPageId(new_root_page_id);
    // UpdateRootPageId(0); // for header
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return;
  }

  page_id_t parent_page_id = old_node->GetParentPageId();
  Page *parent_page_ptr =
      SafelyGetFrame(parent_page_id, "Out of memory in `InsertIntoParent` !");
  InnerNode *parent_ptr =
      reinterpret_cast<InnerNode *>(parent_page_ptr->GetData());

  int new_size = parent_ptr->InsertNodeAfter(old_node->GetPageId(), key,
                                             new_node->GetPageId());

  if (new_size >= parent_ptr->GetMaxSize() + 1) {
    InnerNode *new_parent_ptr = Split<InnerNode>(parent_ptr);

    parent_ptr->MoveHalfTo(new_parent_ptr, buffer_pool_manager_);

    InsertIntoParent(parent_ptr, new_parent_ptr->array_[0].first,
                     new_parent_ptr);

    buffer_pool_manager_->UnpinPage(new_parent_ptr->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_page_id, true);
};

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: 1 means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BTree::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRoot()) {
    return AdjustRoot(node, transaction);
  }

  page_id_t parent_page_id = node->GetParentPageId();
  page_id_t prev_page_id = INVALID_PAGE_ID;
  page_id_t next_page_id = INVALID_PAGE_ID;

  NodeRAII *parent_node_raii =
      new NodeRAII(buffer_pool_manager_, parent_page_id);
  NodeRAII *prev_node_raii = NULL;
  NodeRAII *next_node_raii = NULL;
  N *prev_node;
  N *next_node;
  bool ret = false;

  InnerNode *parent_ptr =
      reinterpret_cast<InnerNode *>(parent_node_raii->GetNode());
  // parent_node always is dirty, set dirty flag;
  // "node" is set dirty automataly, leaf node is set dirty in remove function;
  // inner node is set by "parent_node".
  parent_node_raii->SetDirty(true);

  int node_index = parent_ptr->ValueIndex(node->GetPageId());
  if (node_index > 0) {
    // 1. find the left sibling node to borrow a key
    // if node_index==0 the node is the first son of parents, it has no left
    // sibling node
    prev_page_id = parent_ptr->array_[node_index - 1].second;
    prev_node_raii = new NodeRAII(buffer_pool_manager_, prev_page_id);
    prev_node = reinterpret_cast<N *>(prev_node_raii->GetNode());

    if (prev_node->GetSize() > prev_node->GetMinSize()) {
      Redistribute(prev_node, node, 1);
      prev_node_raii->SetDirty(true);
      goto GC;
    }
  } else if (node_index != parent_ptr->GetSize() - 1) {
    // 2. find the right sibling node to borrow a key
    // also ensures has the right sibliing node
    next_page_id = parent_ptr->array_[node_index + 1].second;
    next_node_raii = new NodeRAII(buffer_pool_manager_, next_page_id);
    next_node = reinterpret_cast<N *>(next_node_raii->GetNode());

    if (next_node->GetSize() > next_node->GetMinSize()) {
      Redistribute(next_node, node, 0);
      next_node_raii->SetDirty(true);
      goto GC;
    }
  }

  if (prev_page_id != INVALID_PAGE_ID) {
    // 3. cannot borrow keys from sibling nodes,
    // has to merge the node to its left sibling nodes
    prev_node = reinterpret_cast<N *>(prev_node_raii->GetNode());
    ret = Coalesce(&prev_node, &node, &parent_ptr, node_index, transaction);
    prev_node_raii->SetDirty(true);
    goto GC;
  }

  // 4. cannot borrow keys from sibling nodes,
  // and the left sibling does not exists
  // has to merge its right sibling nodes to the node
  // prev_page_id == INVALID_PAGE_ID,
  // next node will be delete, no need for set
  ret = Coalesce(&node, &next_node, &parent_ptr, node_index + 1, transaction);
GC:
  if (parent_node_raii != nullptr) delete parent_node_raii;
  if (prev_node_raii != nullptr) delete prev_node_raii;
  if (next_node_raii != nullptr) delete next_node_raii;
  return ret;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 *
 * the node is always to be deleted and all of its pairs is merged to
 * neighbor_node
 *
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  1 means parent node should be deleted, false means no deletion
 * happend
 */
template <typename N>
bool BTree::Coalesce(N **neighbor_node, N **node, InnerNode **parent, int index,
                     Transaction *transaction) {
  if ((*node)->IsLeaf()) {  // NOLINT
    LeafNode *op_node = reinterpret_cast<LeafNode *>(*node);
    LeafNode *op_neighbor_node = reinterpret_cast<LeafNode *>(*neighbor_node);

    op_node->MoveAllTo(op_neighbor_node);

    transaction->AddIntoDeletedPageSet(op_node->GetPageId());
  } else {
    InnerNode *op_node = reinterpret_cast<InnerNode *>(*node);
    InnerNode *op_neighbor_node = reinterpret_cast<InnerNode *>(*neighbor_node);

    KeyType middle_key = (*parent)->array_[index].first;
    op_node->MoveAllTo(op_neighbor_node, middle_key, buffer_pool_manager_);

    transaction->AddIntoDeletedPageSet(op_node->GetPageId());
  }

  (*parent)->Remove(index);  // NOLINT
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
template <typename N>
bool BTree::Redistribute(N *neighbor_node, N *node, int index) {
  NodeRAII parent_node_raii =
      NodeRAII(buffer_pool_manager_, node->GetParentPageId());
  InnerNode *parent_ptr =
      reinterpret_cast<InnerNode *>(parent_node_raii.GetNode());

  if (node->IsLeaf()) {
    LeafNode *op_node = reinterpret_cast<LeafNode *>(node);
    LeafNode *op_neighbor_node = reinterpret_cast<LeafNode *>(neighbor_node);

    if (index == 0) {
      op_neighbor_node->MoveFirstToEndOf(op_node);

      int node_index = parent_ptr->ValueIndex(op_neighbor_node->GetPageId());
      parent_ptr->array_[node_index].first = op_neighbor_node->KeyAt(0);
    } else {
      op_neighbor_node->MoveLastToFrontOf(op_node);

      int node_index = parent_ptr->ValueIndex(op_node->GetPageId());
      // update the parent's key, index ==1, node is on the right of the two
      // sibling nodes
      parent_ptr->array_[node_index].first = op_node->KeyAt(0);
    }
  } else {
    InnerNode *op_node = reinterpret_cast<InnerNode *>(node);
    InnerNode *op_neighbor_node = reinterpret_cast<InnerNode *>(neighbor_node);

    if (index == 0) {
      int node_index = parent_ptr->ValueIndex(op_neighbor_node->GetPageId());
      KeyType middle_key = parent_ptr->array_[node_index].first;
      KeyType next_middle_key = op_neighbor_node->array_[1].first;

      op_neighbor_node->MoveFirstToEndOf(op_node, middle_key,
                                         buffer_pool_manager_);
      parent_ptr->array_[node_index].first = next_middle_key;
    } else {
      int node_index = parent_ptr->ValueIndex(op_node->GetPageId());
      KeyType middle_key = parent_ptr->array_[node_index].first;
      KeyType next_middle_key =
          op_neighbor_node->array_[op_neighbor_node->GetSize() - 1].first;

      op_neighbor_node->MoveLastToFrontOf(op_node, middle_key,
                                          buffer_pool_manager_);
      parent_ptr->array_[node_index].first = next_middle_key;
    }
  }
  parent_node_raii.SetDirty(true);
  return true;
};

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : 1 means root page should be deleted, false means no deletion
 * happend
 */
bool BTree::AdjustRoot(Node *old_root_node, Transaction *transaction) {
  if (old_root_node->GetSize() > 1) {
    return false;
  }

  page_id_t new_root_id;
  if (old_root_node->IsLeaf()) {
    if (old_root_node->GetSize() == 1) {
      return false;
    }
    new_root_id = INVALID_PAGE_ID;
  } else {
    InnerNode *old_root_internal_node =
        reinterpret_cast<InnerNode *>(old_root_node);
    new_root_id = old_root_internal_node->RemoveAndReturnOnlyChild();

    NodeRAII new_root_raii(buffer_pool_manager_, new_root_id);
    InnerNode *new_root_ptr =
        reinterpret_cast<InnerNode *>(new_root_raii.GetNode());

    new_root_ptr->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    transaction->AddIntoDeletedPageSet(old_root_node->GetPageId());
  }

  root_page_id_ = new_root_id;
  // UpdateRootPageId(0);

  return true;
}

/*
 * When the child node is safe,or the whole operation is done, call this
 * function to release the page latch `page_ptr` == nullptr means latch on
 * `root_page_id`
 * 1) For read(mode == 0): release the read latch For delete(mode ==
 * 2) and inset(mode == 1): release the write latch
 * 3) For value update(mode == 3):
 * release the read latch for internal node and release write latch for leaf
 * node (not implemented yet)
 *
 * In this function we will ALWAYS Unpin(page_id, false), is's other functions'
 * duty to SetDirty()
 */
void BTree::ReleaseLatchQueue(Transaction *transaction, LatchMode mode) {
  std::shared_ptr<std::deque<Page *>> deque_ptr = transaction->GetPageSet();
  while (!deque_ptr->empty()) {
    Page *page_ptr = deque_ptr->front();
    deque_ptr->pop_front();

    if (page_ptr == nullptr) {
      if (mode == LATCH_MODE_WRITE || mode == LATCH_MODE_DELETE) {
        root_id_latch_.WUnlock();
      } else {
        // read and update operation
        root_id_latch_.RUnlock();
      }
    } else {
      page_id_t page_id = page_ptr->GetPageId();
      if (mode == LATCH_MODE_WRITE || mode == LATCH_MODE_DELETE) {
        page_ptr->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, false);
      } else if (mode == LATCH_MODE_READ || mode == LATCH_MODE_UPDATE) {
        page_ptr->RUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, false);
      } else {
        FATAL_PRINT("Not Supported LatchMode\n");
      }
    }
  }
}

bool BTree::CheckSafe(Node *tree_ptr, LatchMode mode) {
  if (mode == LATCH_MODE_READ || mode == LATCH_MODE_UPDATE) {
    return true;
  }
  if (mode == LATCH_MODE_WRITE) {
    /*
    - insert: return safe when
    - leafpage size < maxsize - 1
    - internalpage size < maxsize
     */
    if (tree_ptr->IsLeaf() &&
        (tree_ptr->GetSize() < tree_ptr->GetMaxSize() - 1)) {
      return true;
    } else if (!tree_ptr->IsLeaf() &&
               (tree_ptr->GetSize() < tree_ptr->GetMaxSize())) {
      return true;
    }
  }

  // Root page is different!
  //  return safe when delete size > minsize
  if (mode == LATCH_MODE_DELETE) {
    if (tree_ptr->IsRoot() && tree_ptr->IsLeaf() && tree_ptr->GetSize() > 1) {
      return true;
    }
    if (tree_ptr->IsRoot() && !tree_ptr->IsLeaf() && tree_ptr->GetSize() > 2) {
      return true;
    }
    if (!tree_ptr->IsRoot() && tree_ptr->GetSize() > tree_ptr->GetMinSize()) {
      return true;
    }
  }

  return false;
}

void BTree::DeletePages(Transaction *transaction) {
  const auto pages = transaction->GetDeletedPageSet();
  for (const auto &page_id : *pages) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  pages->clear();
};

void BTree::SetRootPageId(page_id_t root_page_id) {
  root_page_id_ = root_page_id;
};
/*
 * Helper function to decide whether current b+tree is empty
 */
bool BTree::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
void BTree::ToGraph(std::ofstream &out) const {
  if (IsEmpty()) {
    return;
  }
  InnerNode *root_node = reinterpret_cast<InnerNode *>(
      buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  if (root_node->IsLeaf()) {
    auto n = reinterpret_cast<LeafNode *>(root_node);
    n->ToGraph(out);
  } else {
    auto n = reinterpret_cast<InnerNode *>(root_node);
    n->ToGraph(out, buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
};

void BTree::ToString() const {
  Node *root_node = reinterpret_cast<Node *>(
      buffer_pool_manager_->FetchPage(root_page_id_)->GetData());

  if (root_node->IsLeaf()) {
    auto n = reinterpret_cast<LeafNode *>(root_node);
    n->ToString();
  } else {
    auto n = reinterpret_cast<InnerNode *>(root_node);
    n->ToString(buffer_pool_manager_);
  }
};

void BTree::Draw(std::string path) const {
  std::ofstream out(path, std::ofstream::trunc);
  assert(!out.fail());
  out << "digraph G {" << std::endl;
  ToGraph(out);
  out << "}" << std::endl;
  out.close();
  // ToString();
  std::cout << path << " dot file flushed now" << std::endl;
};

int BTree::GetHeight() const {
  int height = 0;
  if (root_page_id_ != INVALID_PAGE_ID) {
    NodeRAII root_raii(buffer_pool_manager_, root_page_id_);
    Node *root = reinterpret_cast<Node *>(root_raii.GetNode());

    InnerNode *innerNode = reinterpret_cast<InnerNode *>(root);
    height++;

    while (innerNode->IsLeaf() == false) {
      NodeRAII child_raii(buffer_pool_manager_, innerNode->array_[0].second);
      Node *child = reinterpret_cast<Node *>(child_raii.GetNode());
      innerNode = reinterpret_cast<InnerNode *>(child);
      height++;
    }
  }
  return height;
}

void BTree::GetNodeNums() const {
  u64 innerNodeCount = 0;
  u64 leafNodeCount = 0;
  double avgInnerNodeKeys = 0;
  double avgLeafNodeKeys = 0;
  u32 height = 0;

  if (root_page_id_ != INVALID_PAGE_ID) {
    std::stack<std::pair<page_id_t, u32>> nodeStack;
    nodeStack.push({root_page_id_, 1});

    while (!nodeStack.empty()) {
      NodeRAII node_raii(buffer_pool_manager_, nodeStack.top().first);
      Node *node = reinterpret_cast<Node *>(node_raii.GetNode());
      u32 depth = nodeStack.top().second;
      nodeStack.pop();

      height = std::max(height, depth);

      if (node->IsLeaf()) {
        LeafNode *leafNode = reinterpret_cast<LeafNode *>(node);
        leafNodeCount++;
        avgLeafNodeKeys += leafNode->GetSize();
      } else {
        InnerNode *innerNode = reinterpret_cast<InnerNode *>(node);
        innerNodeCount++;
        avgInnerNodeKeys += innerNode->GetSize();

        for (int i = 0; i < innerNode->GetSize(); i++) {
          nodeStack.push({innerNode->array_[i].second, depth + 1});
        }
      }
    }
  }

  if (innerNodeCount > 0) {
    avgInnerNodeKeys /= innerNodeCount;
  }

  if (leafNodeCount > 0) {
    avgLeafNodeKeys /= leafNodeCount;
  }
  printf(
      "Tree Height: %2u InnerNodeCount: %4lu LeafNodeCount: %6lu Avg Inner "
      "Node "
      "pairs: %3.1lf "
      "Avg Leaf Node pairs %3.1lf\n",
      height, innerNodeCount, leafNodeCount, avgInnerNodeKeys, avgLeafNodeKeys);
}

}  // namespace BTree
