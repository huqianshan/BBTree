#include "btree.h"

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
 * Helper methods to set lsn
 */
// void Node::SetLSN(lsn_t lsn) { lsn_ = lsn; }
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
  // InsertAt(index + 1, new_key, new_value);
  /*  int len = GetSize();
   int index = 0;
   for (int i = 0; i < len; i++) {
     if (array_[i].second == old_value) {
       index = i;
     }
   } */
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
  // recipient->CopyNFrom(array_ + half_size, raw_size - half_size,
  //  buffer_pool_manager);

  PairType *items = array_;
  for (int i = half_size; i < raw_size; i++) {
    // CopyLastFrom(items[i], buffer_pool_manager);
    // 1. set the paraent of items'child  to me
    page_id_t child_page_id = items[i].second;
    Page *child_page_ptr = buffer_pool_manager->FetchPage(child_page_id);
    Node *node = reinterpret_cast<Node *>(child_page_ptr->GetData());
    node->SetParentPageId(recipient->GetPageId());
    // 2. flush to disk
    buffer_pool_manager->UnpinPage(child_page_id, true);
    // 3. copy n item
    recipient->InsertAt(recipient->GetSize(), items[i].first, items[i].second);
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
  this->array_[0].first = middle_key;
  for (int i = 0; i < move_size; i++) {
    recipient->array_[begin + i] = this->array_[i];

    page_id_t child_page_id = this->array_[i].second;
    Page *child_page_ptr = buffer_pool_manager->FetchPage(child_page_id);
    Node *child_node = reinterpret_cast<Node *>(child_page_ptr->GetData());

    child_node->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(this->array_[i].second, true);
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
  page_id_t child_page_id = array_[0].second;
  Page *child_page_ptr = buffer_pool_manager->FetchPage(child_page_id);
  Node *node = reinterpret_cast<Node *>(child_page_ptr->GetData());
  node->SetParentPageId(this->GetPageId());
  // 2. flush to disk
  buffer_pool_manager->UnpinPage(child_page_id, true);
  // 3. add to the recip-> last
  recipient->array_[recipient->GetSize()] = {middle_key, child_page_id};
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
  page_id_t child_page_id = array_[GetSize() - 1].second;
  Page *child_page_ptr = buffer_pool_manager->FetchPage(child_page_id);
  Node *node = reinterpret_cast<Node *>(child_page_ptr->GetData());
  node->SetParentPageId(recipient->GetPageId());

  int size = recipient->GetSize();
  int index = 0;
  for (int i = size - 1; i >= index; --i) {
    recipient->array_[i + 1] = recipient->array_[i];
  }
  recipient->array_[index] = {middle_key, child_page_id};
  recipient->IncreaseSize(1);

  recipient->array_[1].first = middle_key;
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
  // Remove(0);
  // array_[0] = array_[1];
  IncreaseSize(-1);
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
  if (size == 0) {
    array_[0] = {key, value};
    IncreaseSize(1);
    return 1;
  }

  int index = KeyIndex(key);
  if (array_[index].first == key) {
    return size;
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
  IncreaseSize(-1);
}

void LeafNode::MoveHalfTo(LeafNode *recipient) {
  int size = GetSize();
  int half = size / 2;
  // memcpy(recipient->array_)
  int begin = size - half;
  for (int i = begin; i < size; ++i) {
    recipient->InsertAt(recipient->GetSize(), array_[i].first,
                        array_[i].second);
  }
  IncreaseSize(-half);
}
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't
 * forget to update the next_page id in the sibling page
 */
void LeafNode::MoveAllTo(LeafNode *recipient) {
  // recipient->SetNextPageId(GetNextPageId());
  recipient->next_page_id_ = this->next_page_id_;
  int move_size = GetSize();
  // memcpy(recipient->array_)
  int begin = recipient->GetSize();
  for (int i = 0; i < move_size; ++i) {
    recipient->InsertAt(begin + i, array_[i].first, array_[i].second);
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

  if (array_[index].first == key) {
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

  if (array_[index].first == key) {
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
  // recipient->CopyFirstFrom(GetItem(size - 1));
  recipient->InsertAt(0, array_[size - 1].first, array_[size - 1].second);
  RemoveAt(size - 1);
};

page_id_t LeafNode::GetNextPageId() const { return next_page_id_; };

void InnerNode::ToGraph(std::ofstream &out,
                        ParallelBufferPoolManager *bpm) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  // Print node name
  out << internal_prefix << this->GetPageId();
  // Print node properties
  out << "[shape=plain color=pink ";  // why not?
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
      n->ToGraph(out, bpm);
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
  }

  bpm->UnpinPage(GetPageId(), false);
}

std::string InnerNode::ToString(ParallelBufferPoolManager *bpm) const {
  std::cout << "Internal Page: " << GetPageId()
            << " parent: " << GetParentPageId() << std::endl;
  for (int i = 0; i < GetSize(); i++) {
    std::cout << array_[i].first << ": " << array_[i].second << ",";
  }
  std::cout << std::endl;
  std::cout << std::endl;
  for (int i = 0; i < GetSize(); i++) {
    auto child_page = reinterpret_cast<Node *>(
        bpm->FetchPage(this->array_[i].second)->GetData());

    if (child_page->IsLeaf()) {
      auto n = reinterpret_cast<LeafNode *>(child_page);
      n->ToString(bpm);
    } else {
      auto n = reinterpret_cast<InnerNode *>(child_page);
      n->ToString(bpm);
    }
  }
  bpm->UnpinPage(GetPageId(), false);
  return {};
}

std::string LeafNode::ToString(ParallelBufferPoolManager *bpm) const {
  std::string res;
  res += "[LeafNode: " + std::to_string(GetPageId()) +
         " parent: " + std::to_string(GetParentPageId()) +
         " next: " + std::to_string(GetNextPageId());
  std::cout << res << std::endl;
  res.clear();
  for (int i = 0; i < GetSize(); i++) {
    res += " " + std::to_string(array_[i].first) + ":" +
           std::to_string(array_[i].second);
  }
  res += "]";
  bpm->UnpinPage(GetPageId(), false);
  std::cout << res << std::endl;
  return res;
}

void LeafNode::ToGraph(std::ofstream &out,
                       ParallelBufferPoolManager *bpm) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  // LeafNode *leaf = reinterpret_cast<LeafNode *>(this);
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

bool BTree::Insert(const KeyType &key, const ValueType &value) {
  bool ret;

  root_id_latch_.WLock();

  if (IsEmpty()) {
    StartNewTree(key, value);
    ret = true;
  } else {
    ret = InsertIntoLeaf(key, value);
  }

  root_id_latch_.WUnlock();
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
  root_id_latch_.WLock();

  if (IsEmpty()) {
    root_id_latch_.WUnlock();
    return false;
  }

  Page *leaf_page_ptr = FindLeafPage(key, false, 2);
  LeafNode *leaf_ptr = reinterpret_cast<LeafNode *>(leaf_page_ptr->GetData());

  // may be unnecessary
  // if (!leaf_ptr->CheckDuplicated(key)) {
  //   root_id_latch_.WUnlock();
  //   return false;
  // }

  ValueType val;
  if (leaf_ptr->Lookup(key, &val) == false) {
    root_id_latch_.WUnlock();
    return false;
  }

  int index = leaf_ptr->KeyIndex(key);
  leaf_ptr->RemoveAt(index);
  bool ret = false;
  if (leaf_ptr->GetSize() < leaf_ptr->GetMinSize()) {
    ret = CoalesceOrRedistribute<LeafNode>(leaf_ptr);
  }

  if (!ret) {
    leaf_page_ptr->SetDirty(true);
  }

  root_id_latch_.WUnlock();
  // ReleaseLatchQueue(transaction, 2);
  // DeletePages(transaction);
  return true;
};

bool BTree::Update(const KeyType &key, const ValueType &value) {
  root_id_latch_.RLock();

  if (IsEmpty()) {
    root_id_latch_.RUnlock();
    return false;
  }

  Page *page_ptr = FindLeafPage(key);
  LeafNode *leaf_ptr = reinterpret_cast<LeafNode *>(page_ptr->GetData());

  bool ret = leaf_ptr->Update(key, value);
  // page_ptr->RUnlatch();
  if (!ret) {
    return false;
  }
  page_ptr->SetDirty(true);
  buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
  return true;
};

bool BTree::Get(const KeyType &key, ValueType *result) {
  root_id_latch_.RLock();

  if (IsEmpty()) {
    root_id_latch_.RUnlock();
    return false;
  }

  Page *page_ptr = FindLeafPage(key);
  LeafNode *leaf_ptr = reinterpret_cast<LeafNode *>(page_ptr->GetData());

  bool ret = leaf_ptr->Lookup(key, result);

  // release the latch first!
  // page_ptr->RUnlatch();
  root_id_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
  if (!ret) {
    return false;
  }
  return true;
};

bool BTree::Scan(const KeyType &key_begin, const KeyType &key_end,
                 std::vector<ValueType> result) {
  return true;
};

/**
 * @brief Support function
 *
 */

Page *BTree::FindLeafPage(const KeyType &key, bool leftMost, int mode) {
  // It's outer function's duty to lock `root_id_latch_`
  if (IsEmpty()) {
    return nullptr;
  }

  page_id_t page_id = root_page_id_;
  page_id_t last_page_id = INVALID_PAGE_ID;
  Page *page_ptr = nullptr;
  Page *last_page_ptr = nullptr;
  Node *tree_ptr;

  while (true) {
    page_ptr = buffer_pool_manager_->FetchPage(page_id);
    VERIFY(page_ptr != nullptr);
    tree_ptr = reinterpret_cast<Node *>(page_ptr->GetData());

    // if (mode == 0) {
    //   // page_ptr->RLatch();
    // } else {
    //   // for write operation
    //   // page_ptr->WLatch();
    // }

    if (last_page_ptr != nullptr) {
      // last_page_ptr->RUnlatch();
      buffer_pool_manager_->UnpinPage(last_page_id, false);
      // } else {
      // root_id_latch_.RUnlock();
    }

    bool is_leaf = tree_ptr->IsLeaf();
    if (is_leaf) {
      break;
    }

    last_page_id = page_id;
    last_page_ptr = page_ptr;

    InnerNode *internal_ptr = reinterpret_cast<InnerNode *>(tree_ptr);
    if (leftMost) {
      page_id = internal_ptr->array_[0].first;
    } else {
      page_id = internal_ptr->Lookup(key);
      // printf("key:%lu find in page_id%d\n", key, page_id);
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
 * keys return false, otherwise return 1.
 */
bool BTree::InsertIntoLeaf(const KeyType &key, const ValueType &value) {
  Page *page_ptr = FindLeafPage(key, false, 1);
  if (page_ptr == nullptr) {
    return false;
  }

  LeafNode *leaf_ptr = reinterpret_cast<LeafNode *>(page_ptr->GetData());

  //  already done de-duplicated in leaf_ptr->Insert
  // if (leaf_ptr->CheckDuplicated(key)) {
  //   buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
  //   return false;
  // }

  int size = leaf_ptr->Insert(key, value);

  if (size == leaf_ptr->GetMaxSize()) {
    LeafNode *new_leaf_ptr = Split<LeafNode>(leaf_ptr);

    leaf_ptr->MoveHalfTo(new_leaf_ptr);
    new_leaf_ptr->SetNextPageId(leaf_ptr->GetNextPageId());
    leaf_ptr->SetNextPageId(new_leaf_ptr->GetPageId());

    // After `MoveHalfTo` middle key is kept in array[0]
    InsertIntoParent(leaf_ptr, new_leaf_ptr->KeyAt(0), new_leaf_ptr);

    buffer_pool_manager_->UnpinPage(new_leaf_ptr->GetPageId(), 1);
  }

  // page_ptr->is_dirty_ = true;
  page_ptr->SetDirty(true);
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
bool BTree::CoalesceOrRedistribute(N *node) {
  if (node->IsRoot()) {
    return AdjustRoot(node);
  }

  page_id_t parent_page_id;
  page_id_t prev_page_id = INVALID_PAGE_ID;
  page_id_t next_page_id = INVALID_PAGE_ID;
  Page *parent_page_ptr;
  Page *prev_page_ptr;
  Page *next_page_ptr;
  InnerNode *parent_ptr;
  N *prev_node;
  N *next_node;

  parent_page_id = node->GetParentPageId();
  parent_page_ptr = SafelyGetFrame(
      parent_page_id, "Out of memory in `CoalesceOrRedistribute`, get parent");
  parent_ptr = reinterpret_cast<InnerNode *>(parent_page_ptr->GetData());

  int node_index = parent_ptr->ValueIndex(node->GetPageId());
  if (node_index > 0) {
    // 1. find the left sibling node to borrow a key
    // if node_index==0 the node is the first son of parents, it has no left
    // sibling node
    prev_page_id = parent_ptr->array_[node_index - 1].second;
    prev_page_ptr = SafelyGetFrame(
        prev_page_id,
        "Out of memory in `CoalesceOrRedistribute`, get prev node");
    prev_node = reinterpret_cast<N *>(prev_page_ptr->GetData());

    if (prev_node->GetSize() > prev_node->GetMinSize()) {
      Redistribute(prev_node, node, 1);

      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      buffer_pool_manager_->UnpinPage(prev_page_id, true);
      return false;
    }
  } else if (node_index != parent_ptr->GetSize() - 1) {
    // 2. find the right sibling node to borrow a key
    // also ensures has the right sibliing node
    next_page_id = parent_ptr->array_[node_index + 1].second;
    next_page_ptr = SafelyGetFrame(
        next_page_id,
        "Out of memory in `CoalesceOrRedistribute`, get next node");
    next_node = reinterpret_cast<N *>(next_page_ptr->GetData());
    if (next_node->GetSize() > next_node->GetMinSize()) {
      Redistribute(next_node, node, 0);

      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      if (node_index > 0) {
        buffer_pool_manager_->UnpinPage(prev_page_id, false);
      }
      buffer_pool_manager_->UnpinPage(next_page_id, true);

      return false;
    }
  }

  bool ret = false;
  if (prev_page_id != INVALID_PAGE_ID) {
    // 3. cannot borrow keys from sibling nodes,
    // has to merge the node to its left sibling nodes
    ret = Coalesce(&prev_node, &node, &parent_ptr, node_index);

    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    // if (ret) {
    //   transaction->AddIntoDeletedPageSet(parent_page_id);
    // }
    buffer_pool_manager_->UnpinPage(prev_page_id, true);
    if (next_page_id != INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(next_page_id, false);
    }

    return true;
  }

  // 4. cannot borrow keys from sibling nodes,
  // and the left sibling does not exists
  // has to merge its right sibling nodes to the node
  // prev_page_id == INVALID_PAGE_ID
  ret = Coalesce(&node, &next_node, &parent_ptr, node_index + 1);
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
  buffer_pool_manager_->UnpinPage(next_page_id, true);
  // transaction->AddIntoDeletedPageSet(next_page_id);
  // if (ret) {
  //   transaction->AddIntoDeletedPageSet(parent_page_id);
  // }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 *
 *  node always the left node and to be delted
 *
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  1 means parent node should be deleted, false means no deletion
 * happend
 */
template <typename N>
bool BTree::Coalesce(N **neighbor_node, N **node, InnerNode **parent,
                     int index) {
  if ((*node)->IsLeaf()) {  // NOLINT
    LeafNode *op_node = reinterpret_cast<LeafNode *>(*node);
    LeafNode *op_neighbor_node = reinterpret_cast<LeafNode *>(*neighbor_node);

    op_node->MoveAllTo(op_neighbor_node);

    buffer_pool_manager_->UnpinPage(op_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(op_node->GetPageId());
  } else {
    InnerNode *op_node = reinterpret_cast<InnerNode *>(*node);
    InnerNode *op_neighbor_node = reinterpret_cast<InnerNode *>(*neighbor_node);

    KeyType middle_key = (*parent)->array_[index].first;
    op_node->MoveAllTo(op_neighbor_node, middle_key, buffer_pool_manager_);

    buffer_pool_manager_->UnpinPage(op_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(op_node->GetPageId());
  }

  (*parent)->Remove(index);  // NOLINT
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent);
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
  page_id_t parent_page_id = node->GetParentPageId();
  Page *parent_page_ptr =
      SafelyGetFrame(parent_page_id, "Out of memory in `Redistribute`");
  InnerNode *parent_ptr =
      reinterpret_cast<InnerNode *>(parent_page_ptr->GetData());

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
      // parent_ptr->SetKeyAt(node_index, next_middle_key);
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

  buffer_pool_manager_->UnpinPage(parent_page_id, true);
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
bool BTree::AdjustRoot(Node *old_root_node) {
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

    Page *new_root_page_ptr =
        SafelyGetFrame(new_root_id, "Out of memory in `AdjustRoot");
    InnerNode *new_root_ptr =
        reinterpret_cast<InnerNode *>(new_root_page_ptr->GetData());
    new_root_ptr->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
  }

  root_page_id_ = new_root_id;
  // UpdateRootPageId(0);

  return true;
}

void BTree::SetRootPageId(page_id_t root_page_id) {
  root_page_id_ = root_page_id;
};
/*
 * Helper function to decide whether current b+tree is empty
 */
bool BTree::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
void BTree::ToGraph(std::ofstream &out, ParallelBufferPoolManager *bpm) const {
  InnerNode *root_node =
      reinterpret_cast<InnerNode *>(bpm->FetchPage(root_page_id_)->GetData());

  if (root_node->IsLeaf()) {
    auto n = reinterpret_cast<LeafNode *>(root_node);
    n->ToGraph(out, bpm);
  } else {
    auto n = reinterpret_cast<InnerNode *>(root_node);
    n->ToGraph(out, bpm);
  }
};

void BTree::ToString(ParallelBufferPoolManager *bpm) const {
  Node *root_node =
      reinterpret_cast<Node *>(bpm->FetchPage(root_page_id_)->GetData());

  if (root_node->IsLeaf()) {
    auto n = reinterpret_cast<LeafNode *>(root_node);
    n->ToString(bpm);
  } else {
    auto n = reinterpret_cast<InnerNode *>(root_node);
    n->ToString(bpm);
  }
};

}  // namespace BTree
