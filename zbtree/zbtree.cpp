/*
 * BTreeOLC_child_layout.h - This file contains a modified version that
 *                           uses the key-value pair layout
 *
 * We use this to test whether child node layout will affect performance
 */

#include "zbtree.h"

namespace btreeolc {
BTreeLeaf::BTreeLeaf() { Init(); }

bool BTreeLeaf::isFull() { return count == LeafNodeMaxEntries; };

unsigned BTreeLeaf::lowerBound(Key k) {
  unsigned lower = 0;
  unsigned upper = count;
  do {
    unsigned mid = ((upper - lower) / 2) + lower;
    // This is the key at the pivot position
    const Key& middle_key = data[mid].first;

    if (k < middle_key) {
      upper = mid;
    } else if (k > middle_key) {
      lower = mid + 1;
    } else {
      return mid;
    }
  } while (lower < upper);
  return lower;
}

/**
 * @brief
 *
 * @param k
 * @param p
 * @return true insert
 * @return false  update
 */
bool BTreeLeaf::insert(Key k, Value p) {
  assert(count < LeafNodeMaxEntries);
  if (count) {
    unsigned pos = lowerBound(k);
    if ((pos < count) && (data[pos].first == k)) {
      // Upsert
      data[pos].second = p;
      return false;
    }
    memmove(data + pos + 1, data + pos, sizeof(KeyValueType) * (count - pos));
    // memmove(payloads+pos+1,payloads+pos,sizeof(Payload)*(count-pos));
    data[pos].first = k;
    data[pos].second = p;
  } else {
    data[0].first = k;
    data[0].second = p;
  }
  count++;
  return true;
}

void BTreeLeaf::Init(uint16_t num, page_id_t id) {
  count = num;
  page_id = id;
  type = typeMarker;
  data = NULL;
}

BTreeLeaf* BTreeLeaf::split(Key& sep, ParallelBufferPoolManager* bpm) {
  BTreeLeaf* newLeaf = new BTreeLeaf();
  page_id_t new_page_id;
  NodeRAII new_page(bpm, &new_page_id);

  new_page.SetDirty(true);
  new_page.SetLeafPtr(reinterpret_cast<void*>(newLeaf));

  newLeaf->Init(count - (count / 2), new_page_id);
  newLeaf->data = reinterpret_cast<KeyValueType*>(new_page.GetNode());

  count = count - newLeaf->count;
  memcpy(newLeaf->data, data + count, sizeof(KeyValueType) * newLeaf->count);
  sep = data[count - 1].first;
  return newLeaf;
}

void BTreeLeaf::Print(ParallelBufferPoolManager* bpm) {
  INFO_PRINT("[LeafNode page_id:%lu addr:%p count: %3u ", this->page_id, this,
             this->count);
  NodeRAII node(bpm, this->page_id);
  this->data = reinterpret_cast<KeyValueType*>(node.GetNode());
  for (int i = 0; i < count; i++) {
    INFO_PRINT(" %lu->%lu ", reinterpret_cast<u64>(this->data[i].first),
               reinterpret_cast<u64>(this->data[i].second))
  }
  INFO_PRINT("]\n");
}

void BTreeLeaf::ToGraph(std::ofstream& out, ParallelBufferPoolManager* bpm) {
  std::string leaf_prefix("LEAF_");
  // Print node name
  out << leaf_prefix << this;
  // Print node properties
  out << "[shape=plain color=green ";
  // Print data of the node
  out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
         "CELLPADDING=\"4\">\n";
  // Print data
  out << "<TR><TD COLSPAN=\"" << this->count << "\">P=" << this->page_id
      << " addr:" << this << "</TD></TR>\n";
  out << "<TR><TD COLSPAN=\"" << this->count << "\">"
      << "max_size=" << LeafNodeMaxEntries
      << ",min_size=" << LeafNodeMaxEntries / 2 << ",size=" << this->count
      << "</TD></TR>\n";
  out << "<TR>";

  NodeRAII node(bpm, this->page_id);
  this->data = reinterpret_cast<KeyValueType*>(node.GetNode());
  for (int i = 0; i < this->count; i++) {
    out << "<TD>" << this->data[i].first << "</TD>\n";
  }
  out << "</TR>";
  // Print table end
  out << "</TABLE>>];\n";
};

BTreeInner::BTreeInner() {
  count = 0;
  type = typeMarker;
}

// ensure correct when maxEntries is even: 3,5
bool BTreeInner::isFull() { return count == (InnerNodeMaxEntries + 1); };

unsigned BTreeInner::lowerBoundBF(Key k) {
  auto base = keys;
  unsigned n = count;
  while (n > 1) {
    const unsigned half = n / 2;
    base = (base[half] < k) ? (base + half) : base;
    n -= half;
  }
  return (*base < k) + base - keys;
}

unsigned BTreeInner::lowerBound(Key k) {
  unsigned lower = 0;
  unsigned upper = count;
  do {
    unsigned mid = ((upper - lower) / 2) + lower;
    if (k < keys[mid]) {
      upper = mid;
    } else if (k > keys[mid]) {
      lower = mid + 1;
    } else {
      return mid;
    }
  } while (lower < upper);
  return lower;
}

BTreeInner* BTreeInner::split(Key& sep) {
  BTreeInner* newInner = new BTreeInner();
  newInner->count = count - (count / 2);
  count = count - newInner->count - 1;
  sep = keys[count];
  memcpy(newInner->keys, keys + count + 1, sizeof(Key) * (newInner->count + 1));
  memcpy(newInner->children, children + count + 1,
         sizeof(NodeBase*) * (newInner->count + 1));
  return newInner;
}

void BTreeInner::insert(Key k, NodeBase* child) {
  // assert(count <= InnerNodeMaxEntries);
  unsigned pos = lowerBound(k);
  memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
  memmove(children + pos + 1, children + pos,
          sizeof(NodeBase*) * (count - pos + 1));
  keys[pos] = k;
  children[pos] = child;
  std::swap(children[pos], children[pos + 1]);
  count++;
}

void BTreeInner::Print(ParallelBufferPoolManager* bpm) {
  INFO_PRINT("[Internal Page: %4p count: %3u ", this, this->count);
  for (int i = 0; i < this->count; i++) {
    INFO_PRINT(" %lu -> %4p", this->keys[i], this->children[i]);
  }
  INFO_PRINT("]\n");
  for (int i = 0; i <= this->count; i++) {
    NodeBase* child_page = this->children[i];

    if (child_page->type == PageType::BTreeLeaf) {
      auto n = reinterpret_cast<BTreeLeaf*>(child_page);
      n->Print(bpm);
    } else {
      auto n = reinterpret_cast<BTreeInner*>(child_page);
      n->Print(bpm);
    }
  }
}

void BTreeInner::ToGraph(std::ofstream& out, ParallelBufferPoolManager* bpm) {
  std::string internal_prefix("INT_");
  std::string leaf_prefix("LEAF_");
  // Print node name
  out << internal_prefix << this;
  // Print node properties
  out << "[shape=plain color=pink ";
  // Print data of the node
  out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
         "CELLPADDING=\"4\">\n";
  // Print data
  out << "<TR><TD COLSPAN=\"" << this->count + 1 << "\">P=" << this
      << "</TD></TR>\n";
  out << "<TR><TD COLSPAN=\"" << this->count + 1 << "\">"
      << "max_size=" << InnerNodeMaxEntries
      << ",min_size=" << InnerNodeMaxEntries / 2 << ",size=" << this->count
      << "</TD></TR>\n";
  out << "<TR>";

  for (int i = 0; i <= this->count; i++) {
    out << "<TD PORT=\"p" << this->children[i] << "\">";
    if (i != this->count) {
      out << this->keys[i];
    } else {
      out << " ";
    }
    out << "</TD>\n";
  }
  out << "</TR>";
  // Print table end
  out << "</TABLE>>];\n";

  // Print Parent link
  for (int i = 0; i <= this->count; i++) {
    NodeBase* child_node = this->children[i];
    if (child_node->type == PageType::BTreeLeaf) {
      out << internal_prefix << this << ":p" << child_node << " -> "
          << leaf_prefix << child_node << ";\n";
    } else {
      out << internal_prefix << this << ":p" << child_node << " -> "
          << internal_prefix << child_node << ";\n";
    }
  }

  // Print leaves
  for (int i = 0; i <= this->count; i++) {
    NodeBase* child_node = this->children[i];

    if (child_node->type == PageType::BTreeLeaf) {
      auto n = reinterpret_cast<BTreeLeaf*>(child_node);
      n->ToGraph(out, bpm);
    } else {
      auto n = reinterpret_cast<BTreeInner*>(child_node);
      n->ToGraph(out, bpm);
    }

    if (i > 0) {
      auto sibling_page = this->children[i - 1];
      if (sibling_page->type != PageType::BTreeLeaf &&
          child_node->type != PageType::BTreeLeaf) {
        out << "{rank=same " << internal_prefix << sibling_page << " "
            << internal_prefix << child_node << "};\n";
      }
    }
  }
};

BTree::BTree(ParallelBufferPoolManager* buffer) {
  bpm = buffer;
  auto tem = new BTreeLeaf();
  root.store(tem, std::memory_order_release);

  page_id_t new_page_id;
  NodeRAII new_page(bpm, &new_page_id);
  new_page.SetDirty(true);
  new_page.SetLeafPtr(reinterpret_cast<void*>(tem));

  auto leaf = reinterpret_cast<BTreeLeaf*>(root.load());
  leaf->Init(0, new_page_id);

  // INFO_PRINT("BTree::BTree() root_page_id = %u\n", new_page_id);
  INFO_PRINT(
      "Nodebase size = %lu leaf node size = %lu entries = %lu inner "
      "node size = %lu  entries = %lu \n",
      sizeof(NodeBase), sizeof(BTreeLeaf), LeafNodeMaxEntries,
      sizeof(BTreeInner), InnerNodeMaxEntries);
}

BTree::~BTree() {
  GetNodeNums();
  // Print();
  if (bpm) {
    delete bpm;
    bpm = nullptr;
  }
}

bool BTree::IsEmpty() const { return root.load() == nullptr; }

void BTree::makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild) {
  auto inner = new BTreeInner();
  inner->count = 1;
  inner->keys[0] = k;
  inner->children[0] = leftChild;
  inner->children[1] = rightChild;
  // INFO_PRINT("old root = %p new root = %p\n", root.load(), inner);
  root.store(inner, std::memory_order_release);
}

void BTree::yield(int count) {
  if (count > 3)
    sched_yield();
  else
    _mm_pause();
}

bool BTree::Insert(Key k, Value v) {
  int restartCount = 0;
restart:
  if (restartCount++) yield(restartCount);
  bool needRestart = false;

  // Current node
  NodeBase* node = root.load();
  uint64_t versionNode = node->readLockOrRestart(needRestart);
  if (needRestart || (node != root)) goto restart;

  // Parent of current node
  BTreeInner* parent = nullptr;
  uint64_t versionParent;

  while (node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner*>(node);

    // Split eagerly if full
    if (inner->isFull()) {
      // Lock
      if (parent) {
        parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
        if (needRestart) goto restart;
      }
      node->upgradeToWriteLockOrRestart(versionNode, needRestart);
      if (needRestart) {
        if (parent) parent->writeUnlock();
        goto restart;
      }
      if (!parent && (node != root)) {  // there's a new parent
        node->writeUnlock();
        goto restart;
      }
      // Split
      Key sep;
      BTreeInner* newInner = inner->split(sep);
      if (parent)
        parent->insert(sep, newInner);
      else
        makeRoot(sep, inner, newInner);
      // Unlock and restart
      node->writeUnlock();
      if (parent) parent->writeUnlock();
      goto restart;
    }

    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) goto restart;
    }

    parent = inner;
    versionParent = versionNode;

    node = inner->children[inner->lowerBound(k)];
    inner->checkOrRestart(versionNode, needRestart);
    if (needRestart) goto restart;
    versionNode = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;
  }

  auto leaf = static_cast<BTreeLeaf*>(node);

  // Split leaf if full
  if (leaf->isFull()) {
    // Lock
    if (parent) {
      parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
      if (needRestart) goto restart;
    }
    node->upgradeToWriteLockOrRestart(versionNode, needRestart);
    if (needRestart) {
      if (parent) parent->writeUnlock();
      goto restart;
    }
    if (!parent && (node != root)) {  // there's a new parent
      node->writeUnlock();
      goto restart;
    }
    // Split
    Key sep;
    BTreeLeaf* newLeaf;
    {
      NodeRAII leaf_page(bpm, leaf->page_id);
      leaf_page.SetDirty(true);
      leaf->data = reinterpret_cast<KeyValueType*>(leaf_page.GetNode());
      newLeaf = leaf->split(sep, bpm);
    }
    if (parent)
      parent->insert(sep, newLeaf);
    else
      makeRoot(sep, leaf, newLeaf);
    // Unlock and restart
    node->writeUnlock();
    if (parent) parent->writeUnlock();
    goto restart;
  } else {
    // only lock leaf node
    node->upgradeToWriteLockOrRestart(versionNode, needRestart);
    if (needRestart) goto restart;
    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        node->writeUnlock();
        goto restart;
      }
    }
    NodeRAII leaf_page(bpm, leaf->page_id);
    leaf_page.SetDirty(true);
    leaf->data = reinterpret_cast<KeyValueType*>(leaf_page.GetNode());
    auto ret = leaf->insert(k, v);
    node->writeUnlock();
    return ret;
  }
  return false;
}

bool BTree::Get(Key k, Value& result) {
  int restartCount = 0;
restart:
  if (restartCount++) yield(restartCount);
  bool needRestart = false;

  NodeBase* node = root.load();
  uint64_t versionNode = node->readLockOrRestart(needRestart);
  if (needRestart || (node != root)) goto restart;

  // Parent of current node
  BTreeInner* parent = nullptr;
  uint64_t versionParent;

  while (node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner*>(node);

    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) goto restart;
    }

    parent = inner;
    versionParent = versionNode;

    node = inner->children[inner->lowerBound(k)];
    inner->checkOrRestart(versionNode, needRestart);
    if (needRestart) goto restart;
    versionNode = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;
  }

  // make sure leaf_page auto exit
  bool success;
  {
    BTreeLeaf* leaf = static_cast<BTreeLeaf*>(node);
    NodeRAII leaf_page(bpm, leaf->page_id);
    leaf->data = reinterpret_cast<KeyValueType*>(leaf_page.GetNode());
    unsigned pos = leaf->lowerBound(k);

    if ((pos < leaf->count) && (leaf->data[pos].first == k)) {
      success = true;
      result = leaf->data[pos].second;
    }
  }

  if (parent) {
    parent->readUnlockOrRestart(versionParent, needRestart);
    if (needRestart) goto restart;
  }
  node->readUnlockOrRestart(versionNode, needRestart);
  if (needRestart) goto restart;

  return success;
}

uint64_t BTree::Scan(Key k, int range, Value* output) {
  int restartCount = 0;
restart:
  if (restartCount++) yield(restartCount);
  bool needRestart = false;

  NodeBase* node = root;
  uint64_t versionNode = node->readLockOrRestart(needRestart);
  if (needRestart || (node != root)) goto restart;

  // Parent of current node
  BTreeInner* parent = nullptr;
  uint64_t versionParent;

  while (node->type == PageType::BTreeInner) {
    auto inner = static_cast<BTreeInner*>(node);

    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) goto restart;
    }

    parent = inner;
    versionParent = versionNode;

    node = inner->children[inner->lowerBound(k)];
    inner->checkOrRestart(versionNode, needRestart);
    if (needRestart) goto restart;
    versionNode = node->readLockOrRestart(needRestart);
    if (needRestart) goto restart;
  }

  BTreeLeaf* leaf = static_cast<BTreeLeaf*>(node);
  unsigned pos = leaf->lowerBound(k);
  int count = 0;
  for (unsigned i = pos; i < leaf->count; i++) {
    if (count == range) break;
    output[count++] = leaf->data[i].second;
  }

  if (parent) {
    parent->readUnlockOrRestart(versionParent, needRestart);
    if (needRestart) goto restart;
  }
  node->readUnlockOrRestart(versionNode, needRestart);
  if (needRestart) goto restart;

  return count;
}

void BTree::Print() const {
  if (root.load() == nullptr) {
    return;
  }

  NodeBase* node = root.load();
  if (node->type == PageType::BTreeInner) {
    auto tmp = reinterpret_cast<BTreeInner*>(node);
    tmp->Print(bpm);
  } else {
    auto tmp = reinterpret_cast<BTreeLeaf*>(node);
    tmp->Print(bpm);
  }
}

void BTree::GetNodeNums() const {
  u64 innerNodeCount = 0;
  u64 leafNodeCount = 0;
  double avgInnerNodeKeys = 0;
  double avgLeafNodeKeys = 0;
  u32 height = 0;

  if (root.load() == nullptr) {
    INFO_PRINT("Root node is Null\n");
    return;
  }

  std::stack<std::pair<NodeBase*, u32>> nodeStack;
  nodeStack.push({root.load(), 1});

  while (!nodeStack.empty()) {
    NodeBase* node = nodeStack.top().first;
    u32 depth = nodeStack.top().second;
    nodeStack.pop();

    height = std::max(height, depth);

    if (node->type == PageType::BTreeLeaf) {
      BTreeLeaf* leafNode = reinterpret_cast<BTreeLeaf*>(node);
      leafNodeCount++;
      avgLeafNodeKeys += leafNode->count;
    } else {
      BTreeInner* innerNode = reinterpret_cast<BTreeInner*>(node);
      innerNodeCount++;
      avgInnerNodeKeys += innerNode->count;

      // count begin from zero and the ptr is always one more than the count
      for (int i = 0; i <= innerNode->count; i++) {
        nodeStack.push({innerNode->children[i], depth + 1});
      }
    }
  }

  if (innerNodeCount > 0) {
    avgInnerNodeKeys /= innerNodeCount;
  }

  if (leafNodeCount > 0) {
    avgLeafNodeKeys /= leafNodeCount;
  }
  INFO_PRINT(
      "Tree Height: %2u InnerNodeCount: %4lu LeafNodeCount: %6lu Avg Inner "
      "Node "
      "pairs: %3.1lf "
      "Avg Leaf Node pairs %3.1lf\n",
      height, innerNodeCount, leafNodeCount, avgInnerNodeKeys, avgLeafNodeKeys);
}

void BTree::ToGraph(std::ofstream& out) const {
  if (IsEmpty()) {
    return;
  }
  NodeBase* root_node = root.load();

  if (root_node->type == PageType::BTreeLeaf) {
    auto n = reinterpret_cast<BTreeLeaf*>(root_node);
    n->ToGraph(out, bpm);
  } else {
    auto n = reinterpret_cast<BTreeInner*>(root_node);
    n->ToGraph(out, bpm);
  }
};

void BTree::Draw(std::string path) const {
  std::ofstream out(path, std::ofstream::trunc);
  assert(!out.fail());
  out << "digraph G {" << std::endl;
  ToGraph(out);
  out << "}" << std::endl;
  out.close();
  INFO_PRINT("%s dot file flushed now\n", path.c_str());
};

}  // namespace btreeolc