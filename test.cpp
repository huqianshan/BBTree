#include <iostream>

#include "btree.h"

int main(int argc, char const *argv[]) {
  BTree::DiskManager *disk =
      new BTree::DiskManager("/data/public/hjl/bbtree/hjl.db");
  BTree::ParallelBufferPoolManager *para =
      new BTree::ParallelBufferPoolManager(16, 1024, disk);
  BTree::BTree *btree = new BTree::BTree(para);

  BTree::KeyType key = 1024;
  BTree::ValueType value = 4021;
  // btree->Insert(key, value);
  for (BTree::KeyType begin = 1; begin < 7; begin++) {
    btree->Insert(begin, begin * begin);
  }
  btree->ToString(para);

  std::ofstream out("./bbtree.dot", std::ofstream::trunc);
  assert(!out.fail());
  out << "digraph G {" << std::endl;
  btree->ToGraph(out, para);
  out << "}" << std::endl;
  out.close();

  BTree::ValueType ret = 0;
  auto flag = btree->Get(key, &ret);

  std::cout << key << " " << value << " " << ret << " " << flag << std::endl;

  delete para;
  delete disk;
  delete btree;
  return 0;
}
