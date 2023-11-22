#include <algorithm>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include "btree.h"
// using namespace BTree;

int main(int argc, char const *argv[]) {
  BTree::DiskManager *disk =
      new BTree::DiskManager("/data/public/hjl/bbtree/hjl.db");
  BTree::ParallelBufferPoolManager *para =
      new BTree::ParallelBufferPoolManager(16, 1024, disk);
  BTree::BTree *btree = new BTree::BTree(para);

  // btree->Insert(key, value);
  std::vector<BTree::KeyType> keys(39);
  std::iota(keys.begin(), keys.end(), 0);
  // std::random_device rd;
  // std::mt19937 g(rd());
  // std::shuffle(keys.begin(), keys.end(), g);

  for (auto key : keys) {
    btree->Insert(key, key * key);
  }

  {
    std::ofstream out("./bbtree-before.dot", std::ofstream::trunc);
    assert(!out.fail());
    out << "digraph G {" << std::endl;
    btree->ToGraph(out, para);
    out << "}" << std::endl;
    out.close();
    btree->ToString(para);
  }

  for (auto key : keys) {
    BTree::ValueType ret = 0;
    btree->Get(key, &ret);
    // std::cout << key << " "
    //           << " " << ret << " " << flag << std::endl;
    assert(key * key == ret);
  }

  btree->Remove(4);
  btree->Remove(3);
  btree->Remove(5);
  // for (auto key : keys) {
  //   btree->Remove(key);
  //   std::cout << key << " ";
  //   if (key >= 0) {
  //     break;
  //   }
  // }

  btree->ToString(para);

  {
    std::ofstream out("./bbtree.dot", std::ofstream::trunc);
    assert(!out.fail());
    out << "digraph G {" << std::endl;
    btree->ToGraph(out, para);
    out << "}" << std::endl;
    out.close();
  }
  /* for (BTree::KeyType begin = 0; begin < 31; begin++) {
    btree->Insert(begin, begin * begin);
  } */

  para->FlushAllPages();

  delete para;
  delete disk;
  delete btree;
  return 0;
}
