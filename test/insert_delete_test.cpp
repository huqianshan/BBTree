#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include "../btree/btree.h"

namespace BTree {

const u32 INSTANCE_SIZE = 1;
const u32 PAGES_SIZE = 36;
const std::string FILE_NAME = "/data/public/hjl/bbtree/hjl.db";
const std::string DOTFILE_NAME_BEFORE = "./bbtree-before.dot";
const std::string DOTFILE_NAME_AFTER = "./bbtree-after.dot";

// Sequential insert
TEST(BTreeCRUDTest1, InsertSeq1) {
  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  BTree *btree = new BTree(para);

  int key_nums = 1024;
  std::vector<int> keys(key_nums);
  std::iota(keys.begin(), keys.end(), 1);

  for (const auto &key : keys) {
    EXPECT_TRUE(btree->Insert(key, key));
  }

  btree->Draw(DOTFILE_NAME_BEFORE);

  for (const auto &key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, &value));
    EXPECT_EQ(key, value);
  }

  delete para;
  delete disk;
  delete btree;
}

// Random insert
TEST(BTreeCRUDTest1, InsertRadnom2) {
  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  BTree *btree = new BTree(para);

  int key_nums = 1024;
  std::vector<int> keys(key_nums);
  std::iota(keys.begin(), keys.end(), 1);

  // random shuffle
  std::mt19937 g(1024);
  std::shuffle(keys.begin(), keys.end(), g);

  for (const auto &key : keys) {
    EXPECT_TRUE(btree->Insert(key, key));
  }

  btree->Draw(DOTFILE_NAME_BEFORE);

  for (const auto &key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, &value));
    EXPECT_EQ(key, value);
  }

  delete para;
  delete disk;
  delete btree;
}

// Random insert
TEST(BTreeCRUDTest1, InsertDuplicated3) {
  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  BTree *btree = new BTree(para);

  int key_nums = 1024;
  std::vector<int> keys(key_nums);
  std::iota(keys.begin(), keys.end(), 1);

  // random shuffle
  std::mt19937 g(1024);
  std::shuffle(keys.begin(), keys.end(), g);

  for (const auto &key : keys) {
    EXPECT_TRUE(btree->Insert(key, key));
  }

  btree->Draw(DOTFILE_NAME_BEFORE);

  for (const auto &key : keys) {
    ValueType value = -1;
    EXPECT_FALSE(btree->Insert(key, key * key));
    EXPECT_TRUE(btree->Get(key, &value));
    EXPECT_EQ(key, value);
  }

  delete para;
  delete disk;
  delete btree;
}

// sequential insert sequential delete
TEST(BTreeCRUDTest1, DeleteSeqSmall4) {
  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  BTree *btree = new BTree(para);

  int key_nums = 8;
  std::vector<int> keys(key_nums);
  std::iota(keys.begin(), keys.end(), 1);

  for (auto key : keys) {
    btree->Insert(key, key);
  }

  btree->Draw(DOTFILE_NAME_BEFORE);

  for (auto key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, &value));
    EXPECT_EQ(key, value);
  }

  // keys = {2, 7, 1, 3, 8, 6, 5, 4};
  for (auto key : keys) {
    btree->Remove(key);
    ValueType value = -1;
    EXPECT_FALSE(btree->Get(key, &value));
    EXPECT_EQ(value, -1);
  }

  btree->Draw(DOTFILE_NAME_AFTER);

  delete para;
  delete disk;
  delete btree;
}

// sequential insert sequential delete
TEST(BTreeCRUDTest1, DeleteSeqBig5) {
  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  // buffer pool has bug
  BTree *btree = new BTree(para);

  int key_nums = 1024;
  std::vector<int> keys(key_nums);
  std::iota(keys.begin(), keys.end(), 1);

  for (auto key : keys) {
    btree->Insert(key, key);
  }

  btree->Draw(DOTFILE_NAME_BEFORE);

  for (auto key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, &value));
    EXPECT_EQ(key, value);
  }

  for (auto key : keys) {
    btree->Remove(key);
    ValueType value = -1;
    EXPECT_FALSE(btree->Get(key, &value));
    EXPECT_EQ(value, -1);
  }

  // reverse delete

  for (auto key : keys) {
    btree->Insert(key, key);
  }

  btree->Draw(DOTFILE_NAME_BEFORE);

  for (auto key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, &value));
    EXPECT_EQ(key, value);
  }

  std::reverse(keys.begin(), keys.end());
  for (auto key : keys) {
    btree->Remove(key);
    ValueType value = -1;
    EXPECT_FALSE(btree->Get(key, &value));
    EXPECT_EQ(value, -1);
  }

  btree->Draw(DOTFILE_NAME_AFTER);

  delete para;
  delete disk;
  delete btree;
}

// sequential insert random delete
TEST(BTreeCRUDTest1, DeleteRandomSmall6) {
  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  // buffer pool has bug
  // small size will cannot delete keys
  BTree *btree = new BTree(para);

  int key_nums = 99;
  std::vector<int> keys(key_nums);
  std::iota(keys.begin(), keys.end(), 1);

  for (auto key : keys) {
    btree->Insert(key, key);
  }

  btree->Draw(DOTFILE_NAME_BEFORE);

  for (auto key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, &value));
    EXPECT_EQ(key, value);
  }

  // random shuffle
  std::mt19937 g(1024);
  std::shuffle(keys.begin(), keys.end(), g);

  int i = 0;
  for (auto key : keys) {
    if (key == 75) {
      btree->Draw(DOTFILE_NAME_AFTER);
    }
    i++;
    btree->Remove(key);
    ValueType value = -1;
    EXPECT_FALSE(btree->Get(key, &value));
    EXPECT_EQ(value, -1);
  }

  btree->Draw(DOTFILE_NAME_AFTER);

  delete para;
  delete disk;
  delete btree;
}

// sequential insert random delete
TEST(BTreeCRUDTest1, DeleteRandomBig7) {
  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  // buffer pool has bug
  // small size cause no left buffer
  // big size random read zero id
  BTree *btree = new BTree(para);

  int key_nums = 1024;
  std::vector<int> keys(key_nums);
  std::iota(keys.begin(), keys.end(), 1);

  for (auto key : keys) {
    btree->Insert(key, key);
  }

  btree->Draw(DOTFILE_NAME_BEFORE);

  for (auto key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, &value));
    EXPECT_EQ(key, value);
  }

  // random shuffle
  std::mt19937 g(1024);
  std::shuffle(keys.begin(), keys.end(), g);

  int i = 0;
  for (auto key : keys) {
    i++;
    btree->Remove(key);
    ValueType value = -1;
    EXPECT_FALSE(btree->Get(key, &value));
    EXPECT_EQ(value, -1);
  }

  btree->Draw(DOTFILE_NAME_AFTER);

  delete para;
  delete disk;
  delete btree;
}

// sequential insert sequential delete
// TEST(BTreeCRUDTest1, Update7) {
//   DiskManager *disk = new DiskManager(FILE_NAME.c_str());
//   ParallelBufferPoolManager *para = new ParallelBufferPoolManager(
//       INSTANCE_SIZE, PAGES_SIZE * PAGES_SIZE, disk);
//   // buffer pool has bug
//   BTree *btree = new BTree(para);

//   int key_nums = 1024;
//   std::vector<int> keys(key_nums);
//   std::iota(keys.begin(), keys.end(), 1);

//   for (auto key : keys) {
//     btree->Insert(key, key);
//   }

//   btree->Draw(DOTFILE_NAME_BEFORE);

//   for (auto key : keys) {
//     ValueType value = -1;
//     EXPECT_TRUE(btree->Get(key, &value));
//     EXPECT_EQ(key, value);
//   }

//   // random shuffle
//   std::mt19937 g(1024);
//   std::shuffle(keys.begin(), keys.end(), g);

//   for (auto key : keys) {
//     EXPECT_TRUE(btree->Update(key, key * key));
//     ValueType value = -1;
//     EXPECT_FALSE(btree->Get(key, &value));
//     EXPECT_EQ(value, key * key);
//   }

//   btree->Draw(DOTFILE_NAME_AFTER);

//   delete para;
//   delete disk;
//   delete btree;
// }

}  // namespace BTree
