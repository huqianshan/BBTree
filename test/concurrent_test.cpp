#include <gtest/gtest.h>

#include <iostream>
#include <thread>
#include <vector>

#include "../btree/btree.h"

namespace BTree {

template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BTree *tree, const std::vector<KeyType> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  // create transaction
  // Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    tree->Insert(key, value);
  }
  // delete transaction;
}

// helper function to insert
void InsertHelperSplit(BTree *tree, const std::vector<KeyType> &keys,
                       int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  // create transaction
  // Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      tree->Insert(key, value);
    }
  }
  // delete transaction;
}

// helper function to insert
void DeleteHelper(BTree *tree, const std::vector<KeyType> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  // create transaction
  // Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    tree->Remove(key);
  }
  // delete transaction;
}

// helper function to insert
void DeleteHelperSplit(BTree *tree, const std::vector<KeyType> &keys,
                       int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  // create transaction
  // Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      tree->Remove(key);
    }
  }
  // delete transaction;
}

TEST(ConcurrentTest, Insert1) {
  DiskManager *disk = new DiskManager("/data/public/hjl/bbtree/hjl.db");
  ParallelBufferPoolManager *para = new ParallelBufferPoolManager(1, 30, disk);
  BTree *btree = new BTree(para);

  // keys to Insert
  std::vector<KeyType> keys;
  int64_t scale_factor = 300;
  for (KeyType key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(16, InsertHelper, btree, keys);

  // btree->Draw("./bbtree-after.dot");

  for (auto key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, &value));
    EXPECT_EQ(key, value);
  }

  delete para;
  delete disk;
  delete btree;
}

// TEST(ConcurrentTest, Insert2) {
//   DiskManager *disk = new DiskManager("/data/public/hjl/bbtree/hjl.db");
//   ParallelBufferPoolManager *para =
//       new ParallelBufferPoolManager(16, 1024, disk);
//   BTree *btree = new BTree(para);

//   // keys to Insert
//   std::vector<KeyType> keys;
//   int64_t scale_factor = 100;
//   for (KeyType key = 1; key < scale_factor; key++) {
//     keys.push_back(key);
//   }
//   LaunchParallelTest(1, InsertHelperSplit, btree, keys, 1);

//   for (auto key : keys) {
//     ValueType value = -1;
//     EXPECT_TRUE(btree->Get(key, &value));
//     EXPECT_EQ(key, value);
//   }

//   delete para;
//   delete disk;
//   delete btree;
// }

// TEST(ConcurrentTest, Delete1) {
//   DiskManager *disk = new DiskManager("/data/public/hjl/bbtree/hjl.db");
//   ParallelBufferPoolManager *para =
//       new ParallelBufferPoolManager(16, 1024, disk);
//   BTree *btree = new BTree(para);

//   // sequential insert
//   std::vector<KeyType> keys = {1, 2, 3, 4, 5};
//   InsertHelper(btree, keys);

//   btree->Draw("./bbtree-before.dot");

//   std::vector<KeyType> remove_keys = {1, 5, 3, 4};
//   LaunchParallelTest(1, DeleteHelper, btree, remove_keys);

//   btree->Draw("./bbtree-after.dot");

//   ValueType value = -1;
//   EXPECT_TRUE(btree->Get(2, &value));
//   EXPECT_EQ(2, value);

//   delete para;
//   delete disk;
//   delete btree;
// }

// TEST(ConcurrentTest, Delete2) {
//   remove("/data/public/hjl/bbtree/hjl.db");
//   DiskManager *disk = new DiskManager("/data/public/hjl/bbtree/hjl.db");
//   ParallelBufferPoolManager *para = new ParallelBufferPoolManager(1, 4,
//   disk); BTree *btree = new BTree(para);

//   // sequential insert
//   std::vector<KeyType> keys = {1, 2, 3, 4, 5, 6};  //, }//7, 8, 9, 10};
//   InsertHelper(btree, keys);

//   btree->Draw("./bbtree-before.dot");

//   std::vector<KeyType> remove_keys = {1, 4, 3, 2, 5, 6};
//   LaunchParallelTest(1, DeleteHelper, btree, remove_keys);

//   btree->Draw("./bbtree-after.dot");

//   ValueType value = -1;
//   KeyType key = 2;
//   EXPECT_FALSE(btree->Get(2, &value));
//   EXPECT_EQ(-1, value);

//   /*   key = 7;
//     EXPECT_TRUE(btree->Get(key, &value));
//     EXPECT_EQ(key, value);

//     key = 10;
//     EXPECT_TRUE(btree->Get(key, &value));
//     EXPECT_EQ(key, value); */

//   delete para;
//   delete disk;
//   delete btree;
// }

/* TEST(ConcurrentTest, Mixed1) {
  DiskManager *disk = new DiskManager("/data/public/hjl/bbtree/hjl.db");
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(16, 1024, disk);
  BTree *btree = new BTree(para);

  // sequential insert
  int64_t len = 25120;
  std::vector<KeyType> keys(len);
  for (int i = 1; i < 10; i++) {
    keys[i] = (i) * (i - 1);
  }
  InsertHelper(btree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 0; i <= len; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, btree, keys);

  // concurrent delete
  std::vector<KeyType> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, btree, remove_keys);

  ValueType value = -1;
  KeyType key = 2;
  EXPECT_TRUE(btree->Get(2, &value));
  EXPECT_EQ(key, value);

  key = 7;
  EXPECT_TRUE(btree->Get(key, &value));
  EXPECT_EQ(key, value);

  key = 10;
  EXPECT_TRUE(btree->Get(key, &value));
  EXPECT_EQ(key, value);

  para->FlushAllPages();
  delete para;
  delete disk;
  delete btree;
} */

}  // namespace BTree
