#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include "../zbtree/buffer.h"
#include "../zbtree/zbtree.h"
#include "common.h"
// namespace BTree {

TEST(ReplacerTest, 0_FIFO) {
  FIFOReplacer *fifo = new FIFOReplacer(8);

  std::vector<frame_id_t> frames(8);
  std::iota(frames.begin(), frames.end(), 0);

  for (const auto &frame : frames) {
    EXPECT_TRUE(fifo->Add(frame));
  }
  EXPECT_FALSE(fifo->Add(8));

  frame_id_t victim = -1;
  std::vector<frame_id_t> new_frames(8);
  std::iota(new_frames.begin(), new_frames.end(), 8);
  for (const auto &frame : new_frames) {
    EXPECT_TRUE(fifo->Victim(&victim));
    EXPECT_EQ(frame, victim + 8);

    EXPECT_TRUE(fifo->Add(frame));
  }
  // EXPECT_FALSE(fifo->Victim(&victim));
  // EXPECT_TRUE(fifo->Add(8));
  // EXPECT_TRUE(fifo->Victim(&victim));
  // EXPECT_EQ(8, victim);

  /*   std::shuffle(new_frames.begin(), frames.end(), std::mt19937(1024));
    for (const auto &frame : frames) {
      std::cout << frame << " ";
    }
    std::cout << std::endl;
    for (const auto &frame : frames) {
      EXPECT_TRUE(fifo->Add(frame));
    }
    for (const auto &frame : frames) {
      EXPECT_TRUE(fifo->Victim(&victim));
      EXPECT_EQ(frame, victim) << "test";
      std::cout << frame << " ";
    }
    std::cout << std::endl; */
  delete fifo;
}

// Sequential insert
TEST(BTreeCRUDTest1, 1_InsertSeq) {
  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  // BTree::BTree *tree = new BTree::BTree(para);
  // btreeolc::bpm = para;
  btreeolc::BTree *btree = new btreeolc::BTree(para);

  int key_nums = 199;
  std::vector<u64> keys(key_nums);
  std::iota(keys.begin(), keys.end(), 1);

  int i = 0;
  for (const auto &key : keys) {
    if (key == 19) {
      printf("key at %d\n", i);
      btree->Draw(DOTFILE_NAME_BEFORE);
    }
    i++;
    EXPECT_TRUE(btree->Insert(key, key));
  }

  btree->Draw(DOTFILE_NAME_AFTER);

  for (const auto &key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, value));
    EXPECT_EQ(key, value);
    // if (key % 1000 == 0) {
    // printf("key = %lu value = %lu\n", key, value);
    // }
  }

  delete btree;
}

// Random insert
TEST(BTreeCRUDTest1, 2_InsertRandom) {
  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  // BTree::BTree *tree = new BTree::BTree(para);
  // btreeolc::bpm = para;
  btreeolc::BTree *btree = new btreeolc::BTree(para);

  int key_nums = 1024;
  std::vector<int> keys(key_nums);
  std::iota(keys.begin(), keys.end(), 1);

  // random shuffle
  std::mt19937 g(1024);
  std::shuffle(keys.begin(), keys.end(), g);
  int i = 0;
  for (const auto &key : keys) {
    // if (key == 8 || key == 16) {
    //   btree->Draw(DOTFILE_NAME_BEFORE);
    // }
    EXPECT_TRUE(btree->Insert(key, key));
    i++;
  }

  btree->Draw(DOTFILE_NAME_AFTER);
  for (const auto &key : keys) {
    ValueType value = -1;
    EXPECT_TRUE(btree->Get(key, value));
    EXPECT_EQ(key, value);
  }

  delete btree;
}

// // Random insert
// TEST(BTreeCRUDTest1, 3_InsertDuplicated) {
//   DiskManager *disk = new DiskManager(FILE_NAME.c_str());
//   ParallelBufferPoolManager *para =
//       new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
//   BTree *btree = new BTree(para);

//   int key_nums = 1024;
//   std::vector<int> keys(key_nums);
//   std::iota(keys.begin(), keys.end(), 1);

//   // random shuffle
//   std::mt19937 g(1024);
//   std::shuffle(keys.begin(), keys.end(), g);

//   for (const auto &key : keys) {
//     EXPECT_TRUE(btree->Insert(key, key));
//   }

//   btree->Draw(DOTFILE_NAME_BEFORE);

//   for (const auto &key : keys) {
//     ValueType value = -1;
//     EXPECT_FALSE(btree->Insert(key, key * key));
//     EXPECT_TRUE(btree->Get(key, &value));
//     EXPECT_EQ(key, value);
//   }

//   delete btree;
// }

// // sequential insert sequential delete
// TEST(BTreeCRUDTest1, 4_DeleteSeqSmall) {
//   DiskManager *disk = new DiskManager(FILE_NAME.c_str());
//   ParallelBufferPoolManager *para =
//       new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
//   BTree *btree = new BTree(para);

//   int key_nums = 8;
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

//   // keys = {2, 7, 1, 3, 8, 6, 5, 4};
//   for (auto key : keys) {
//     btree->Remove(key);
//     ValueType value = -1;
//     EXPECT_FALSE(btree->Get(key, &value));
//     EXPECT_EQ(value, -1);
//   }

//   btree->Draw(DOTFILE_NAME_AFTER);

//   delete btree;
// }

// // sequential insert sequential delete
// TEST(BTreeCRUDTest1, 5_DeleteSeqBig) {
//   DiskManager *disk = new DiskManager(FILE_NAME.c_str());
//   ParallelBufferPoolManager *para =
//       new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
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

//   for (auto key : keys) {
//     btree->Remove(key);
//     ValueType value = -1;
//     EXPECT_FALSE(btree->Get(key, &value));
//     EXPECT_EQ(value, -1);
//   }

//   // reverse delete

//   for (auto key : keys) {
//     btree->Insert(key, key);
//   }

//   btree->Draw(DOTFILE_NAME_BEFORE);

//   for (auto key : keys) {
//     ValueType value = -1;
//     EXPECT_TRUE(btree->Get(key, &value));
//     EXPECT_EQ(key, value);
//   }

//   std::reverse(keys.begin(), keys.end());
//   for (auto key : keys) {
//     btree->Remove(key);
//     ValueType value = -1;
//     EXPECT_FALSE(btree->Get(key, &value));
//     EXPECT_EQ(value, -1);
//   }

//   btree->Draw(DOTFILE_NAME_AFTER);

//   delete btree;
// }

// // sequential insert random delete
// TEST(BTreeCRUDTest1, 6_DeleteRandomSmall) {
//   remove(FILE_NAME.c_str());
//   DiskManager *disk = new DiskManager(FILE_NAME.c_str());
//   ParallelBufferPoolManager *para =
//       new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
//   // buffer pool has bug
//   // small size cannot delete keys
//   BTree *btree = new BTree(para);

//   int key_nums = 99;
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

//   int i = 0;
//   int bkt = 53;
//   for (auto key : keys) {
//     i++;
//     // if (i == bkt) {  // 92
//     //   btree->Draw(DOTFILE_NAME_BEFORE);
//     //   printf("i = %d key = %d\n", i, key);
//     // }
//     btree->Remove(key);
//     // if (i == bkt) {
//     //   btree->Draw(DOTFILE_NAME_AFTER);
//     //   printf("i = %d key = %d\n", i, key);
//     // }
//     ValueType value = -1;
//     EXPECT_FALSE(btree->Get(key, &value)) << "key = " << key << std::endl;
//     EXPECT_EQ(value, -1);
//   }

//   btree->Draw(DOTFILE_NAME_AFTER);

//   delete btree;
// }

// // sequential insert random delete
// TEST(BTreeCRUDTest1, 7_DeleteRandomBig) {
//   DiskManager *disk = new DiskManager(FILE_NAME.c_str());
//   ParallelBufferPoolManager *para =
//       new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
//   // buffer pool has bug
//   // small size cause no left buffer
//   // big size random read zero id
//   BTree *btree = new BTree(para);

//   int key_nums = 1024 * 2;
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

//   int i = 0;
//   for (auto key : keys) {
//     i++;
//     btree->Remove(key);
//     ValueType value = -1;
//     EXPECT_FALSE(btree->Get(key, &value));
//     EXPECT_EQ(value, -1);
//   }

//   btree->Draw(DOTFILE_NAME_AFTER);

//   delete btree;
// }

// // sequential insert sequential update
// TEST(BTreeCRUDTest1, 8_UpdateSeq) {
//   DiskManager *disk = new DiskManager(FILE_NAME.c_str());
//   ParallelBufferPoolManager *para =
//       new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
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
//   // std::mt19937 g(1024);
//   // std::shuffle(keys.begin(), keys.end(), g);

//   for (auto key : keys) {
//     EXPECT_TRUE(btree->Update(key, key * key));
//     ValueType value = -1;
//     EXPECT_TRUE(btree->Get(key, &value));
//     EXPECT_EQ(value, key * key);
//   }

//   btree->Draw(DOTFILE_NAME_AFTER);

//   delete btree;
// }

// // sequential insert sequential update
// TEST(BTreeCRUDTest1, 9_UpdateRand) {
//   DiskManager *disk = new DiskManager(FILE_NAME.c_str());
//   ParallelBufferPoolManager *para =
//       new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
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
//     EXPECT_TRUE(btree->Get(key, &value));
//     EXPECT_EQ(value, key * key);
//   }

//   btree->Draw(DOTFILE_NAME_AFTER);

//   delete btree;
// }

// }  // namespace BTree
