#pragma once

#include "../zbtree/config.h"
#include "timer.h"

// namespace BTree {
const u32 BUFFER_POOL_SIZE = 1024 * 64;  // in Bytes
const u32 INSTANCE_SIZE = 1;
const u32 PAGES_SIZE = BUFFER_POOL_SIZE / (INSTANCE_SIZE * PAGE_SIZE);
#define WORK_DIR "/home/hjl/Code/Academic/BBTree/build/"
const std::string DOTFILE_NAME_BEFORE = WORK_DIR "bbtree-before.dot";
const std::string DOTFILE_NAME_AFTER = WORK_DIR "bbtree-after.dot";

// #define REGURLAR_DEVICE "/dev/nvme0n1p1"
#define REGURLAR_DEVICE "/dev/nvme1n1p1"
#define ZNS_DEVICE "/dev/nvme2n2"

const std::string FILE_NAME = "/data/public/hjl/bbtree/bbtree.db";
// const std::string FILE_NAME = "/data/public/hjl/bbtree/f2fs/bbtree.db";

const std::string YCSB_LOAD_FILE_NAME =
    "/data/public/hjl/YCSB/ycsb_load_workload";
const std::string YCSB_RUN_FILE_NAME =
    "/data/public/hjl/YCSB/ycsb_run_workload";

// }  // namespace BTree
