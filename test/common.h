#pragma once
#include "../btree/btree.h"
#include "timer.h"

namespace BTree {
const u32 INSTANCE_SIZE = 1;
const u32 PAGES_SIZE = 4 * 1024;
const std::string DOTFILE_NAME_BEFORE = "./bbtree-before.dot";
const std::string DOTFILE_NAME_AFTER = "./bbtree-after.dot";

const std::string FILE_NAME = "/data/public/hjl/bbtree/f2fs/bbtree.db";
const std::string YCSB_LOAD_FILE_NAME =
    "/data/public/hjl/YCSB/ycsb_load_workload";
const std::string YCSB_RUN_FILE_NAME =
    "/data/public/hjl/YCSB/ycsb_run_workload";

}  // namespace BTree
