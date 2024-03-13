#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <regex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "../zbtree/buffer.h"
#include "../zbtree/zbtree.h"
#include "common.h"
// using namespace BTree;
using namespace btreeolc;
using namespace std;

const u64 MILLION = 1000 * 1000;

/**
 * Test parameters
 */
// #define LATENCY
#define DRAM_CONSUMPTION
// #define GDB

/**
 * Tree index
 *
 */
// #define RAW_BTREE_ON_FS
// #define BBTREE_ON_EXT4_SSDD
#define BBTREE_ON_ZNS

#ifdef RAW_BTREE_ON_FS
const std::string TREE_NAME = "BTreeBufferOnExt4SSD";
#elif defined(BBTREE_ON_EXT4_SSDD)
#include "../zbtree/buffer_btree.h"
const std::string TREE_NAME = "BatchTreeBufferOnExt4SSD";
#elif defined(BBTREE_ON_ZNS)
const std::string TREE_NAME = "BatchTreeHybridBufferOnZNS";
#endif

enum OP { OP_INSERT, OP_READ, OP_DELETE, OP_UPDATE, OP_SCAN };

enum TreeIndex {
  BufferBTreeF2Fs = 0,
};

u64 LOAD_SIZE;
u64 RUN_SIZE;
// u64 MAX_SIZE_LOAD = 200000000ULL;
//  u64 MAX_SIZE_RUN = 200000000ULL;

const u64 UNITS_SIZE = 1000 * 512;

std::string exec(const char *cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

std::pair<u64, u64> getDataUnits(const std::string &device) {
  std::string cmd_base =
      "sudo nvme smart-log " + device + " | grep 'Data Units ";
  std::string output_read =
      exec((cmd_base + "Read' | awk '{print $5}'").c_str());
  std::string output_written =
      exec((cmd_base + "Written' | awk '{print $5}'").c_str());
  return {stoull(output_read), stoull(output_written)};
}

void GetDRAMSpace() {
  auto pid = getpid();
  std::array<char, 128> buffer;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(
      popen(("cat /proc/" + to_string(pid) + "/status").c_str(), "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    string result = buffer.data();
    if (result.find("VmRSS") != string::npos) {
      std::string mem_ocp = std::regex_replace(
          result, std::regex("[^0-9]*([0-9]+).*"), std::string("$1"));
      printf("DRAM consumption: %4.2f MB.\n", stof(mem_ocp) / 1024);
      break;
    }
  }
}

void run_test(int num_thread, string load_data, string run_data,
              string workload, u64 max_load_size, u64 max_run_size) {
  u64 MAX_SIZE_LOAD = max_load_size;
  u64 MAX_SIZE_RUN = max_run_size;
  string insert("INSERT");
  string remove("REMOVE");
  string read("READ");
  string update("UPDATE");

  ifstream infile_load(load_data);
  string op;
  u64 key;
  u64 value_len;

  vector<u64> init_keys(MAX_SIZE_LOAD);
  vector<u64> keys(MAX_SIZE_RUN);
  vector<u64> init_value_lens(MAX_SIZE_LOAD);
  vector<u64> value_lens(MAX_SIZE_RUN);
  vector<int> ops(MAX_SIZE_RUN);

  int count = 0;
  while ((count < MAX_SIZE_LOAD) && infile_load.good()) {
    infile_load >> op >> key >> value_len;
    if (!op.size()) continue;
    if (op.size() && op.compare(insert) != 0) {
      cout << "READING LOAD FILE FAIL!\n";
      cout << op << endl;
      return;
    }
    init_keys[count] = key;
    init_value_lens[count] = value_len;
    count++;
  }
  LOAD_SIZE = count;
  infile_load.close();
  printf("Loaded %8lu keys for initialing.\n", LOAD_SIZE);

  ifstream infile_run(run_data);
  count = 0;
  while ((count < MAX_SIZE_RUN) && infile_run.good()) {
    infile_run >> op >> key;
    if (op.compare(insert) == 0) {
      infile_run >> value_len;
      ops[count] = OP_INSERT;
      keys[count] = key;
      value_lens[count] = value_len;
    } else if (op.compare(update) == 0) {
      infile_run >> value_len;
      ops[count] = OP_UPDATE;
      keys[count] = key;
      value_lens[count] = value_len;
    } else if (op.compare(read) == 0) {
      ops[count] = OP_READ;
      keys[count] = key;
    } else if (op.compare(remove) == 0) {
      ops[count] = OP_DELETE;
      keys[count] = key;
    } else {
      continue;
    }
    count++;
  }
  RUN_SIZE = count;

  printf("Loaded %8lu keys for running.\n", RUN_SIZE);
#ifdef DRAM_CONSUMPTION
  //   test();
  //   int ret = system((dram_shell + process_name).c_str());
  GetDRAMSpace();
#endif
  Timer tr;
  tr.start();

#ifdef RAW_BTREE_ON_FS
  // DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para = new ParallelBufferPoolManager(
      INSTANCE_SIZE, PAGES_SIZE, FILE_NAME, false);
  btreeolc::BTree *tree = new btreeolc::BTree(para);
#elif defined(BBTREE_ON_EXT4_SSDD)
  ParallelBufferPoolManager *para = new ParallelBufferPoolManager(
      INSTANCE_SIZE, PAGES_SIZE, FILE_NAME, false);
  std::shared_ptr<btreeolc::BTree> device_tree =
      std::make_shared<btreeolc::BTree>(para);
  std::shared_ptr<btreeolc::BufferBTree<KeyType, ValueType> > tree(
      new btreeolc::BufferBTree<KeyType, ValueType>(device_tree));
#elif defined(BBTREE_ON_ZNS)
  // DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  // FIFOBatchBufferPool *para = new FIFOBatchBufferPool(PAGES_SIZE, disk);
  ZoneManagerPool *para =
      new ZoneManagerPool(MAX_CACHED_PAGES_PER_ZONE, MAX_NUMS_ZONE, ZNS_DEVICE);
  btreeolc::BTree *tree = new btreeolc::BTree(para);
#endif

  printf("Tree init:" KWHT " %s" KRESET " %4.2f ms.\n", TREE_NAME.c_str(),
         tr.elapsed<std::chrono::milliseconds>());

  auto part = LOAD_SIZE / num_thread;

  {
    // Load
    Timer sw;
    thread ths[num_thread];
    sw.start();
    auto insert = [&](size_t start, size_t len, int tid) {
      auto end = start + len;
      // cout << "start:" << start << "end:" << start + len << endl;
      for (size_t i = start; i < end; i++) {
        tree->Insert(init_keys[i], init_keys[i]);
      }
    };

    for (size_t i = 0; i < num_thread; i++) {
      ths[i] = thread(insert, part * i, part, i);
    }
    for (size_t i = 0; i < num_thread; i++) {
      ths[i].join();
    }
    auto t = sw.elapsed<std::chrono::milliseconds>();
    printf("Throughput: load, " KGRN "%3.3f" KRESET " Kops/s\n",
           (LOAD_SIZE * 1.0) / (t));
    printf("Load time: %4.4f sec\n", t / 1000.0);
  }
  part = RUN_SIZE / num_thread;
  // Run
  Timer sw;
#ifdef LATENCY
  vector<size_t> latency_all;
  Mutex latency_mtx;
#endif
  std::function<void(size_t start, size_t len, int tid)> fun;
  auto operate = [&](size_t start, size_t len, int tid) {
    vector<size_t> latency;
    auto end = start + len;
    Timer l;

    bool rf = false;
    for (size_t i = start; i < end; i++) {
#ifdef LATENCY
      l.start();
#endif
      if (ops[i] == OP_INSERT) {
        tree->Insert(keys[i], keys[i]);
      } else if (ops[i] == OP_UPDATE) {
        // tree->Update(keys[i], keys[i] + 1);
      } else if (ops[i] == OP_READ) {
        u64 v;
        auto r = tree->Get(keys[i], v);
      } else if (ops[i] == OP_DELETE) {
        // tree->Remove(keys[i]);
      }
#ifdef LATENCY
      latency.push_back(l.elapsed<std::chrono::nanoseconds>());
#endif
    }

#ifdef LATENCY
    lock_guard<Mutex> lock(latency_mtx);
    latency_all.insert(latency_all.end(), latency.begin(), latency.end());
#endif
  };

  fun = operate;
  thread ths[num_thread];

  sw.start();
  for (size_t i = 0; i < num_thread; i++) {
    ths[i] = thread(fun, part * i, part, i);
  }
  for (size_t i = 0; i < num_thread; i++) {
    ths[i].join();
  }
#ifdef BBTREE_ON_EXT4_SSDD
  tree->flush_all();
#endif
  auto t = sw.elapsed<std::chrono::milliseconds>();

  printf("Throughput: run, " KRED "%f" KRESET " Kops/s\n",
         (RUN_SIZE * 1.0) / (t));
  printf("Run time: %4.4f sec\n", t / 1000.0);

#ifdef LATENCY
  sort(latency_all.begin(), latency_all.end());
  auto sz = latency_all.size();
  size_t avg = 0;
  for (size_t i = 0; i < sz; i++) {
    avg += latency_all[i];
  }
  avg /= sz;

  cout << "Latency: " << avg << " ns\n";
  cout << "\t0 " << latency_all[0] << "\n"
       << "\t50% " << latency_all[size_t(0.5 * sz)] << "\n"
       << "\t90% " << latency_all[size_t(0.9 * sz)] << "\n"
       << "\t99% " << latency_all[size_t(0.99 * sz)] << "\n"
       << "\t99.9% " << latency_all[size_t(0.999 * sz)] << "\n"
       << "\t99.99% " << latency_all[size_t(0.9999 * sz)] << "\n"
       << "\t99.999% " << latency_all[size_t(0.99999 * sz)] << "\n"
       << "\t100% " << latency_all[sz - 1] << endl;
#endif
#ifdef DRAM_CONSUMPTION
  //   ret = system((dram_shell + process_name).c_str());
  GetDRAMSpace();
#endif

  para->FlushAllPages();

  auto file_size = para->GetFileSize();
  auto read_count = para->GetReadCount();
  auto write_count = para->GetWriteCount();
#ifdef BBTREE_ON_EXT4_SSDD
  auto wal_size = tree->wal_->Size();
  double wal_bytes_avg_write = 1.0 * wal_size / (LOAD_SIZE + RUN_SIZE);
  printf("[WriteAheadLog]:  Write amp: %6.2f bytes/op\n", wal_bytes_avg_write);
#endif

#ifndef BBTREE_ON_EXT4_SSDD
  delete para;
  delete tree;
#endif

  double page_read_avg = 1.0 * read_count * PAGE_SIZE / file_size;
  double page_write_avg = 1.0 * write_count * PAGE_SIZE / file_size;
  double bytes_read_avg = 1.0 * read_count * PAGE_SIZE / (LOAD_SIZE + RUN_SIZE);
  double bytes_write_avg =
      1.0 * write_count * PAGE_SIZE / (LOAD_SIZE + RUN_SIZE);
  printf(
      "[Storage] Write_count=%8lu read_count=%8lu PAGES fielsize=%8lu PAGES "
      "PAGE= %4d Bytes\n",
      write_count, read_count, file_size / PAGE_SIZE, PAGE_SIZE);
  printf(
      "[BTreeIndex]: In-place read in a page: %6.2f, In-place write in a page: "
      "%6.2f\n",
      page_read_avg, page_write_avg);
  printf("[BTreeIndex]: Read amp: " KBLU "%6.2f" KRESET
         " bytes/op, Write amp: " KYEL "%6.2f" KRESET " bytes/op\n",
         bytes_read_avg, bytes_write_avg);
}

int main(int argc, char **argv) {
#ifndef GDB
  if (argc != 4) {
    printf("Usage: %s <workload> <threads> <size>\n", argv[0]);
    exit(0);
  };
#endif

  printf("Test Begin\n");

#ifndef GDB
  string workload = argv[1];
  printf(KNRM "workload: " KWHT "%s" KRESET ", threads: " KWHT "%s" KRESET "\n",
         argv[1], argv[2]);
#else
  string workload = "ycsba";
  int num_thread = 1;
  int GDB_SIZE = 1;
  printf("workload: %s, threads: %2d\n", workload.c_str(), num_thread);
#endif
  string load_data = "";
  string run_data = "";
  if (workload.find("ycsb") != string::npos) {
    load_data = YCSB_LOAD_FILE_NAME;
    load_data += workload[workload.size() - 1];
    run_data = YCSB_RUN_FILE_NAME;
    run_data += workload[workload.size() - 1];
  } else {
    printf("Wrong workload!\n");
    return 0;
  }

#ifndef GDB
  int num_thread = atoi(argv[2]);
  u64 max_load_size = MILLION * atoi(argv[3]);
  u64 max_run_size = MILLION * atoi(argv[3]);
#else
  u64 max_load_size = MILLION * GDB_SIZE;
  u64 max_run_size = MILLION * GDB_SIZE;
#endif

  remove(FILE_NAME.c_str());
#ifndef GDB
  auto read_reg_before = 0, written_reg_before = 0;
  std::tie(read_reg_before, written_reg_before) = getDataUnits(REGURLAR_DEVICE);
  auto read_zone_before = 0, written_zone_before = 0;
  std::tie(read_zone_before, written_zone_before) = getDataUnits(ZNS_DEVICE);
#endif
  run_test(num_thread, load_data, run_data, workload, max_load_size,
           max_run_size);
#ifndef GDB
  auto read_reg = 0, written_reg = 0;
  std::tie(read_reg, written_reg) = getDataUnits(REGURLAR_DEVICE);
  auto read_zone = 0, written_zone = 0;
  std::tie(read_zone, written_zone) = getDataUnits(ZNS_DEVICE);

  auto read_total_reg = static_cast<u64>(read_reg - read_reg_before);
  auto write_total_reg = static_cast<u64>(written_reg - written_reg_before);
  auto read_total_zone = static_cast<u64>(read_zone - read_zone_before);
  auto write_total_zone = static_cast<u64>(written_zone - written_zone_before);

  printf("[Reg] Read: %6lu Units, Written: %6lu Units\n", read_total_reg,
         write_total_reg);
  printf("[Zone] Read: %6lu Units, Written: %6lu Units\n", read_total_zone,
         write_total_zone);

  auto write_amplification_reg =
      write_total_reg * 1.0 * UNITS_SIZE / (LOAD_SIZE + RUN_SIZE);
  auto write_amplification_zone =
      write_total_zone * 1.0 * UNITS_SIZE / (LOAD_SIZE + RUN_SIZE);
  auto read_amplification_reg =
      read_total_reg * 1.0 * UNITS_SIZE / (LOAD_SIZE + RUN_SIZE);
  auto read_amplification_zone =
      read_total_zone * 1.0 * UNITS_SIZE / (LOAD_SIZE + RUN_SIZE);

  printf("[Reg] Read amp:" KBLU " %6.2f" KRESET " bytes/op, Write amp: " KYEL
         "%6.2f " KRESET "bytes/op \n",
         read_amplification_reg, write_amplification_reg);
  printf("[Zone] Read amp: " KBLU "%6.2f" KRESET " bytes/op, Write amp: " KYEL
         "%6.2f " KRESET "bytes/op \n",
         read_amplification_zone, write_amplification_zone);
#endif
  printf("Test End\n");
  return 0;
}