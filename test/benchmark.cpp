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

#include "../include/config.h"
#include "../zbtree/BTreeOLC.h"
#include "common.h"

// using namespace BTree;
using namespace btreeolc;
using namespace std;

const u64 MILLION = 1000 * 1000;

// #define LATENCY
// #define PM_PCM
#define DRAM_CONSUMPTION

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
  fprintf(stdout, "Loaded %8lu keys for initialing.\n", LOAD_SIZE);

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

  fprintf(stdout, "Loaded %8lu keys for running.\n", RUN_SIZE);
#ifdef DRAM_CONSUMPTION
  //   test();
  //   int ret = system((dram_shell + process_name).c_str());
  GetDRAMSpace();
#endif
  Timer tr;
  tr.start();

  DiskManager *disk = new DiskManager(FILE_NAME.c_str());
  ParallelBufferPoolManager *para =
      new ParallelBufferPoolManager(INSTANCE_SIZE, PAGES_SIZE, disk);
  // BTree::BTree *tree = new BTree::BTree(para);
  bpm = para;
  btreeolc::BTree *tree = new btreeolc::BTree();

  printf("Tree init: %s %4.2f ms.\n", "BufferBTreeF2Fs",
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
    printf("Throughput: load, %3.3f Kops/s\n", (LOAD_SIZE * 1.0) / (t));
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
  auto t = sw.elapsed<std::chrono::milliseconds>();

  printf("Throughput: run, %f Kops/s\n", (RUN_SIZE * 1.0) / (t));
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

  auto file_size = disk->get_file_size();
  auto read_count = disk->get_read_count();
  auto write_count = disk->get_write_count();

  delete tree;

  double page_read_avg = 1.0 * read_count * PAGE_SIZE / file_size;
  double page_write_avg = 1.0 * write_count * PAGE_SIZE / file_size;
  double bytes_read_avg = 1.0 * read_count * PAGE_SIZE / (LOAD_SIZE + RUN_SIZE);
  double bytes_write_avg =
      1.0 * write_count * PAGE_SIZE / (LOAD_SIZE + RUN_SIZE);
  printf(
      "[BufferPool]: In-place read in a page: %6.2f, In-place write in a page: "
      "%6.2f\n",
      page_read_avg, page_write_avg);
  printf("[BTreeIndex]: Read amp: %6.2f bytes/op, Write amp: %6.2f bytes/op\n ",
         bytes_read_avg, bytes_write_avg);
}

int main(int argc, char **argv) {
  if (argc != 4) {
    printf("Usage: %s <workload> <threads> <size>\n", argv[0]);
    exit(0);
  };

  printf("Test Begin\n");
  printf("workload: %s, threads: %s\n", argv[1], argv[2]);

  string workload = argv[1];
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

  int num_thread = atoi(argv[2]);
  u64 max_load_size = MILLION * atoi(argv[3]);
  u64 max_run_size = MILLION * atoi(argv[3]);

  remove(FILE_NAME.c_str());

  auto [read_reg_before, written_reg_before] = getDataUnits(REGURLAR_DEVICE);
  auto [read_zone_before, written_zone_before] = getDataUnits(ZNS_DEVICE);

  run_test(num_thread, load_data, run_data, workload, max_load_size,
           max_run_size);

  auto [read_reg, written_reg] = getDataUnits(REGURLAR_DEVICE);
  auto [read_zone, written_zone] = getDataUnits(ZNS_DEVICE);

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

  printf("[Reg] Read amp: %6.2f bytes/op, Write amp: %6.2f bytes/op \n",
         read_amplification_reg, write_amplification_reg);
  printf("[Zone] Read amp: %6.2f bytes/op, Write amp: %6.2f bytes/op \n",
         read_amplification_zone, write_amplification_zone);
  printf("Load size: %lu, Run size: %lu\n", LOAD_SIZE, RUN_SIZE);

  printf("Test End\n");
  return 0;
}