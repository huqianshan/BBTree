#pragma once
#include <mutex>
#include <string>
#include <vector>

#include "config.h"
#include "storage.h"

#define FLUSH_SIZE (PAGE_SIZE)

class SingleWAL {
 public:
  SingleWAL(const char* wal_name, u64 length = FLUSH_SIZE);
  ~SingleWAL();
  void Append(const bytes_t* data, size_t size);
  void Append(const u64 key, const u64 val);
  void Flush();
  u64 Size();
  u64 GetOffset() { return offset_; }

 private:
  bytes_t* wal_area_;
  u64 offset_;
  //   u64 cur_size_;
  page_id_t page_id_;
  u32 length_;
  DiskManager* disk_manager_;
  std::mutex mutex_;
};

class WAL {
 public:
  WAL(const char* wal_name, u32 instance, u32 length = FLUSH_SIZE);
  ~WAL();
  void Append(const bytes_t* data, size_t size);
  void Append(const u64 key, const u64 val);
  void FlushAll();
  u64 Size();
  void Print();

 private:
  std::vector<SingleWAL*> wal_list_;
  u32 num_instances_;
  std::atomic<size_t> index_;
  u32 length_;
};