#include "wal.h"

#include <string.h>

SingleWAL::SingleWAL(const char* wal_name, u64 length) {
  //   wal_area_ = new bytes_t[length];
  wal_area_ = (bytes_t*)aligned_alloc(PAGE_SIZE, length);
  length_ = length;
  disk_manager_ = new DiskManager(wal_name);
  offset_ = 0;
  //   cur_size_ = 0;
  page_id_ = 0;
}
SingleWAL::SingleWAL(SingleWAL&& wal)
{
  wal_area_ = wal.wal_area_;
  length_ = wal.length_;
  disk_manager_ = wal.disk_manager_;
  offset_ = wal.offset_;
  page_id_ = wal.page_id_;
  wal.wal_area_ = nullptr;
  wal.disk_manager_ = nullptr;
}

SingleWAL::~SingleWAL() {
  Flush();
  if (wal_area_) {
    free(wal_area_);
  }
  if (disk_manager_) {
    delete disk_manager_;
  }
}

void SingleWAL::Append(const bytes_t* data, u64 size) {
  std::lock_guard<std::mutex> lock(mutex_);

  bytes_t* cur_buf = wal_area_ + offset_;
  offset_ += size;
  memcpy(cur_buf, data, size);

  if (offset_ >= FLUSH_SIZE) {
    Flush();
  }
}

void SingleWAL::Append(const u64 key, const u64 val) {
  std::lock_guard<std::mutex> lock(mutex_);

  bytes_t* cur_buf = wal_area_ + offset_;
  assert(offset_ + sizeof(PairType)<= length_);
  offset_ += sizeof(PairType);
  // offset_ += (sizeof(u64) * 2);
  PairType* addr = (PairType*)wal_area_;
  addr[offset_ / sizeof(PairType)] = std::make_pair(key, val);
  // pair[0] = {key, val};
  // memcpy(cur_buf, data, size);
  // PairType* pair = (PairType*)cur_buf;
  // PairType* addr = (PairType*)cur_buf;
  // addr[0] = std::make_pair(key, val);

  if (offset_ >= FLUSH_SIZE) {
    Flush();
  }
}

void SingleWAL::Flush() {
  //   std::lock_guard<std::mutex> lock(mutex_);
  if (offset_ > 0) {
    disk_manager_->write_n_pages(page_id_, FLUSH_SIZE / PAGE_SIZE,
                                 (const char*)wal_area_);
    page_id_++;
    offset_ = 0;
    memset(wal_area_, 0x00, FLUSH_SIZE);
  }
}

u64 SingleWAL::Size() { return page_id_; }

WAL::WAL(const char* wal_name, u32 instance, u32 length) {
  num_instances_ = instance;
  length_ = length;
  index_ = 0;

  // wal_list_.resize(num_instances_);
  for (u32 i = 0; i < num_instances_; i++) {
    std::string name_with_suffix =
        std::string(wal_name) + "_" + std::to_string(i);
    // wal_list_[i] = new SingleWAL(name_with_suffix.c_str(), length_);
    // wal_list_.emplace_back(name_with_suffix.c_str(), length_);
    wal_list_.push_back(SingleWAL(name_with_suffix.c_str(), length_));
  }
}

WAL::~WAL() {
  // Print();
  // for (auto& wal : wal_list_) {
  //   delete wal;
  // }
}

void WAL::Append(const bytes_t* data, u64 size) {
  size_t loop_index = index_.load();
  index_.store((index_.load() + 1) % num_instances_);
  assert(loop_index < num_instances_);
  // wal_list_[loop_index]->Append(data, size);
  wal_list_[loop_index].Append(data, size);
}

void WAL::Append(const u64 key, const u64 val) {
  size_t loop_index = index_.load();
  index_.store((index_.load() + 1) % num_instances_);
  assert(loop_index < num_instances_);
  // wal_list_[loop_index]->Append(key, val);
  wal_list_[loop_index].Append(key, val);
}

void WAL::FlushAll() {
  for (u32 i = 0; i < num_instances_; i++) {
    // wal_list_[i]->Flush();
    wal_list_[i].Flush();
  }
}

u64 WAL::Size() {
  u64 size = 0;
  for (u32 i = 0; i < num_instances_; i++) {
    // size += (wal_list_[i]->Size() * PAGE_SIZE);
    // size += wal_list_[i]->GetOffset();
    size += (wal_list_[i].Size() * PAGE_SIZE);
    size += wal_list_[i].GetOffset();
  }
  return size;
}

void WAL::Print() {
  std::cout << "WAL size: " << Size() << " bytes " << std::endl;
}