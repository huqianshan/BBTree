#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

// #include "stats.h"
#include "storage.h"

namespace BTree {

DiskManager *db_io = nullptr;

static void assert_msg(bool expr, const char *msg) {
  if (!expr) {
    perror(msg);
    abort();
  }
  return;
}

void DiskManager::write_n_pages(page_id_t page_id, size_t nr_pages,
                                const char *page_data) {
  // DEBUG_PRINT("write_n_pages: page_id=%u, nr_pages=%u\n", page_id,
  // (unsigned)nr_pages);
  write_count_.fetch_add(1, std::memory_order_relaxed);
  ssize_t len = nr_pages * PAGE_SIZE;
  ssize_t ret = pwrite(fd_, page_data, len, page_id * PAGE_SIZE);
  // fsync(fd_);
  assert_msg(ret > 0, "write failed\n");
}

void DiskManager::read_n_pages(page_id_t page_id, size_t nr_pages,
                               char *page_data) {
  read_count_.fetch_add(1, std::memory_order_relaxed);
  ssize_t len = nr_pages * PAGE_SIZE;
  ssize_t ret = pread(fd_, page_data, len, page_id * PAGE_SIZE);
  assert_msg(ret > 0, "read failed\n");
}

DiskManager::DiskManager(const char *db_file) {
  int flags = O_CREAT | O_RDWR | O_SYNC | O_TRUNC;
#ifdef DIRECT_IO
  flags |= O_DIRECT;
#endif
  fd_ = open(db_file, flags, S_IRUSR | S_IWUSR);
  file_name_ = std::string(db_file);
  assert_msg(fd_ > 0, "open");
  write_count_.store(0, std::memory_order_relaxed);
  read_count_.store(0, std::memory_order_relaxed);
}

DiskManager::~DiskManager() {
  u64 size = get_file_size();
  (void)size;
  INFO_PRINT(
      "[Storage] Write_count=%8lu read_count=%8lu PAGES fielsize=%8lu PAGES "
      "PAGE= %4d "
      "Bytes\n",
      write_count_.load(), read_count_.load(), size / PAGE_SIZE, PAGE_SIZE);
  int ret = close(fd_);
  assert_msg(!ret, "close");
}
/**
 * Write the contents of the specified page into disk file
 */
void DiskManager::write_page(page_id_t page_id, const char *page_data) {
  write_n_pages(page_id, 1, page_data);
}

/**
 * Read the contents of the specified page into the given memory area
 */
void DiskManager::read_page(page_id_t page_id, char *page_data) {
  read_n_pages(page_id, 1, page_data);
}

u64 DiskManager::get_file_size() {
  struct stat statbuf;
  int ret = fstat(fd_, &statbuf);
  assert_msg(ret != -1, "Failed to get file size");
  return statbuf.st_size;
}

u64 DiskManager::get_read_count() { return read_count_.load(); }
u64 DiskManager::get_write_count() { return write_count_.load(); }

}  // namespace BTree
