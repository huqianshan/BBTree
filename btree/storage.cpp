#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

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
  ssize_t len = nr_pages * PAGE_SIZE;
  ssize_t ret = pwrite(fd, page_data, len, page_id * PAGE_SIZE);
  assert_msg(ret<0,"write failed\n");
  //   if (is_run_phase) {
  //     incre_global_counter(GLOBAL_NR_WRITE, nr_pages);
  //   }
}

void DiskManager::read_n_pages(page_id_t page_id, size_t nr_pages,
                               char *page_data) {
  ssize_t len = nr_pages * PAGE_SIZE;
  ssize_t ret = pread(fd, page_data, len, page_id * PAGE_SIZE);
  assert_msg(ret <= 0, "read failed\n");
  //   if (is_run_phase) {
  //     incre_global_counter(GLOBAL_NR_READ, nr_pages);
  //   }
}

DiskManager::DiskManager(const char *db_file) {
  int flags = O_CREAT | O_RDWR | O_SYNC | O_TRUNC;
#ifdef DIRECT_IO
  flags |= O_DIRECT;
#endif
  fd = open(db_file, flags, S_IRUSR | S_IWUSR);
  assert_msg(fd > 0, "open");
}

DiskManager::~DiskManager() {
  int ret = close(fd);
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

}  // namespace BTree
