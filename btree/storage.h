#pragma once

#include <cstdint>
#include "config.h"

namespace BTree {

class DiskManager {
 public:
  DiskManager() = delete;
  DiskManager(const char *db_file);
  ~DiskManager();

  /**
   * Write the contents of the specified page into disk file
   */
  void write_page(page_id_t page_id, const char *page_data);

  /**
   * Read the contents of the specified page into the given memory area
   */
  void read_page(page_id_t page_id, char *page_data);

 private:
  int fd;

  void write_n_pages(page_id_t page_id, size_t nr_pages, const char *page_data);

  void read_n_pages(page_id_t page_id, size_t nr_pages, char *page_data);
};

extern DiskManager *db_io;

}  // namespace BTree
