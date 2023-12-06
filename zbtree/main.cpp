#include <iostream>

#include "../include/rwlatch.h"

int main() {
  ReaderWriterLatch latch;
  std::cout << "Size of ReaderWriterLatch: " << sizeof(latch) << " bytes"
            << std::endl;
  return 0;
}
