#include <libzbd/zbd.h>

#include <cstdint>
#include <iostream>

#include "../zns/zone_device.h"
#include "common.h"

// const std::string DEVICE_NAME = "/dev/nvme2n2";
const uint64_t BUF_SIZE = 4096;

void test_zbdlib_backend() {
  auto zns = new ZbdlibBackend(ZNS_DEVICE);
  unsigned int max_active_zones = 0;
  unsigned int max_open_zones = 0;

  auto status = zns->Open(true, false, &max_active_zones, &max_open_zones);
  (void)status;
  printf("max active zones:%u max open zones:%u data in bytes\n ",
         max_active_zones, max_open_zones);
  auto zone_list = zns->ListZones();
  auto nr_zones = zone_list->ZoneCount();
  auto zones_array = zone_list->GetData();
  for (int i = 0; i < nr_zones; i += 1000) {
    // struct zbd_zone c_zone = zones[i];
    ZbdlibBackend::print_zone_info(((struct zbd_zone *)zones_array + i));
  }
  // std::cout << sizeof(struct zbd_zone) * nr_zones << std::endl; 226kB
  delete zns;
}

void test_zoned_blocked_device() {
  auto zns_block = new ZonedBlockDevice(ZNS_DEVICE);

  bool readonly = false;
  bool exclusive = true;

  auto status = zns_block->Open(readonly, exclusive);
  (void)status;
  printf(
      "recalimable:%8lu free: %s used: %s open:%2ld active:%2ld data in "
      "4096 bytes\n",
      zns_block->GetReclaimableSpace(),
      CalSize(zns_block->GetFreeSpace()).c_str(),
      CalSize(zns_block->GetUsedSpace()).c_str(), zns_block->GetOpenIOZones(),
      zns_block->GetActiveIOZones());

  uint64_t offset = 0;
  Zone *zone = zns_block->GetZoneFromOffset(offset);
  if (zone != nullptr) {
    zone->Print();
    // auto ret = zbd_open_zones(zone, 0, info.zone_size * OPEN_ZONE_NUMS);
    // must align to PAGESIZE 4096
    auto data = (char *)aligned_alloc(ZNS_PAGE_SIZE, ZNS_PAGE_SIZE);
    memset((void *)data, 0x5a, BUF_SIZE);

    zone->Append(data, BUF_SIZE);
    zone->Print();
    free(data);
  }

  // std::cout << sizeof(struct zbd_zone) * nr_zones << std::endl; 226kB
  // delete zone;
  printf("\n");
  zns_block->PrintUsedZones();
  delete zns_block;
  return;
}

int main(int argc, char const *argv[]) {
  test_zoned_blocked_device();
  test_zbdlib_backend();
  return 0;
}
