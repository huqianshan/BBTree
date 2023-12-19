
#pragma once
#include "assert.h"
#include "zone_backend.h"

#define KB (1024)
#define MB (1024 * KB)

#define DIM(x) (sizeof(x) / sizeof(*(x)))

static const char *sizes[] = {"EiB", "PiB", "TiB", "GiB", "MiB", "KiB", "B"};
const uint64_t exbibytes =
    1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL;

std::string CalSize(uint64_t size);

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (0)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

class ZonedBlockDevice;

class Zone {
  ZonedBlockDevice *zbd_;
  ZonedBlockDeviceBackend *zbd_be_;
  std::atomic_bool busy_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
                std::unique_ptr<ZoneList> &zones, unsigned int idx);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  //   Env::WriteLifeTimeHint lifetime_;
  std::atomic<uint64_t> used_capacity_;

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size);
  IOStatus Read(char *data, uint32_t size, uint64_t offset, bool direct = true);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  // bool IsOffline();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  bool IsBusy() { return this->busy_.load(std::memory_order_relaxed); }
  bool Acquire() {
    bool expected = false;
    return this->busy_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel);
  }
  bool Release() {
    bool expected = true;
    return this->busy_.compare_exchange_strong(expected, false,
                                               std::memory_order_acq_rel);
  }

  void EncodeJson(std::ostream &json_stream);
  void Print();
  void Print_zbd();

  inline IOStatus CheckRelease();
};

class ZonedBlockDevice {
 private:
  std::unique_ptr<ZonedBlockDeviceBackend> zbd_be_;
  std::vector<Zone *> io_zones;
  std::vector<Zone *> meta_zones;
  time_t start_time_;
  //   std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;
  std::atomic<uint64_t> bytes_written_{0};
  std::atomic<uint64_t> gc_bytes_written_{0};

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  /* Protects zone_resuorces_  condition variable, used
     for notifying changes in open_io_zones_ */
  std::mutex zone_resources_mtx_;
  std::condition_variable zone_resources_;
  std::mutex zone_deferred_status_mutex_;
  IOStatus zone_deferred_status_;

  std::condition_variable migrate_resource_;
  std::mutex migrate_zone_mtx_;
  std::atomic<bool> migrating_{false};

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  //   std::shared_ptr<ZenFSMetrics> metrics_;

  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);

 public:
  explicit ZonedBlockDevice(std::string path);
  virtual ~ZonedBlockDevice();

  IOStatus Open(bool readonly, bool exclusive);

  Zone *GetIOZone(uint64_t offset);
  Zone *GetIOZoneFromOffset(uint64_t offset);
  //   IOStatus AllocateIOZone(Env::WriteLifeTimeHint file_lifetime, IOType
  //   io_type, Zone **out_zone);
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

  std::string GetFilename();
  uint32_t GetBlockSize();

  IOStatus ResetUnusedIOZones();
  void LogZoneStats();
  void LogZoneUsage();
  void LogGarbageInfo();

  uint64_t GetZoneSize();
  uint32_t GetNrZones();
  std::vector<Zone *> GetMetaZones() { return meta_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void PutOpenIOZoneToken();
  void PutActiveIOZoneToken();

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  //   std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }

  //   void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

  int Read(char *buf, uint64_t offset, int n, bool direct);
  IOStatus InvalidateCache(uint64_t pos, uint64_t size);

  IOStatus ReleaseMigrateZone(Zone *zone);

  //   IOStatus TakeMigrateZone(Zone **out_zone, Env::WriteLifeTimeHint
  //   lifetime,
  //    uint32_t min_capacity);

  void AddBytesWritten(uint64_t written) {
    bytes_written_.fetch_add(written, std::memory_order_relaxed);
  };
  void AddGCBytesWritten(uint64_t written) {
    gc_bytes_written_.fetch_add(written, std::memory_order_relaxed);
  };
  uint64_t GetUserBytesWritten() {
    return bytes_written_.load() - gc_bytes_written_.load();
  };
  uint64_t GetTotalBytesWritten() { return bytes_written_.load(); };
  uint64_t GetActiveIOZones() { return active_io_zones_.load(); };
  uint64_t GetOpenIOZones() { return open_io_zones_.load(); };

  void PrintUsedZones();

  //  private:
  IOStatus GetZoneDeferredStatus();
  bool GetActiveIOZoneTokenIfAvailable();
  void WaitForOpenIOZoneToken(bool prioritized);
  IOStatus ApplyFinishThreshold();
  IOStatus FinishCheapestIOZone();
  //   IOStatus GetBestOpenZoneMatch(Env::WriteLifeTimeHint file_lifetime,
  // unsigned int *best_diff_out, Zone **zone_out,
  // uint32_t min_capacity = 0);
  IOStatus AllocateEmptyZone(Zone **zone_out);
};