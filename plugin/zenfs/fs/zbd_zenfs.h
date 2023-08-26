// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <queue>
#include <deque>

#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class ZonedBlockDevice;
class ZonedBlockDeviceBackend;
class ZoneSnapshot;
class ZenFSSnapshotOptions;

class ZoneList {
 private:
  void *data_;
  unsigned int zone_count_;

 public:
  ZoneList(void *data, unsigned int zone_count)
      : data_(data), zone_count_(zone_count){};
  void *GetData() { return data_; };
  unsigned int ZoneCount() { return zone_count_; };
  ~ZoneList() { free(data_); };
};

class Zone {
  ZonedBlockDevice *zbd_;
  ZonedBlockDeviceBackend *zbd_be_;
  std::atomic_bool busy_;
  std::atomic_bool finish_busy_;

  Env *env_;
 public:
  explicit Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
                std::unique_ptr<ZoneList> &zones, unsigned int idx);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  Env::WriteLifeTimeHint lifetime_;
  Env::WriteLifeTimeHint min_lifetime_;
  std::atomic<uint64_t> used_capacity_;
  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  int GetIOZoneID() { return start_/2147483648; }
  
  IOStatus Append(char *data, uint32_t size);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
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
  
  bool FinishLock() {
    bool expected = false;
    return this->finish_busy_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel);
  }

  bool FinishUnlock() {
    bool expected = true;
    return this->finish_busy_.compare_exchange_strong(expected, false,
                                                 std::memory_order_acq_rel);
  }

  bool IsFinishBusy() { return this->finish_busy_.load(std::memory_order_relaxed); }

  void EncodeJson(std::ostream &json_stream);

  IOStatus CheckRelease();
};

class ZonedBlockDeviceBackend {
 public:
  uint32_t block_sz_ = 0;
  uint64_t zone_sz_ = 0;
  uint32_t nr_zones_ = 0;
  uint64_t zone_capacity_ = 0;
 public:
  virtual IOStatus Open(bool readonly, bool exclusive,
                        unsigned int *max_active_zones,
                        unsigned int *max_open_zones) = 0;

  virtual std::unique_ptr<ZoneList> ListZones() = 0;
  virtual IOStatus Reset(uint64_t start, bool *offline,
                         uint64_t *max_capacity) = 0;
  virtual IOStatus Finish(uint64_t start) = 0;
  virtual IOStatus Close(uint64_t start) = 0;
  virtual int Read(char *buf, int size, uint64_t pos, bool direct) = 0;
  virtual int Write(char *data, uint32_t size, uint64_t pos) = 0;
  virtual bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                         unsigned int idx) = 0;
  virtual bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                              unsigned int idx) = 0;
  virtual bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                            unsigned int idx) = 0;
  virtual bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                   unsigned int idx) = 0;
  virtual uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual std::string GetFilename() = 0;
  uint32_t GetBlockSize() { return block_sz_; };
  uint64_t GetZoneSize() { return zone_sz_; };
  uint32_t GetNrZones() { return nr_zones_; };
  virtual ~ZonedBlockDeviceBackend(){};
};

enum class ZbdBackendType {
  kBlockDev,
  kZoneFS,
};

class ZonedBlockDevice {
 public:
  int called_AllocateIOZone = 0;
  int called_AllocateNewZone = 0;
  int called_LIFETIME_DIFF_COULD_BE_WORSE = 0;
  int called_LIFETIME_DIFF_NOT_GOOD = 0;
  int called_FinishCheapestIOZone = 0;
  std::atomic<int> called_ResetUnusedIOZones{0};
  // std::atomic<uint32_t> EZF_lifetime_[6]{0};
  std::atomic<uint32_t> EZF_wait{0};
  std::atomic<uint32_t> EZF_not_wait{0};
  std::atomic<uint32_t> EZF_alloc{0};
  
  // std::deque<std::vector<bool>> finish_req_queue_;
  std::queue<std::vector<bool>> finish_req_queue_;
  std::mutex finish_q_mtx_;
  std::atomic<bool> doing_finish{false};
  std::atomic<int> finish_cnt_table_[6];
  std::mutex finish_mutex_;
  std::mutex open_mutex_;
  std::atomic<uint32_t> num_blocking_{0};
  
  uint32_t finish_count_{0};
  std::vector<Zone *> io_zones;
  std::mutex level_mtx_[4000];
 private:
  std::unique_ptr<ZonedBlockDeviceBackend> zbd_be_;

  std::vector<Zone *> meta_zones;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;
  uint32_t max_background_jobs_ = 0;
  std::atomic<uint64_t> bytes_written_{0};
  std::atomic<uint64_t> gc_bytes_written_{0};

  bool logging_mode;
  //for wait-count when if lifetime
  std::atomic<int64_t> short_cnt_{0};
  std::atomic<int64_t> medium_cnt_{0};
  std::atomic<int64_t> long_cnt_{0};
  std::atomic<int64_t> extreme_cnt_{0};

  std::atomic<bool> ing;

  //for wait-time
  std::atomic<int64_t> short_accumulated_{0}; //2
  std::atomic<int64_t> medium_accumulated_{0}; //3
  std::atomic<int64_t> long_accumulated_{0}; //4
  std::atomic<int64_t> extreme_accumulated_{0}; //5
  std::atomic<int64_t> total_blocking_time_{0};
  Env* env_;

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

  std::shared_ptr<ZenFSMetrics> metrics_;

  uint64_t zone_max_capacity_;

  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);

 public:
  std::atomic<uint64_t> alloc_count_{0};
  std::atomic<uint64_t> alloc_time_{0};
  std::atomic<int64_t> diff_cnt_[200]{0};
  std::atomic<int64_t> n_fixed_zones_[10]{0}; // for static zone allocation
  double blocking_ratio[6] = {0};
  double d_req_zone_num[6] = {0};
  double temp_br[6] = {0};
  int blocking_time[6] = {0};
  int req_zone_num[6] = {0};
  int req_zone_diff[6] = {0};
  unsigned int zone_alloc_mode; // 1: default
  unsigned int num_threads_;
  unsigned int base_zones_num;
  unsigned int reset_cnt;

  // unsigned int finish_elapsed;
  // int finish_cnt;
  std::atomic<unsigned int> finish_elapsed;
  std::atomic<int> finish_req_cnt_;
  std::atomic<bool> victim_zone_group[6];

  std::atomic<uint64_t> finish_elapsed_atomic{0};
  std::atomic<uint32_t> finish_cnt_atomic{0};
  int prev_long_cnt_, prev_extreme_cnt_, curr_long_cnt_, curr_extreme_cnt_;
  unsigned int redist_cnt;
  bool reset_flags_;
  std::atomic<int64_t> global_cnt_;
  bool reset_on_;
  bool debugging_flags_;
  int total_blocking_time = 0;
  std::atomic<uint32_t> zg_ext_{0};

  void ActiveZoneStatus() {
    int num_medium = 0;
    int num_long = 0;
    int num_extreme = 0;

    for (const auto z : io_zones) {
      if (!z->IsFull()) {
        if (z->lifetime_ == 3) {
          num_medium++;
        }

        if (z->lifetime_ == 4) {
          num_long++;
        }

        if (z->lifetime_ == 5) {
          num_extreme++;
        }
      }
    }
    fprintf(stdout, "MEDIUM : %d LONG : %d EXTREME : %d\n", num_medium, num_long, num_extreme);
  };

  int GetCalledAllocateIOZone() { return called_AllocateIOZone; };
  int GetCalledFinishCheapestIOZone() { return called_FinishCheapestIOZone; };

  explicit ZonedBlockDevice(std::string path, ZbdBackendType backend,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics =
                                std::make_shared<NoZenFSMetrics>());
  virtual ~ZonedBlockDevice();

  IOStatus Open(bool readonly, bool exclusive);
  void SetDZRbaseZone();
  unsigned int GetDZRBaseZone();
  
  IOStatus LockLevelMutex(Env::WriteLifeTimeHint file_lifetime);
  IOStatus UnLockLevelMutex(Env::WriteLifeTimeHint file_lifetime);

  Zone *GetIOZone(uint64_t offset);

  IOStatus AllocateIOZone(Env::WriteLifeTimeHint file_lifetime, IOType io_type,
                          Zone **out_zone);
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

  uint64_t GetBlockingTime(int lifetime);
  void ResetBlockingTime();
  uint64_t GetConcurrentFiles(int lifetime);
  uint64_t GetOccupiedZoneNum(int lifetime);
  uint64_t GetMaxIOZones();
  IOStatus AdjustIOZones(int lifetime, int value);
  IOStatus AdjustIOZonesActive(bool *candidate_levels, int value, bool migrate/*, Env::WriteLifeTimeHint file_lifetime, bool *allocated, Zone **out_zone*/);

  void GetPrintReclaimableSpace();
  uint64_t GetLastZoneNum();

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
  void SetMaxBackgroundJobThread(uint32_t max_background_jobs) { max_background_jobs_ = max_background_jobs; }

  void PutOpenIOZoneToken();
  void PutActiveIOZoneToken();

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }

  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

  int Read(char *buf, uint64_t offset, int n, bool direct);

  IOStatus ReleaseMigrateZone(Zone *zone);

  IOStatus TakeMigrateZone(Zone **out_zone, Env::WriteLifeTimeHint lifetime,
                           uint32_t min_capacity);

  void AddBytesWritten(uint64_t written) { bytes_written_ += written; };
  void AddGCBytesWritten(uint64_t written) { gc_bytes_written_ += written; };
  uint64_t GetUserBytesWritten() {
    return bytes_written_.load() - gc_bytes_written_.load();
  };
  uint64_t GetTotalBytesWritten() { return bytes_written_.load(); };

  void ZoneUtilization();
  uint64_t GetZoneCapacity();
 private:
  IOStatus GetZoneDeferredStatus();
  bool GetActiveIOZoneTokenIfAvailable();
  bool GetActiveIOZoneTokenIfAvailableNoLock();
  void WaitForOpenIOZoneToken(bool prioritized);
  IOStatus ApplyFinishThreshold();
  IOStatus FinishCheapestIOZone();
  IOStatus GetBestOpenZoneMatch(Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out, Zone **zone_out, uint32_t min_capacity = 0);
  IOStatus GetDynamicOpenZone(Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out, Zone **zone_out, uint32_t min_capacity = 0);
  IOStatus GetDZROpenZone(Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out, Zone **zone_out, uint32_t min_capacity = 0);
  IOStatus AllocateEmptyZone(Zone **zone_out);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
