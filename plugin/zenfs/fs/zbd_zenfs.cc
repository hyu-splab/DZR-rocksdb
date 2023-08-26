// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include "zbd_zenfs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <semaphore.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "snapshot.h"
#include "zbdlib_zenfs.h"
#include "zonefs_zenfs.h"

#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

namespace ROCKSDB_NAMESPACE {

Zone::Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
           std::unique_ptr<ZoneList> &zones, unsigned int idx)
    : zbd_(zbd),
      zbd_be_(zbd_be),
      busy_(false),
      start_(zbd_be->ZoneStart(zones, idx)),
      max_capacity_(zbd_be->ZoneMaxCapacity(zones, idx)),
      wp_(zbd_be->ZoneWp(zones, idx)) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (zbd_be->ZoneIsWritable(zones, idx))
    capacity_ = max_capacity_ - (wp_ - start_);
  env_ = Env::Default();
  finish_busy_ = false;
}

bool Zone::IsUsed() { return (used_capacity_ > 0); }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}

IOStatus Zone::Reset() {
  bool offline;
  uint64_t max_capacity;

  assert(!IsUsed());
  assert(IsBusy());

  IOStatus ios = zbd_be_->Reset(start_, &offline, &max_capacity);
  if (ios != IOStatus::OK()) return ios;

  if (offline)
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = max_capacity;

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;

  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  assert(IsBusy());

  IOStatus ios = zbd_be_->Finish(start_);
  if (ios != IOStatus::OK()) return ios;

  capacity_ = 0;
  wp_ = start_ + zbd_->GetZoneSize();

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  assert(IsBusy());

  if (!(IsEmpty() || IsFull())) {
    IOStatus ios = zbd_be_->Close(start_);
    if (ios != IOStatus::OK()) return ios;
  }
  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size) {
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_ZONE_WRITE_LATENCY,
                                 Env::Default());
  zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_WRITE_THROUGHPUT, size);
  char *ptr = data;
  uint32_t left = size;
  int ret;

  if (capacity_ < size)
    return IOStatus::NoSpace("Not enough capacity for append");

  assert((size % zbd_->GetBlockSize()) == 0);

  while (left) {
    ret = zbd_be_->Write(ptr, left, wp_);

    if (ret < 0) {
      return IOStatus::IOError(strerror(errno));
    }

    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;
    zbd_->AddBytesWritten(ret);
  }

  return IOStatus::OK();
}

// inline IOStatus Zone::CheckRelease() {
IOStatus Zone::CheckRelease() {
  if (!Release()) {
    assert(false);
    return IOStatus::Corruption("Failed to unset busy flag of zone " +
                                std::to_string(GetZoneNr()));
  }

  return IOStatus::OK();
}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zbd_be_->GetZoneSize()))
      return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string path, ZbdBackendType backend,
                                   std::shared_ptr<Logger> logger,
                                   std::shared_ptr<ZenFSMetrics> metrics)
    : logger_(logger), metrics_(metrics) {
  if (backend == ZbdBackendType::kBlockDev) {
    zbd_be_ = std::unique_ptr<ZbdlibBackend>(new ZbdlibBackend(path));
    Info(logger_, "New Zoned Block Device: %s", zbd_be_->GetFilename().c_str());
  } else if (backend == ZbdBackendType::kZoneFS) {
    zbd_be_ = std::unique_ptr<ZoneFsBackend>(new ZoneFsBackend(path));
    Info(logger_, "New zonefs backing: %s", zbd_be_->GetFilename().c_str());
  }

  /*
     @ type 1 : default zenFS
     @ type 6 : group zone allocation + DZR
  */
  debugging_flags_ = false;
  reset_cnt = 0;
  zone_alloc_mode = 6;
  redist_cnt = 0;
  reset_flags_ = false;
  reset_on_ = true;
  finish_req_cnt_ = 0;
  finish_elapsed = 0;
  global_cnt_ = 0;
  prev_long_cnt_ = prev_extreme_cnt_ = curr_long_cnt_ = curr_extreme_cnt_ = 0;

  logging_mode = false;
  env_ = Env::Default();
  ing = false;

  if (zone_alloc_mode == 6) {  // default zone settings for DZR
    n_fixed_zones_[0] = 2;     // Metadata - Lifetime 0
                               // None     - Lifetime 1
    n_fixed_zones_[2] = 1;     // WAL      - Lifetime 2
    n_fixed_zones_[3] = 4;     // Short    - Lifetime 3
    n_fixed_zones_[4] = 4;     // Long     - Lifetime 4
    n_fixed_zones_[5] = 1;     // Extreme  - Lifetime 5
  }
  short_cnt_ = 0;
  medium_cnt_ = 0;
  long_cnt_ = 0;
  extreme_cnt_ = 0;
}

IOStatus ZonedBlockDevice::Open(bool readonly, bool exclusive) {
  std::unique_ptr<ZoneList> zone_rep;
  unsigned int max_nr_active_zones;
  unsigned int max_nr_open_zones;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  // Reserve one zone for metadata and another one for extent migration
  int reserved_zones = 2;

  if (!readonly && !exclusive)
    return IOStatus::InvalidArgument("Write opens must be exclusive");

  IOStatus ios = zbd_be_->Open(readonly, exclusive, &max_nr_active_zones,
                               &max_nr_open_zones);
  if (ios != IOStatus::OK()) return ios;

  if (zbd_be_->GetNrZones() < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported("To few zones on zoned backend (" +
                                  std::to_string(ZENFS_MIN_ZONES) +
                                  " required)");
  }

  if (max_nr_active_zones == 0)
    max_nr_active_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_active_io_zones_ = max_nr_active_zones - reserved_zones;

  if (max_nr_open_zones == 0)
    max_nr_open_io_zones_ = zbd_be_->GetNrZones();
  else
    max_nr_open_io_zones_ = max_nr_open_zones - reserved_zones;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       zbd_be_->GetNrZones(), max_nr_active_zones, max_nr_open_zones);

  zone_rep = zbd_be_->ListZones();
  if (zone_rep == nullptr || zone_rep->ZoneCount() != zbd_be_->GetNrZones()) {
    Error(logger_, "Failed to list zones");
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_META_ZONES && i < zone_rep->ZoneCount()) {
    /* Only use sequential write required zones */
    if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        meta_zones.push_back(new Zone(this, zbd_be_.get(), zone_rep, i));
      }
      m++;
    }
    i++;
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (; i < zone_rep->ZoneCount(); i++) {
    /* Only use sequential write required zones */
    if (zbd_be_->ZoneIsSwr(zone_rep, i)) {
      if (!zbd_be_->ZoneIsOffline(zone_rep, i)) {
        Zone *newZone = new Zone(this, zbd_be_.get(), zone_rep, i);
        if (!newZone->Acquire()) {
          assert(false);
          return IOStatus::Corruption("Failed to set busy flag of zone " +
                                      std::to_string(newZone->GetZoneNr()));
        }
        io_zones.push_back(newZone);
        if (zbd_be_->ZoneIsActive(zone_rep, i)) {
          active_io_zones_++;
          if (zbd_be_->ZoneIsOpen(zone_rep, i)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
        IOStatus status = newZone->CheckRelease();
        if (!status.ok()) {
          return status;
        }
      }
    }
  }

  start_time_ = time(NULL);

  return IOStatus::OK();
}

void ZonedBlockDevice::SetDZRbaseZone() {
  /* DZR based zone settings */

  assert(max_background_jobs_ != 0);
  fprintf(stdout, "SetDZRbaseZone, max_background_jobs_: %d\n", max_background_jobs_);

  num_threads_ = max_background_jobs_;
  int num_zones_groups = 3;
  base_zones_num = std::min(GetMaxIOZones() / num_threads_,
                            GetMaxIOZones() / num_zones_groups);

  std::cout << "[DZR status] " << base_zones_num << " = min(" << GetMaxIOZones()
            << "/" << num_threads_ << ", " << GetMaxIOZones() << "/"
            << num_zones_groups << ")\n";
}

unsigned int ZonedBlockDevice::GetDZRBaseZone() { return base_zones_num; }

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetOccupiedZoneNum(int lifetime) {
  return n_fixed_zones_[lifetime];
}

uint64_t ZonedBlockDevice::GetMaxIOZones() {
  return max_nr_active_io_zones_ - n_fixed_zones_[0] - n_fixed_zones_[2];
}

IOStatus ZonedBlockDevice::AdjustIOZones(int lifetime, int value) {
  level_mtx_[lifetime].lock();

  if (value < 0) {
    Zone *candidate_zone = nullptr;
    IOStatus s;
    int64_t candidate_capacity;
    int used_zone_num = 0;

    for (const auto z : io_zones) {
      if (z->lifetime_ == lifetime && !z->IsFull()) {
        used_zone_num++;
      }
    }

    if (n_fixed_zones_[lifetime] + value >= used_zone_num) {
      n_fixed_zones_[lifetime] += value;
    } else {
      int target = (value * -1) - (n_fixed_zones_[lifetime] - used_zone_num);

      for (int i = 1; i <= target; i++) {
        candidate_capacity = -1;
        candidate_zone = nullptr;

        for (const auto z : io_zones) {
          if (z->lifetime_ == lifetime && !z->IsFull()) {
            if (candidate_capacity == -1 ||
                candidate_capacity > (int64_t)z->GetCapacityLeft()) {
              candidate_zone = z;
              candidate_capacity = z->GetCapacityLeft();
            }
          }
        }
        if (candidate_zone == nullptr) {
          std::cout << "[AdjustIOZones] candidate not found!\n";
          break;
        }
        while (!candidate_zone->Acquire());
        if (!candidate_zone->IsFull()) {
          s = candidate_zone->Finish();
          if (!s.ok()) {
            candidate_zone->Release();
          }
          s = candidate_zone->CheckRelease();
          if (!s.ok()) {
          }
          PutActiveIOZoneToken();
        }
      }
      n_fixed_zones_[lifetime] += value;
    }
  } else {
    n_fixed_zones_[lifetime] += value;
  }

  level_mtx_[lifetime].unlock();

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::AdjustIOZonesActive(bool *candidate_levels,
                                               int value, bool migrate) {
  Zone *candidate_zone = nullptr;
  Zone *acquired_candidate_zone = nullptr;
  IOStatus s;
  int64_t candidate_capacity;

  if (value > 0) return IOStatus::OK();
  while (true) {
    candidate_capacity = -1;
    candidate_zone = nullptr;

    acquired_candidate_zone = nullptr;

    for (const auto z : io_zones) {
      if (candidate_levels[z->lifetime_] && !z->IsFull()) {
        if (candidate_capacity == -1 ||
            candidate_capacity >
                (int64_t)z->GetCapacityLeft()) {

          candidate_zone = z;
          candidate_capacity = z->GetCapacityLeft();

          if (z->Acquire()) {
            if (acquired_candidate_zone != nullptr) {
              acquired_candidate_zone->Release();
            }
            acquired_candidate_zone = z;
          }
        }
      }
    }

    if (candidate_zone == nullptr) {
      std::cout << "[DZR Status] candidate not found! " << migrate << "\n";
      break;
    }

    if (acquired_candidate_zone != nullptr) {
      candidate_zone = acquired_candidate_zone;
    } else {
      while (!candidate_zone->Acquire());
    }
    if (!candidate_zone->IsFull()) {
      s = candidate_zone->Finish();
      if (!s.ok()) {
        std::cout << "[AdjustIOZones] finish failed\n";
        candidate_zone->Release();
      }
      s = candidate_zone->CheckRelease();
      if (!s.ok()) {
        std::cout << "[AdjustIOZones] release failed\n";
      }

      PutActiveIOZoneToken();
    }
    break;
  }

  return IOStatus::OK();
}

uint64_t ZonedBlockDevice::GetBlockingTime(int lifetime) {
  uint64_t blocking_time_ = 0;
  if (lifetime == 3) {
    blocking_time_ = medium_accumulated_;
  } else if (lifetime == 4) {
    blocking_time_ = long_accumulated_;
  } else {
    blocking_time_ = extreme_accumulated_;
  }
  return blocking_time_;
}
void ZonedBlockDevice::ResetBlockingTime() {
  medium_accumulated_ = long_accumulated_ = extreme_accumulated_ = 0;
}

uint64_t ZonedBlockDevice::GetConcurrentFiles(int lifetime) {
  uint64_t num_files = 0;
  if (lifetime == 3) {
    num_files = short_cnt_;
  } else if (lifetime == 4) {
    num_files = long_cnt_;
  } else {
    num_files = extreme_cnt_;
  }
  return (num_files == 0) ? 0 : 1;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
  }
  return reclaimable;
}

void ZonedBlockDevice::GetPrintReclaimableSpace() {
  uint64_t used = 0;

  for (const auto z : meta_zones) {
    used = z->used_capacity_;
    fprintf(stdout, "%lu\n", used / (1024 * 1024));
  }

  for (const auto z : io_zones) {
    used = z->used_capacity_;
    fprintf(stdout, "%lu\n", used / (1024 * 1024));
  }
}

uint64_t ZonedBlockDevice::GetLastZoneNum() {
  uint64_t last = 0;
  for (auto it = io_zones.rbegin(); it != io_zones.rend(); ++it) {
    Zone *z = *it;
    if (z->IsFull() || z->IsUsed() || !z->IsEmpty()) {
      last = z->GetIOZoneID();
      break;
    }
  }
  return last;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
  }
}

void ZonedBlockDevice::LogGarbageInfo() {
  // Log zone garbage stats vector.
  //
  // The values in the vector represents how many zones with target garbage
  // percent. Garbage percent of each index: [0%, <10%, < 20%, ... <100%, 100%]
  // For example `[100, 1, 2, 3....]` means 100 zones are empty, 1 zone has less
  // than 10% garbage, 2 zones have  10% ~ 20% garbage ect.
  //
  // We don't need to lock io_zones since we only read data and we don't need
  // the result to be precise.
  int zone_gc_stat[12] = {0};
  for (auto z : io_zones) {
    if (!z->Acquire()) {
      continue;
    }

    if (z->IsEmpty()) {
      zone_gc_stat[0]++;
      z->Release();
      continue;
    }

    double garbage_rate =
        double(z->wp_ - z->start_ - z->used_capacity_) / z->max_capacity_;
    assert(garbage_rate > 0);
    int idx = int((garbage_rate + 0.1) * 10);
    zone_gc_stat[idx]++;

    z->Release();
  }

  std::stringstream ss;
  ss << "Zone Garbage Stats: [";
  for (int i = 0; i < 12; i++) {
    ss << zone_gc_stat[i] << " ";
  }
  ss << "]";
  Info(logger_, "%s", ss.str().data());
}

ZonedBlockDevice::~ZonedBlockDevice() {
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }
}
uint64_t ZonedBlockDevice::GetZoneCapacity() { return zbd_be_->zone_capacity_; }

void ZonedBlockDevice::ZoneUtilization() {
  uint32_t occupied = 0;
  int zone_capacity = GetZoneCapacity();
  int zidx = 3;  // io zones start from zone index 3
  for (const auto zone : io_zones) {
    if (!zone->IsEmpty()) {
      uint64_t valid = zone->used_capacity_;
      double util = (double)valid / (double)zone_capacity;
      fprintf(stdout, "%d %lu %lf\n", zidx, valid, util);
      occupied += 1;
    }
    zidx += 1;
  }
  fprintf(stdout, "# of occupied zones : %u\n", occupied);
}

#define LIFETIME_DIFF_NOT_GOOD (100)
#define LIFETIME_DIFF_COULD_BE_WORSE (50)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;
  if (zone_lifetime == file_lifetime) return LIFETIME_DIFF_COULD_BE_WORSE;

  return LIFETIME_DIFF_NOT_GOOD;
}

IOStatus ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone) {
  assert(out_meta_zone);
  *out_meta_zone = nullptr;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_META_ALLOC_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_META_ALLOC_QPS, 1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
          continue;
        }
        *out_meta_zone = z;
        return IOStatus::OK();
      }
    }
  }
  assert(true);
  Error(logger_, "Out of metadata zones, we should go to read only now.");
  return IOStatus::NoSpace("Out of metadata zones");
}

IOStatus ZonedBlockDevice::ResetUnusedIOZones() {
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (!z->IsEmpty() && !z->IsUsed()) {
        called_ResetUnusedIOZones++;
        bool full = z->IsFull();
        IOStatus reset_status = z->Reset();
        IOStatus release_status = z->CheckRelease();
        if (!reset_status.ok()) return reset_status;
        if (!release_status.ok()) return release_status;
        if (!full) PutActiveIOZoneToken();

      } else {
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
      }
    }
  }
  return IOStatus::OK();
}

void ZonedBlockDevice::WaitForOpenIOZoneToken(bool prioritized) {  // if wal -> true
  long allocator_open_limit;

  /* Avoid non-priortized allocators from starving prioritized ones */
  if (prioritized) {
    allocator_open_limit = max_nr_open_io_zones_;
  } else {
    allocator_open_limit = max_nr_open_io_zones_ - 1;
  }

  /* Wait for an open IO Zone token - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutOpenIOZoneToken to return the resource
   */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  zone_resources_.wait(lk, [this, allocator_open_limit] {
    if (open_io_zones_.load() < allocator_open_limit) {
      open_io_zones_++;
      return true;
    } else {
      return false;
    }
  });
}

bool ZonedBlockDevice::GetActiveIOZoneTokenIfAvailable() {
  /* Grap an active IO Zone token if available - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutActiveIOZoneToken to return the resource
   */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  if (active_io_zones_.load() < max_nr_active_io_zones_) {
    active_io_zones_++;
    return true;
  }
  return false;
}

void ZonedBlockDevice::PutOpenIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    open_io_zones_--;
  }
  zone_resources_.notify_one();
}

void ZonedBlockDevice::PutActiveIOZoneToken() {
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    active_io_zones_--;
  }
  zone_resources_.notify_one();
}

IOStatus ZonedBlockDevice::ApplyFinishThreshold() {
  IOStatus s;

  if (finish_threshold_ == 0) return IOStatus::OK();

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      bool within_finish_threshold =
          z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100);
      if (!(z->IsEmpty() || z->IsFull()) && within_finish_threshold) {
        /* If there is less than finish_threshold_% remaining capacity in a
         * non-open-zone, finish the zone */
        s = z->Finish();
        if (!s.ok()) {
          z->Release();
          Debug(logger_, "Failed finishing zone");
          return s;
        }
        s = z->CheckRelease();
        if (!s.ok()) return s;
        PutActiveIOZoneToken();
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::FinishCheapestIOZone() {
  IOStatus s;
  Zone *finish_victim = nullptr;
  called_FinishCheapestIOZone++;

  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty() || z->IsFull()) {
        s = z->CheckRelease();
        if (!s.ok()) return s;
        continue;
      }
      if (finish_victim == nullptr) {
        finish_victim = z;
        continue;
      }
      if (finish_victim->capacity_ > z->capacity_) {
        s = finish_victim->CheckRelease();
        if (!s.ok()) return s;
        finish_victim = z;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  // If all non-busy zones are empty or full, we should return success.
  if (finish_victim == nullptr) {
    Info(logger_, "All non-busy zones are empty or full, skip.");
    return IOStatus::OK();
  }

  s = finish_victim->Finish();
  IOStatus release_status = finish_victim->CheckRelease();

  if (s.ok()) {
    PutActiveIOZoneToken();
  }

  if (!release_status.ok()) {
    return release_status;
  }

  return s;
}

IOStatus ZonedBlockDevice::GetBestOpenZoneMatch(
    Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out,
    Zone **zone_out, uint32_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;

  for (const auto z : io_zones) {
    if (z->Acquire()) {  // if not busy
      if ((z->used_capacity_ > 0) && !z->IsFull() &&
          z->capacity_ >= min_capacity) {
        unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
        if (diff <= best_diff) {
          if (allocated_zone != nullptr) {  // if not first
            s = allocated_zone->CheckRelease();  // release previous allocated zone
            if (!s.ok()) {
              IOStatus s_ = z->CheckRelease();
              if (!s_.ok()) return s_;
              return s;
            }
          }
          // first allocation {
          allocated_zone = z;  // allocate new zone
          best_diff = diff;
          //}
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  *best_diff_out = best_diff;
  *zone_out = allocated_zone;

  return IOStatus::OK();
}

std::string time_in_HH_MM_SS_MMM() {
  using namespace std::chrono;

  // get current time
  auto now = system_clock::now();

  // get number of milliseconds for the current second
  // (remainder after division into seconds)
  auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

  // convert to std::time_t in order to convert to std::tm (broken time)
  auto timer = system_clock::to_time_t(now);

  // convert to broken time
  std::tm bt = *std::localtime(&timer);

  std::ostringstream oss;

  oss << std::put_time(&bt, "%H:%M:%S");  // HH:MM:SS
  oss << '.' << std::setfill('0') << std::setw(3) << ms.count();

  return oss.str();
}

IOStatus ZonedBlockDevice::LockLevelMutex(Env::WriteLifeTimeHint file_lifetime) {
  if (true) {  // for tracing concurrent write files
    if (file_lifetime == 2) {
      short_cnt_++;
    } else if (file_lifetime == 3) {
      medium_cnt_++;
    } else if (file_lifetime == 4) {
      long_cnt_++;
    } else if (file_lifetime == 5) {
      extreme_cnt_++;
    }
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::UnLockLevelMutex(Env::WriteLifeTimeHint file_lifetime) {
  if (true) {  // for tracing concurrent write files
    if (file_lifetime == 2) {
      short_cnt_--;
    } else if (file_lifetime == 3) {
      medium_cnt_--;
    } else if (file_lifetime == 4) {
      long_cnt_--;
    } else if (file_lifetime == 5) {
      extreme_cnt_--;
    }
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::GetDynamicOpenZone(
    Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out,
    Zone **zone_out, uint32_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;
  int n_fixed_zones = 0;  // the number of fixed zone
  int n_skip_zones = 0;
  n_fixed_zones = n_fixed_zones_[file_lifetime];
  int b_begin = 0;
  int b_end = 0;
  int b_diff = 0;

  while (true) {
    n_skip_zones = 0;
    for (const auto z : io_zones) {
      if (z->lifetime_ == file_lifetime && !z->IsFull() &&
          z->capacity_ >= min_capacity) {
        if (z->Acquire()) {
          if (z->GetCapacityLeft() < (z->max_capacity_ * 0.1)) {
            s = z->Finish();
            if (s.ok()) s = z->CheckRelease();
            if (s.ok()) PutActiveIOZoneToken();
            continue;
          } else {
            allocated_zone = z;
            best_diff = 0;
            break;
          }
        } else {
          n_skip_zones++;
          if (n_skip_zones == n_fixed_zones) {
            break;
          }
        }
      }
    }

    if (n_skip_zones < n_fixed_zones) {
      break;
    }
    b_begin = env_->NowMicros();
  }

  b_end = env_->NowMicros();
  if (b_begin != 0) {
    b_diff = b_end - b_begin;
  }

  best_diff = b_diff;  // blocking time diff
  *best_diff_out = best_diff;
  *zone_out = allocated_zone;

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::GetDZROpenZone(Env::WriteLifeTimeHint file_lifetime,
                                          unsigned int *best_diff_out,
                                          Zone **zone_out,
                                          uint32_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;
  int n_fixed_zones = 0;  // the number of fixed zone
  int n_skip_zones = 0;
  n_fixed_zones = n_fixed_zones_[file_lifetime];
  int b_begin = 0;
  int b_end = 0;
  int b_diff = 0;
  int migration = min_capacity != 0;
  std::atomic<bool> req_finish = false;
  bool do_finish = false;

  while (true) {
    n_skip_zones = 0;
    int used_levels[10] = {0};
    int64_t least_capacity = -1;

    for (const auto z : io_zones) {
      if (!z->IsFull() && z->capacity_ >= min_capacity && z->lifetime_ == file_lifetime) {
        if (z->Acquire()) {
          if (least_capacity == -1) {
            least_capacity = z->capacity_;
            allocated_zone = z;
            best_diff = 0;
          } else if (least_capacity > (int64_t)z->capacity_) {
            allocated_zone->CheckRelease();
            least_capacity = z->capacity_;
            allocated_zone = z;
            best_diff = 0;
          }  else {
            z->CheckRelease();
          }
        } else {
          n_skip_zones++;
        }
      }
    }

    if (allocated_zone != nullptr) {
      break;
    }

    if (n_skip_zones < n_fixed_zones) {  // 2-1
      for (const auto z : io_zones) {
        if (!z->IsFull() /*&& z->capacity_ >= min_capacity*/) {
          used_levels[z->lifetime_]++;
        }
      }
      int used_zones = used_levels[3] + used_levels[4] + used_levels[5];

      if (used_zones >= (int)GetMaxIOZones() && req_finish == false) {
        do_finish = true;
        if (used_levels[file_lifetime] + finish_cnt_table_[file_lifetime] >= n_fixed_zones) {
          goto block;
        }

        if (debugging_flags_) {
          std::cout << "[Zone Reduction Trigger-LEVEL" << file_lifetime
                    << "] MEDIUM:" << used_levels[3]
                    << ", LONG:" << used_levels[4]
                    << ", EXTREME:" << used_levels[5] << "\n";
        }

        std::vector<bool> candidates(6);
        for (int i = 3; i <= 5; i++) {
          if (i == file_lifetime) continue;
          if (used_levels[i] > n_fixed_zones_[i]) {
            candidates[i] = true;
          }
        }

        finish_q_mtx_.lock();
        finish_req_queue_.push(candidates);
        finish_q_mtx_.unlock();
        req_finish = true;
        finish_cnt_table_[file_lifetime]++;
        finish_req_cnt_++;
      }
    block:
      int total_used = 0;
      open_mutex_.lock();
      for (const auto z : io_zones) {
        if (!z->IsFull() /*&& z->capacity_ >= min_capacity*/ &&
            (z->lifetime_ == 3 || z->lifetime_ == 4 || z->lifetime_ == 5)) {
          total_used++;
        }
      }
      if (allocated_zone == nullptr && total_used < (int)GetMaxIOZones() && GetActiveIOZoneTokenIfAvailable()) {  // 2-2-1
        s = AllocateEmptyZone(&allocated_zone);
        allocated_zone->lifetime_ = file_lifetime;
        open_mutex_.unlock();
        break;
      }
      open_mutex_.unlock();
    }
    
    if (b_begin == 0 && do_finish == false && migration == false) {
      b_begin = env_->NowMicros();
      num_blocking_++;
    }
  }

  b_end = env_->NowMicros();
  if (b_begin != 0) {
    b_diff = b_end - b_begin;
  }
  
  best_diff = b_diff;  // blocking time diff
  *best_diff_out = best_diff;
  *zone_out = allocated_zone;

  return IOStatus::OK();
}


IOStatus ZonedBlockDevice::AllocateEmptyZone(Zone **zone_out) {
  IOStatus s;
  Zone *allocated_zone = nullptr;
  for (const auto z : io_zones) {
    if (z->Acquire()) {
      if (z->IsEmpty()) {
        allocated_zone = z;
        break;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }
  *zone_out = allocated_zone;
  return IOStatus::OK();
}

int ZonedBlockDevice::Read(char *buf, uint64_t offset, int n, bool direct) {
  int ret = 0;
  int left = n;
  int r = -1;

  while (left) {
    r = zbd_be_->Read(buf, left, offset, direct);
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ret += r;
    buf += r;
    left -= r;
    offset += r;
  }

  if (r < 0) return r;
  return ret;
}

IOStatus ZonedBlockDevice::ReleaseMigrateZone(Zone *zone) {
  IOStatus s = IOStatus::OK();
  {
    std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
    migrating_ = false;
    if (zone != nullptr) {
      s = zone->CheckRelease();
      Info(logger_, "ReleaseMigrateZone: %lu", zone->start_);
    }
  }
  migrate_resource_.notify_one();
  return s;
}

IOStatus ZonedBlockDevice::TakeMigrateZone(Zone **out_zone,
                                           Env::WriteLifeTimeHint file_lifetime,
                                           uint32_t min_capacity) {
  std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
  migrate_resource_.wait(lock, [this] { return !migrating_; });

  migrating_ = true;
  IOStatus s;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;

  if (zone_alloc_mode == 1) {
    s = GetBestOpenZoneMatch(file_lifetime, &best_diff, out_zone, min_capacity);
  } else if (zone_alloc_mode == 6) {
    level_mtx_[file_lifetime].lock();
    s = GetDZROpenZone(file_lifetime, &best_diff, out_zone, min_capacity);
    level_mtx_[file_lifetime].unlock();
  }

  if (s.ok() && (*out_zone) != nullptr) {
    Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
  } else {
    migrating_ = false;
  }

  return s;
}

IOStatus ZonedBlockDevice::AllocateIOZone(Env::WriteLifeTimeHint file_lifetime,
                                          IOType io_type, Zone **out_zone) {
  Zone *allocated_zone = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  IOStatus s;
  called_AllocateIOZone++;

  auto tag = ZENFS_WAL_IO_ALLOC_LATENCY;
  if (io_type != IOType::kWAL) {
    // L0 flushes have lifetime MEDIUM
    if (file_lifetime == Env::WLTH_MEDIUM) {
      tag = ZENFS_L0_IO_ALLOC_LATENCY;
    } else {
      tag = ZENFS_NON_WAL_IO_ALLOC_LATENCY;
    }
  }

  ZenFSMetricsLatencyGuard guard(metrics_, tag, Env::Default());
  metrics_->ReportQPS(ZENFS_IO_ALLOC_QPS, 1);

  if (zone_alloc_mode >= 4 && file_lifetime > 2) {
    level_mtx_[file_lifetime].lock();
  }

  // Check if a deferred IO error was set
  s = GetZoneDeferredStatus();
  if (!s.ok()) {
    return s;
  }

  if (io_type != IOType::kWAL) {  // If finish_threashold > remaining_capacity

    s = ApplyFinishThreshold();
    if (!s.ok()) {
      return s;
    }
  }

  WaitForOpenIOZoneToken(io_type == IOType::kWAL);  // Waiting for new token (max open zones limit)

  /*
     @ type 1 : default zenFS
     @ type 6 : new DZR!
  */

  if (zone_alloc_mode == 1) {
    uint64_t start = 0;
    uint64_t elapsed = 0;
    if (file_lifetime > 2) {
      start = env_->NowMicros();
      alloc_count_ += 1;
    }
    /* Try to fill an already open zone(with the best life time diff) */
    s = GetBestOpenZoneMatch(file_lifetime, &best_diff, &allocated_zone);
    if (!s.ok()) {
      PutOpenIOZoneToken();
      return s;
    }

    if (best_diff >= LIFETIME_DIFF_COULD_BE_WORSE) {  // level diff too big or equal?
      /* COULD_BE_WORSE = 50, if file_lifetime == zone_lifetime*/
      if (best_diff == LIFETIME_DIFF_COULD_BE_WORSE)
        called_LIFETIME_DIFF_COULD_BE_WORSE++;
      if (best_diff == LIFETIME_DIFF_NOT_GOOD) called_LIFETIME_DIFF_NOT_GOOD++;

      // uint64_t start = env_->NowMicros();
      bool got_token = GetActiveIOZoneTokenIfAvailable();

      /* If we did not get a token, try to use the best match, even if the life
       * time diff not good but a better choice than to finish an existing zone
       * and open a new one
       */
      if (allocated_zone != nullptr) {
        if (!got_token && best_diff == LIFETIME_DIFF_COULD_BE_WORSE) {
          Debug(logger_,
                "Allocator: avoided a finish by relaxing lifetime diff "
                "requirement\n");
        } else {
          s = allocated_zone->CheckRelease();
          if (!s.ok()) {
            PutOpenIOZoneToken();
            if (got_token) PutActiveIOZoneToken();
            return s;
          }
          allocated_zone = nullptr;
        }
      }

      /* If we haven't found an open zone to fill, open a new zone */
      if (allocated_zone == nullptr) {
        /* We have to make sure we can open an empty zone */
        while (!got_token && !GetActiveIOZoneTokenIfAvailable()) {
          uint64_t begin = env_->NowMicros();
          s = FinishCheapestIOZone();
          uint64_t end = env_->NowMicros() - begin;
          finish_cnt_atomic += 1;
          finish_elapsed_atomic += end;
          if (!s.ok()) {
            PutOpenIOZoneToken();
            return s;
          }
        }

        s = AllocateEmptyZone(&allocated_zone);
        if (!s.ok()) {
          PutActiveIOZoneToken();
          PutOpenIOZoneToken();
          return s;
        }

        if (allocated_zone != nullptr) {
          assert(allocated_zone->IsBusy());
          allocated_zone->lifetime_ = file_lifetime;
          allocated_zone->min_lifetime_ = file_lifetime;
          new_zone = true;
        } else {
          PutActiveIOZoneToken();
        }
      }
    }

    if (file_lifetime > 2) {
      elapsed = env_->NowMicros() - start;
      alloc_time_ += elapsed;
    }
    if (allocated_zone->min_lifetime_ > file_lifetime) {
      allocated_zone->min_lifetime_ = file_lifetime;
    }

    int diff_min = file_lifetime - allocated_zone->min_lifetime_;
    int diff_max = allocated_zone->lifetime_ - file_lifetime;
    int diff_result = 0;
    if (diff_min > diff_max) {
      diff_result = diff_min;
    } else {
      diff_result = diff_max;
    }
    diff_cnt_[diff_result]++;

    if (io_type == IOType::kWAL) {
      if (allocated_zone->lifetime_ != file_lifetime) {
      } else {
      }
    }

  } else if (zone_alloc_mode == 6) {  // zone group allocation + DZR
    uint64_t start = 0;
    uint64_t elapsed = 0;

    if (file_lifetime > 2) {
      start = env_->NowMicros();
      alloc_count_ += 1;
    }

    if (file_lifetime <= 2) {  // for WAL
      s = GetDynamicOpenZone(file_lifetime, &best_diff, &allocated_zone);

      if (allocated_zone == nullptr) {
        if (!GetActiveIOZoneTokenIfAvailable()) {
          std::cout << "WAL token not available!\n";
        }

        s = AllocateEmptyZone(&allocated_zone);
        allocated_zone->lifetime_ = file_lifetime;
      }

    } else {  // for SST files
      s = GetDZROpenZone(file_lifetime, &best_diff, &allocated_zone);
    }

    if (!s.ok()) {
      PutOpenIOZoneToken();
      return s;
    }

    if (file_lifetime > 2) {
      elapsed = env_->NowMicros() - start;
      alloc_time_ += elapsed;
    }

    std::atomic<uint64_t> elapsed_micros = best_diff;

    if (file_lifetime == 2) {
      short_accumulated_ += elapsed_micros;
    } else if (file_lifetime == 3) {
      medium_accumulated_ += elapsed_micros;
    } else if (file_lifetime == 4) {
      long_accumulated_ += elapsed_micros;
    } else {
      extreme_accumulated_ += elapsed_micros;
    }
    if (!ing && (long_accumulated_ || medium_accumulated_ || extreme_accumulated_) && (file_lifetime > 2)) {
      ing = true;

      int remain_zones = 0;
      blocking_time[3] = medium_accumulated_,
      blocking_time[4] = long_accumulated_,
      blocking_time[5] = extreme_accumulated_;
      total_blocking_time = blocking_time[3] + blocking_time[4] + blocking_time[5];

      remain_zones = GetMaxIOZones();
      int max_zone_group = 0;
      int max_req_zone = 0;

      for (int i = 3; i <= 5; i++) {
        if (blocking_time[i] != 0) {
          blocking_ratio[i] = (double)blocking_time[i] / (double)total_blocking_time;
        } else {
          blocking_ratio[i] = 0;
        }

        d_req_zone_num[i] = blocking_ratio[i] * (double)remain_zones;
        req_zone_num[i] = blocking_ratio[i] * remain_zones;

        if (max_zone_group == 0) {
          max_zone_group = i;
          max_req_zone = req_zone_num[i];
        } else {
          if (max_req_zone < req_zone_num[i]) {
            max_zone_group = i;
            max_req_zone = req_zone_num[i];
          }
        }
      }

      remain_zones = GetMaxIOZones() - req_zone_num[3] - req_zone_num[4] - req_zone_num[5];

      for (int i = 3; i <= 5; i++) {
        if (req_zone_num[i] == 0) {
          if (remain_zones > 0) {
            req_zone_num[i] += 1;
            remain_zones -= 1;
          } else {
            req_zone_num[i] += 1;
            req_zone_num[max_zone_group] -= 1;
          }
        }
      }

      assert(req_zone_num[3] != 0 || req_zone_num[4] != 0 || req_zone_num[5] != 0);

      if (remain_zones > 0) {
        double m_ratio = 1 - ((double)req_zone_num[3] / d_req_zone_num[3]);
        double l_ratio = 1 - ((double)req_zone_num[4] / d_req_zone_num[4]);
        double e_ratio = 1 - ((double)req_zone_num[5] / d_req_zone_num[5]);

        while (remain_zones--) {
          double max_ratio = std::max({m_ratio, l_ratio, e_ratio});

          if (max_ratio == m_ratio) {
            req_zone_num[3] += 1;
            m_ratio = 0;
          } else if (max_ratio == l_ratio) {
            req_zone_num[4] += 1;
            l_ratio = 0;
          } else if (max_ratio == e_ratio) {
            req_zone_num[5] += 1;
            e_ratio = 0;
          } else {
            exit(1);
          }
        }
      }

      for (int i = 3; i <= 5; i++) {
        req_zone_diff[i] = req_zone_num[i] - n_fixed_zones_[i];
      }

      if (req_zone_diff[3] || req_zone_diff[4] || req_zone_diff[5]) {
        for (int i = 5; i >= 3; i--) {
          n_fixed_zones_[i] += req_zone_diff[i];
          finish_cnt_table_[i] = 0;
        }

        finish_q_mtx_.lock();
        while (!finish_req_queue_.empty())
          finish_req_queue_.pop();
        finish_q_mtx_.unlock();

        redist_cnt++;

        if (debugging_flags_) {
          std::cout << "[Group-Redist] # RDIST/RESET:" << redist_cnt << "/"
                    << reset_cnt << ",  MEDIUM:" << n_fixed_zones_[3]
                    << ", LONG:" << n_fixed_zones_[4]
                    << ", EXTREME:" << n_fixed_zones_[5]
                    << ", FINISH_CNT:" << finish_count_ << "\n";
          
        }
        if (reset_on_ && reset_flags_) {
          ResetBlockingTime();
          reset_flags_ = false;
          reset_cnt++;
          prev_long_cnt_ = prev_extreme_cnt_ = 0;
        }
      }

      if (reset_on_ && !reset_flags_) {
        uint64_t temp_long_cnt_ = GetConcurrentFiles(4);
        uint64_t temp_extreme_cnt_ = GetConcurrentFiles(5);

        if (prev_long_cnt_ == 0 && prev_extreme_cnt_ == 0) {
          prev_long_cnt_ = temp_long_cnt_;
          prev_extreme_cnt_ = temp_extreme_cnt_;

        } else {
          bool xor_long = prev_long_cnt_ ^ temp_long_cnt_;
          bool xor_extreme = prev_extreme_cnt_ ^ temp_extreme_cnt_;

          if (xor_long || xor_extreme) {
            reset_flags_ = true;
          }
          prev_long_cnt_ = temp_long_cnt_;
          prev_extreme_cnt_ = temp_extreme_cnt_;
        }
      }

      ing = false;
    }

  } else {  // what the hell

  }

  if (allocated_zone) {
    assert(allocated_zone->IsBusy());
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
  } else {
    PutOpenIOZoneToken();
  }

  if (io_type != IOType::kWAL) {
    LogZoneStats();
  }

  if (zone_alloc_mode >= 4 && file_lifetime > 2) {
    level_mtx_[file_lifetime].unlock();
  }

  *out_zone = allocated_zone;
  auto now_time =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

  if (false || (!logging_mode && false)) {  // 5 -> debugging mode
    std::cout << "[" << std::this_thread::get_id() << "]"
              << " file_liftime: " << file_lifetime
              << ", zone_lifetime: " << allocated_zone->lifetime_
              << ", zone_nr: " << allocated_zone->GetZoneNr()
              << ", active_io_zone: " << active_io_zones_
              << ", open_io_zone: " << open_io_zones_ << "  "
              << std::ctime(&now_time) << "\n";
  }

  if (true && false) {  // hit-ratio
    int n_io_zones = 0;
    int n_zones_each[6] = {0};
    for (const auto z : io_zones) {
      if (z->IsUsed() && !z->IsFull()) {
        n_io_zones++;
        n_zones_each[z->lifetime_]++;
      }
    }
  }

  metrics_->ReportGeneral(ZENFS_OPEN_ZONES_COUNT, open_io_zones_);
  metrics_->ReportGeneral(ZENFS_ACTIVE_ZONES_COUNT, active_io_zones_);

  return IOStatus::OK();
}

std::string ZonedBlockDevice::GetFilename() { return zbd_be_->GetFilename(); }

uint32_t ZonedBlockDevice::GetBlockSize() { return zbd_be_->GetBlockSize(); }

uint64_t ZonedBlockDevice::GetZoneSize() { return zbd_be_->GetZoneSize(); }

uint32_t ZonedBlockDevice::GetNrZones() { return zbd_be_->GetNrZones(); }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream,
                                      const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, meta_zones);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones);
  json_stream << "}";
}

IOStatus ZonedBlockDevice::GetZoneDeferredStatus() {
  std::lock_guard<std::mutex> lock(zone_deferred_status_mutex_);
  return zone_deferred_status_;
}

void ZonedBlockDevice::SetZoneDeferredStatus(IOStatus status) {
  std::lock_guard<std::mutex> lk(zone_deferred_status_mutex_);
  if (!zone_deferred_status_.ok()) {
    zone_deferred_status_ = status;
  }
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot) {
  for (auto *zone : io_zones) {
    snapshot.emplace_back(*zone);
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
