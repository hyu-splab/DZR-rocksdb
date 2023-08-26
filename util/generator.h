#pragma once
#include <stdint.h>
#include <random>
#include <chrono>
#include <string>
#include <atomic>
#include <bitset>
#include <mutex>
#include <exception>
#include <cstdio>

#include "util/zipf.h"

namespace rocksdb {
namespace util {

enum DBOperation: uint64_t {
  INSERT,
  READ,
  SCAN,
  UPDATE,
  READMODIFYWRITE,
  MAXOPTYPE
};

const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325ull;
const uint64_t kFNVPrime64 = 1099511628211ull;

// uint64_t fnvhash64(uint64_t val);
inline uint64_t FNVHash64(uint64_t val) {
  //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
  uint64_t hashval = kFNVOffsetBasis64;

  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hashval = hashval ^ octet;
    hashval = hashval * kFNVPrime64;
  }
  return hashval;
}

inline uint64_t Hash(uint64_t val) {
  return FNVHash64(val);
}

template<typename Value>
class Generator {
  public:
   virtual Value Next() = 0;
   virtual Value Last() = 0;
   virtual ~Generator() { }
};

class ConstGenerator : public Generator<uint64_t> {
 public:
  ConstGenerator(int constant) : constant_(constant) { }
  uint64_t Next() { return constant_; }
  uint64_t Last() { return constant_; }
 private:
  uint64_t constant_;
};


class CounterGenerator : public Generator<uint64_t> {
 public:
  CounterGenerator(uint64_t start) : counter_(start) { }
  uint64_t Next() { return counter_.fetch_add(1); }
  uint64_t Last() { return counter_.load() - 1; }
 private:
  std::atomic<uint64_t> counter_;
};

class AcknowledgedCounterGenerator : public CounterGenerator {
 public:
  AcknowledgedCounterGenerator(uint64_t start)
      : CounterGenerator(start), limit_(start - 1), ack_window_(kWindowSize, false) {}
  
  uint64_t Last() { return limit_.load(); }

  void Acknowledge(uint64_t value) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t cur_slot = value & kWindowMask;

  if (ack_window_[cur_slot]) {
    abort();
    throw new std::runtime_error("Too many unacknowledged insertion keys.");
  }

  ack_window_[cur_slot] = true;
  uint64_t limit = limit_.load();
  size_t until = limit + kWindowSize;
  size_t i;
  for (i = limit + 1; i < until; i++) {
    size_t slot = i & kWindowMask;
    if (!ack_window_[slot]) {
      break;
    }
    ack_window_[slot] = false;
  }
  limit_.store(i - 1);
}
 private:
  static const size_t kWindowSize = (1 << 16);
  static const size_t kWindowMask = kWindowSize - 1;
  std::atomic<uint64_t> limit_;
  std::vector<bool> ack_window_;
  std::mutex mutex_;
};

double ThreadLocalRandomDouble(double min = 0.0, double max = 1.0) {
  static thread_local std::random_device rd;
  static thread_local std::minstd_rand rn(rd());
  static thread_local std::uniform_real_distribution<double> uniform(min, max);
  return uniform(rn);
}

template <typename Value>
class DiscreteGenerator : public Generator<Value> {
 public:
  DiscreteGenerator() : sum_(0) { }

  void AddValue(Value value, double weight) {
    if (values_.empty()) {
      last_ = value;
    }
    values_.push_back(std::make_pair(value, weight));
    sum_ += weight;
  }

  Value Next() {
    double chooser = ThreadLocalRandomDouble();

    for (auto p = values_.cbegin(); p != values_.cend(); ++p) {
      if (chooser < p->second / sum_) {
      return last_ = p->first;
      }
      chooser -= p->second / sum_;
    }

    assert(false);
    return last_;
  }

  Value Last() { return last_; }

 private:
  std::vector<std::pair<Value, double>> values_;
  double sum_;
  std::atomic<Value> last_;
};



class ZipfianGenerator : public Generator<uint64_t> {
 public:
  static constexpr double kZipfianConst = 0.99;
  static constexpr uint64_t kMaxNumItems = (UINT64_MAX >> 24);

  ZipfianGenerator(uint64_t num_items) :
      ZipfianGenerator(0, num_items - 1) {}

  ZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const = kZipfianConst) :
      ZipfianGenerator(min, max, zipfian_const, Zeta(max - min + 1, zipfian_const)) {}

  ZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const, double zeta_n) :
      items_(max - min + 1), base_(min), theta_(zipfian_const), allow_count_decrease_(false) {
    assert(items_ >= 2 && items_ < kMaxNumItems);

    zeta_2_ = Zeta(2, theta_);

    alpha_ = 1.0 / (1.0 - theta_);
    zeta_n_ = zeta_n;
    count_for_zeta_ = items_;
    eta_ = Eta();

    Next();
  }

  uint64_t Next(uint64_t num) {
    assert(num >= 2 && num < kMaxNumItems);
    if (num != count_for_zeta_) {
      // recompute zeta and eta
      std::lock_guard<std::mutex> lock(mutex_);
      if (num > count_for_zeta_) {
      zeta_n_ = Zeta(count_for_zeta_, num, theta_, zeta_n_);
      count_for_zeta_ = num;
      eta_ = Eta();
      } else if (num < count_for_zeta_ && allow_count_decrease_) {
      // TODO
      }
    }

    double u = ThreadLocalRandomDouble();
    double uz = u * zeta_n_;

    if (uz < 1.0) {
      return last_value_ = base_;
    }

    if (uz < 1.0 + std::pow(0.5, theta_)) {
      return last_value_ = base_ + 1;
    }

    return last_value_ = base_ + num * std::pow(eta_ * u - eta_ + 1, alpha_);
  }

  uint64_t Next() { return Next(items_); }

  uint64_t Last() {
    return last_value_;
  }

 private:
  double Eta() {
    return (1 - std::pow(2.0 / items_, 1 - theta_)) / (1 - zeta_2_ / zeta_n_);
  }

  ///
  /// Calculate the zeta constant needed for a distribution.
  /// Do this incrementally from the last_num of items to the cur_num.
  /// Use the zipfian constant as theta. Remember the new number of items
  /// so that, if it is changed, we can recompute zeta.
  ///
  static double Zeta(uint64_t last_num, uint64_t cur_num, double theta, double last_zeta) {
    double zeta = last_zeta;
    for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
      zeta += 1 / std::pow(i, theta);
    }
    return zeta;
  }

  static double Zeta(uint64_t num, double theta) {
    return Zeta(0, num, theta, 0);
  }

  uint64_t items_;
  uint64_t base_; /// Min number of items to generate

  // Computed parameters for generating the distribution
  double theta_, zeta_n_, eta_, alpha_, zeta_2_;
  uint64_t count_for_zeta_; /// Number of items used to compute zeta_n
  uint64_t last_value_;
  std::mutex mutex_;
  bool allow_count_decrease_;
};

uint32_t ThreadLocalRandomInt() {
  static thread_local std::random_device rd;
  static thread_local std::minstd_rand rn(rd());
  return rn();
}

class RandomByteGenerator : public Generator<char> {
 public:
  RandomByteGenerator() : off_(6) {}

  char Next() {
    if (off_ == 6) {
      int bytes = ThreadLocalRandomInt();
      buf_[0] = static_cast<char>((bytes & 31) + ' ');
      buf_[1] = static_cast<char>(((bytes >> 5) & 63) + ' ');
      buf_[2] = static_cast<char>(((bytes >> 10) & 95) + ' ');
      buf_[3] = static_cast<char>(((bytes >> 15) & 31) + ' ');
      buf_[4] = static_cast<char>(((bytes >> 20) & 63) + ' ');
      buf_[5] = static_cast<char>(((bytes >> 25) & 95) + ' ');
      off_ = 0;
    }
    return buf_[off_++];
  }

  char Last() {
    return buf_[(off_ - 1 + 6) % 6];
  }

 private:
  char buf_[6];
  int off_;
};

class ScrambledZipfianGenerator : public Generator<uint64_t> {
 public:
  ScrambledZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const) :
      base_(min), num_items_(max - min + 1),
      generator_(zipfian_const == kUsedZipfianConstant ?
                    ZipfianGenerator(0, kItemCount, zipfian_const, kZetan) :
                    ZipfianGenerator(0, kItemCount, zipfian_const)) { }

  ScrambledZipfianGenerator(uint64_t min, uint64_t max) :
      ScrambledZipfianGenerator(min, max, ZipfianGenerator::kZipfianConst) { }

  ScrambledZipfianGenerator(uint64_t num_items) :
      ScrambledZipfianGenerator(0, num_items - 1) { }

  uint64_t Next() {
    return Scramble(generator_.Next());
  }

  uint64_t Last() {
    return Scramble(generator_.Last());
  }

 private:
  static constexpr double kUsedZipfianConstant = 0.99;
  static constexpr double kZetan = 26.46902820178302;
  static constexpr uint64_t kItemCount = 10000000000LL;
  const uint64_t base_;
  const uint64_t num_items_;
  ZipfianGenerator generator_;

  uint64_t Scramble(uint64_t value) const {
      return base_ + FNVHash64(value) % num_items_;
  }
};

class SkewedLatestGenerator : public Generator<uint64_t> {
 public:
  SkewedLatestGenerator(CounterGenerator &counter) :
      basis_(counter), zipfian_(basis_.Last()) {
    Next();
  }
  
  uint64_t Next() {
    uint64_t max = basis_.Last();
    return last_ = max - zipfian_.Next(max);
  }

  uint64_t Last() { return last_; }
 private:
  CounterGenerator &basis_;
  ZipfianGenerator zipfian_;
  std::atomic<uint64_t> last_;
};

class UniformGenerator : public Generator<uint64_t> {
 public:
  // Both min and max are inclusive
  UniformGenerator(uint64_t min, uint64_t max) : dist_(min, max) { Next(); }

  uint64_t Next() {
    return last_int_ = dist_(generator_);
  }

  uint64_t Last() {
    return last_int_;
  }

 private:
  std::mt19937_64 generator_;
  std::uniform_int_distribution<uint64_t> dist_;
  uint64_t last_int_;
};

// class Generator {
//  protected:
//   std::mt19937_64 generator_;  
//  public:
//   Generator(uint64_t s): generator_(s) {}

//   virtual ~Generator() {}

//   virtual uint64_t next_val() = 0;
// };

// class ConstGenerator: public Generator {
//   public:
//     ConstGenerator(int value) : value_(value) { }

//   private:
//   uint64_t value_;
// };

// class CounterGenerator: public Generator {
//  private:
//   std::atomic<uint64_t> counter_;

//  public:
//   explicit CounterGenerator(uint64_t count_start)
//     : Generator(0), 
//       counter_(count_start) {}

//   virtual uint64_t next_val() {
//     return counter_.fetch_add(1);
//   }
// };

// class AcknowledgedCounterGenerator: public CounterGenerator {
//  private:
//   static const uint64_t WINDOW_SIZE = (1 << 20);
//   static const uint64_t WINDOW_MASK = (1 << 20) - 1;

//   bool slide_window_[WINDOW_SIZE] = {false};
//   std::mutex window_mutex_;
//   std::atomic<uint64_t> limit_;

//  public:
//   explicit AcknowledgedCounterGenerator(uint64_t count_start)
//     : CounterGenerator(count_start), 
//       limit_(count_start - 1) {}

//   uint64_t last_val() {
//     /*if (limit_.load() % 100000 == 0) {
//       fprintf(stderr, "DEBUG: %lu\n", limit_.load());
//     }*/
//     return limit_.load();
//   }

//   void acknowledge(uint64_t value) {
//     //fprintf(stderr, "DEBUG %lu\n", value);
//     uint64_t current_slot = (uint64_t)(value & WINDOW_MASK);
//     if (slide_window_[current_slot]) {
//       abort();
//       throw new std::runtime_error("Too many unacknowledged insertion keys.");
//     }

//     slide_window_[current_slot] = true;

//     if (window_mutex_.try_lock()) {
//       // move a contiguous sequence from the window
//       // over to the "limit" variable
//       try {
//         // Only loop through the entire window at most once.
//         uint64_t before_first_slot = (limit_.load() & WINDOW_MASK);
//         uint64_t index;
//         for (index = limit_.load() + 1; index != before_first_slot; ++index) {
//           uint64_t slot = (uint64_t)(index & WINDOW_MASK);
//           if (!slide_window_[slot]) {
//             break;
//           }

//           slide_window_[slot] = false;
//         }

//         limit_.store(index - 1);
//       } catch (...) {

//       }
//       window_mutex_.unlock();
//     }
//   }
// };

// class ZipfianGenerator: public Generator {
// protected:
//   AcknowledgedCounterGenerator *trx_insert_keyseq_;
//   uint64_t lb_;
//   uint64_t ub_;
//   uint64_t total_range_;
//   double zipf_factor_;
// public:
// //   ZipfianGenerator(uint64_t items) {
// //     this(0, items);
// //   }

// //   ZipfianGenerator(uint64_t min, uint64_t max) {
// //     this(min, max, ZIPFIAN_CONSTANT);
// //   }

// //   ZipfianGenerator(uint64_t items, double zipfianconstant) {
// //     this(0, items-1, zipfianconstant)
// //   }

//   explicit ZipfianGenerator(uint64_t s, AcknowledgedCounterGenerator *keyseq, uint64_t ub, uint64_t lb=0,  double factor=1.0)
//     : Generator(s), 
//       trx_insert_keyseq_(keyseq), 
//       lb_(lb), 
//       ub_(ub), 
//       total_range_(ub - lb),
//       zipf_factor_(factor) {}

//   virtual uint64_t next_val() {
//     if (trx_insert_keyseq_ != nullptr) {
//       ub_ = trx_insert_keyseq_->last_val();
//       total_range_ = ub_ - lb_;
//     }
//     return lb_ + next(total_range_);
//   }

//   uint64_t next(uint64_t range) {
//     return zipf_distribution<uint64_t, double>(range, zipf_factor_)(generator_);
//   }
// };

// class ScrambledZipfianGenerator: public ZipfianGenerator {
//  public:
//   explicit ScrambledZipfianGenerator(uint64_t s, AcknowledgedCounterGenerator *keyseq, uint64_t u, uint64_t l=0, double factor=1.0)
//     : ZipfianGenerator(s, keyseq, u, l, factor) {}

//   virtual uint64_t next_val() {
//     uint64_t ret = ZipfianGenerator::next_val();
//     return lb_ + fnvhash64(ret) % total_range_;
//   }
// };

// class UnixEpochTimestampGenerator: public Generator {

//   private:
//     long interval_;
//     long timeUnits_;
//     long currentTimestamp_;
//     long lastTimestamp_;

//   public:
//     explicit UnixEpochTimestampGenerator(long interval, long timeUnits, long startTimestamp)
//       : Generator(0),
//         interval_(interval),
//         timeUnits_(timeUnits),
// 	currentTimestamp_(startTimestamp),
// 	lastTimestamp_(startTimestamp){}

//   virtual uint64_t next_val(){
//     lastTimestamp_ = currentTimestamp_;
//     currentTimestamp_ = currentTimestamp_+ (interval_*timeUnits_) ;
//     return currentTimestamp_;
//   }
//   virtual uint64_t last_val(){
//     return lastTimestamp_;
//   }
        

// };




// class UniformGenerator: public Generator {
//  private:
//   AcknowledgedCounterGenerator *trx_insert_keyseq_;
//   uint64_t lb_;
//   uint64_t ub_;

//  public:
//   explicit UniformGenerator(uint64_t s, AcknowledgedCounterGenerator *keyseq, uint64_t u, uint64_t l=0)
//     : Generator(s), 
//       trx_insert_keyseq_(keyseq), 
//       lb_(l), 
//       ub_(u) {};

//   virtual uint64_t next_val() {
//     if (trx_insert_keyseq_) {
//       ub_ = trx_insert_keyseq_->last_val();
//     }
//     return lb_ + std::uniform_int_distribution<uint64_t>(lb_, ub_)(generator_);
//   }
// };

// class DiscreteGenerator: public Generator {
//  private:
//   std::vector<std::pair<DBOperation, double>> values_;

//  public:
//   explicit DiscreteGenerator(uint64_t s): Generator(s) {}

//   virtual uint64_t next_val() {
//     double val = std::uniform_real_distribution<double>(0.0, 1.0)(generator_);

//     for (auto p : values_) {
//       double pw = p.second;
//       if (val < pw) {
//         return static_cast<uint64_t>(p.first);
//       }

//       val -= pw;
//     }

//     throw new std::runtime_error("Should not reach here.");
//   }

//   void add_value(DBOperation value, double weight) {
//     values_.push_back(std::make_pair(value, weight));
//   }
// };

// class SkewedLatestGenerator: public Generator {
//  private:
//   AcknowledgedCounterGenerator *trx_insert_keyseq_;
//   ZipfianGenerator gen_;

//  public:
//   SkewedLatestGenerator(uint64_t s, AcknowledgedCounterGenerator *keyseq, uint64_t u, uint64_t l=0, double factor=1.0)
//     : Generator(s),
//       trx_insert_keyseq_(keyseq),
//       gen_(s, keyseq, u, l, factor) {}

//   uint64_t next_val() {
//     uint64_t max = trx_insert_keyseq_->last_val();
//     return max - gen_.next(max);
//   }
// };


} // namespace util
} // namespace rocksdb
