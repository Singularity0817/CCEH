#ifndef KV_UTIL_TRACE_H
#define KV_UTIL_TRACE_H

#include <stdint.h>
#include <vector>
#include <thread>
#include <cmath>
#include <cassert>
#include <algorithm>
#include "./src2/util/slice.h"

#include "geninfo.h"

namespace util {
const uint64_t kRAND64_MAX = ((((uint64_t)RAND_MAX) << 31) + ((uint64_t)RAND_MAX));
const double   kRAND64_MAX_D = ((double)(kRAND64_MAX));
const uint64_t kRANDOM_RANGE = UINT64_C(2000000000000);

const uint64_t kYCSB_SEED = 1729; 
const uint64_t kYCSB_LATEST_SEED = 1089; 

enum YCSBLoadType {kYCSB_A, kYCSB_B, kYCSB_C, kYCSB_D, kYCSB_E, kYCSB_F};
enum YCSBOpType {kYCSB_Write, kYCSB_Read, kYCSB_Query, kYCSB_ReadModifyWrite};

struct YCSB_Op {
  YCSBOpType type;
  uint64_t key;
};

class Trace {
public:

  Trace(int seed):seed_(seed), init_(seed), gi_(nullptr){
    if (seed_ == 0) {
      seed_ = random();
    }
  }
  
  virtual ~Trace() {if(gi_ != nullptr) delete gi_;}
  virtual uint64_t Next() = 0;
  void Reset() {seed_ = init_;}
  uint32_t Random() {
    static thread_local const uint32_t M = 2147483647L;   // 2^31-1
    static thread_local const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if ((uint32_t)seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  inline uint64_t Random64() {
    // 62 bit random value;
    const uint64_t rand64 = (((uint64_t)Random()) << 31) + ((uint64_t)Random());
    return rand64;
  }

  inline double RandomDouble() {
    // random between 0.0 - 1.0
    const double r = (double)Random64();
    const double rd = r / kRAND64_MAX_D;
    return rd;
  }

  int seed_;
  int init_;
  GenInfo* gi_;

};

class TraceSeq: public Trace {
public:
  explicit TraceSeq(uint64_t start_off = 0, uint64_t interval = 1, uint64_t minimum = 0, uint64_t maximum = kRANDOM_RANGE): Trace(0) {
    start_off_ = start_off;
    interval_  = interval;
    min_ = minimum;
    max_ = maximum;
    cur_ = start_off_;
  }
  inline uint64_t Next() override {
    cur_ += interval_;
    if (cur_ >= max_) 
      cur_ = 0;
    return cur_;
  }
private:
  uint64_t start_off_;
  uint64_t interval_;
  uint64_t min_;
  uint64_t max_;
  uint64_t cur_;
};
class TraceUniform: public Trace {
public:
  explicit TraceUniform(int seed, uint64_t minimum = 0, uint64_t maximum = kRANDOM_RANGE);
  ~TraceUniform() {}
  uint64_t Next() override;
  
};

class TraceZipfian: public Trace {
public:
  explicit TraceZipfian(int seed, uint64_t minimum = 0, uint64_t maximum = UINT64_C(0xc0000000000));
  ~TraceZipfian() {}
  uint64_t Next() override;
  uint64_t NextRaw();
  double Zeta(const uint64_t n, const double theta);
  double ZetaRange(const uint64_t start, const uint64_t count, const double theta);
  uint64_t FNVHash64(const uint64_t value);

private:
  uint64_t zetalist_u64[17] = {0,
    UINT64_C(0x4040437dd948c1d9), UINT64_C(0x4040b8f8009bce85),
    UINT64_C(0x4040fe1121e564d6), UINT64_C(0x40412f435698cdf5),
    UINT64_C(0x404155852507a510), UINT64_C(0x404174d7818477a7),
    UINT64_C(0x40418f5e593bd5a9), UINT64_C(0x4041a6614fb930fd),
    UINT64_C(0x4041bab40ad5ec98), UINT64_C(0x4041cce73d363e24),
    UINT64_C(0x4041dd6239ebabc3), UINT64_C(0x4041ec715f5c47be),
    UINT64_C(0x4041fa4eba083897), UINT64_C(0x4042072772fe12bd),
    UINT64_C(0x4042131f5e380b72), UINT64_C(0x40421e53630da013),
  };

  double* zetalist_double = (double*)zetalist_u64;
  uint64_t zetalist_step = UINT64_C(0x10000000000);
  uint64_t zetalist_count = 16;
  // double zetalist_theta = 0.99;
  uint64_t range_;
};


class TraceExponential: public Trace {
public:
  #define FNV_OFFSET_BASIS_64 ((UINT64_C(0xCBF29CE484222325)))
  #define FNV_PRIME_64 ((UINT64_C(1099511628211)))
  explicit TraceExponential(int seed, const double percentile = 50, double range = kRANDOM_RANGE);
  ~TraceExponential()  {}
  uint64_t Next() override;
private:
  uint64_t range_;
};


class TraceExponentialReverse: public Trace {
public:
  #define FNV_OFFSET_BASIS_64 ((UINT64_C(0xCBF29CE484222325)))
  #define FNV_PRIME_64 ((UINT64_C(1099511628211)))
  explicit TraceExponentialReverse(int seed, const double percentile = 50, double range = kRANDOM_RANGE);
  ~TraceExponentialReverse()  {}
  uint64_t Next() override;
private:
  uint64_t range_;
};


class TraceNormal: public Trace {
public:
  explicit TraceNormal(int seed, uint64_t minimum = 0, uint64_t maximum = kRANDOM_RANGE);
  ~TraceNormal() {}
  uint64_t Next() override;
};

void RandomSequence(uint64_t num, std::vector<uint64_t>& sequence);
// generate ycsb workload. For workload D, we need insertion order sequence: ycsb_insertion_sequence
std::vector<YCSB_Op> YCSB_LoadGenerate(int64_t range, uint64_t max_num, YCSBLoadType type, Trace* selector, const std::vector<uint64_t>& ycsb_insertion_sequence);

// https://stackoverflow.com/questions/4351371/c-performance-challenge-integer-to-stdstring-conversion
struct itostr_helper;
std::string itostr(int64_t o);

class RandomString {
public:
  RandomString(util::Trace* trace):
    trace_(trace) {}
  inline std::string next() {
		return itostr(trace_->Next());
  }
private:
  util::Trace* trace_;
};



int ShuffleA(int i) {
  static Trace* trace = new TraceUniform(142857);
  return trace->Next() % i;
}

int ShuffleB(int i) {
  static Trace* trace = new TraceUniform(285714);
  return trace->Next() % i;
}

int ShuffleC(int i) {
  static Trace* trace = new TraceUniform(428571);
  return trace->Next() % i;
}

int ShuffleD(int i) {
  static Trace* trace = new TraceUniform(571428);
  return trace->Next() % i;
}

int ShuffleE(int i) {
  static Trace* trace = new TraceUniform(714285);
  return trace->Next() % i;
}

int ShuffleF(int i) {
  static Trace* trace = new TraceUniform(857142);
  return trace->Next() % i;
}

static TraceUniform sequence_shuffle(12345678);
int ShuffleSeq(int i) {
    return sequence_shuffle.Next() % i;
}

void RandomSequence(uint64_t num, std::vector<uint64_t>& sequence){
    sequence.resize(num);
    for (uint64_t i = 0; i < num; ++i) {
      sequence[i] = i;
    }
    sequence_shuffle.Reset(); // make sure everytime we generate the same random sequence
    std::random_shuffle(sequence.begin(), sequence.end(), ShuffleSeq);
}

// uniform
TraceUniform::TraceUniform(int seed, uint64_t minimum, uint64_t maximum):
    Trace(seed) {
    gi_ = new GenInfo();
    gi_->gen.uniform.min = minimum;
    gi_->gen.uniform.max = maximum;
    gi_->gen.uniform.interval = (double)(maximum - minimum);
    gi_->type = GEN_UNIFORM;
}

uint64_t TraceUniform::Next() {
    uint64_t off = (uint64_t)(RandomDouble() * gi_->gen.uniform.interval);
    return gi_->gen.uniform.min + off;
}
// zipfian
TraceZipfian::TraceZipfian(int seed, uint64_t minimum, uint64_t maximum):
    Trace(seed), range_(maximum) {
    gi_ = new GenInfo();
    struct GenInfo_Zipfian * const gz = &(gi_->gen.zipfian);
    
    const uint64_t items = maximum - minimum + 1;
    gz->nr_items = items;
    gz->base = minimum;
    gz->zipfian_constant = ZIPFIAN_CONSTANT;
    gz->theta = ZIPFIAN_CONSTANT;
    gz->zeta2theta = Zeta(2, ZIPFIAN_CONSTANT);
    gz->alpha = 1.0 / (1.0 - ZIPFIAN_CONSTANT);
    double zetan = Zeta(items, ZIPFIAN_CONSTANT);
    gz->zetan = zetan;
    gz->eta = (1.0 - std::pow(2.0 / (double)items, 1.0 - ZIPFIAN_CONSTANT)) / (1.0 - (gz->zeta2theta / zetan));
    gz->countforzeta = items;
    gz->min = minimum;
    gz->max = maximum;

    gi_->type = GEN_ZIPFIAN;
}

double TraceZipfian::Zeta(const uint64_t n, const double theta) {
    // assert(theta == zetalist_theta);
    const uint64_t zlid0 = n / zetalist_step;
    const uint64_t zlid = (zlid0 > zetalist_count) ? zetalist_count : zlid0;
    const double sum0 = zetalist_double[zlid];
    const uint64_t start = zlid * zetalist_step;
    const uint64_t count = n - start;
    assert(n > start);
    const double sum1 = ZetaRange(start, count, theta);
    return sum0 + sum1;
}

double TraceZipfian::ZetaRange(const uint64_t start, const uint64_t count, const double theta) {
    double sum = 0.0;
    if (count > 0x10000000) {
        fprintf(stderr, "zeta_range would take a long time... kill me or wait\n");
    }
    for (uint64_t i = 0; i < count; i++) {
        sum += (1.0 / pow((double)(start + i + 1), theta));
    }
    return sum;
}

uint64_t TraceZipfian::FNVHash64(const uint64_t value) {
    uint64_t hashval = FNV_OFFSET_BASIS_64;
    uint64_t val = value;
    for (int i = 0; i < 8; i++)
    {
        const uint64_t octet=val & 0x00ff;
        val = val >> 8;
        // FNV-1a
        hashval = (hashval ^ octet) * FNV_PRIME_64;
    }
    return hashval;
}

uint64_t TraceZipfian::NextRaw() {
// simplified: no increamental update
    const GenInfo_Zipfian *gz = &(gi_->gen.zipfian);
    const double u = RandomDouble();
    const double uz = u * gz->zetan;
    if (uz < 1.0) {
        return gz->base + 0lu;
    } else if (uz < (1.0 + pow(0.5, gz->theta))) {
        return gz->base + 1lu;
    }
    const double x = ((double)gz->nr_items) * pow(gz->eta * (u - 1.0) + 1.0, gz->alpha);
    const uint64_t ret = gz->base + (uint64_t)x;
    return ret;
}

uint64_t TraceZipfian::Next() {
    // ScrambledZipfian. scatters the "popular" items across the itemspace.
    const uint64_t z = NextRaw();
    const uint64_t xz = gi_->gen.zipfian.min + (FNVHash64(z) % gi_->gen.zipfian.nr_items);
    return xz % range_;
}

// exponential
TraceExponential::TraceExponential(int seed, const double percentile, double range):
    Trace(seed), range_(range) {
    range = range * 0.15;
    gi_ = new GenInfo();
    gi_->gen.exponential.gamma = - log(1.0 - (percentile/100.0)) / range;

    gi_->type = GEN_EXPONENTIAL;
}

uint64_t TraceExponential::Next() {
    uint64_t d = (uint64_t)(- log(RandomDouble()) / gi_->gen.exponential.gamma) % range_;
    return d;
}


// exponential reverse
TraceExponentialReverse::TraceExponentialReverse(int seed, const double percentile, double range):
    Trace(seed), range_(range) {
    range = range * 0.15;
    gi_ = new GenInfo();
    gi_->gen.exponential.gamma = - log(1.0 - (percentile/100.0)) / range;

    gi_->type = GEN_EXPONENTIAL;
}

uint64_t TraceExponentialReverse::Next() {
    uint64_t d = range_ - ((uint64_t)(- log(RandomDouble()) / gi_->gen.exponential.gamma) % range_);
    return d;
}

// ===================================================
// = Normal Distribution Reference                   =
// = https://www.johndcook.com/blog/cpp_phi_inverse/ =
// ===================================================
TraceNormal::TraceNormal(int seed, uint64_t minimum, uint64_t maximum):
    Trace(seed) {
    gi_ = new GenInfo();
    gi_->gen.normal.min = minimum;
    gi_->gen.normal.max = maximum;
    gi_->gen.normal.mean = (maximum + minimum) / 2;
    gi_->gen.normal.stddev = (maximum - minimum) / 4;
    gi_->type = GEN_NORMAL;
}

uint64_t TraceNormal::Next() {
    double p;
    double val = 0;
    double random = 0;
    do {
      p = RandomDouble();
      if (p < 0.8)
      {
          // F^-1(p) = - G^-1(p)
          double t = sqrt(-2.0 * log(p));
          random = -(t - ((0.010328 * t + 0.802853) * t + 2.515517) / 
                (((0.001308 * t + 0.189269) * t + 1.432788) * t + 1.0));
      }
      else {
        double t = sqrt(-2.0 * log(1 - p));
        random = t - ((0.010328 * t + 0.802853)*t + 2.515517) / 
                (((0.001308 * t + 0.189269) * t + 1.432788) * t + 1.0);
      }
      val = (random + 5) * (gi_->gen.normal.max - gi_->gen.normal.min) / 10;
    }while(val < gi_->gen.normal.min || val > gi_->gen.normal.max);

    return  val;
}

// using given distribution trace to generate ycsb workload
// range:   key range
// max_num: number of operations
std::vector<YCSB_Op> YCSB_LoadGenerate(int64_t range, uint64_t max_num, YCSBLoadType type, Trace* trace, const std::vector<uint64_t>& ycsb_insertion_sequence) {
    std::vector<YCSB_Op> res;
    switch (type) {
        case kYCSB_A: {
            // 50% reads
            for (uint64_t i = 0; i < max_num / 2; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type = kYCSB_Read;
                res.push_back(ops);
            }
            // 50% updates(writes)
            for (uint64_t i = 0; i < max_num / 2; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type = kYCSB_Write;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleA);
            break;
        }
        
        case kYCSB_B: {
            // B: 95% reads, 5% writes
            uint64_t R95 = max_num * 0.95;
            uint64_t W5  = max_num - R95;
            for (uint64_t i = 0; i < R95; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Read;
                res.push_back(ops);
            }
            for (uint64_t i = 0; i < W5; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Write;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleB);
            break;
        }
        

        case kYCSB_C: {
            // 100% reads
            for (uint64_t i = 0; i < max_num; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Read;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleC);
            break;
        }

        case kYCSB_D: {
            // D: operate on latest inserted records
            uint64_t R95 = max_num * 0.95;
            uint64_t W5  = max_num - R95;
            for (uint64_t i = 0; i < R95; ++i) {
                YCSB_Op ops;
                ops.key = ycsb_insertion_sequence[trace->Next() % range];
                ops.type= kYCSB_Read;
                res.push_back(ops);
            }
            for (uint64_t i = 0; i < W5; ++i) {
                YCSB_Op ops;
                ops.key = ycsb_insertion_sequence[trace->Next() % range];
                ops.type= kYCSB_Write;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleD);
            break;
        }
       
        case kYCSB_E: {
            // 95% range queries, 5% writes
            // 
            uint64_t R95 = max_num * 0.95;
            uint64_t W5  = max_num - R95;
            for (uint64_t i = 0; i < R95; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Query;
                // number of scan is uniform distribution
                res.push_back(ops);
            }
            for (uint64_t i = 0; i < W5; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type= kYCSB_Write;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleE);
            break;
        }
        

        case kYCSB_F: {
            // 50% reads
            for (uint64_t i = 0; i < max_num / 2; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type = kYCSB_Read;
                res.push_back(ops);
            }
            // 50% read-modified-writes
            for (uint64_t i = 0; i < max_num / 2; ++i) {
                YCSB_Op ops;
                ops.key = trace->Next() % range;
                ops.type = kYCSB_ReadModifyWrite;
                res.push_back(ops);
            }
            std::random_shuffle(res.begin(), res.end(), ShuffleF);
            break;
        }

        default: {
            perror("Not a valid ycsb workload type.\n");
        }
    }
    if (res.size() != max_num) {
        perror("YCSB workload size incorrect\n");
        abort();
    }
    return res;
}


struct itostr_helper {
	static __attribute__ ((aligned(1024))) unsigned out[10000];

	itostr_helper() {
		for (int i = 0; i < 10000; i++) {
			unsigned v = i;
			char * o = (char*)(out + i);
			o[3] = v % 10 + '0';
			o[2] = (v % 100) / 10 + '0';
			o[1] = (v % 1000) / 100 + '0';
			o[0] = (v % 10000) / 1000;
			if (o[0]) o[0] |= 0x30;
			else if (o[1] != '0') o[0] |= 0x20;
			else if (o[2] != '0') o[0] |= 0x10;
			else o[0] |= 0x00;
		}
	}
} hlp_init;
unsigned itostr_helper::out[10000];

std::string itostr(int64_t o) {
	typedef itostr_helper hlp;
    static unsigned blocks[3];
	unsigned *b = blocks + 2;
	blocks[0] = o < 0 ? ~o + 1 : o;
	blocks[2] = blocks[0] % 10000; blocks[0] /= 10000;
	blocks[2] = hlp::out[blocks[2]];

	if (blocks[0]) {
		blocks[1] = blocks[0] % 10000; blocks[0] /= 10000;
		blocks[1] = hlp::out[blocks[1]];
		blocks[2] |= 0x30303030;
		b--;
	}

	if (blocks[0]) {
		blocks[0] = hlp::out[blocks[0] % 10000];
		blocks[1] |= 0x30303030;
		b--;
	}

	char* f = ((char*)b);
	f += 3 - (*f >> 4);

	char* str = (char*)blocks;
	if (o < 0) *--f = '-';

	str += 12;
    return std::string(f, str);
}

}
    // hopman_fast


#endif
