#pragma once
#include <unistd.h>
#include <vector>
#include <algorithm>
#include <thread>         // std::thread
#include <mutex>          // std::mutex
#include <condition_variable> // std::condition_variable
#include <stdlib.h>
#include <random>
#include <sys/time.h>

#include "trace.h"
#include "./src2/util/histogram.h"
using namespace std;
//#define SPEED_UP_MODE_TEST
#define ONE_SECOND 1

#define DURATION_MODE
#define SEPERATE_RW_MODE

const int           FLAGS_readtime = 600; // read time for readwhilewriting test for read thread
const int           FLAGS_writetime = 20;
const int           FLAGS_rwdelay = 1;   // delay between each write in us
const int           FLAGS_sleep = 30;     // write thread delay FLAGS_sleep second to start
const int           FLAGS_speedupmodedelay = 5; // delay to enable speed up mode in s
const int           FLAGS_delaybetweenbenchmarks = 10; // in s

const unsigned      FLAGS_get_lat_threshold_up = 1800;
const unsigned      FLAGS_get_lat_threshold_down = 1500;

const int           FLAGS_value_size = 8;
const size_t        FLAGS_num = 1000*1000*1000;//10000000;
const size_t        FLAGS_ycsb_op_num = FLAGS_num / 8 / 10;
const size_t        FLAGS_rwtest_get_num = 250*1000;
const size_t        FLAGS_rwtest_op_num = 20*1000*1000 / 8;
const size_t        FLAGS_reads = 1000000000;//10000000;
const int           FLAGS_thread = 8;
const size_t        FLAGS_stats_interval = 1000*1000;
const size_t        FLAGS_report_interval = 10;          // Report interval in 0.1 seconds. if set to 0, we use FLAGS_stats_interval 
const std::string   FLAGS_benchmarks = "ycsb_load,rwtest,rwtest";//,rwtest,rwtest,rwtest"; //ycsb_d,ycsb_a,ycsb_b,ycsb_c";

const double        FLAGS_get_ratio = 0.25;

namespace util {

inline uint64_t GetTimeNsec()
{
    struct timespec nowtime;
    clock_gettime(CLOCK_REALTIME, &nowtime);
    return nowtime.tv_sec * 1000000000 + nowtime.tv_nsec;
}
/**
 *  Note: When using this trace generator, we should do diffrent operation based on 
 *        the YCSB_op type
 * if (operation.type == kYCSB_Write) {
      res = db_->Insert...
    } else if (operation.type == kYCSB_Read) {
      res = db_->Read...
    } else if (operation.type == kYCSB_ReadModifyWrite) {
        res = db_->Read...
        if (res == true) {
          db_->Insert...
        }
    }
 * 
*/

class YCSBGenerator {

// ---------------- Public Function ----------------
public:
    YCSBGenerator(size_t range_min, size_t range_max, size_t count):
        min_(range_min),
        max_(range_max),
        seed_(31415926),
        trace_(seed_, min_, max_)
    {
        keys_.reserve(count);
        keys_.resize(count);

        // initial keys sequence using provided trace
        for (size_t i = 0; i < count; ++i) {
            keys_[i] = trace_.Next();
            /*
            if ((i & 0xFFFFF) == 0) {
                fprintf(stderr, "Generate keys: %.1f %%\r", (double)i/count*100.0);
            }
            */
        }
        re_ = default_random_engine(time(0));
        u_ = uniform_int_distribution<size_t>(min_, max_);
    }

    inline size_t Next() {
        return trace_.Next();
    }

    inline size_t Next_rand() {
        return u_(re_);
    }

    inline void Reset_trace() {
        seed_ = ((seed_ + 5) * 3 + 7);
        trace_ = TraceUniform(seed_, min_, max_);
    }

    const std::vector<size_t>& InsertionSequence() {
        // return the insertion sequence
        return keys_;
    }

    std::vector<YCSB_Op> Sequence_ycsba(size_t count) {
        // ycsba: 50% reads, 50% writes
        std::vector<YCSB_Op> res;

        // 50% reads
        for (uint64_t i = 0; i < count / 2; ++i) {
            YCSB_Op ops;
            ops.key = trace_.Next();
            ops.type = kYCSB_Read;
            res.push_back(ops);
        }

        // 50% updates(writes)
        for (uint64_t i = 0; i < count / 2; ++i) {
            YCSB_Op ops;
            ops.key = trace_.Next();
            ops.type = kYCSB_Write;
            res.push_back(ops);
        }
        std::random_shuffle(res.begin(), res.end(), ShuffleA);
        return res;
    }

    std::vector<YCSB_Op> Sequence_ycsbb(size_t count) {
        // ycsbb: 95% reads, 5% writes
        std::vector<YCSB_Op> res;
        uint64_t R95 = count * 0.95;
        uint64_t W5  = count - R95;

        // 95% reads
        for (uint64_t i = 0; i < R95; ++i) {
            YCSB_Op ops;
            ops.key = trace_.Next();
            ops.type= kYCSB_Read;
            res.push_back(ops);
        }

        // 5% writes
        for (uint64_t i = 0; i < W5; ++i) {
            YCSB_Op ops;
            ops.key = trace_.Next();
            ops.type= kYCSB_Write;
            res.push_back(ops);
        }
        std::random_shuffle(res.begin(), res.end(), ShuffleB);
        return res;
    }

    std::vector<YCSB_Op> Sequence_ycsbc(size_t count) {
        // ycsbc: 100% reads
        std::vector<YCSB_Op> res;
        for (uint64_t i = 0; i < count; ++i) {
            YCSB_Op ops;
            ops.key = trace_.Next();
            ops.type= kYCSB_Read;
            res.push_back(ops);
        }
        std::random_shuffle(res.begin(), res.end(), ShuffleC);
        return res;
    }

    // Better to run ycsbd right after the insertion. 
    // So we can read the most recent records
    std::vector<YCSB_Op> Sequence_ycsbd(size_t count) {
        // ycsbd: read latest inserted records
        std::vector<YCSB_Op> res;
        size_t keys_len = keys_.size();
        for (uint64_t i = keys_len - count - 1; i < keys_len; ++i) {
            YCSB_Op ops;
            ops.key = keys_[i];
            ops.type= kYCSB_Read;
            res.push_back(ops);
        }
        std::random_shuffle(res.begin(), res.end(), ShuffleD);
        return res;
    }


    std::vector<YCSB_Op> Sequence_ycsbf(size_t count) {
        // ycsbf: 50% reads, 50% read-modified-writes
        std::vector<YCSB_Op> res;

        // 50% reads
        for (uint64_t i = 0; i < count / 2; ++i) {
            YCSB_Op ops;
            ops.key = trace_.Next();
            ops.type = kYCSB_Read;
            res.push_back(ops);
        }

        // 50% read-modified-writes
        for (uint64_t i = 0; i < count / 2; ++i) {
            YCSB_Op ops;
            ops.key = trace_.Next();
            ops.type = kYCSB_ReadModifyWrite;
            res.push_back(ops);
        }
        std::random_shuffle(res.begin(), res.end(), ShuffleF);
        return res;
    }

    // put get mixed workload, defined by the ratio of get opts
    std::vector<YCSB_Op> Sequence_getputmixed(size_t count, double get_r) {
        std::vector<YCSB_Op> res;
        size_t get_count = (size_t)(count * get_r);
        size_t put_count = count - get_count;

        for (uint64_t i = 0; i < get_count; ++i) {
            YCSB_Op ops;
            ops.key = trace_.Next();
            ops.type = kYCSB_Read;
            res.push_back(ops);
        }

        for (uint64_t i = 0; i < put_count; ++i) {
            YCSB_Op ops;
            ops.key = trace_.Next();
            ops.type = kYCSB_Write;
            res.push_back(ops);
        }

        std::random_shuffle(res.begin(), res.end(), ShuffleMix);
        return res;
    }
    
// ---------------- Private Function ---------------
private:

static int ShuffleA(int i) {
  static Trace* trace = new TraceUniform(142857);
  return trace->Next() % i;
}

static int ShuffleB(int i) {
  static Trace* trace = new TraceUniform(285714);
  return trace->Next() % i;
}

static int ShuffleC(int i) {
  static Trace* trace = new TraceUniform(428571);
  return trace->Next() % i;
}

static int ShuffleD(int i) {
  static Trace* trace = new TraceUniform(571428);
  return trace->Next() % i;
}

static int ShuffleE(int i) {
  static Trace* trace = new TraceUniform(714285);
  return trace->Next() % i;
}

static int ShuffleF(int i) {
  static Trace* trace = new TraceUniform(857142);
  return trace->Next() % i;
}

static int ShuffleMix(int i) {
  static Trace* trace = new TraceUniform(314159);
  return trace->Next() % i;
}

// ---------------- Private Member ----------------
private:
    size_t min_;
    size_t max_;
    std::vector<size_t> keys_;
    TraceUniform trace_;
    uint64_t seed_;
    default_random_engine re_;
    uniform_int_distribution<size_t> u_;
};


inline uint64_t NowMicros() {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
}

inline uint64_t NowNanos() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000L + ts.tv_nsec;
}


// the base clase to implement initial, put and get function
class KVBase {
public:
    KVBase(){};
    virtual ~KVBase(){}

    virtual void Initial(int thread_num) = 0;
    virtual int Put(const int64_t& key, size_t& v_size, const char* value, int tid) = 0;
    virtual int Get(const int64_t  key, int64_t* value, int tid) = 0;
    //virtual void TurnOnSpeedUpMode() = 0;
    //virtual void TurnOffSpeedUpMode() = 0;
    //virtual size_t getLastLevelCompactionNum() = 0;
    //virtual size_t getReserveSpaceMergeNum() = 0;
    //virtual size_t getDumpedTableNum() = 0;
    //virtual size_t getDoubleLastLevelShardNum() = 0;
    //std::atomic<bool> speedupmode;
};

class Stats {
public:
    int tid_;
    double start_;
    double finish_;
    double seconds_;
    double next_report_time_;
    double last_op_finish_;
    unsigned last_level_compaction_num_;
    HistogramImpl hist_;

    uint64_t done_;
    uint64_t last_report_done_;
    uint64_t last_report_finish_;
    uint64_t next_report_;
    std::string message_;

    Stats() {Start(); }
    explicit Stats(int id) :
        tid_(id){ Start(); }
    
    void Start() {
        start_ = NowMicros();
        next_report_time_ = start_ + FLAGS_report_interval * 100000;
        next_report_ = 100;
        last_op_finish_ = start_;
        last_report_done_ = 0;
        last_report_finish_ = start_;
        last_level_compaction_num_ = 0;
        done_ = 0;
        seconds_ = 0;
        finish_ = start_;
        message_.clear();
        hist_.Clear();
    }

    void Merge(const Stats& other) {
        hist_.Merge(other.hist_);
        done_ += other.done_;
        seconds_ += other.seconds_;
        if (other.start_ < start_) start_ = other.start_;
        if (other.finish_ > finish_) finish_ = other.finish_;

        // Just keep the messages from one thread
        if (message_.empty()) message_ = other.message_;
    }
    
    void Stop() {
        finish_ = NowMicros();
        seconds_ = (finish_ - start_) * 1e-6;;
    }

    void StartSingleOp() {
        //last_op_finish_ = NowMicros();
        last_op_finish_ = NowNanos();
    }

    void PrintSpeed() {
        uint64_t now = NowMicros();
        int64_t usecs_since_last = now - last_report_finish_;

        std::string cur_time = TimeToString(now/1000000);
        fprintf(stdout,
                "%s ... thread %d: (%lu,%lu) ops and "
                "( %.1f,%.1f ) ops/second in (%.6f,%.6f) seconds\n",
                cur_time.c_str(), 
                tid_,
                done_ - last_report_done_, done_,
                (done_ - last_report_done_) /
                (usecs_since_last / 1000000.0),
                done_ / ((now - start_) / 1000000.0),
                (now - last_report_finish_) / 1000000.0,
                (now - start_) / 1000000.0);
        last_report_finish_ = now;
        last_report_done_ = done_;
        fflush(stdout);
    }
    
    static void AppendWithSpace(std::string* str, const std::string& msg) {
        if (msg.empty()) return;
        if (!str->empty()) {
            str->push_back(' ');
        }
        str->append(msg.data(), msg.size());
    }

    void AddMessage(const std::string& msg) {
        AppendWithSpace(&message_, msg);
    }
    
    inline int FinishedSingleOp(bool is_hist=false) {
        double now = NowNanos();
        if (is_hist) {
            double nanos = now - last_op_finish_;
            hist_.Add(nanos);
        }
        last_op_finish_ = now;

        done_++;
        if (done_ >= next_report_) {
            if      (next_report_ < 1000)   next_report_ += 100;
            else if (next_report_ < 5000)   next_report_ += 500;
            else if (next_report_ < 10000)  next_report_ += 1000;
            else if (next_report_ < 50000)  next_report_ += 5000;
            else if (next_report_ < 100000) next_report_ += 10000;
            else if (next_report_ < 500000) next_report_ += 50000;
            else                            next_report_ += 100000;
            //fprintf(stderr, "... finished %llu ops%30s\r", (unsigned long long )done_, "");
            
            if(FLAGS_report_interval == 0 && (done_ % FLAGS_stats_interval) == 0) {
                PrintSpeed(); 
                if (!hist_.Empty()) {
                    /*
                    int speed_up = 0;
                    if (hist_.Percentile(99) >= FLAGS_get_lat_threshold_up) {
                        speed_up = 1;
                    } else if (hist_.Percentile(99) <= FLAGS_get_lat_threshold_down) {
                        speed_up = 2;
                    }
                    */
                    fprintf(stdout, "Nanoseconds per op:\n%s\n", hist_.ToString().c_str());
                    hist_.Clear();
                    //return speed_up;
                }
                return 0;
            }
            //fflush(stderr);
            //fflush(stdout);
        }

        if (FLAGS_report_interval != 0 && NowMicros() > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 100000;
            PrintSpeed(); 
            //if (is_hist) {
            if (!hist_.Empty()) {
                /*
                int speed_up = 0;
                if (hist_.Percentile(99) >= FLAGS_get_lat_threshold_up) {
                    speed_up = 1;
                } else if (hist_.Percentile(99) <= FLAGS_get_lat_threshold_down) {
                    speed_up = 2;
                }
                */
                fprintf(stdout, "Nanoseconds per op:\n%s\n", hist_.ToString().c_str());
                hist_.Clear();
                //return speed_up;
            }
            return 0;
        }
        return -1;
    }

    std::string TimeToString(uint64_t secondsSince1970) {
        const time_t seconds = (time_t)secondsSince1970;
        struct tm t;
        int maxsize = 64;
        std::string dummy;
        dummy.reserve(maxsize);
        dummy.resize(maxsize);
        char* p = &dummy[0];
        localtime_r(&seconds, &t);
        snprintf(p, maxsize,
                "%04d/%02d/%02d-%02d:%02d:%02d ",
                t.tm_year + 1900,
                t.tm_mon + 1,
                t.tm_mday,
                t.tm_hour,
                t.tm_min,
                t.tm_sec);
        return dummy;
    }


 
    void Report(const Slice& name, bool print_hist = false) {
        // Pretend at least one op was done in case we are running a benchmark
        // that does not call FinishedSingleOp().
        if (done_ < 1) done_ = 1;

        std::string extra;
        double elapsed = (finish_ - start_) * 1e-6;

        double throughput = (double)done_/elapsed;
        fprintf(stdout, "%-12s : %11.3f micros/op %lf Mops/s;%s%s\n",
                name.ToString().c_str(),
                elapsed * 1e6 / done_,
                throughput/1024/1024,
                (extra.empty() ? "" : " "),
                extra.c_str());
        fprintf(stderr, "%-12s : %11.3f micros/op %lf Mops/s;%s%s\n",
                name.ToString().c_str(),
                elapsed * 1e6 / done_,
                throughput/1024/1024,
                (extra.empty() ? "" : " "),
                extra.c_str());
        if (print_hist) {
            fprintf(stdout, "Nanoseconds per op:\n%s\n", hist_.ToString().c_str());
        }
        fflush(stdout);
        fflush(stderr);
    }
};


// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  std::mutex mu;
  std::condition_variable cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState(int total):
    total(total), num_initialized(0), num_done(0), start(false) { }
};


// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
    int tid;             // 0..n-1 when running in n threads
    // Random rand;         // Has different seeds for different threads
    Stats stats;
    SharedState* shared;
    KVBase *_db;

    ThreadState(int index, KVBase *db)
        : tid(index),
            stats(index),
            _db(db) {
            // printf("Random seed: %d\n", seed);
    }
};


class Duration {
 public:
  Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_seconds_ = max_seconds;
    max_ops_= max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = NowMicros();
  }

  inline int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  inline bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;    // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_seconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = 1000;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = NowMicros();
        return ((now - start_at_) / 1000000) >= max_seconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

  inline int64_t Ops() {
    return ops_;
  }
 private:
  uint64_t max_seconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};

class Duration_usec {
 public:
  Duration_usec(uint64_t max_useconds, int64_t max_ops, int64_t ops_per_stage = 0) {
    max_useconds_ = max_useconds;
    max_ops_= max_ops;
    ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
    ops_ = 0;
    start_at_ = NowMicros();
  }

  inline int64_t GetStage() { return std::min(ops_, max_ops_ - 1) / ops_per_stage_; }

  inline bool Done(int64_t increment) {
    if (increment <= 0) increment = 1;    // avoid Done(0) and infinite loops
    ops_ += increment;

    if (max_useconds_) {
      // Recheck every appx 1000 ops (exact iff increment is factor of 1000)
      auto granularity = 10;
      if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
        uint64_t now = NowMicros();
        return (now - start_at_) >= max_useconds_;
      } else {
        return false;
      }
    } else {
      return ops_ > max_ops_;
    }
  }

  inline int64_t Ops() {
    return ops_;
  }
 private:
  uint64_t max_useconds_;
  int64_t max_ops_;
  int64_t ops_per_stage_;
  int64_t ops_;
  uint64_t start_at_;
};

#if defined(__linux)
static std::string TrimSpace(std::string s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return std::string(s.data() + start, limit - start);
}
#endif
class Benchmark {
public:
    KVBase* kv_;
    uint64_t num_;
    int value_size_;
    size_t reads_;
    std::vector<YCSBGenerator*> ycsb_gens_;
    std::vector<std::vector<YCSB_Op> > ycsb_ops_;
    std::atomic<int> finish_thread_num;
    std::atomic<bool> flag_begin_compaction;
    Benchmark(KVBase* kv) :
        kv_(kv),
        num_(FLAGS_num),
        value_size_(FLAGS_value_size),
        reads_(FLAGS_reads),
        finish_thread_num(0),
        flag_begin_compaction(false)
        {}

    
    void Run() {
        PrintHeader();

        // Initial kv using given thread num
        kv_->Initial(FLAGS_thread); 
        size_t key_range_for_each = FLAGS_num / FLAGS_thread;

        // Create ycsb generator for each thread
        for (int i = 0; i < FLAGS_thread; ++i) {
            ycsb_gens_.push_back(new YCSBGenerator(1, key_range_for_each, key_range_for_each));
        }

        // run benchmark
        bool print_hist = false;
        const char* benchmarks = FLAGS_benchmarks.c_str();
        while (benchmarks != nullptr) {
            void (Benchmark::*method)(ThreadState*) = nullptr;
            ycsb_ops_.clear();
            const char* sep = strchr(benchmarks, ',');
            std::string name;
            if (sep == nullptr) {
                name = benchmarks;
                benchmarks = nullptr;
            } else {
                name = std::string(benchmarks, sep - benchmarks);
                benchmarks = sep + 1;
            }
            if (name == "ycsb_load") {
                method = &Benchmark::YCSB_LOAD;
            } else if (name == "ycsb_a") {
                method = &Benchmark::YCSB;
                if (ycsb_ops_.size() != 0) {
                    printf("YCSB workload not reset before testing.\n");
                    exit(1);
                }
                for (int i = 0; i < FLAGS_thread; ++i) {
                    ycsb_ops_.emplace_back(std::move(ycsb_gens_[i]->Sequence_ycsba(FLAGS_ycsb_op_num)));
                }
            } else if (name == "ycsb_b") {
                method = &Benchmark::YCSB;
                if (ycsb_ops_.size() != 0) {
                    printf("YCSB workload not reset before testing.\n");
                    exit(1);
                }
                for (int i = 0; i < FLAGS_thread; ++i) {
                    ycsb_ops_.emplace_back(std::move(ycsb_gens_[i]->Sequence_ycsbb(FLAGS_ycsb_op_num)));
                }
            } else if (name == "ycsb_c") {
                method = &Benchmark::YCSB;
                if (ycsb_ops_.size() != 0) {
                    printf("YCSB workload not reset before testing.\n");
                    exit(1);
                }
                for (int i = 0; i < FLAGS_thread; ++i) {
                    ycsb_ops_.emplace_back(std::move(ycsb_gens_[i]->Sequence_ycsbc(FLAGS_ycsb_op_num)));
                }
            } else if (name == "ycsb_d") {
                method = &Benchmark::YCSB;
                if (ycsb_ops_.size() != 0) {
                    printf("YCSB workload not reset before testing.\n");
                    exit(1);
                }
                for (int i = 0; i < FLAGS_thread; ++i) {
                    ycsb_ops_.emplace_back(std::move(ycsb_gens_[i]->Sequence_ycsbd(FLAGS_ycsb_op_num)));
                }
            } else if (name == "ycsb_f") {
                method = &Benchmark::YCSB;
                if (ycsb_ops_.size() != 0) {
                    printf("YCSB workload not reset before testing.\n");
                    exit(1);
                }
                for (int i = 0; i < FLAGS_thread; ++i) {
                    ycsb_ops_.emplace_back(std::move(ycsb_gens_[i]->Sequence_ycsbf(FLAGS_ycsb_op_num)));
                }
            } else if (name == "rwtest") {
                method = &Benchmark::ReadWhileWriting;
                if (ycsb_ops_.size() != 0) {
                    printf("YCSB workload not reset before testing.\n");
                    exit(1);
                }
                for (int i = 0; i < FLAGS_thread; ++i) {
                    ycsb_ops_.emplace_back(
                            std::move(ycsb_gens_[i]->Sequence_getputmixed(FLAGS_rwtest_op_num, FLAGS_get_ratio)));
                }
                print_hist = true;
            }

            if (method != nullptr) RunBenchmark(FLAGS_thread, name, method, print_hist);
            ::usleep(FLAGS_delaybetweenbenchmarks * 1000000);
        }
        
        
    }

    
    inline void YCSB_Ops(const YCSB_Op& operation, int tid) {
        int res = -1;
        char value[8] = "value.";
        int64_t gval;
        size_t value_size = FLAGS_value_size;
        if (operation.type == kYCSB_Write) {
            res = kv_->Put(operation.key, value_size, value, tid);
        } else if (operation.type == kYCSB_Read) {
            res = kv_->Get(operation.key, &gval, tid);
        } else if (operation.type == kYCSB_ReadModifyWrite) {
            res = kv_->Get(operation.key, &gval, tid);
            if (res == true) {
                res = kv_->Put(operation.key, value_size, value, tid);
            }
        }
    }
    
    void YCSB_LOAD(ThreadState* thread) {
        // obtain the insertion sequence reference
        auto& insertion_keys = ycsb_gens_[thread->tid]->InsertionSequence();
        size_t len = insertion_keys.size();
        // printf("Load. current thread info: %d. %lu\n", thread->tid, len);
        char value[8] = "value.";
        size_t value_size = FLAGS_value_size;

        thread->stats.Start();
        for (size_t i = 0; i < len; ++i) {
            kv_->Put(insertion_keys[i], value_size, value, thread->tid);
            thread->stats.FinishedSingleOp();
        }
    }

    void YCSB(ThreadState* thread) {
        // printf("A. current thread info: %d\n", stats->tid);
        int tid = thread->tid;
        size_t op_len = ycsb_ops_[tid].size();
        thread->stats.Start();
        for (size_t i = 0; i < op_len; ++i) {
            YCSB_Ops(ycsb_ops_[tid][i], tid);
            thread->stats.FinishedSingleOp();
        }

    }

    inline void ReadRandomHist(ThreadState* thread) {
        //int res = -1;
        //char value[8] = "value.";
        thread_local int64_t gval = 0;
        int res_speedup = 0;
        bool speedupmodeoff = true;
        int tid = thread->tid;
        //size_t value_size = FLAGS_value_size;
        Duration duration(FLAGS_readtime, 0);
        //uint64_t s_time = NowMicros() + 
        //        (FLAGS_sleep + FLAGS_writetime) * 1000000;
        while (!duration.Done(1)) {
            size_t key = ycsb_gens_[tid]->Next_rand();
            // assuming thraed num is power of 2
            //res = kv_->Get(key, &gval, thread->tid);
            kv_->Get(key, &gval, tid);
            // the dicision to turn on/off speedup mode is made by FinishedSingleOp()
            /*
            if (tid == 0) {
                thread->stats.FinishedSingleOp(true);
            } else {
                thread->stats.FinishedSingleOp();
            }
            */
            thread->stats.FinishedSingleOp();

            /*
#ifdef SPEED_UP_MODE_TEST
            if (speedupmodeoff == true 
                    && res_speedup == 1) {
                thread->_db->TurnOnSpeedUpMode();
                //fprintf(stdout, "Percentile SpeedUp mode ON\n");
                //fflush(stdout);
                speedupmodeoff = false;
            }
#endif
            */
        }
        fprintf(stderr, "Fninish Get.\n");
    }

    void ReadWhileWriting(ThreadState* thread) {
#ifdef SEPERATE_RW_MODE
        // if ((thread->tid & 0x1) == 0) {
        //     // even thread for read
        //     ReadRandomHist(thread);
        // } else {
        {
#else
        //for (unsigned t = 0; t < 5; t++) {
        {
#endif
            // Delay for FLAGS_sleep seconds, then send put requests.
            //::usleep(FLAGS_sleep * 1000000);
            int res_speedup = 0;
            bool speedupmodeoff = true;
            thread->stats.Start();
            // odd thread for write.
            
            //int res = -1;
            char value[8] = "value.";
            thread_local int64_t gval = 0;
            //int64_t gval;
            //bool check_time = true;
            size_t value_size = FLAGS_value_size;
#ifdef DURATION_MODE
            {
                //::usleep(FLAGS_sleep * 1000000);
                Duration duration_1(FLAGS_sleep, 0);
                while (!duration_1.Done(1)) {
                    //size_t key = ycsb_gens_[tid]->Next_rand();
                    size_t key = ycsb_gens_[thread->tid]->Next_rand();
                    // assuming thraed num is power of 2
                    //res = kv_->Get(key, &gval, thread->tid);
                    thread->stats.StartSingleOp();
                    kv_->Get(key, &gval, thread->tid);
                    if (thread->tid == 1) {
                        res_speedup = thread->stats.FinishedSingleOp(true);
                    } else {
                        res_speedup = thread->stats.FinishedSingleOp();
                    }
                    /*
                    if (thread->tid == 1 && res_speedup != -1) {
                        fprintf(stdout, "Compaction ll_merge_num %lu last_level_compaction_num %lu dumped_table_num %lu\n", 
                                thread->_db->getReserveSpaceMergeNum(),
                                thread->_db->getLastLevelCompactionNum(),
                                thread->_db->getDumpedTableNum());
                        fflush(stdout);
                    }
                    */
                }
                uint64_t mix_workload_start = GetTimeNsec();
                //Duration duration(60, 0);
#ifdef SPEED_UP_MODE_TEST
                thread->_db->TurnOnSpeedUpMode();
                speedupmodeoff = false;
#endif
                size_t op_len = ycsb_ops_[thread->tid].size();
                for (size_t i = 0; i < op_len; ++i) {
                    thread->stats.StartSingleOp();
                    YCSB_Ops(ycsb_ops_[thread->tid][i], thread->tid);
                    //if (tid == 0 && ycsb_ops_[tid][i].type == kYCSB_Read) {
                    if (thread->tid == 1 && ycsb_ops_[thread->tid][i].type == kYCSB_Read) {
                        res_speedup = thread->stats.FinishedSingleOp(true);
                    } else {
                        res_speedup = thread->stats.FinishedSingleOp();
                    }
                    /*
                    if (thread->tid == 1 && res_speedup != -1) {
                        fprintf(stdout, "Compaction ll_merge_num %lu last_level_compaction_num %lu dumped_table_num %lu\n", 
                                thread->_db->getReserveSpaceMergeNum(),
                                thread->_db->getLastLevelCompactionNum(),
                                thread->_db->getDumpedTableNum());
                        fflush(stdout);
                    }
                    */
                }
                /*
                for (size_t i = 0; i < FLAGS_rwtest_op_num; ++i) {
                    // Between each put, we add a delay to control the put speed
                    //uint64_t delay_end = NowMicros() + 1*FLAGS_rwdelay;

                    // Since each thread loads the same key range in YCSB_Load phase,
                    // the key can be used in any thread
                    size_t key = ycsb_gens_[thread->tid]->Next();
                    // Randomly pick a position. Assuming thread num is power of 2
                    kv_->Put(key, value_size, value, thread->tid);
                    res_speedup = thread->stats.FinishedSingleOp();
                    
                    if (thread->tid == 1 && res_speedup != -1) {
                        fprintf(stdout, "Compaction ll_merge_num %lu last_level_compaction_num %lu dumped_table_num %lu\n", 
                                thread->_db->getReserveSpaceMergeNum(),
                                thread->_db->getLastLevelCompactionNum(),
                                thread->_db->getDumpedTableNum());
                        fflush(stdout);
                    }
                    
                    //while (NowMicros() < delay_end) {
                    //}
                }
                */
               uint64_t mix_workload_interval = (GetTimeNsec()-mix_workload_start)/1000000000; //in seconds
#ifdef SPEED_UP_MODE_TEST
                finish_thread_num.fetch_add(1);
                if (finish_thread_num.load(std::memory_order_acquire) == FLAGS_thread) {
                    thread->_db->TurnOffSpeedUpMode();
                    speedupmodeoff = true;
                    finish_thread_num.store(0, std::memory_order_release);
                    flag_begin_compaction.store(true, std::memory_order_release);
                }
                fprintf(stderr, "Finish Put, waiting background compactions....\n");
                while(!flag_begin_compaction.load(std::memory_order_acquire)) {}
                fprintf(stderr, "Begin second round get period.\n");
                // fflush(stderr);
                // uint64_t compaction_start = GetTimeNsec();
                // while (Env::Default()->GetThreadPoolQueueLen(Env::LOW) > 0) {}
                // fprintf(stderr, "Compaction finished, time spent %.1lf s\n", (GetTimeNsec() - compaction_start)/1000000000.0);
#endif
                uint64_t rest_time = FLAGS_readtime - FLAGS_sleep - mix_workload_interval;
                Duration duration_2(rest_time, 0);
                while (!duration_2.Done(1)) {
                    //size_t key = ycsb_gens_[tid]->Next_rand();
                    size_t key = ycsb_gens_[thread->tid]->Next_rand();
                    // assuming thraed num is power of 2
                    //res = kv_->Get(key, &gval, thread->tid);
                    thread->stats.StartSingleOp();
                    kv_->Get(key, &gval, thread->tid);
                    if (thread->tid == 1) {
                        res_speedup = thread->stats.FinishedSingleOp(true);
                    } else {
                        res_speedup = thread->stats.FinishedSingleOp();
                    }
                    /*
                    if (thread->tid == 1 && res_speedup != -1) {
                        fprintf(stdout, "Compaction ll_merge_num %lu last_level_compaction_num %lu dumped_table_num %lu\n", 
                                thread->_db->getReserveSpaceMergeNum(),
                                thread->_db->getLastLevelCompactionNum(),
                                thread->_db->getDumpedTableNum());
                        fflush(stdout);
                    }
                    */
                }
                /*
                if (thread->tid == 1) {
                    fprintf(stdout, "Final_Compaction ll_merge_num %lu last_level_compaction_num %lu dumped_table_num %lu\n", 
                            thread->_db->getReserveSpaceMergeNum(),
                            thread->_db->getLastLevelCompactionNum(),
                            thread->_db->getDumpedTableNum());
                    fflush(stdout);
                }
                */
            }
#else /*DURATION_MODE*/
            {
                int tid = thread->tid;
                //ycsb_gens_[tid]->Reset_trace();
                Duration duration_1(30, 0);
                //for (size_t i = 0; i < FLAGS_rwtest_get_num; ++i) {
                while (!duration_1.Done(1)) {
                    //size_t key = ycsb_gens_[tid]->Next_rand();
                    size_t key = ycsb_gens_[tid]->Next_rand();
                    // assuming thraed num is power of 2
                    //res = kv_->Get(key, &gval, thread->tid);
                    kv_->Get(key, &gval, tid);
                    // the dicision to turn on/off speedup mode is made by FinishedSingleOp()
                    /*
                    if (tid == 0) {
                        res_speedup = thread->stats.FinishedSingleOp(true);
                    } else {
                        res_speedup = thread->stats.FinishedSingleOp();
                    }
                    */
                    res_speedup = thread->stats.FinishedSingleOp(true);
                    if (thread->tid == 0 && res_speedup != -1) {
                        fprintf(stdout, "Compaction ll_merge_num %lu last_level_compaction_num %lu dumped_table_num %lu\n", 
                                thread->_db->getReserveSpaceMergeNum(),
                                thread->_db->getLastLevelCompactionNum(),
                                thread->_db->getDumpedTableNum());
                        fflush(stdout);
                    }
                }
                uint64_t mix_workload_start = GetTimeNsec();
                size_t op_len = ycsb_ops_[tid].size();
                //thread->stats.Start();
#ifdef SPEED_UP_MODE_TEST
                // turn on speedup mode directly
                thread->_db->TurnOnSpeedUpMode();
                speedupmodeoff = false;
#endif
                for (size_t i = 0; i < op_len; ++i) {
                    YCSB_Ops(ycsb_ops_[tid][i], tid);
                    //if (tid == 0 && ycsb_ops_[tid][i].type == kYCSB_Read) {
                    if (ycsb_ops_[tid][i].type == kYCSB_Read) {
                        res_speedup = thread->stats.FinishedSingleOp(true);
                    } else {
                        res_speedup = thread->stats.FinishedSingleOp();
                    }
/*
#ifdef SPEED_UP_MODE_TEST
                    if (speedupmodeoff == true 
                            && res_speedup == 1) {
                        thread->_db->TurnOnSpeedUpMode();
                        speedupmodeoff = false;
                    }
#endif
*/
                    
                    if (thread->tid == 0 && res_speedup != -1) {
                        fprintf(stdout, "Compaction ll_merge_num %lu last_level_compaction_num %lu dumped_table_num %lu\n", 
                                thread->_db->getReserveSpaceMergeNum(),
                                thread->_db->getLastLevelCompactionNum(),
                                thread->_db->getDumpedTableNum());
                        fflush(stdout);
                    }
                }
                uint64_t mix_workload_interval = (GetTimeNsec()-mix_workload_start)/1000000000; //in seconds
                
#ifdef SPEED_UP_MODE_TEST
                //::usleep(FLAGS_sleep * 1000000);
                //uint64_t compaction_start = GetTimeNsec();
                thread->_db->TurnOffSpeedUpMode();
                if(tid == 0) {
                    fprintf(stderr, "Before %u merge and %u compaction to go, shards with two last levels %lu\n", 
                            Env::Default()->GetThreadPoolQueueLen(Env::LOW),
                            Env::Default()->GetThreadPoolQueueLen(Env::HIGH),
                            thread->_db->getDoubleLastLevelShardNum());
                }
                speedupmodeoff == true;
//                 while (Env::Default()->GetThreadPoolQueueLen(Env::LOW) > 0) {
// #else /*SPEED_UP_MODE_TEST*/
//                 for (size_t i = 0; i < FLAGS_rwtest_get_num * 10; ++i) {
#endif
                //ycsb_gens_[tid]->Reset_trace();
                Duration duration_2(270 - mix_workload_interval, 0);
                while (!duration_2.Done(1)) {
                //for (size_t i = 0; i < FLAGS_rwtest_get_num * 10; ++i) {
                    //size_t key = ycsb_gens_[tid]->Next_rand();
                    size_t key = ycsb_gens_[tid]->Next_rand();
                    // assuming thraed num is power of 2
                    kv_->Get(key, &gval, tid);
                    /*
                    if (tid == 0) {
                        res_speedup = thread->stats.FinishedSingleOp(true);
                    } else {
                        res_speedup = thread->stats.FinishedSingleOp();
                    }
                    */
                    res_speedup = thread->stats.FinishedSingleOp(true);
                    if (thread->tid == 0 && res_speedup != -1) {
                        fprintf(stdout, "Compaction ll_merge_num %lu last_level_compaction_num %lu dumped_table_num %lu\n", 
                                thread->_db->getReserveSpaceMergeNum(),
                                thread->_db->getLastLevelCompactionNum(),
                                thread->_db->getDumpedTableNum());
                        fflush(stdout);
                    }
                }
#ifdef SPEED_UP_MODE_TEST
                if (tid == 0) {
                    fprintf(stderr, "After %u merge and %u compaction to go, shards with two last levels %lu\n", 
                            Env::Default()->GetThreadPoolQueueLen(Env::LOW),
                            Env::Default()->GetThreadPoolQueueLen(Env::HIGH),
                            thread->_db->getDoubleLastLevelShardNum());
                }
                //double compaction_elapse = (GetTimeNsec() - compaction_start) / 1000000000.0;
                /*
                if (tid == 0) {
                    fprintf(stderr, "Merging_time_span %.4lf\n", tid, compaction_elapse);
                }
                */
#endif
            }
#endif /*DURATION_MODE*/
        }
    }

    


private:
    struct ThreadArg {
        Benchmark* bm;
        SharedState* shared;
        ThreadState* thread;
        void (Benchmark::*method)(ThreadState*);
    };

    static void ThreadBody(void* v) {
        ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
        SharedState* shared = arg->shared;
        ThreadState* thread = arg->thread;
        {
            std::unique_lock<std::mutex> lck(shared->mu);
            shared->num_initialized++;
            if (shared->num_initialized >= shared->total) {
                shared->cv.notify_all();
            }
            while (!shared->start) {
                shared->cv.wait(lck);
            }
        }

        thread->stats.Start();
        (arg->bm->*(arg->method))(thread);
        thread->stats.Stop();

        {
            std::unique_lock<std::mutex> lck(shared->mu);
            shared->num_done++;
            if (shared->num_done >= shared->total) {
                shared->cv.notify_all();
            }
        }
    }

    void RunBenchmark(int thread_num, const std::string& name, 
                      void (Benchmark::*method)(ThreadState*), bool print_hist) {
        SharedState shared(thread_num);
        ThreadArg* arg = new ThreadArg[thread_num];
        std::thread server_threads[thread_num];
        for (int i = 0; i < thread_num; i++) {
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = &shared;
            arg[i].thread = new ThreadState(i, kv_);
            arg[i].thread->shared = &shared;
            server_threads[i] = std::thread(ThreadBody, &arg[i]);
        }

        std::unique_lock<std::mutex> lck(shared.mu);
        while (shared.num_initialized < thread_num) {
            shared.cv.wait(lck);
        }

        shared.start = true;
        shared.cv.notify_all();
        while (shared.num_done < thread_num) {
            shared.cv.wait(lck);
        }

        Stats merge_stats;
        for (int i = 0; i < thread_num; i++) {
            merge_stats.Merge(arg[i].thread->stats);
        }
        merge_stats.Report(name, print_hist);
        
        for (auto& th : server_threads) th.join();
    }


    void PrintEnvironment() {
#if defined(__linux)
        time_t now = time(nullptr);
        fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

        FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
        if (cpuinfo != nullptr) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
                const char* sep = strchr(line, ':');
                if (sep == nullptr) {
                continue;
                }
                std::string key = TrimSpace(std::string(line, sep - 1 - line));
                std::string val = TrimSpace(std::string(sep + 1));
                if (key == "model name") {
                ++num_cpus;
                cpu_type = val;
                } else if (key == "cache size") {
                cache_size = val;
                }
            }
            fclose(cpuinfo);
            fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
            fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
        }
#endif
    }

    void PrintHeader() {
        const int kKeySize = 8;
        fprintf(stdout, "------------------------------------------------\n");
        PrintEnvironment();
        fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
        fprintf(stdout, "Values:     %d bytes each\n", FLAGS_value_size);
        fprintf(stdout, "Entries:    %lu\n", num_);
        fprintf(stdout, "------------------------------------------------\n");
    }

};


};
