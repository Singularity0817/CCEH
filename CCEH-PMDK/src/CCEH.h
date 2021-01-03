#ifndef CCEH_H_
#define CCEH_H_

#include <cstring>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <libpmemobj.h>
#include <libpmem.h>
#include <unistd.h>
#include <cmath>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <queue>
#include "../util/pair.h"
#include "../src/hash.h"
#include "./wal.h"
//#include "../util/hash.h"
//#include "../util/persist.h"
//#include "../util/hash.h"

#define LAYOUT "CCEH"
#define TOID_ARRAY(x) TOID(x)

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits)-1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 16;//4; infects the probe time, by default, it is 4
constexpr size_t kOptaneUnitSize = 256;
constexpr size_t kPoolSize = PMEMOBJ_MIN_POOL*1024;

POBJ_LAYOUT_BEGIN(CCEH_LAYOUT);
POBJ_LAYOUT_ROOT(CCEH_LAYOUT, struct CCEH_pmem);
POBJ_LAYOUT_TOID(CCEH_LAYOUT, struct Directory_pmem);
POBJ_LAYOUT_TOID(CCEH_LAYOUT, struct Segment_pmem);
POBJ_LAYOUT_TOID(CCEH_LAYOUT, TOID(struct Segment_pmem));
POBJ_LAYOUT_TOID(CCEH_LAYOUT, Pair);
POBJ_LAYOUT_TOID(CCEH_LAYOUT, size_t);
POBJ_LAYOUT_TOID(CCEH_LAYOUT, unsigned);
POBJ_LAYOUT_END(CCEH_LAYOUT);

struct Segment_pmem{
	size_t pair_size;
	size_t local_depth;
	int64_t sema;
	size_t pattern;
  unsigned l0_pair_num;
  TOID(Pair) l0_pairs;
	TOID(Pair) pairs;
  //size_t link_head;
};

struct Directory_pmem{
	size_t capacity;
	size_t depth;
	bool lock;
	int sema = 0;
  size_t checkpoint;
  TOID(size_t) link_head_pmem;
  TOID(unsigned) link_size_pmem;
	TOID_ARRAY(TOID(struct Segment_pmem)) segments;
};

struct CCEH_pmem{
	size_t global_depth;
	TOID(struct Directory_pmem) directories;	
};

struct Segment {
  static const size_t kNumSlot = kSegmentSize/sizeof(Pair);
  static const size_t kNumSlotMask = kNumSlot - 1;
  static const size_t kBufferSlot = kOptaneUnitSize/sizeof(Pair);
  static const size_t kL0Slot = kBufferSlot*4;

  void* operator new(size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  void* operator new[](size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  int Insert(Key_t&, Value_t, size_t, size_t);
  void Insert4split(Key_t&, Value_t, size_t);
  bool Put(Key_t&, Value_t, size_t);
  int Delete(Key_t& key, size_t loc, size_t key_hash);
  Segment** Split(PMEMobjpool* pop);
  int minor_compaction();
  int major_compaction();

  Pair *dpairs;
  Pair *spared_dpairs;
  std::atomic<Pair *> imm_dpairs;
  unsigned dpair_num = 0;
  TOID(Pair) l0_pairs;
  unsigned l0_pair_num = 0;
  size_t local_depth;
  int64_t sema = 0;
  size_t pattern = 0;
  PMEMobjpool *pool_handler;
  size_t link_head = INVALID;
  //mutex m_;
  
  TOID(struct Segment_pmem) seg_pmem;
  TOID(Pair) pairs;
  bool locked = false;
  std::mutex m_;
  std::condition_variable cv_;

  //size_t numElem(void);

  Segment(PMEMobjpool *pop, size_t depth)
  :local_depth{depth}
  {
    dpairs = (Pair *)malloc(kBufferSlot*sizeof(Pair));
    spared_dpairs = (Pair *)malloc(kBufferSlot*sizeof(Pair));
    imm_dpairs.store(nullptr, std::memory_order_release);
    pool_handler = pop;
    POBJ_ALLOC(pop, &seg_pmem, struct Segment_pmem, sizeof(struct Segment_pmem),NULL, NULL);
    D_RW(seg_pmem)->local_depth = local_depth;
    D_RW(seg_pmem)->sema = sema;
    D_RW(seg_pmem)->pattern = pattern;
    D_RW(seg_pmem)->pair_size = kNumSlot;

    D_RW(seg_pmem)->l0_pair_num = l0_pair_num;
    POBJ_ALLOC(pop, &l0_pairs, Pair, sizeof(Pair)*4*kBufferSlot, NULL, NULL);
    D_RW(seg_pmem)->l0_pairs = l0_pairs;

    POBJ_ALLOC(pop, &pairs, Pair, sizeof(Pair)*kNumSlot, NULL, NULL);
    D_RW(seg_pmem)->pairs = pairs;
    for(int i=0;i<kNumSlot;i++){
	    D_RW(pairs)[i].key = -1;
    }
  }

  Segment(){ }

  void load_pmem(PMEMobjpool *pop, TOID(struct Segment_pmem) seg_pmem_){
    pool_handler = pop;
    seg_pmem = seg_pmem_;
    pairs = D_RO(seg_pmem)->pairs;
    sema = D_RO(seg_pmem)->sema;
    pattern = D_RO(seg_pmem)->pattern;
    local_depth = D_RO(seg_pmem)->local_depth;

    l0_pairs = D_RO(seg_pmem)->l0_pairs;
    l0_pair_num = D_RO(seg_pmem)->l0_pair_num;
    //kNumSlot = D_RO(seg_pmem)->pair_size;
	}

  ~Segment(void) {
    free(dpairs);
    while(imm_dpairs.load(std::memory_order_acquire) != nullptr) {asm("nop");}
    free(spared_dpairs);
  }

  bool lock(void) {
    //bool status = false;
    //return CAS(&locked, &status, true);
    m_.lock();
    return true;
  }

  bool unlock(void) {
    //bool status = true;
    //return CAS(&locked, &status, false);
    m_.unlock();
    return true;
  }

  void set_pattern_pmem(size_t pattern){
    D_RW(seg_pmem)->pattern = pattern;
  }

  Key_t get_key(size_t y){
    return D_RO(D_RO(seg_pmem)->pairs)[y].key;
  }

  Value_t get_value(size_t y){
    return D_RO(D_RO(seg_pmem)->pairs)[y].value;
  }

  void pair_insert_pmem(size_t y, Key_t key, Value_t value){
    D_RW(D_RW(seg_pmem)->pairs)[y].key = key;	
    D_RW(D_RW(seg_pmem)->pairs)[y].value = value;
  }

  void pair_insert_dram(Key_t key, Value_t value){
    dpairs[dpair_num].key = key;
    dpairs[dpair_num].value = value;
    ++dpair_num;
  }
};

struct Directory {
public:
  static const size_t kDefaultDepth = 10;
  Segment** _;
  size_t *link_head;
  unsigned *link_size;
  size_t capacity;
  size_t depth;
  int sema = 0 ;
  bool lock;
  size_t last_checkpoint = 0;
  PMEMobjpool *pop;
  TOID(struct Directory_pmem) dir_pmem;
  TOID_ARRAY(TOID(struct Segment_pmem)) segments;
  
  Directory(PMEMobjpool *pop_, size_t depth_, TOID(struct Directory_pmem) dir_pmem_){
    dir_pmem = dir_pmem_;
    capacity = pow(2,depth_);
    depth = depth_;
    sema = 0;
    lock = false;
    pop=pop_;
    _ = new Segment*[capacity];
    link_head = new size_t[capacity];
    link_size = new unsigned[capacity];
    for (unsigned i = 0; i < capacity; ++i) link_head[i]=INVALID;
    D_RW(dir_pmem)->depth = depth;
    D_RW(dir_pmem)->capacity = capacity;
    D_RW(dir_pmem)->lock = lock;
    D_RW(dir_pmem)->sema = sema;

    POBJ_ALLOC(pop, &segments, TOID(struct Segment_pmem), sizeof(TOID(struct Segment_pmem))*capacity, NULL,NULL);  
    D_RW(dir_pmem)->segments = segments;
    POBJ_ALLOC(pop, &(D_RW(dir_pmem)->link_head_pmem), size_t, sizeof(size_t)*capacity, NULL, NULL);
    POBJ_ALLOC(pop, &(D_RW(dir_pmem)->link_size_pmem), unsigned, sizeof(unsigned)*capacity, NULL, NULL);
    D_RW(dir_pmem)->checkpoint = 0;
    //D_RW(dir_pmem)->link_head_pmem
  }
  Directory(){ }
  ~Directory(void){ }
  void doubling_pmem(){
    D_RW(dir_pmem)->capacity = capacity;
    //printf("Doubling to %d\n",capacity); fflush(stdout);
    POBJ_REALLOC(pop, &segments, TOID(struct Segment_pmem),capacity*sizeof(TOID(struct Segment_pmem)));
    D_RW(dir_pmem)->segments = segments;
    for(size_t i=capacity/2; i<capacity; i++){
	    D_RW(D_RW(dir_pmem)->segments)[i] = D_RO(D_RO(dir_pmem)->segments)[i-capacity/2];
    }
    /* reallocate pmem space for link_head and link_size */
    POBJ_REALLOC(pop, &D_RW(dir_pmem)->link_head_pmem, size_t, sizeof(size_t)*capacity);
    pmemobj_memcpy_persist(pop, D_RW(D_RW(dir_pmem)->link_head_pmem)+capacity/2, D_RW(D_RW(dir_pmem)->link_head_pmem), sizeof(size_t)*capacity/2);
    POBJ_REALLOC(pop, &D_RW(dir_pmem)->link_size_pmem, unsigned, sizeof(unsigned)*capacity);
    pmemobj_memcpy_persist(pop, D_RW(D_RW(dir_pmem)->link_size_pmem)+capacity/2, D_RW(D_RW(dir_pmem)->link_size_pmem), sizeof(unsigned)*capacity/2);
  } 
  void segment_bind_pmem(size_t dir_index, struct Segment* s){
    D_RW(D_RW(dir_pmem)->segments)[dir_index] = s->seg_pmem;
  }
  void do_checkpoint(size_t checkpoint) {
    pmemobj_memcpy_persist(pop, D_RW(D_RW(dir_pmem)->link_size_pmem), link_size, sizeof(unsigned)*capacity);
    pmemobj_memcpy_persist(pop, D_RW(D_RW(dir_pmem)->link_head_pmem), link_head, sizeof(size_t)*capacity);
    D_RW(dir_pmem)->checkpoint = checkpoint;
    pmemobj_persist(pop, &D_RW(dir_pmem)->checkpoint, sizeof(size_t));
    /*
    printf("====== new checkpoing %lu : last checkpoint %lu =====\n", checkpoint, last_checkpoint);
    for (unsigned i = 0; i < capacity; ++i) {
      printf("%8lu ", D_RO(D_RO(dir_pmem)->link_head_pmem)[i]);
    }
    printf("\n");
    for (unsigned i = 0; i < capacity; ++i) {
      printf("%8u ", D_RO(D_RO(dir_pmem)->link_size_pmem)[i]);
    }
    printf("\n");
    */
    last_checkpoint = checkpoint;
  }
  void load_pmem(PMEMobjpool* pop_, TOID(struct Directory_pmem) dir_pmem_){
    pop = pop_;
    dir_pmem = dir_pmem_;
    capacity = D_RO(dir_pmem)->capacity;
    sema = D_RO(dir_pmem)->sema;
    lock = D_RO(dir_pmem)->lock;
    depth = D_RO(dir_pmem)->depth;
    _ = new Segment*[capacity];
    segments = D_RO(dir_pmem)->segments;
    for(size_t i=0;i<capacity;i++){
	    TOID(struct Segment_pmem) seg_pmem = D_RO(segments)[i];
      if(i == D_RO(seg_pmem)->pattern){
        _[i] = new Segment();
        _[i]->load_pmem(pop, seg_pmem);
      }else{
        _[i] = _[D_RO(seg_pmem)->pattern];
      }
    }
  } 
  bool Acquire(void) {
    bool unlocked = false;
    return CAS(&lock, &unlocked, true);
  }

  bool Release(void) {
    bool locked = true;
    return CAS(&lock, &locked, false);
  }
  
  void SanityCheck(void*);
  void LSBUpdate(int, int, int, int, Segment**);

  size_t segment_size() {
    return capacity*sizeof(struct Segment);
  }
  size_t segment_num() {
    return capacity;
  }
};

class CCEH {
  public:
    CCEH(const char*);
    CCEH(size_t, const char*);
    ~CCEH(void);
    void Insert(Key_t&, char *);
    //Value_t Get(Key_t&);
    char *Get(Key_t&);
    int Delete(Key_t&);
    /*
    bool InsertOnly(Key_t&, Value_t);
    bool Delete(Key_t&);
    Value_t FindAnyway(Key_t&);
    double Utilization(void);
    size_t Capacity(void);
    bool Recovery(void);
    */
    TOID(struct CCEH_pmem) cceh_pmem;
    TOID(struct Directory_pmem) dir_pmem;
    PMEMobjpool *pop;
    Directory* dir;
    size_t global_depth;
    Wal *log;
    std::atomic<bool> shutting_down;
    std::thread background_worker;
    bool background_worker_working = false;
    std::queue<size_t> segment_q;
    std::mutex q_lock;
    std::condition_variable q_cv;
    int init_pmem(const char* path){
      size_t pool_size = kPoolSize;//PMEMOBJ_MIN_POOL*1024*3;//PMEMOBJ_MIN_POOL*1024*12; //for one thread
      if(access(path, F_OK) != 0){
        int sds_write_value = 0;
		    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
        pop = pmemobj_create(path, LAYOUT, pool_size, 0666);
        //pop = pmemobj_create(path, LAYOUT, 8*1024*1024*1024, 0666);
        if(pop==NULL){
          perror(path);
          exit(-1);
        }
        printf("Pmem pool size %.1lfGB\n", (double)pool_size/(1024.0*1024*1024));
        cceh_pmem = POBJ_ROOT(pop, struct CCEH_pmem);
        POBJ_ALLOC(pop, &dir_pmem, struct Directory_pmem, sizeof(struct Directory_pmem), NULL,NULL);
        D_RW(cceh_pmem)->directories = dir_pmem;
        string log_path(path);
        log_path += ".log";
        log = new Wal();
        log->create(log_path.c_str(), kPoolSize);
        return 1;
      }else{
        pop = pmemobj_open(path, LAYOUT);
        if(pop==NULL){
          perror(path);
          exit(-1);	
        }
        cceh_pmem = POBJ_ROOT(pop, struct CCEH_pmem);
        global_depth = D_RO(cceh_pmem)->global_depth;
        dir_pmem = D_RO(cceh_pmem)->directories;
        dir = new Directory();
        dir->load_pmem(pop, dir_pmem);
        string log_path(path);
        log_path += ".log";
        log = new Wal();
        log->open(log_path.c_str());
        return 0;
      }
    }
    void constructor(size_t global_depth_){
      global_depth = global_depth_;
      D_RW(cceh_pmem)->global_depth = global_depth;
      dir = new Directory(pop,global_depth, dir_pmem);
    }
    void set_global_depth_pmem(size_t global_depth){
	    D_RW(cceh_pmem)->global_depth = global_depth;
    }
    void* operator new(size_t size) {
      void *ret;
      posix_memalign(&ret, 64, size);
      return ret;
    }
    static void compactor(CCEH *db);
    void stop_compaction() {
      shutting_down.store(true, std::memory_order_release);
      q_cv.notify_all();
      background_worker.join();
    }
};

#endif  // EXTENDIBLE_PTR_H_
