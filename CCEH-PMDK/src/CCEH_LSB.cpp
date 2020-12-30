#include <iostream>
#include <cmath>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include "util/persist.h"
#include "util/hash.h"
#include "src/CCEH.h"

extern size_t perfCounter;

//unsigned long put_entry_num = 0;
//unsigned long put_probe_time = 0;
//unsigned long get_entry_num = 0;
//unsigned long get_probe_time = 0;
/*

// This function does not allow resizing
bool CCEH::InsertOnly(Key_t& key, Value_t value) {
  auto key_hash = h(&key, sizeof(key));
  auto x = (key_hash % dir->capacity);
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

  auto ret = dir->_[x]->Insert(key, value, y, key_hash);
  if (ret == 0) {
    clflush((char*)&dir->_[x]->_[y], 64);
    return true;
  }

  return false;
}

// TODO
bool CCEH::Delete(Key_t& key) {
  return false;
}



double CCEH::Utilization(void) {
  size_t sum = 0;
  std::unordered_map<Segment*, bool> set;
  for (size_t i = 0; i < dir->capacity; ++i) {
    set[dir->_[i]] = true;
  }
  for (auto& elem: set) {
    for (unsigned i = 0; i < Segment::kNumSlot; ++i) {
      if (elem.first->_[i].key != INVALID) sum++;
    }
  }
  return ((double)sum)/((double)set.size()*Segment::kNumSlot)*100.0;
}

size_t CCEH::Capacity(void) {
  std::unordered_map<Segment*, bool> set;
  for (size_t i = 0; i < dir->capacity; ++i) {
    set[dir->_[i]] = true;
  }
  return set.size() * Segment::kNumSlot;
}

size_t Segment::numElem(void) {
  size_t sum = 0;
  for (unsigned i = 0; i < kNumSlot; ++i) {
    if (_[i].key != INVALID) {
      sum++;
    }
  }
  return sum;
}

bool CCEH::Recovery(void) {
  return false;
}

// for debugging
Value_t CCEH::FindAnyway(Key_t& key) {
  using namespace std;
  for (size_t i = 0; i < dir->capacity; ++i) {
     for (size_t j = 0; j < Segment::kNumSlot; ++j) {
       if (dir->_[i]->_[j].key == key) {
         auto key_hash = h(&key, sizeof(key));
         auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
         auto y = (key_hash & kMask) * kNumPairPerCacheLine;
         cout << bitset<32>(i) << endl << bitset<32>((x>>1)) << endl << bitset<32>(x) << endl;
         return dir->_[i]->_[j].value;
       }
     }
  }
  return NONE;
}

void Directory::SanityCheck(void* addr) {
  using namespace std;
  for (unsigned i = 0; i < capacity; ++i) {
    if (_[i] == addr) {
      cout << i << " " << _[i]->sema << endl;
      exit(1);
    }
  }
}*/
int Segment::Delete(Key_t& key, size_t loc, size_t key_hash){
  // if(sema==-1) return 2;
  // if((key_hash & (size_t)pow(2, local_depth)-1) != pattern) return 2;
  // auto lock = sema;
  // int ret = 1;
  // while(!CAS(&sema, &lock, lock+1)){
  //   lock = sema;
  // }
  //  Key_t LOCK = key;
  // for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
  //   auto slot = (loc + i) % kNumSlot;
  //   Key_t key_ = get_key(slot);
  //   if (CAS(&key_, &LOCK, SENTINEL)) {
  //     pair_insert_pmem(slot,-1,-1); 
  //     ret = 0;
  //     break;
  //   } else {
  //     LOCK = key;
  //   }
  // }
  // lock = sema;
  // while (!CAS(&sema, &lock, lock-1)) {
  //   lock = sema;
  // }
  // return ret;
}
int CCEH::Delete(Key_t& key) {
  return 0;
// STARTOVER:
//   auto key_hash = h(&key, sizeof(key));
//   auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

// RETRY:
//   auto x = (key_hash % dir->capacity);
//   auto target = dir->_[x];
//   //printf("{{{%d %d}}}\n",key,key_hash);
//   auto ret = target->Delete(key, y, key_hash);

//   if (ret == 1) {
//     return -1; //DATA NOT FOUND 
//   } else if (ret == 2) {
//     goto STARTOVER;
//   } else {
//     return 0;
//   }
}

int Segment::Insert(Key_t& key, Value_t value, size_t loc, size_t key_hash) {
  if (sema == -1) return 2;
  if ((key_hash & (size_t)pow(2, local_depth)-1) != pattern) return 2;
  int ret = 0;
  while (!lock()) { asm("nop"); }
  if (sema == -1) return 2;
  pair_insert_dram(key, value);
  if (dpair_num == kBufferSlot) {
    ret = 1;
    sema = -1;
  }
  unlock();
  return ret;
}


void Segment::Insert4split(Key_t& key, Value_t value, size_t loc) {
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc+i) % kNumSlot;
    Key_t key_ = get_key(slot);
    if (key_ == INVALID) {
      pair_insert_pmem(slot,key,value);
      return;
    }
  }
}

Segment** Segment::Split(PMEMobjpool *pop) {
  using namespace std;
  int64_t lock = 0;
  if (!CAS(&sema, &lock, -1)) return nullptr;

 Segment** split = new Segment*[2];
  split[0] = new Segment(pop, local_depth+1);
  split[1] = new Segment(pop, local_depth+1);
  //printf("Split\n");
  for (unsigned i = 0; i < kNumSlot; ++i) {
    //auto key_hash = h(&_[i].key, sizeof(Key_t));
    Key_t key_ = get_key(i);
    Value_t value_ = get_value(i);
    auto key_hash = h(&key_, sizeof(Key_t));
    if (key_hash & ((size_t) 1 << (local_depth))) {
      split[1]->Insert4split
        (key_, value_, (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine);
    } else {
      split[0]->Insert4split
        (key_, value_, (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine);
    }
  }

  clflush((char*)split[0], sizeof(Segment));
  clflush((char*)split[1], sizeof(Segment));

  return split;
}

int Segment::minor_compaction() {
  if (dpair_num < kBufferSlot) {
    fprintf(stderr, "ERROR: Too early to triger minor compaction.\n");
  } else if (dpair_num > kBufferSlot) {
    fprintf(stderr, "ERROR: Buffer overflown.\n");
  }
  if (l0_pair_num >= kL0Slot) {
    fprintf(stderr, "ERROR: L0 over flown.\n");
  }
  pmemobj_memcpy_persist(pool_handler, 
                        &(D_RW(l0_pairs)[l0_pair_num]),
                        dpairs,
                        kBufferSlot*sizeof(Pair));
  //pmem_persist(&(D_RW(l0_pairs)[l0_pair_num]), kBufferSlot*sizeof(Pair));
  l0_pair_num += kBufferSlot;
  D_RW(seg_pmem)->l0_pair_num = l0_pair_num;
  dpair_num = 0;
  int ret = (l0_pair_num == kL0Slot ? 1 : 0);
  return ret;
}

int Segment::major_compaction() {
  int ret = 0;
  Pair *l0_pair_buffer = (Pair *)malloc(kL0Slot*sizeof(Pair));
  pmemobj_memcpy(pool_handler,
                l0_pair_buffer, 
                &(D_RO(l0_pairs)[0]), 
                kL0Slot*sizeof(Pair),
                PMEMOBJ_F_MEM_NONTEMPORAL);
  Pair *pmem_pairs_buffer = (Pair *)malloc(kNumSlot*sizeof(Pair));
  pmemobj_memcpy(pool_handler,
                pmem_pairs_buffer,
                &(D_RO(pairs)[0]),
                kNumSlot*sizeof(Pair),
                PMEMOBJ_F_MEM_NONTEMPORAL);
  for (unsigned i = 0; i < kL0Slot; ++i) {
    size_t key_hash = h(&(l0_pair_buffer[i].key), sizeof(Key_t));
    size_t loc = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;
    bool successed = false;
    for (unsigned j = 0; j < kNumPairPerCacheLine * kNumCacheLine; ++j) {
      auto slot = (loc+j)&kNumSlotMask;
      if (pmem_pairs_buffer[slot].key == -1 || pmem_pairs_buffer[slot].key == l0_pair_buffer[i].key) {
        pmem_pairs_buffer[slot].key = l0_pair_buffer[i].key;
        pmem_pairs_buffer[slot].value = l0_pair_buffer[i].value;
        successed = true;
        break;
      }
    }
    if (!successed) {
      fprintf(stderr, "ERROR: Failed to finish major compaction.\n");
      exit(1);
      ret = 1;
      return ret;
    }
  }
  TOID(Pair) new_pairs;
  POBJ_ALLOC(pool_handler, &new_pairs, Pair, sizeof(Pair)*kNumSlot, NULL, NULL);
  pmemobj_memcpy_persist(pool_handler,
                        &(D_RW(new_pairs)[0]), 
                        pmem_pairs_buffer, 
                        kNumSlot*sizeof(Pair));
  //pmem_persist(&(D_RW(new_pairs)[0]), kNumSlot*sizeof(Pair));
  D_RW(seg_pmem)->pairs = new_pairs;
  mfence();
  D_RW(seg_pmem)->l0_pair_num = 0;
  pmemobj_persist(pool_handler, D_RO(seg_pmem), sizeof(seg_pmem));
  TOID(Pair) temp = pairs;
  pairs = new_pairs;
  POBJ_FREE(&temp);
  l0_pair_num = 0;
  free(l0_pair_buffer);
  free(pmem_pairs_buffer);
  return ret;
}

void Directory::LSBUpdate(int local_depth, int global_depth, int dir_cap, int x, Segment** s) {
  int depth_diff = global_depth - local_depth;
  if (depth_diff == 0) {
    if ((x % dir_cap) >= dir_cap/2) {
      _[x-dir_cap/2] = s[0];
      segment_bind_pmem(x-dir_cap/2, s[0]);
      clflush((char*)&_[x-dir_cap/2], sizeof(Segment*));
      _[x] = s[1];
      segment_bind_pmem(x, s[1]);
      clflush((char*)&_[x], sizeof(Segment*));
      //printf("lsb update : %d %d\n",x-dir_cap/2, x);
    } else {
      _[x] = s[0];
      segment_bind_pmem(x, s[0]);
      clflush((char*)&_[x], sizeof(Segment*));
      _[x+dir_cap/2] = s[1];
      segment_bind_pmem(x+dir_cap/2, s[1]);
      clflush((char*)&_[x+dir_cap/2], sizeof(Segment*));
      //printf("lsb update : %d %d\n",x, x+dir_cap/2);
    }
  } else {
    if ((x%dir_cap) >= dir_cap/2) {
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x-dir_cap/2, s);
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x, s);
    } else {
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x, s);
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x+dir_cap/2, s);
    }
  }
  return;
}

void CCEH::Insert(Key_t& key, Value_t value) {
STARTOVER:
  auto key_hash = h(&key, sizeof(key));
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

RETRY:
  auto x = (key_hash % dir->capacity);
  auto target = dir->_[x];
  auto ret = target->Insert(key, value, y, key_hash);

  if (ret == 1) {
    while(!target->lock()) { asm("nop"); }
    ret = target->minor_compaction();
    if (ret == 1) {
      ret = target->major_compaction();
      if (ret == 1) {
        // TODO: should do split when major compaction failed
        fprintf(stderr, "Major compaction failed.\n");
        exit(1);
      }
    }
    target->sema = 0;
    target->unlock();
    return;


    Segment** s = target->Split(pop);
    if (s == nullptr) {
      goto RETRY;
    }

    s[0]->pattern = (key_hash % (size_t)pow(2, s[0]->local_depth-1));
    s[1]->pattern = s[0]->pattern + (1 << (s[0]->local_depth-1));
    s[0]->set_pattern_pmem(s[0]->pattern);
    s[1]->set_pattern_pmem(s[1]->pattern);
    // Directory management
    while (!dir->Acquire()) {
      asm("nop");
    }
    { // CRITICAL SECTION - directory update
      x = (key_hash % dir->capacity);
      if (dir->_[x]->local_depth < global_depth) {  // normal split
        dir->LSBUpdate(s[0]->local_depth, global_depth, dir->capacity, x, s);
      } else {  // directory doubling
        auto d = dir->_;
        auto _dir = new Segment*[dir->capacity*2];
        memcpy(_dir, d, sizeof(Segment*)*dir->capacity);
        memcpy(_dir+dir->capacity, d, sizeof(Segment*)*dir->capacity);
        _dir[x] = s[0];
        _dir[x+dir->capacity] = s[1];
        clflush((char*)&dir->_[0], sizeof(Segment*)*dir->capacity);
        dir->_ = _dir;
        clflush((char*)&dir->_, sizeof(void*));
        dir->capacity *= 2;
        clflush((char*)&dir->capacity, sizeof(size_t));
        global_depth += 1;
        clflush((char*)&global_depth, sizeof(global_depth));

        dir->doubling_pmem();
        dir->segment_bind_pmem(x, s[0]);
        dir->segment_bind_pmem(x+dir->capacity/2, s[1]);
        set_global_depth_pmem(global_depth);
        delete d;
        // TODO: requiered to do this atomically
      }
    }  // End of critical section
    while (!dir->Release()) {
      asm("nop");
    }
    goto RETRY;
  } else if (ret == 2) {
    // Insert(key, value);
    goto STARTOVER;
  } else {
    asm("nop");
    //clflush((char*)&dir->_[x]->_[y], 64);
  }
}


CCEH::CCEH(const char* path)
{
  if(init_pmem(path)){
    constructor(0);
    size_t capacity = dir->capacity;
    for(unsigned i=0;i<capacity;i++){
      dir->_[i] = new Segment(pop, global_depth);
      dir->segment_bind_pmem(i, dir->_[i]);
      dir->_[i]->pattern = i;    
    }
  }
}

CCEH::CCEH(size_t initCap, const char* path)
{
  if(init_pmem(path)){
    constructor(log2(initCap));
    std::cout << "initCap " << initCap << " depth " << log2(initCap) << std::endl;
    size_t capacity = dir->capacity;
    for(unsigned i=0;i<capacity;i++){
      dir->_[i] = new Segment(pop, global_depth);
      dir->segment_bind_pmem(i, dir->_[i]);
      dir->_[i]->pattern = i;    
    }
  }
}

CCEH::~CCEH(void)
{
  std::cout << "SizeofDirectly: " << sizeof(struct Directory) << ", SizeofSegments: " << dir->segment_size() << std::endl
            << "SegmentNum: " << dir->segment_num() << ", SingleSegmentSize: " << sizeof(struct Segment) << std::endl;
  //std::cout << "Total entries put: " << put_entry_num << ", probe time per entry: " << put_probe_time/(double)put_entry_num << std::endl;
  //std::cout << "Total entries get: " << get_entry_num << ", probe time per entry: " << get_probe_time/(double)get_entry_num << std::endl;
}

Value_t CCEH::Get(Key_t& key) {
  
STARTOVER:
  auto key_hash = h(&key, sizeof(key));
  const size_t mask = dir->capacity-1;
  auto x = (key_hash & mask);
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

  auto seg = dir->_[x];
  if (seg->sema == -1) goto STARTOVER;
  while (!seg->lock()) { asm("nop"); }
  //if (seg->sema == -1) goto STARTOVER;
  for (unsigned i = 0; i < seg->dpair_num; ++i) {
    if (seg->dpairs[i].key == key) {
      Value_t v = seg->dpairs[i].value;
      seg->unlock();
      return v;
    }
  }
  for (unsigned i = 0; i < seg->l0_pair_num; ++i) {
    if (D_RO(seg->l0_pairs)[i].key == key) {
      Value_t v = D_RO(seg->l0_pairs)[i].value;
      seg->unlock();
      return v;
    }
  }
  //get_entry_num++;
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    //auto slot = (y+i) % Segment::kNumSlot;
    auto slot = (y+i) & Segment::kNumSlotMask;
    //get_probe_time++;
    Key_t key_ = dir->_[x]->get_key(slot);
    if (key_ == key) {
      Value_t v = dir->_[x]->get_value(slot);
      seg->unlock();
      return v;
    }
  }
  seg->unlock();
  return NONE;
}
