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

//#define NO_LOCK;

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
  //while (!lock()) { asm("nop"); }
  //lock();
  std::unique_lock<std::mutex> lck(m_);
  //if (sema == -1) return 2;
  pair_insert_dram(key, value);
  link_head = value;

  if (dpair_num == kBufferSlot) { /* change to imm_dpairs */
    //while(imm_dpairs.load(std::memory_order_acquire) != nullptr) {asm("nop");}
    cv_.wait(lck, [this]{return (this->imm_dpairs.load(std::memory_order_acquire) == nullptr);});
    if (sema == -1) return 0;
    imm_dpairs.store(dpairs, std::memory_order_release);
    if (spared_dpairs == nullptr) {
      fprintf(stderr, "The spared dpairs should not be empty!\n");
      fflush(stderr);
      exit(1);
    }
    dpairs = spared_dpairs;
    spared_dpairs = nullptr;
    dpair_num = 0;
    ret = 1;
    //ret = 1;
    //sema = -1;
  }
  //unlock();
  return ret;
}


void Segment::Insert4split(Key_t& key, Value_t value, size_t loc) {
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    //auto slot = (loc+i) % kNumSlot;
    auto slot = (loc+i)&kNumSlotMask;
    Key_t key_ = get_key(slot);
    if (key_ == INVALID) {
      pair_insert_pmem(slot,key,value);
      return;
    }
  }
}

Segment** Segment::Split(PMEMobjpool *pop) {
  using namespace std;
  //int64_t lock = 0;
  //if (!CAS(&sema, &lock, -1)) return nullptr;
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

  Segment** split = new Segment*[2];

  split[0] = new Segment(pop, local_depth+1);
  Pair *pmem_pairs_buffer_s1 = (Pair *)malloc(kNumSlot*sizeof(Pair));
  pmemobj_memcpy(pool_handler,
                pmem_pairs_buffer_s1,
                &(D_RO(split[0]->pairs)[0]),
                kNumSlot*sizeof(Pair),
                PMEMOBJ_F_MEM_NONTEMPORAL);

  split[1] = new Segment(pop, local_depth+1);
  Pair *pmem_pairs_buffer_s2 = (Pair *)malloc(kNumSlot*sizeof(Pair));
  pmemobj_memcpy(pool_handler,
                pmem_pairs_buffer_s2,
                &(D_RO(split[1]->pairs)[0]),
                kNumSlot*sizeof(Pair),
                PMEMOBJ_F_MEM_NONTEMPORAL);
  // split entries in the pmem part
  for (unsigned i = 0; i < kNumSlot; ++i) {
    if (pmem_pairs_buffer[i].key == INVALID) continue;
    auto key_hash = h(&(pmem_pairs_buffer[i].key), sizeof(Key_t));
    auto loc = (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine;
    bool success = false;
    if (!(key_hash & ((size_t) 1 << (local_depth)))) {
      for (unsigned j = 0; j < kNumPairPerCacheLine * kNumCacheLine; ++j) {
        auto slot = (loc+j)&kNumSlotMask;
        if (pmem_pairs_buffer_s1[slot].key == INVALID || 
              pmem_pairs_buffer_s1[slot].key == pmem_pairs_buffer[i].key) {
          //printf("  move key %lu to s1 slot %lu\n", pmem_pairs_buffer[i].key, slot);
          pmem_pairs_buffer_s1[slot].key = pmem_pairs_buffer[i].key;
          pmem_pairs_buffer_s1[slot].value = pmem_pairs_buffer[i].value;
          success = true;
          break;
        }
      }
    } else {
      for (unsigned j = 0; j < kNumPairPerCacheLine * kNumCacheLine; ++j) {
        auto slot = (loc+j)&kNumSlotMask;
        if (pmem_pairs_buffer_s2[slot].key == INVALID || 
              pmem_pairs_buffer_s2[slot].key == pmem_pairs_buffer[i].key) {
          //printf("  move key %lu to s2 slot %lu\n", pmem_pairs_buffer[i].key, slot);
          pmem_pairs_buffer_s2[slot].key = pmem_pairs_buffer[i].key;
          pmem_pairs_buffer_s2[slot].value = pmem_pairs_buffer[i].value;
          success = true;
          break;
        }
      }
    }
    if (!success) {
      fprintf(stderr, "Failed to split segment!\n");
      fflush(stderr);
      exit(1);
    }
  }
  // split entries in the l0 part
  for (unsigned i = 0; i < kL0Slot; ++i) {
    auto key_hash = h(&(l0_pair_buffer[i].key), sizeof(Key_t));
    auto loc = (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine;
    bool success = false;
    if (!(key_hash & ((size_t) 1 << (local_depth)))) {
      for (unsigned j = 0; j < kNumPairPerCacheLine * kNumCacheLine; ++j) {
        auto slot = (loc+j)&kNumSlotMask;
        if (pmem_pairs_buffer_s1[slot].key == INVALID || 
              pmem_pairs_buffer_s1[slot].key == l0_pair_buffer[i].key) {
          //printf("  move key %lu to s1 slot %lu\n", l0_pair_buffer[i].key, slot);
          pmem_pairs_buffer_s1[slot].key = l0_pair_buffer[i].key;
          pmem_pairs_buffer_s1[slot].value = l0_pair_buffer[i].value;
          success = true;
          break;
        }
      }
    } else {
      for (unsigned j = 0; j < kNumPairPerCacheLine * kNumCacheLine; ++j) {
        auto slot = (loc+j)&kNumSlotMask;
        if (pmem_pairs_buffer_s2[slot].key == INVALID || 
              pmem_pairs_buffer_s2[slot].key == l0_pair_buffer[i].key) {
          //printf("  move key %lu to s2 slot %lu\n", l0_pair_buffer[i].key, slot);
          pmem_pairs_buffer_s2[slot].key = l0_pair_buffer[i].key;
          pmem_pairs_buffer_s2[slot].value = l0_pair_buffer[i].value;
          success = true;
          break;
        }
      }
    }
    if (!success) {
      fprintf(stderr, "Failed to split segment!\n");
      fflush(stderr);
      exit(1);
    }
  }
  // split entries in the dram part
  for (unsigned i = 0; i < dpair_num; ++i) {
    auto key_hash = h(&(l0_pair_buffer[i].key), sizeof(Key_t));
    auto loc = (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine;
    bool success = false;
    if (!(key_hash & ((size_t) 1 << (local_depth)))) {
      for (unsigned j = 0; j < kNumPairPerCacheLine * kNumCacheLine; ++j) {
        auto slot = (loc+j)&kNumSlotMask;
        if (pmem_pairs_buffer_s1[slot].key == INVALID || 
              pmem_pairs_buffer_s1[slot].key == l0_pair_buffer[i].key) {
          //printf("  move key %lu to s1 slot %lu\n", l0_pair_buffer[i].key, slot);
          pmem_pairs_buffer_s1[slot].key = l0_pair_buffer[i].key;
          pmem_pairs_buffer_s1[slot].value = l0_pair_buffer[i].value;
          success = true;
          break;
        }
      }
    } else {
      for (unsigned j = 0; j < kNumPairPerCacheLine * kNumCacheLine; ++j) {
        auto slot = (loc+j)&kNumSlotMask;
        if (pmem_pairs_buffer_s2[slot].key == INVALID || 
              pmem_pairs_buffer_s2[slot].key == l0_pair_buffer[i].key) {
          //printf("  move key %lu to s2 slot %lu\n", l0_pair_buffer[i].key, slot);
          pmem_pairs_buffer_s2[slot].key = l0_pair_buffer[i].key;
          pmem_pairs_buffer_s2[slot].value = l0_pair_buffer[i].value;
          success = true;
          break;
        }
      }
    }
    if (!success) {
      fprintf(stderr, "Failed to split segment!\n");
      fflush(stderr);
      exit(1);
    }
  }

  pmemobj_memcpy_persist(pop,
                        &D_RW(split[0]->pairs)[0],
                        pmem_pairs_buffer_s1,
                        kNumSlot*sizeof(Pair));
  pmemobj_memcpy_persist(pop,
                        &D_RW(split[1]->pairs)[0],
                        pmem_pairs_buffer_s2,
                        kNumSlot*sizeof(Pair));
  clflush((char*)split[0], sizeof(Segment));
  clflush((char*)split[1], sizeof(Segment));
  free(l0_pair_buffer);
  free(pmem_pairs_buffer);
  free(pmem_pairs_buffer_s1);
  free(pmem_pairs_buffer_s2);
  return split;
}

int Segment::minor_compaction() {
  /*
  if (dpair_num < kBufferSlot) {
    fprintf(stderr, "ERROR: Too early to triger minor compaction.\n");
  } else if (dpair_num > kBufferSlot) {
    fprintf(stderr, "ERROR: Buffer overflown.\n");
  }*/
  if (l0_pair_num >= kL0Slot) {
    fprintf(stderr, "ERROR: L0 over flown.\n");
  }
  //fprintf(stderr, "P1, l0_pair_num %lu, imm_dpairs %lu\n", l0_pair_num, imm_dpairs.load());
  pmemobj_memcpy_persist(pool_handler, 
                        &(D_RW(l0_pairs)[l0_pair_num]),
                        imm_dpairs.load(std::memory_order_acquire),
                        kBufferSlot*sizeof(Pair));
  
  //pmem_persist(&(D_RW(l0_pairs)[l0_pair_num]), kBufferSlot*sizeof(Pair));
  l0_pair_num += kBufferSlot;
  D_RW(seg_pmem)->l0_pair_num = l0_pair_num;
  //dpair_num = 0;
  spared_dpairs = imm_dpairs.load(std::memory_order_acquire);
  imm_dpairs.store(nullptr, std::memory_order_release);
  int ret = (l0_pair_num == kL0Slot ? 1 : 0);
  //fprintf(stderr, " Minor compaction success.\n");
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
        //printf("   key %lu moved to pos %lu with old key %lu.\n", l0_pair_buffer[i].key, slot, pmem_pairs_buffer[slot].key);
        pmem_pairs_buffer[slot].key = l0_pair_buffer[i].key;
        pmem_pairs_buffer[slot].value = l0_pair_buffer[i].value;
        successed = true;
        break;
      }
    }
    if (!successed) {
      //fprintf(stderr, "ERROR: Failed to finish major compaction.\n");
      //exit(1);
      free(l0_pair_buffer);
      free(pmem_pairs_buffer);
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

void CCEH::Insert(Key_t& key, char *value) {
  bool log_entry_inserted = false;
  size_t log_entry_pos = INVALID;//reinterpret_cast<Value_t>(value);//INVALID;
STARTOVER:
  auto key_hash = h(&key, sizeof(key));
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

RETRY:
  auto x = (key_hash % dir->capacity);
  auto target = dir->_[x];
  if (!log_entry_inserted) {
    size_t entry_size = 24+strlen(value)+1;
    char *log_entry = (char *)malloc(entry_size);
    *(Key_t *)log_entry = key;
    *(size_t *)(log_entry+8) = target->link_head;
    *(size_t *)(log_entry+16) = strlen(value)+1;
    memcpy(log_entry+24, value, strlen(value)+1);
    log_entry_pos = log->append(log_entry, entry_size);
    log_entry_inserted = true;
  }
  //auto ret = target->Insert(key, value, y, key_hash);
  auto ret = target->Insert(key, log_entry_pos, y, key_hash);
  if (ret && !background_worker_working) {
    background_worker_working = true;
    background_worker = std::thread(compactor, this);
  }
  if (ret) {
    std::lock_guard<mutex> lck(q_lock);
    segment_q.push(x);
    q_cv.notify_all();
  }
  return;

  if (ret == 1) {
    while(!target->lock()) { asm("nop"); }
    ret = target->minor_compaction();
    if (ret == 1) {
      ret = target->major_compaction();
      if (ret == 1) {
        // TODO: should do split when major compaction failed
        //fprintf(stderr, "Major compaction failed.\n");
        //exit(1);
        //fprintf(stderr, "Spliting...\n");
        Segment **s = target->Split(pop);
        
        /* update directory */
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
            fprintf(stderr, "Doubling to %lu.\n", global_depth);
            fflush(stderr);
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
        /* end of update directory */
        //fprintf(stderr, "Split successed\n");
      }
    }
    target->sema = 0;
    target->unlock();
    return;


    // Segment** s = target->Split(pop);
    // if (s == nullptr) {
    //   goto RETRY;
    // }

    // s[0]->pattern = (key_hash % (size_t)pow(2, s[0]->local_depth-1));
    // s[1]->pattern = s[0]->pattern + (1 << (s[0]->local_depth-1));
    // s[0]->set_pattern_pmem(s[0]->pattern);
    // s[1]->set_pattern_pmem(s[1]->pattern);
    // // Directory management
    // while (!dir->Acquire()) {
    //   asm("nop");
    // }
    // { // CRITICAL SECTION - directory update
    //   x = (key_hash % dir->capacity);
    //   if (dir->_[x]->local_depth < global_depth) {  // normal split
    //     dir->LSBUpdate(s[0]->local_depth, global_depth, dir->capacity, x, s);
    //   } else {  // directory doubling
    //     auto d = dir->_;
    //     auto _dir = new Segment*[dir->capacity*2];
    //     memcpy(_dir, d, sizeof(Segment*)*dir->capacity);
    //     memcpy(_dir+dir->capacity, d, sizeof(Segment*)*dir->capacity);
    //     _dir[x] = s[0];
    //     _dir[x+dir->capacity] = s[1];
    //     clflush((char*)&dir->_[0], sizeof(Segment*)*dir->capacity);
    //     dir->_ = _dir;
    //     clflush((char*)&dir->_, sizeof(void*));
    //     dir->capacity *= 2;
    //     clflush((char*)&dir->capacity, sizeof(size_t));
    //     global_depth += 1;
    //     clflush((char*)&global_depth, sizeof(global_depth));

    //     dir->doubling_pmem();
    //     dir->segment_bind_pmem(x, s[0]);
    //     dir->segment_bind_pmem(x+dir->capacity/2, s[1]);
    //     set_global_depth_pmem(global_depth);
    //     delete d;
    //     // TODO: requiered to do this atomically
    //   }
    // }  // End of critical section
    // while (!dir->Release()) {
    //   asm("nop");
    // }
    // goto RETRY;
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
    shutting_down.store(false);
    constructor(0);
    std::cout << "initCap 1 depth 0" << std::endl;
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
    shutting_down.store(false);
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
  //shutting_down.store(true, std::memory_order_release);
  //background_worker.join();
  stop_compaction();
  std::cout << "SizeofDirectly: " << sizeof(struct Directory) << ", SizeofSegments: " << dir->segment_size() << std::endl
            << "SegmentNum: " << dir->segment_num() << ", SingleSegmentSize: " << sizeof(struct Segment) << std::endl;
  //std::cout << "Total entries put: " << put_entry_num << ", probe time per entry: " << put_probe_time/(double)put_entry_num << std::endl;
  //std::cout << "Total entries get: " << get_entry_num << ", probe time per entry: " << get_probe_time/(double)get_entry_num << std::endl;
}

//Value_t CCEH::Get(Key_t& key) {
char *CCEH::Get(Key_t& key) {
  //printf("key %5lu", key);
STARTOVER:
  auto key_hash = h(&key, sizeof(key));
  const size_t mask = dir->capacity-1;
  auto x = (key_hash & mask);
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

  auto seg = dir->_[x];
  //printf("getting key %lu from segment %lu.\n", key, x);
  if (seg->sema == -1) goto STARTOVER;
  //while (!seg->lock()) { asm("nop"); }
  {/* critical section */
  std::lock_guard<std::mutex> lck(seg->m_);
  if (seg->sema == -1) goto STARTOVER;
  for (unsigned i = 0; i < seg->dpair_num; ++i) {
    if (seg->dpairs[i].key == key) {
      Value_t v = seg->dpairs[i].value;
      //printf("  found in dram pos %u\n", i);
      //seg->unlock();
      char *res = log->get_entry(v)+24;
      return res;
      //return v;
    }
  }
  if (seg->imm_dpairs.load(std::memory_order_acquire) != nullptr) {
    for (unsigned i = 0; i < seg->kBufferSlot; ++i) {
      if (seg->imm_dpairs[i].key == key) {
        Value_t v = seg->imm_dpairs[i].value;
        //printf("  found in imm dram pos %u\n", i);
        //seg->unlock();
        char *res = log->get_entry(v)+24;
        return res;
        //return v;
      }
    }
  }
  for (unsigned i = 0; i < seg->l0_pair_num; ++i) {
    if (D_RO(seg->l0_pairs)[i].key == key) {
      Value_t v = D_RO(seg->l0_pairs)[i].value;
      //printf("  found in l0 pos %u\n", i);
      //seg->unlock();
      char *res = log->get_entry(v)+24;
      return res;
      //return v;
    }
  }
  //get_entry_num++;
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    //auto slot = (y+i) % Segment::kNumSlot;
    auto slot = (y+i) & Segment::kNumSlotMask;
    //get_probe_time++;
    Key_t key_ = dir->_[x]->get_key(slot);
    //printf("  checking slot %lu with key %lu.\n", slot, key_);
    if (key_ == key) {
      Value_t v = dir->_[x]->get_value(slot);
      //printf("  found in pmem pos %lu\n", slot);
      //seg->unlock();
      char *res = log->get_entry(v)+24;
      return res;
      //return v;
    }
  }
  }/* end of critical section */
  //printf("  not found\n");
  //seg->unlock();
  //return NONE;
  return nullptr;
}

void CCEH::compactor(CCEH *db) {
  fprintf(stderr, "Begin background compaction.\n");
  while(!db->shutting_down.load(std::memory_order_acquire)) {
    //for (size_t i = 0; i < db->dir->capacity; ++i) {
    std::unique_lock<mutex> lck(db->q_lock);
    db->q_cv.wait(lck, [db]{return !(db->segment_q.empty());});
    lck.unlock();
    while (!db->segment_q.empty()) {
      lck.lock();
      auto x = db->segment_q.front();
      db->segment_q.pop();
      lck.unlock();
      if (db->dir->_[x]->imm_dpairs.load(std::memory_order_acquire) != nullptr) {
        /* imm_dpairs is not empty, minor compaction is needed */
        Segment *target = db->dir->_[x];
        target->lock();
        //fprintf(stderr, "Minor compact segment %lu.\n", i);
        int res = target->minor_compaction();
        if (res == 1) {/* need major compaction */
          //fprintf(stderr, " Major compact segment %lu.\n", i);
          res = target->major_compaction();
          if (res == 1) {/* major compaction failed, need to split the segment */
            //fprintf(stderr, "   Splitting segment %lu....\n", i);
            target->sema = -1;
            Segment **s = target->Split(db->pop);
            
            Key_t key = D_RO(target->l0_pairs)[0].key;
            size_t key_hash = h(&key, sizeof(key));
            /* update directory */
            s[0]->pattern = (key_hash & ((size_t)pow(2, s[0]->local_depth-1)-1));
            s[1]->pattern = s[0]->pattern + (1 << (s[0]->local_depth-1));
            s[0]->set_pattern_pmem(s[0]->pattern);
            s[1]->set_pattern_pmem(s[1]->pattern);
            // Directory management
            while (!(db->dir->Acquire())) {
              asm("nop");
            }
            { // CRITICAL SECTION - directory update
              auto x = (key_hash & (db->dir->capacity-1));
              if (db->dir->_[x]->local_depth < db->global_depth) {  // normal split
                db->dir->LSBUpdate(s[0]->local_depth, db->global_depth, db->dir->capacity, x, s);
              } else {  // directory doubling
                fprintf(stderr, "Doubling to %lu.\n", db->global_depth+1);
                fflush(stderr);
                auto d = db->dir->_;
                auto _dir = new Segment*[db->dir->capacity*2];
                memcpy(_dir, d, sizeof(Segment*)*db->dir->capacity);
                memcpy(_dir+db->dir->capacity, d, sizeof(Segment*)*db->dir->capacity);
                _dir[x] = s[0];
                _dir[x+db->dir->capacity] = s[1];
                clflush((char*)&db->dir->_[0], sizeof(Segment*)*db->dir->capacity);
                db->dir->_ = _dir;
                clflush((char*)&db->dir->_, sizeof(void*));
                db->dir->capacity *= 2;
                clflush((char*)&db->dir->capacity, sizeof(size_t));
                db->global_depth += 1;
                clflush((char*)&db->global_depth, sizeof(global_depth));

                db->dir->doubling_pmem();
                db->dir->segment_bind_pmem(x, s[0]);
                db->dir->segment_bind_pmem(x+db->dir->capacity/2, s[1]);
                db->set_global_depth_pmem(db->global_depth);
                delete d;
                //fprintf(stderr, "   finish doubling %lu.\n", db->global_depth);
                //fflush(stderr);
                // TODO: requiered to do this atomically
              }
            }  // End of critical section
            while (!db->dir->Release()) {
              asm("nop");
            }
            //delete target;
            //fprintf(stderr, " successed\n", i);
          }
        }
        target->cv_.notify_all();
        target->unlock();
      }
    }
  }
  fprintf(stderr, "Finish background compaction.\n");
}
