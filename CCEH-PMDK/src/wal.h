#ifndef WAL_C
#define WAL_C

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <cstring>
#include <libpmem.h>
#include <time.h>
#include <atomic>
#include <mutex>
using namespace std;

#define LOG_POOL_SIZE ((u_int64_t)32 << 30)
#define WAL_HEADER_SIZE 512

#define WAL_MAGIC_NUM "31415926"
#define WAL_MAGIC_NUM_SIZE 8

#define WAL_SIZE_POS WAL_MAGIC_NUM_SIZE
#define WAL_CURR_POS_POS (WAL_SIZE_POS + 8)
#define WAL_CURR_POS_BEGIN (256)
#define WAL_CURR_POS_NUM (256 / 8)
//#define WAL_CURR_POS (WAL_CURR_POS_POS + 8)

#define WAL_BUFFER_SIZE 24576
#define WAL_FLUSH_SIZE 24576

#define WAL_WRITE_SUCCESS 0
#define WAL_WRITE_FAILED 1

inline void wal_persist(const int& is_pmem, const void *addr, const size_t& len)
{
    if (is_pmem) {
        return pmem_drain();
    } else {
        pmem_msync(addr, len);
        return;
    }
}

class WalLock
{
public:
	WalLock(mutex *mu) : mu_(mu) {
		mu_->lock();
	}
	~WalLock() {
		mu_->unlock();
	}
private:
	mutex *mu_;
};

class Wal
{
	public:
		Wal() {
			handler = NULL;
			current_pos_pos = 0;
			current_pos = 0;
			wal_size = 0;
			self_buffer = (char *)malloc(WAL_BUFFER_SIZE*sizeof(char));
			read_buffer = (char *)malloc(256*sizeof(char));
			read_buffer_offset = UINT64_MAX;
			used_buffer_size = 0;
		}
		~Wal() {
			close();
		}
		int open(const char *path) {
			size_t mapped_len;
			handler = pmem_map_file(path, 0, 0, 0, &mapped_len, &is_pmem);
			string magic_num;
			magic_num.append((char *)handler, 8);
			if (magic_num != WAL_MAGIC_NUM) {
				cerr << "The specified file is not a valid WAL file." << endl;
				return 1;
			}
			memcpy(&wal_size, ((uint8_t *)handler + WAL_SIZE_POS), 8);
			memcpy(&current_pos_pos, ((uint8_t *)handler + WAL_CURR_POS_POS), 1);
			memcpy(&current_pos, ((uint8_t *)handler + WAL_CURR_POS_BEGIN + current_pos_pos*8), 8);
			return 0;
		}
		int create(const char *path, size_t poolsize) {
			size_t mapped_len;
			handler = pmem_map_file(path, poolsize, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
			wal_size = (uint64_t)poolsize;
			pmem_memset(handler, 0, poolsize, PMEM_F_MEM_NONTEMPORAL);
			pmem_drain();
			current_pos_pos = 0;
			current_pos = 512;
			pmem_memcpy_persist((uint8_t *)handler+WAL_SIZE_POS, (char *)(&wal_size), 8);
			pmem_memcpy_persist((uint8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, (char *)(&current_pos), 8);
			pmem_memcpy_persist((uint8_t *)handler+WAL_CURR_POS_POS, (char *)(&current_pos_pos), 1);
			pmem_memcpy_persist(handler, WAL_MAGIC_NUM, WAL_MAGIC_NUM_SIZE);
			return 0;
		}
		void close() {
			persistBuffer();
			pmem_unmap(handler, wal_size);
			handler = NULL;
		}
		uint64_t append(const void *buf, const size_t& count) {
			if (used_buffer_size + count > WAL_BUFFER_SIZE) {
				//need to first persist the buffer content
				persistBuffer();
				if (count > WAL_BUFFER_SIZE) {
					//persist the data directly
					return persistData(buf, count);
				} else {
					//copy the data to wal buffer
					memcpy(self_buffer+used_buffer_size, buf, count);
					uint64_t write_point = current_pos + used_buffer_size;
					used_buffer_size += count;
					return write_point;
				}
			} else {
				//just copy data to the log buffer
				memcpy(self_buffer+used_buffer_size, buf, count);
				uint64_t write_point = current_pos + used_buffer_size;
				used_buffer_size += count;
				return write_point;
			}
		}
		void rewind() {
			pmem_memset_persist(handler, 0, WAL_MAGIC_NUM_SIZE);
			current_pos = 512;
			current_pos_pos = (current_pos_pos+1) % WAL_CURR_POS_NUM;
			pmem_memcpy_persist((uint8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, (char *)(&current_pos), 8);
			pmem_memcpy_persist((uint8_t *)handler+WAL_CURR_POS_POS, (char *)(&current_pos_pos), 1);
			pmem_memcpy_persist(handler, WAL_MAGIC_NUM, WAL_MAGIC_NUM_SIZE);
		}
		void *get_handler() {
			return handler;
		}
		char *get_entry(int64_t pos) {
			if ((uint64_t)pos < current_pos) {
				return ((char *)handler + pos);
			} else if ((uint64_t)pos < current_pos + used_buffer_size) {
				return (self_buffer+((uint64_t)pos-current_pos));
			} else {
				printf("ERROR: Get log pos out of range %lu : %lu + %u\n", 
					(uint64_t)pos, current_pos, used_buffer_size);
				exit(1);
			}
		}
		uint64_t get_current_writepoint() {
			return current_pos+used_buffer_size;
		}
		uint64_t get_wal_size() {
			return wal_size;
		}
		void flush() {
			persistBuffer();
		}
		
	private:
		void persistBuffer() {
			if (used_buffer_size != 0) {
				if (current_pos + used_buffer_size >= wal_size) {
					cerr << "Write WAL failed as the wal is full." << endl;
					exit(1);
				}
				pmem_memcpy((uint8_t *)handler+current_pos, (char *)self_buffer, used_buffer_size, PMEM_F_MEM_NONTEMPORAL);
				wal_persist(is_pmem, (uint8_t *)handler+current_pos, used_buffer_size);
				current_pos += used_buffer_size;
				pmem_memcpy((uint8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, (char *)(&current_pos), 8, PMEM_F_MEM_NONTEMPORAL);
				wal_persist(is_pmem, (uint8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, 8);
				used_buffer_size = 0;
			}
		}
		uint64_t persistData(const void *buf, const size_t& count) {
			uint64_t write_point = current_pos;
			if (current_pos + count >= wal_size) {
				cerr << "Write WAL failed as the wal is full." << endl;
				exit(1);
			}
			pmem_memcpy((uint8_t *)handler+current_pos, (char *)buf, count, PMEM_F_MEM_NONTEMPORAL);
			wal_persist(is_pmem, (uint8_t *)handler+current_pos, count);
			current_pos += count;
			pmem_memcpy((uint8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, (char *)(&current_pos), 8, PMEM_F_MEM_NONTEMPORAL);
			wal_persist(is_pmem, (uint8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, 8);
			return write_point;
		}
		void *handler;
		uint8_t current_pos_pos;
		uint64_t current_pos;
		uint64_t wal_size;
		int is_pmem;
		char *self_buffer;
		unsigned used_buffer_size;
		mutex mu_;
		char *read_buffer;
		uint64_t read_buffer_offset;
};
#endif
