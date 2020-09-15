#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <cstring>
#include <libpmem.h>
#include <time.h>
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

#define WAL_BUFFER_SIZE 49152
#define WAL_FLUSH_SIZE 49152

#define WAL_WRITE_SUCCESS 0
#define WAL_WRITE_FAILED 1

#ifndef WAL_C
#define WAL_C
class Wal
{
	public:
		Wal();
		~Wal();
		int open(const char *path);
		int create(const char *path, size_t poolsize);
		void close();
		u_int64_t append(const void *buf, const size_t& count);
		void rewind(); // return the writepoint to the beginning.
		void *get_handler();
		char *get_entry(int64_t pos);
		u_int64_t get_current_writepoint(); //return the position of current writepoint.
		u_int64_t get_wal_size();
		void flush();
		void *get_data_offset(u_int64_t offset);
        void *get_data_handler();
        u_int64_t get_wal_data_size();
		
	private:
		void persistBuffer();
		u_int64_t persistData(const void *buf, const size_t& count);
		void *handler;
		u_int8_t current_pos_pos;
		u_int64_t current_pos;
		u_int64_t wal_size;
        struct timespec time_data_write_start, time_data_write_end, time_metadata_write_start, time_metadata_write_end;
        double time_span_data_write, time_span_metadata_write;
		int is_pmem;
		char *self_buffer;
		unsigned used_buffer_size = 0;
};
#endif
