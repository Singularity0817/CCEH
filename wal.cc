#include "wal.h"
#include <time.h>
//#include "util.h"
//#define WAL_STATISTIC

inline void wal_persist(const int& is_pmem, const void *addr, const size_t& len)
{
    if (is_pmem) {
        return pmem_drain();
    } else {
        pmem_msync(addr, len);
        return;
    }
}

Wal::Wal()
{
	handler = NULL;
	current_pos_pos = 0;
	current_pos = 0;
	wal_size = 0;
    time_span_data_write = 0;
    time_span_metadata_write = 0;
	self_buffer = (char *)malloc(WAL_BUFFER_SIZE*sizeof(char));
}

Wal::~Wal()
{
	close();
	//handler = NULL;
#ifdef WAL_STATISTIC
    cout << "Total time span " << time_span_data_write+time_span_metadata_write 
        << "s, Data write time span " << time_span_data_write << ", meta data write time span " << time_span_metadata_write << ", "
        << time_span_metadata_write / (time_span_data_write + time_span_metadata_write) * 100 << "\% time used to write metadata." << endl;
#endif
}

int Wal::open(const char *path)
{
	cout << "Opening WAL....." << endl;
	size_t mapped_len;
	handler = pmem_map_file(path, 0, 0, 0, &mapped_len, &is_pmem);
	string magic_num;
	magic_num.append((char *)handler, 8);
	if (magic_num != WAL_MAGIC_NUM) {
		cerr << "The specified file is not a valid WAL file." << endl;
		return 1;
	}
	memcpy(&wal_size, ((u_int8_t *)handler + WAL_SIZE_POS), 8);
	memcpy(&current_pos_pos, ((u_int8_t *)handler + WAL_CURR_POS_POS), 1);
	memcpy(&current_pos, ((u_int8_t *)handler + WAL_CURR_POS_BEGIN + current_pos_pos*8), 8);
	return 0;
}

int Wal::create(const char *path, size_t poolsize)
{
	cout << "Initializing WAL....." << endl;
	size_t mapped_len;
	handler = pmem_map_file(path, poolsize, PMEM_FILE_CREATE, 0777, &mapped_len, &is_pmem);
	wal_size = (u_int64_t)poolsize;
	pmem_memset(handler, 0, poolsize, PMEM_F_MEM_NONTEMPORAL);
	pmem_drain();
	current_pos_pos = 0;
	current_pos = 512;
	pmem_memcpy_persist((u_int8_t *)handler+WAL_SIZE_POS, (char *)(&wal_size), 8);
	pmem_memcpy_persist((u_int8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, (char *)(&current_pos), 8);
	pmem_memcpy_persist((u_int8_t *)handler+WAL_CURR_POS_POS, (char *)(&current_pos_pos), 1);
	pmem_memcpy_persist(handler, WAL_MAGIC_NUM, WAL_MAGIC_NUM_SIZE);
	cout << "WAL Initialized." << endl;
	return 0;
}

void Wal::close()
{
	//persist the buffer before shutdown
	persistBuffer();
	pmem_unmap(handler, wal_size);
	handler = NULL;
	free(self_buffer);
}

void Wal::flush()
{
	persistBuffer();
}

void Wal::persistBuffer()
{
	if (used_buffer_size != 0) {
		if (current_pos + used_buffer_size >= wal_size) {
			cerr << "Write WAL failed as the wal is full." << endl;
			exit(1);
		}
		pmem_memcpy((u_int8_t *)handler+current_pos, (char *)self_buffer, used_buffer_size, PMEM_F_MEM_NONTEMPORAL);
		wal_persist(is_pmem, (u_int8_t *)handler+current_pos, used_buffer_size);
		current_pos += used_buffer_size;
		pmem_memcpy((u_int8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, (char *)(&current_pos), 8, PMEM_F_MEM_NONTEMPORAL);
		wal_persist(is_pmem, (u_int8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, 8);
		//std::cout << "Persist wal buffer with size " << used_buffer_size << ", move wal head to " << current_pos << std::endl;
		used_buffer_size = 0;
	}
}

u_int64_t Wal::persistData(const void *buf, const size_t& count)
{
	u_int64_t write_point = current_pos;
	if (current_pos + count >= wal_size) {
		cerr << "Write WAL failed as the wal is full." << endl;
		exit(1);
	}
	pmem_memcpy((u_int8_t *)handler+current_pos, (char *)buf, count, PMEM_F_MEM_NONTEMPORAL);
	wal_persist(is_pmem, (u_int8_t *)handler+current_pos, count);
	current_pos += count;
	pmem_memcpy((u_int8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, (char *)(&current_pos), 8, PMEM_F_MEM_NONTEMPORAL);
	wal_persist(is_pmem, (u_int8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, 8);
	return write_point;
}

u_int64_t Wal::append(const void *buf, const size_t& count)
{
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
			/*
			if (used_buffer_size >= WAL_FLUSH_SIZE) {
				persistBuffer();
			}
			*/
			return write_point;
		}
	} else {
		//just copy data to the log buffer
		memcpy(self_buffer+used_buffer_size, buf, count);
		uint64_t write_point = current_pos + used_buffer_size;
		used_buffer_size += count;
		/*
		//check if we should persist the buffer now
		if (used_buffer_size >= WAL_FLUSH_SIZE) {
			persistBuffer();
		}
		*/
		return write_point;
	}
}

char *Wal::get_entry(int64_t pos) 
{
	if ((uint64_t)pos < current_pos) {
		//printf("target entry in log.\n");
		return ((char *)handler + pos);
	} else if ((uint64_t)pos < current_pos + used_buffer_size) {
		//printf("target entry in buffer.\n");
		return (self_buffer+((uint64_t)pos-current_pos));
	} else {
		printf("ERROR: Get log pos out of range %lu : %lu + %lu\n", 
			(uint64_t)pos, current_pos, used_buffer_size);
		exit(1);
	}
}

void Wal::rewind()
{
	pmem_memset_persist(handler, 0, WAL_MAGIC_NUM_SIZE);
	current_pos = 512;
    current_pos_pos = (current_pos_pos+1) % WAL_CURR_POS_NUM;
	pmem_memcpy_persist((u_int8_t *)handler+WAL_CURR_POS_BEGIN+current_pos_pos*8, (char *)(&current_pos), 8);
	pmem_memcpy_persist((u_int8_t *)handler+WAL_CURR_POS_POS, (char *)(&current_pos_pos), 1);
	pmem_memcpy_persist(handler, WAL_MAGIC_NUM, WAL_MAGIC_NUM_SIZE);
}

void *Wal::get_handler()
{
	return handler;
}

u_int64_t Wal::get_current_writepoint()
{
	return current_pos;
}

u_int64_t Wal::get_wal_size()
{
	return wal_size;
}

void *Wal::get_data_offset(u_int64_t offset) {
    return (void *)((char *)handler+offset);
}

void *Wal::get_data_handler() {
    return (void *)((char *)handler+WAL_HEADER_SIZE);
}

u_int64_t Wal::get_wal_data_size() {
    return (get_current_writepoint()-WAL_HEADER_SIZE);
}

/*
#include "./util.h"

int main(int argc, char* argv[])
{
	u_int64_t entry_size = 0;
    if (argc != 2) {
        entry_size = 256;
    } else {
        for (int i = 0; i < strlen(argv[1]); i++) {
            if (argv[1][i] < '0' || argv[1][i] > '9') {
                cout << "Wrong parameter " << argv[1] << ", the input should be a number." << endl;
            }
            entry_size = entry_size * 10 + (argv[1][i] - '0');
        }
    }
    cout << "Entry_size to test :" << entry_size << endl;
    char name[10] = "WAL";
    PinCore(name);
	class Wal first_wal;
	//if (first_wal.open("/mnt/pmem/zwh/zwh_wal_test") != 0)
	if (first_wal.create("/mnt/pmem/zwh_test/zwh_wal_test", LOG_POOL_SIZE) != 0)
	{
		return 1;
	}
    cout << "WAL created successfully." << endl;
	void *handler = first_wal.get_handler();
	string magic_num;
	magic_num.append((char *)((u_int8_t *)(handler)), 8);
	u_int64_t size;
	memcpy(&size, ((u_int8_t *)(handler) + WAL_SIZE_POS), 8);
    u_int8_t curr_pos_pos;
    memcpy(&curr_pos_pos, ((u_int8_t *)handler + WAL_CURR_POS_POS), 1);
	u_int64_t curr_pos;
	memcpy(&curr_pos, ((u_int8_t *)(handler) + WAL_CURR_POS_BEGIN + 8*curr_pos_pos), 8);
	cout << "magic num: " << magic_num << ", size: " << size << ", curr_pos_pos: " << (unsigned int)curr_pos_pos << ", curr_pos: " << curr_pos << endl;
    //u_int64_t entry_size = 4096;
    u_int64_t entry_num = (size - curr_pos) / entry_size - 1;
	char *entry = (char *)malloc(entry_size * sizeof(char));
    for (int i = 0; i < entry_size; i++) {
        *(entry+i) = '0';
    }
    cout << "Begin to test with entry_size " << entry_size << ", " << entry_num << " entries to go." << endl;
    struct timespec start_tv, end_tv;
    clock_gettime(CLOCK_REALTIME, &start_tv);
	for (u_int64_t i = 0; i < entry_num; i++) {
        first_wal.append(entry, entry_size);
    }
	clock_gettime(CLOCK_REALTIME, &end_tv);
    double time_span = (end_tv.tv_sec - start_tv.tv_sec) + (end_tv.tv_nsec - start_tv.tv_nsec)/1000000000.0;
	double throughput = ((double)entry_size * entry_num) / time_span / 1024 /1024;
    u_int64_t final_write_point = first_wal.get_current_writepoint();
    cout << "Test end, time_span " << time_span << "s, the throughput is " << throughput << "MB/s, final write point is " << final_write_point << endl << endl;
	first_wal.close();
    cout << "Reopening the log..." << endl;
    first_wal.open("/mnt/pmem/zwh_test/zwh_wal_test");
    handler = first_wal.get_handler();
    magic_num.clear();
    magic_num.append((char *)((u_int8_t *)(handler)), 8);
    memcpy(&size, ((u_int8_t *)(handler) + WAL_SIZE_POS), 8);
    memcpy(&curr_pos_pos, ((u_int8_t *)handler + WAL_CURR_POS_POS), 1);
    memcpy(&curr_pos, ((u_int8_t *)(handler) + WAL_CURR_POS_BEGIN + 8*curr_pos_pos), 8);
    cout << "magic num: " << magic_num << ", size: " << size << ", curr_pos_pos: " << (unsigned int)curr_pos_pos << ", curr_pos: " << curr_pos << endl;
    first_wal.close();
    return 0;
}
*/
