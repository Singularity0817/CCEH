#include <cstdio>
#include <ctime>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include "util/pair.h"
#include "src/CCEH.h"
#include <random>
#include "util/pmm_util.h"
#include "util/perf.h"
using namespace std;

std::string zExecute(const std::string& cmd) {
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

int main(int argc, char* argv[]){
    pid_t pid = getpid();
    printf("Process ID %d\n", pid);
    std::string mem_command = "cat /proc/" + std::to_string(pid) + "/status >> mem_dump";
    std::cout << " command is :" << mem_command << endl;
    zExecute(mem_command);
	const size_t initialSize = 4*1024*1024;
	const size_t from = argc>=4? atoi(argv[3]) : 1024; 
	const size_t to = argc==5? atoi(argv[4]) : from+1; 
	int failSearch = 0;
	//printf("Hashtable");fflush(stdout);
	//HashTable->init_pmem(argv[1]);
    debug_perf_ppid();
    /*
	uint64_t* keys = new uint64_t[to];
	uint64_t* values = new uint64_t[to];
	printf("from %d  to %d\n",from,to);
	for(unsigned i=from; i<to; i++){
		keys[i] = i;
		values[i] = i*1931+1;
	}
    */
	struct timespec start, end;
	uint64_t elapsed = 0;
    uint64_t restart_time = 0;
	//CCEH* HashTable = new CCEH(1048576, argv[2]);
	//CCEH* HashTable = new CCEH(1048576, argv[2]);
	printf("Hashtable");fflush(stdout);
    clock_gettime(CLOCK_REALTIME, &start);
	CCEH* HashTable = new CCEH(argv[2]);
    clock_gettime(CLOCK_REALTIME, &end);
    restart_time = ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
    std::cout << "Boot time: " << restart_time << "ns." << std::endl;
    zExecute(mem_command);
    Key_t t_key;
	printf("!");fflush(stdout);
    //util::IPMWatcher watcher("cceh");
	//clock_gettime(CLOCK_MONOTONIC, &start);
	if(!strcmp(argv[1], "-r")){
        default_random_engine re(time(0));
        uniform_int_distribution<Key_t> u(from, to-1);
		for(unsigned i=0;i< 10*1000*1000;i++){
            t_key = u(re);
	        clock_gettime(CLOCK_REALTIME, &start);
			auto ret = HashTable->Get(t_key);
	        clock_gettime(CLOCK_REALTIME, &end);
            elapsed += ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
            //printf("key: %d, value: %d\n", t_key, ret);
            /*
			if(!ret || ret!=(t_key*1931+1)){
				printf("Fail key: %d, value: %d, ans: %d\n", i, ret, i*1931+1);
				failSearch++;
			}
            */
		}
		//printf("Fail Search: %d",failSearch);
        std::cout << "Entries: " << to-from << ", size " << ((double)((to-from)*sizeof(size_t)*2))/1024/1024 << "MB, time: " << elapsed << "ns, avg_time " << ((double)elapsed)/(to-from) << "ns, ops: " << (to-from)/(((double)elapsed)/1000000000)/1024/1024 << "Mops." << std::endl;
	}else if(!strcmp(argv[1], "-w")){
		for(Key_t i=from;i<to;i++) {
	        clock_gettime(CLOCK_REALTIME, &start);
			HashTable->Insert(i, i*1931+1);
	        clock_gettime(CLOCK_REALTIME, &end);
            elapsed += ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
        }
        /*
		int failInsert=0;
		printf("Check\n");fflush(stdout);
		for(Key_t i=from;i<to;i++){
			auto ret= HashTable->Get(i);
			if(!ret || ret!=i*1931+1) failInsert++;
		}
        */
		//printf("Fail Insert : %d\n",failInsert);
		//for(unsigned i=0; i<insertSize; i++){
		//	HashTable->Insert(keys[i], values[i]);
		//}
        std::cout << "Entries: " << to-from << ", size " << ((double)((to-from)*sizeof(size_t)*2))/1024/1024 << "MB, time: " << elapsed << "ns, avg_time " << ((double)elapsed)/(to-from) << "ns, ops: " << (to-from)/(((double)elapsed)/1000000000)/1024/1024 << "Mops." << std::endl;
	} else if(!strcmp(argv[1], "-wr")){
        std::cout << "Begin to put..." << std::endl;fflush(stdout);
        {
        util::IPMWatcher watcher("cceh_put");
        for(Key_t i=from;i<to;i++) {
            clock_gettime(CLOCK_REALTIME, &start);
            HashTable->Insert(i, i*1931+1);
            clock_gettime(CLOCK_REALTIME, &end);
            elapsed += ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
            //zExecute("echo 1 > /proc/sys/vm/drop_caches");
        }
        std::cout << "Put Entries: " << to-from << ", size " << ((double)((to-from)*sizeof(size_t)*2))/1024/1024 << "MB, time: " << elapsed << "ns, avg_time " << ((double)elapsed)/(to-from) << "ns, ops: " << (to-from)/(((double)elapsed)/1000000000)/1024/1024 << "Mops." << std::endl;
        zExecute(mem_command);
	//std::cout << "Before clear page cache " << zExecute("free -m | grep Mem");
	//fflush(stdout);
    //zExecute("echo 1 > /proc/sys/vm/drop_caches");
	//std::cout << "After clear page cache " << zExecute("free -m | grep Mem");
	//fflush(stdout);
        }
        std::cout << "Begin to get..." << std::endl;
        {
	    fflush(stdout);
        default_random_engine re(time(0));
        uniform_int_distribution<Key_t> u(from, to-1);
        elapsed = 0;
	    uint64_t r_span = 0, r_max = 0, r_min = ~0;
        unsigned entries_to_get = 1024*1024;
        util::IPMWatcher watcher("cceh_get");
        debug_perf_switch();
        for(unsigned i=0;i< entries_to_get;i++){
            t_key = u(re);
            //zExecute("echo 1 > /proc/sys/vm/drop_caches");
	        //fflush(stdout);
            clock_gettime(CLOCK_REALTIME, &start);
            auto ret = HashTable->Get(t_key);
            clock_gettime(CLOCK_REALTIME, &end);
            r_span = ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
	        elapsed += r_span;
	        if (r_span > r_max) r_max = r_span;
	        if (r_span < r_min) r_min = r_span;
            //printf("key: %d, value: %d\n", t_key, ret);fflush(stdout);
            /*
            if(!ret || ret!=(t_key*1931+1)){
                printf("Fail key: %d, value: %d, ans: %d\n", i, ret, i*1931+1);
                failSearch++;
            }*/
            /*
	        if(i%1000 == 0){
		        std::cout << i << " entries are got" << std::endl;fflush(stdout);
	        }*/
        }
        debug_perf_stop();
        std::cout << "Get Entries: " << entries_to_get << ", size " << ((double)(entries_to_get*sizeof(size_t)*2))/1024/1024 << "MB, time: " << elapsed << "ns, avg_time " << ((double)elapsed)/entries_to_get << "ns, ops: " << entries_to_get/(((double)elapsed)/1000000000)/1024/1024 << "Mops, min " << r_min << ", max " << r_max << std::endl;
        }
    }else{
		for(uint64_t i=from;i<to;i++)
			HashTable->Delete(i);
        /*
		int failDelete = 0;
		for(int i=from; i<to; i++){
			auto ret= HashTable->Get(keys[i]);
			if(ret) failDelete ++;
		}
		printf("Fail Delete : %d\n",failDelete);
        */
	}
    delete HashTable;
	//clock_gettime(CLOCK_MONOTONIC, &end);
    //elapsed = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec)/1000000000;
    //std::cout << "Entries: " << to-from << ", size " << ((double)((to-from)*sizeof(size_t)*2))/1024/1024 << "MB, time: " << elapsed << "ns, avg_time " << ((double)elapsed)/(to-from) << "ns, ops: " << (to-from)/(((double)elapsed)/1000000000)/1024/1024 << "Mops." << std::endl;
	return 0;
}


