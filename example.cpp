#include <cstdio>
#include <ctime>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <array>
#include <memory>
#include "util/pair.h"
#include <time.h>
#include <random>
#include "src/CCEH.h"
//#include "src/Level_hashing.h"
//#include "src/cuckoo_hash.h"
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

int main(){
    const size_t initialSize = 1024*16*4;
    const size_t insertSize = 50*1024*1024;

    pid_t pid = getpid();
    printf("Process ID %d\n", pid);
    std::string mem_command = "cat /proc/" + std::to_string(pid) + "/status >> mem_dump";
    std::cout << " command is :" << mem_command << std::endl;
    zExecute(mem_command);

    CCEH* HashTable = new CCEH();
    //CuckooHash* HashTable = new CuckooHash(1024*1024);
    //LevelHashing* HashTable = new LevelHashing(10);
    zExecute(mem_command);
    /*
    u_int64_t* keys = new u_int64_t[insertSize];
    for(unsigned i=0; i<insertSize; i++){
	keys[i] = i+1;
    }
    */
    struct timespec time_start, time_end;
    double time_span;
    Key_t key;
    clock_gettime(CLOCK_REALTIME, &time_start);
    for(unsigned i=1; i<=insertSize; i++){
	//HashTable->Insert(keys[i], reinterpret_cast<Value_t>(&keys[i]));
    key = i;
    //std::cout << "Try to put key " << i << std::endl;
	HashTable->Insert(key, reinterpret_cast<Value_t>(&key));
    }
    clock_gettime(CLOCK_REALTIME, &time_end);
    time_span += ((time_end.tv_sec - time_start.tv_sec) + (time_end.tv_nsec - time_start.tv_nsec)/1000000000.0);
    double ops = insertSize/(double)time_span;
    std::cout << "Insert ops: " << ops << std::endl;
    zExecute(mem_command);
    
    int failSearch = 0;
    std::default_random_engine re(time(0));
    std::uniform_int_distribution<Key_t> u(0, insertSize-1);
    uint64_t get_time_span = 0;
    uint64_t get_time_max = 0, get_time_min = ~0, get_time_this;
    unsigned itemstoget = 1000000;
    for(unsigned i=0; i<itemstoget; i++){
	//auto ret = HashTable->Get(keys[i]);
    //key = i;
    key = u(re);
    clock_gettime(CLOCK_REALTIME, &time_start);
	auto ret = HashTable->Get(key);
    clock_gettime(CLOCK_REALTIME, &time_end);
    get_time_this = ((time_end.tv_sec - time_start.tv_sec)*1000000000 + (time_end.tv_nsec - time_start.tv_nsec));
    get_time_span += get_time_this;
    if (get_time_this > get_time_max) get_time_max = get_time_this;
    if (get_time_this < get_time_min) get_time_min = get_time_this;
    /*
	if(!ret){
	    failSearch++;
	}
    */
    }
    std::cout << "Avg Get Lat: " << get_time_span/(double)itemstoget << "ns, max " << get_time_max << "ns, min " << get_time_min << "ns." << std::endl;
    //printf("failedSearch: %d\n", failSearch);
    zExecute(mem_command);
    
    return 0;
}

	
