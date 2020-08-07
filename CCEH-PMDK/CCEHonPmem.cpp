#include <cstdio>
#include <ctime>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include "util/pair.h"
#include "src/CCEH.h"
#include <random>
#include <thread>
#include "util/pmm_util.h"
#include "util/perf.h"
using namespace std;

const char *const CCEH_PATH = "/mnt/pmem/zwh_test/CCEH/";
mutex cout_lock;
const size_t InsertSize = 100*1024*1024;
const int ServerNum = 1;
const size_t InsertSizePerServer = InsertSize/ServerNum;
const Value_t ConstValue[2] = {"VALUE_1", "value_2"};

inline uint64_t GetTimeNsec()
{
    struct timespec nowtime;
    clock_gettime(CLOCK_REALTIME, &nowtime);
    return nowtime.tv_sec * 1000000000 + nowtime.tv_nsec;
}

#define CORES 39
int cores_id[CORES] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
//#define CORES 16
//int cores_id[CORES] = {0, 1, 2, 3, 4, 5, 6, 7, 16, 17, 18, 19, 20, 21, 22, 23};
static int core_num = 0;

void PinCore(const char *name) {
    cout_lock.lock();
    //printf("Pin %s to thread: %2d.\n", name, cores_id[core_num]);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cores_id[core_num], &cpuset);
    pthread_t thread;
    thread = pthread_self();
    int rc = pthread_setaffinity_np(thread,
                                    sizeof(cpu_set_t), &cpuset);
    core_num = (core_num+1)%CORES;
    if (rc != 0) {
        fprintf(stderr,"Error calling pthread_setaffinity_np: %d \n", rc);
    }
    cout_lock.unlock();
}

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

struct server_thread_param {
    int id;
    CCEH *db;
    bool *start;
    int *readyCount;
    server_thread_param(int _id, CCEH *_db, bool *_start, int *_readyCount) :
        id(_id), db(_db), start(_start), readyCount(_readyCount) {}
};

void ServerThread(struct server_thread_param *p)
{
    int id = p->id;
    CCEH *db = p->db;
    Key_t key;
    __sync_fetch_and_add(p->readyCount, 1);
    while(!*(p->start)) {}
    for (unsigned i = 0; i < InsertSizePerServer; i++) {
        db->Insert(i*ServerNum+id, &ConstValue[i%2]);
    }
}

int main(int argc, char* argv[]){
    pid_t pid = getpid();
    printf("Process ID %d\n", pid);
    std::string mem_command = "cat /proc/" + std::to_string(pid) + "/status >> mem_dump";
    zExecute(mem_command);
	int failSearch = 0;
    debug_perf_ppid();
	struct timespec start, end;
	uint64_t elapsed = 0;
    uint64_t restart_time = 0;
    printf("Hashtable");fflush(stdout);
    CCEH *HashTables[ServerNum];
    clock_gettime(CLOCK_REALTIME, &start);
    for (int i = 0; i < ServerNum; i++) {
        string table_path = CCEH_PATH+std::to_string(i)+".data";
        HashTables[i] = new CCEH(table_path.c_str());
    }
    clock_gettime(CLOCK_REALTIME, &end);
    restart_time = ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
    std::cout << "Boot time: " << restart_time << "ns." << std::endl;
    zExecute(mem_command);
	printf("!");fflush(stdout);
	if(!strcmp(argv[1], "-r")){
        return 0;
	}else if(!strcmp(argv[1], "-w")){
        return 0;
	} else if(!strcmp(argv[1], "-wr")){
        thread server_threads[ServerNum];
        struct server_thread_param *params[ServerNum];
        bool start = false;
        int readyCount = 0;
        for (int i = 0; i < ServerNum; i++) {
            params[i] = new server_thread_param(i, HashTables[i], &start, &readyCount);
        }
        std::cout << "Begin to put..." << std::endl;fflush(stdout);
        {
        util::IPMWatcher watcher("cceh_put");
        for (int i = 0; i < ServerNum; i++) {
            server_threads[i] = thread(ServerThread, params[i]);
        }
        while (readyCount < ServerNum) {}
        cout << "All servers are ready." << endl;
        clock_gettime(CLOCK_REALTIME, &start);
        start = true;
        for (int i = 0; i < ServerNum; i++) {
            server_threads[i].join();
        }
        clock_gettime(CLOCK_REALTIME, &end);
        elapsed = ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
        /*
        for(Key_t i = 0; i < InserS;i++) {
            clock_gettime(CLOCK_REALTIME, &start);
            HashTable->Insert(i, i*1931+1);
            clock_gettime(CLOCK_REALTIME, &end);
            elapsed += ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
        }
        */
        std::cout << "Put Entries: " << InsertSize << ", size " 
            << ((double)(InsertSize*sizeof(size_t)*2))/1024/1024 << "MB, time: " << elapsed 
            << "ns, avg_time " << ((double)elapsed)/InsertSize << "ns, ops: " 
            << InsertSize/(((double)elapsed)/1000000000)/1024/1024 << "Mops." << std::endl;
        zExecute(mem_command);
        }
        std::cout << "Begin to get..." << std::endl;
        {
	    fflush(stdout);
        default_random_engine re(time(0));
        uniform_int_distribution<Key_t> u(0, InsertSize-1);
        elapsed = 0;
	    uint64_t r_span = 0, r_max = 0, r_min = ~0;
        unsigned entries_to_get = 1024*1024;
        Key_t t_key;
        util::IPMWatcher watcher("cceh_get");
        debug_perf_switch();
        for(unsigned i = 0; i < entries_to_get; i++){
            t_key = u(re);
            clock_gettime(CLOCK_REALTIME, &start);
            auto ret = HashTables[t_key%ServerNum]->Get(t_key);
            clock_gettime(CLOCK_REALTIME, &end);
            r_span = ((end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec));
	        elapsed += r_span;
	        if (r_span > r_max) r_max = r_span;
	        if (r_span < r_min) r_min = r_span;
        }
        debug_perf_stop();
        std::cout << "Get Entries: " << entries_to_get << ", size " 
            << ((double)(entries_to_get*sizeof(size_t)*2))/1024/1024 << "MB, time: " << elapsed 
            << "ns, avg_time " << ((double)elapsed)/entries_to_get << "ns, ops: " 
            << entries_to_get/(((double)elapsed)/1000000000)/1024/1024 << "Mops, min " << r_min 
            << ", max " << r_max << std::endl;
        }
    }else{
        return 0;
	}
    for (int i = 0; i < ServerNum; i++)
        delete HashTables[i];
	return 0;
}


