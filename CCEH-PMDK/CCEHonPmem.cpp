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
#include <mutex>
#include "./ycsb.h"
using namespace std;

#define RESERVER_SPACE
//#define RECORD_WA
//#define YCSB_TEST

const char *const CCEH_PATH = "/mnt/pmem0/zwh_test/CCEH/";
mutex cout_lock;
const size_t InsertSize = 2000*1024*1024;
const int ServerNum = 1;
const int ReservePow = 22 - (int)log2(ServerNum);
const size_t InsertSizePerServer = InsertSize/ServerNum;
const Value_t ConstValue[2] = {1, 2};

const size_t testTimes = 1;

const Value_t _VALUE_ = 168;

inline uint64_t GetTimeNsec()
{
    struct timespec nowtime;
    clock_gettime(CLOCK_REALTIME, &nowtime);
    return nowtime.tv_sec * 1000000000 + nowtime.tv_nsec;
}

//#define CORES 39
//int cores_id[CORES] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
//	40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59};
#define CORES 16
int cores_id[CORES] = {0, 1, 2, 3, 4, 5, 6, 7, 16, 17, 18, 19, 20, 21, 22, 23};
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

int ReadyCount = 0;
bool ThreadStart = false;

struct server_thread_param {
    int id;
    CCEH *db;
    bool *start;
    int *readyCount;
    server_thread_param(int _id, CCEH *_db, bool *_start, int *_readyCount) :
        id(_id), db(_db), start(_start), readyCount(_readyCount) {}
};
std::atomic<size_t> finishSize(0);
void ServerThread(struct server_thread_param *p)
{
    //PinCore("worker");
    int id = p->id;
    CCEH *db = p->db;
    Key_t key;
    uint64_t counter = 0;
    __sync_fetch_and_add(&ReadyCount, 1);
    //std::cout << "worker " << id << " ready. " << ReadyCount << std::endl;
    while(ThreadStart != true) {fflush(stdout);}
    //std::cout << "worker " << id << " begin to put " << std::endl;
    for (unsigned t = 0; t < testTimes; t++) {
        for (unsigned i = 0; i < InsertSizePerServer; i++) {
            Key_t key = i*ServerNum+id;
            db->Insert(key, ConstValue[i%2]);
            counter++;
            if (counter % 10000 == 0) {
                //__sync_fetch_and_add(&finishSize, counter);
                finishSize.fetch_add(counter);
                counter = 0;
                //printf("Finish item num %llu\n", finishSize);
            }
        }
    }
    //__sync_fetch_and_add(&finishSize, counter);
    finishSize.fetch_add(counter);
}
#ifdef YCSB_TEST
using namespace util;
class DBTest:public KVBase {
    public:
        DBTest(){}
        virtual void Initial(int t_num) {
            printf("KVTest thread #: %d\n", t_num);
            if (t_num != ServerNum) {
                printf("Use t_num the same as SERVERNUM.\n");
            }
            for (int i = 0; i < ServerNum; i++) {
            string table_path = CCEH_PATH+std::to_string(i)+".data";
#ifndef RESERVER_SPACE
                dbs_[i] = new CCEH(table_path.c_str());
#else
                dbs_[i] = new CCEH((size_t)pow(2, ReservePow), table_path.c_str());
#endif
            }
        }

        virtual int Put(const int64_t& key, size_t& v_size, const char* value, int tid) {
            Key_t k = (Key_t)(key);
            dbs_[tid]->Insert(k, _VALUE_);
            return 1;
        }

        virtual int Get(const int64_t  key, int64_t* value, int tid) {
            Key_t k = (Key_t)key;
            Value_t v = dbs_[tid]->Get(k);
            if (v == NONE) {
                return 0;
            } else {
                (*value) = (int64_t)(v);
                return 1;
            }
        }
    private:
        CCEH *dbs_[ServerNum];
};

int main() {
    DBTest dbtest;
    Benchmark benchmark(&dbtest);
    benchmark.Run();
}

#else
int main(int argc, char* argv[]){
    //PinCore("main");
    pid_t pid = getpid();
    //printf("Process ID %d\n", pid);
    std::string mem_command = "cat /proc/" + std::to_string(pid) + "/status >> mem_dump";
    //zExecute(mem_command);
	int failSearch = 0;
    //debug_perf_ppid();
	struct timespec time_start, time_end, time_middle;
	uint64_t elapsed = 0;
    uint64_t restart_time = 0;
    printf("Hashtable thread num %d\n", ServerNum);fflush(stdout);
    CCEH *HashTables[ServerNum];
    clock_gettime(CLOCK_REALTIME, &time_start);
    for (int i = 0; i < ServerNum; i++) {
        string table_path = CCEH_PATH+std::to_string(i)+".data";
        std::cout << "Creating table with path " << table_path << std::endl;
#ifndef RESERVER_SPACE
        HashTables[i] = new CCEH(table_path.c_str());
#else
        HashTables[i] = new CCEH((size_t)pow(2, ReservePow), table_path.c_str());
#endif
    }
    clock_gettime(CLOCK_REALTIME, &time_end);
    restart_time = ((time_end.tv_sec - time_start.tv_sec) * 1000000000 + (time_end.tv_nsec - time_start.tv_nsec));
    std::cout << "Boot time: " << restart_time << "ns." << std::endl;
    //zExecute(mem_command);
	printf("!");fflush(stdout);
	if(!strcmp(argv[1], "-r")){
        uint64_t rtime[1000];
        for (int i = 0; i < 1000; i++) rtime[i] = 0;
        std::cout << "Begin to get..." << std::endl;
        {
	    fflush(stdout);
        default_random_engine re(time(0));
        //uniform_int_distribution<Key_t> u(0, InsertSize-1);
        uniform_int_distribution<Key_t> u(InsertSize, InsertSize*10);
        elapsed = 0;
	    uint64_t r_span = 0, r_max = 0, r_min = ~0;
        unsigned entries_to_get = 100*1024*1024;
        Key_t t_key;
        //util::IPMWatcher watcher("cceh_get");
        //debug_perf_switch();
        size_t fail_get = 0;
        for(unsigned i = 0; i < entries_to_get; i++){
            t_key = u(re);
            clock_gettime(CLOCK_REALTIME, &time_start);
            auto ret = HashTables[t_key%ServerNum]->Get(t_key);
            clock_gettime(CLOCK_REALTIME, &time_end);
            r_span = ((time_end.tv_sec - time_start.tv_sec) * 1000000000 + (time_end.tv_nsec - time_start.tv_nsec));
	        elapsed += r_span;
	        if (r_span > r_max) r_max = r_span;
	        if (r_span < r_min) r_min = r_span;
            if (ret == NONE) fail_get++;
            if (r_span > 10000) {
                rtime[999]++;
            } else {
                rtime[r_span/10]++;
            }
            if (i%1000 == 0) {
                printf("\rprogress %u", i);
                fflush(stdout);
            }
        }
        //debug_perf_stop();
        std::cout << std::endl << "Get Entries: " << entries_to_get << ", fail get " << fail_get << ", size " 
            << ((double)(entries_to_get*sizeof(size_t)*2))/1024/1024 << "MB, time: " << elapsed 
            << "ns, avg_time " << ((double)elapsed)/entries_to_get << "ns, ops: " 
            << entries_to_get/(((double)elapsed)/1000000000)/1024/1024 << "Mops, min " << r_min 
            << ", max " << r_max << std::endl;
        }
        std::cout << "Read Lat PDF" << std::endl;
        for (int i = 0; i < 1000; i++) {
            printf("%d %llu\n", i*10, rtime[i]);
        }
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
        for (int i = 0; i < ServerNum; i++) {
            server_threads[i] = thread(ServerThread, params[i]);
        }
        while (ReadyCount < ServerNum) {
            fflush(stdout);
        }
        cout << "All servers are ready." << endl;
        size_t old_fs = 0;
        //clock_gettime(CLOCK_REALTIME, &time_start);
        uint64_t put_start = GetTimeNsec();
        uint64_t old_progress_checkpoint = put_start;
        uint64_t new_progress_checkpoint = 0;
        ThreadStart = true;
#ifdef RECORD_WA
        util::IPMWatcher watcher("cceh_put");
#endif
        double new_progress, old_progress = 0;
        printf("runtime    progress    ops    wa    avg_ops\n");
        while (finishSize < InsertSize) {
            //std::cout << finishSize << " : " << InsertSize << std::endl;
            size_t fs = finishSize.load(std::memory_order_acquire);
            new_progress = fs/(double)InsertSize*100.0;
            new_progress_checkpoint = GetTimeNsec();
            //if (new_progress_checkpoint - old_progress_checkpoint >= 1000000000) {
            if (new_progress - old_progress >= 0.05) {
                //printf("\rProgress %2.1lf%%", new_progress);
                //clock_gettime(CLOCK_REALTIME, &time_middle);
                //double span = (time_middle.tv_sec - time_start.tv_sec) + (time_middle.tv_nsec - time_start.tv_nsec)/1000000000.0;
                printf("%.1lf    %2.1lf%%    %.1lf    %.2lf    %.1lf\n", 
                    (new_progress_checkpoint - put_start)/1000000000.0, new_progress, 
                    (fs-old_fs)/(double)((new_progress_checkpoint - old_progress_checkpoint)/1000000000.0), 
#ifdef RECORD_WA
                    (double) watcher.CheckDataWriteToDIMM()/(fs*16.0),
#else
                    0.00,
#endif
                    fs/(double)((new_progress_checkpoint - put_start)/1000000000.0));
                //write_watcher.Checkpoint();
                fflush(stdout);
                old_progress = new_progress;
                old_fs = fs;
                old_progress_checkpoint = new_progress_checkpoint;
            }
            fflush(stdout);
        }
        //clock_gettime(CLOCK_REALTIME, &time_end);
        elapsed = GetTimeNsec() - put_start;
        for (int i = 0; i < ServerNum; i++) {
            server_threads[i].join();
        }
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
        //zExecute(mem_command);
        }
        std::cout << "Begin to get..." << std::endl;
        {
	    fflush(stdout);
        default_random_engine re(time(0));
        uniform_int_distribution<Key_t> u(0, InsertSize-1);
        //uniform_int_distribution<Key_t> u(InsertSize, InsertSize*10);
        elapsed = 0;
	    uint64_t r_span = 0, r_max = 0, r_min = ~0;
        unsigned entries_to_get = 10*1024*1024;
        Key_t t_key;
        size_t fail_get = 0;
        //util::IPMWatcher watcher("cceh_get");
        //debug_perf_switch();
        for(unsigned i = 0; i < entries_to_get; i++){
            t_key = u(re);
            clock_gettime(CLOCK_REALTIME, &time_start);
            auto ret = HashTables[t_key%ServerNum]->Get(t_key);
            clock_gettime(CLOCK_REALTIME, &time_end);
            r_span = ((time_end.tv_sec - time_start.tv_sec) * 1000000000 + (time_end.tv_nsec - time_start.tv_nsec));
	        elapsed += r_span;
	        if (r_span > r_max) r_max = r_span;
	        if (r_span < r_min) r_min = r_span;
            if (ret == NONE) fail_get++;
        }
        //debug_perf_stop();
        std::cout << "Get Entries: " << entries_to_get << ", fail get" << fail_get << ", size " 
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
#endif

