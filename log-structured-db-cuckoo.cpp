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
#include <mutex>
#include <thread>
#include "util/pair.h"
#include <time.h>
#include <random>
#include <unordered_map>
#include <algorithm>
#include "wal.h"
#include "util/concurrentqueue.h"
#include "util/ipmwatcher.h"
#include "src/CCEH.h"
#include "robin_hood.h"
//#include "src/cuckoo_hash.h"
//#include "src/Level_hashing.h"
#include "ycsb.h"

using namespace std;

#define LOG_DIR_PATH "/mnt/pmem0/zwh_test/logDB/"
//#define LOG_DIR_PATH "/mnt/pmem/zwh_test/logDB/"
mutex cout_lock;
const size_t InsertSize = 1000*1024*1024;
const size_t recordInterval = 1024*1024;
//const int BatchSize = 1024;
const int ServerNum = 8;
const unsigned GetThreadNum = 16;
const size_t InsertSizePerServer = InsertSize/ServerNum;
const Value_t ConstValue[2] = {"VALUE_1", "value_2"};
const size_t testTimes = 1;
const size_t valueSize = 8;
const size_t LogEntrySize = sizeof(Key_t)+sizeof(size_t)+valueSize;
const size_t EstimateLogSize = testTimes*(512+(LogEntrySize*InsertSizePerServer)+1024);
//const size_t EstimateLogSize = 512*1024*1024;

//#define YCSB_TEST
#define RESERVE_MODE
//#define RECORD_WA
#define RECORD_AS_PROGRESS

#define RECORD_GET_LAT
#define RECORD_PUT_LAT

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

class my_unordered_map
{
    public:
        my_unordered_map() {
            //m = new unordered_map<Key_t, Value_t>;
            m = new robin_hood::unordered_map<Key_t, Value_t>;
#ifdef RESERVE_MODE
            m->reserve(InsertSize/ServerNum/0.75);
#endif
        }
        ~my_unordered_map() {
            delete m;
        }
        inline void Insert(Key_t key, Value_t value) {
            //m->insert(pair<Key_t, Value_t>(key, value));
            m->insert(robin_hood::pair<Key_t, Value_t>(key, value));
        }
        inline int Get(Key_t key, Value_t *pos) {
            auto res = m->find(key);
            if (res == m->end()) {
                return 0;
            } else {
                *pos = res->second;
                return 1;
            }
        }
        uint64_t insert_time = 0;
        uint64_t rehash_time = 0;
    private:
        //unordered_map<Key_t, Value_t> *m;
        robin_hood::unordered_map<Key_t, Value_t> *m;
};

class myDB
{
    public:
        myDB(bool create, const char *log_path) {
            //index = new CCEH();
            //index = new CuckooHash(1024*1024);
            //index = new LevelHashing(10);
            insert_buffer_size = sizeof(Key_t)+sizeof(size_t)+valueSize;
            insert_buffer = (char *)malloc(insert_buffer_size);
            index = new my_unordered_map();
            log = new Wal();
            if (create) {
                log->create(log_path, EstimateLogSize);
                uint64_t log_size = log->get_wal_size();
            } else {
                cout_lock.lock();
                cout << "Recovering db from log " << log_path << endl;
                cout_lock.unlock();
                log->open(log_path);
                void *log_data_handler = log->get_data_handler();
                uint64_t log_size = log->get_wal_data_size();
                //cout << "Log size is " << log_size << endl;
                uint64_t ancho = 0;
                size_t value_size = 0;
                unsigned counter = 0;
                double new_progress, old_progress = 0;
                while (ancho < log_size) {
#ifdef DEBUG
                    cout << "Recovering key " << *((Key_t *)((char *)log_data_handler+ancho)) << " in pos " << ancho+WAL_HEADER_SIZE << endl;
#endif
                    index->Insert(*((Key_t *)((char *)log_data_handler+ancho)), reinterpret_cast<Value_t>(ancho+WAL_HEADER_SIZE));
                    value_size = *(size_t *)((char *)log_data_handler+ancho+sizeof(Key_t));
                    ancho += (sizeof(Key_t)+sizeof(size_t)+value_size);
                    counter++;
                    new_progress = ancho/(double)log_size*100;
                    if (new_progress - old_progress >= 1) {
                        old_progress = new_progress;
                        printf("\rRecover progress %.0lf%%", old_progress);
                        fflush(stdout);
                    }
                }
                cout << endl << "Recover " << counter << " items." << endl;
            }
        }
        ~myDB() {
            delete index;
            delete log;
            //delete request_queue;
        }
        inline void Insert(Key_t &key, char *value) {
            size_t value_size = strlen(value)+1;
            int buffer_size = sizeof(Key_t)+sizeof(size_t)+value_size;
            char *buffer = (char *)malloc(buffer_size);
            memcpy(buffer, &key, sizeof(Key_t));
            memcpy(buffer+sizeof(Key_t), &value_size, sizeof(size_t));
            memcpy(buffer+sizeof(Key_t)+sizeof(size_t), value, value_size);
            auto pos = log->append(buffer, buffer_size);
            index->Insert(key, reinterpret_cast<Value_t>(pos));
            free(buffer);
        }
        inline void Insert(const int64_t &key, size_t value_size, const char *value) {
            //size_t value_size = strlen(value);
            //int buffer_size = sizeof(Key_t)+sizeof(size_t)+value_size;
            //char *buffer = (char *)malloc(buffer_size);
            memcpy(insert_buffer, &key, sizeof(int64_t));
            memcpy(insert_buffer+sizeof(int64_t), &value_size, sizeof(size_t));
            memcpy(insert_buffer+sizeof(int64_t)+sizeof(size_t), value, value_size);
            auto pos = log->append(insert_buffer, insert_buffer_size);
            index->Insert((Key_t)(key), reinterpret_cast<Value_t>(pos));
            //free(buffer);
        }
        void BatchInsert(vector<Pair> *const pairs) {
            uint64_t put_start = GetTimeNsec();
            int buffer_size = 0;
            /*
            for(auto it = pairs->begin(); it != pairs->end(); it++) {
                buffer_size += (sizeof(Key_t)+sizeof(size_t)+strlen((*it).value)+1);
            }
            */
            buffer_size = pairs->size()*(sizeof(Key_t)+sizeof(size_t)+strlen(ConstValue[0])+1);
            char *buffer = (char *)malloc(buffer_size);
            uint64_t offset = 0;
            vector<uint64_t> offsets;
            size_t value_size = 0;
            for(auto it = pairs->begin(); it != pairs->end(); it++) {
                offsets.push_back(offset);
                value_size = strlen((*it).value);
                memcpy(buffer+offset, &((*it).key), sizeof(Key_t));
                memcpy(buffer+offset+sizeof(Key_t), &value_size, sizeof(size_t));
                memcpy(buffer+offset+sizeof(Key_t)+sizeof(size_t), (*it).value, value_size+1);
                offset += (sizeof(Key_t)+sizeof(size_t)+strlen((*it).value)+1);
            }
            uint64_t middle_time = GetTimeNsec();
            insert_prepare_time += (middle_time - put_start);
            auto pos = log->append(buffer, buffer_size);
            uint64_t third_time = GetTimeNsec();
            insert_log_append_time += (third_time - middle_time);
            for(int i = 0; i < pairs->size(); i++) {
                index->Insert((pairs->at(i)).key, reinterpret_cast<Value_t>(pos+offsets[i]));
            }
            free(buffer);
            insert_index_insert_time += (GetTimeNsec() - third_time);
            //cout << "End Batch" << endl;
        }
        inline int Get(Key_t &key, char *value) {
            //Value_t pos;
            uint64_t pos;
            int res = index->Get(key, (Value_t *)(&pos));
            if (res == 0) {
                //cannot find the target key in the index
                return 0;
            } else {
                //read target key from log
                char *data_handler = log->get_entry(pos);
                size_t vsize = *((uint64_t *)data_handler+1);
                memcpy(value, data_handler+16, vsize);
                //*value = (Value_t)((char *)data_handler+sizeof(Key_t)+sizeof(size_t));
                return 1;
            }
        }
        /*
        inline int Get(const int64_t &key, int64_t *value) {
            //Value_t pos;
            uint64_t pos;
            int res = index->Get((Key_t)key, (Value_t *)(&pos));
            if (res == 0) {
                //cannot find the target key in the index
                return 0;
            } else {
                //read target key from log
                void *data_handler = log->get_entry(pos);
                //*value = *((int64_t *)((char *)data_handler+sizeof(int64_t)+sizeof(size_t)));
                memcpy(value, (char *)data_handler+sizeof(int64_t)+sizeof(size_t), sizeof(int64_t));
                return 1;
            }
        }*/
        inline void print_put_stat() {
            cout << "Insert prepare time " << insert_prepare_time << ", log append time " << insert_log_append_time << ", index insert time " << insert_index_insert_time << endl;
            cout << "    index update time " << index->insert_time << ", rehash time " << index->rehash_time << endl;
        }
    private:
        //CCEH* index;
        //CuckooHash* index;
        //LevelHashing* index;
        //Hash* index;
        my_unordered_map *index;
        Wal* log;
        uint64_t insert_prepare_time = 0;
        uint64_t insert_log_append_time = 0;
        uint64_t insert_index_insert_time = 0;
        char *insert_buffer;
        int insert_buffer_size;
        //moodycamel::ConcurrentQueue<struct Pair *> request_queue;
};

struct db_server_param
{
    int id;
    myDB *db;
    std::atomic<bool> *start;
    std::atomic<int> *readyCount;
    size_t finishSize;
    double ops;
    vector<double> put_ops;
#ifdef RECORD_PUT_LAT
    vector<unsigned> put_lats;
#endif
    db_server_param(int _id, myDB* _db, std::atomic<bool> *_start, std::atomic<int> *_readyCount)
        : id(_id), db(_db), start(_start), readyCount(_readyCount), finishSize(0) {
#ifdef RECORD_PUT_LAT
        put_lats = vector<unsigned>(1000, 0);
#endif
        put_ops = vector<double>(1001, 0.0);
        }
};

void db_server(db_server_param *p)
{
    myDB *db = p->db;
    int id = p->id;
    Key_t key;
    unsigned counter = 0;
    unsigned last_counter = 0;
    std::vector<unsigned> keys;
    for (unsigned i = 0; i < InsertSizePerServer; i++) {
        keys.push_back(i*ServerNum+id);
    }
    char value[valueSize];
    memset(value, 'a', valueSize-1);
    value[valueSize-1] = '\0';
#ifdef RECORD_PUT_LAT
    uint64_t put_begin, put_span;
#endif
    cout_lock.lock();
    cout << "Server " << id << " shuffling keys." << endl;
    cout_lock.unlock();
    std::random_shuffle(keys.begin(), keys.end());
    unsigned progress = 0;
    cout_lock.lock();
    cout << "Server " << id << " is ready with db " << db << "." << endl;
    cout_lock.unlock();
    p->readyCount->fetch_add(1);
    while(!(p->start->load(std::memory_order_relaxed))) {}
    uint64_t thread_start = GetTimeNsec();
    uint64_t last_checkpoint = thread_start;
    uint64_t new_checkpoint;
    for (unsigned t = 0; t < testTimes; t++) {
        for (auto it = keys.begin(); it != keys.end(); it++) {
            //key = i*ServerNum+id;
            key = *it;
#ifdef RECORD_PUT_LAT
            put_begin = GetTimeNsec();
#endif
            db->Insert(key, valueSize, value);//ConstValue[0]);
#ifdef RECORD_PUT_LAT
            put_span = GetTimeNsec()-put_begin;
            if (put_span/10 < 1000) ++p->put_lats[put_span/10];
            else ++p->put_lats[999];
#endif
            counter++;
            if (counter*1000.0/InsertSizePerServer >= progress+1) {
                new_checkpoint = GetTimeNsec();
                p->put_ops[progress] = (counter-last_counter)*1000000000.0/(new_checkpoint-last_checkpoint);
                //fprintf(stderr, "\rprogress %2.1f%%, ops %.0lf", progress/10.0, p->put_ops[progress]);
                last_checkpoint = new_checkpoint;
                last_counter = counter;
                progress++;
            }
        }
    }
    p->ops = (InsertSizePerServer * testTimes) / ((GetTimeNsec() - thread_start) / 1000000000.0);
}

struct db_get_thread_param
{
    myDB **dbs;
    unsigned item_to_get;
    uint64_t key_range;
    std::atomic<bool> *start;
    std::atomic<unsigned> *ready_count;
    unsigned failed_get = 0;
    std::vector<unsigned> get_lats;
    db_get_thread_param(myDB **_dbs, unsigned _item, uint64_t _range, std::atomic<bool> *_s,
                     std::atomic<unsigned> *_rc) : 
                     dbs(_dbs), item_to_get(_item), key_range(_range), start(_s), ready_count(_rc) {
        get_lats = std::vector<unsigned>(1000, 0);
    }
};

void GetTestThread(db_get_thread_param *p) {
    std::vector<uint64_t> keys;
    char value[valueSize];
    default_random_engine re(time(0));
    uniform_int_distribution<uint64_t> u(0, p->key_range-1);
    uint64_t tstart, tspan;
    //prepare random keys to get
    for (unsigned i = 0; i < p->item_to_get; ++i) {
        keys.push_back(u(re));
    }
    unsigned db_mask = ServerNum-1;
    p->ready_count->fetch_add(1, std::memory_order_acq_rel);
    while(!p->start->load(std::memory_order_acquire)) {asm("nop");}
    for (unsigned i = 0; i < p->item_to_get; ++i) {
#ifdef RECORD_GET_LAT
        tstart = GetTimeNsec();
#endif
        if (!(p->dbs[keys[i]&db_mask]->Get(keys[i], value))) {
            p->failed_get++;
        }
#ifdef RECORD_GET_LAT
        tspan = GetTimeNsec()-tstart;
        if (tspan/10 < 1000) ++p->get_lats[tspan/10];
        else ++p->get_lats[999];
#endif
    }
}

struct db_open_param
{
    myDB **db;
    string *log_path;
    db_open_param(myDB **_db, string *_log_path) :
        db(_db), log_path(_log_path) {}
};

void db_recover(db_open_param *p) {
    *(p->db) = new myDB(false, (p->log_path)->c_str());
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
            size_t estimate_log_size = WAL_HEADER_SIZE+InsertSizePerServer*32;
            for (int i = 0; i < ServerNum; i++) {
                std::string log_path = LOG_DIR_PATH+std::to_string(i)+".log";
                dbs_[i] = new myDB(true, log_path.c_str());
            }
        }

        virtual int Put(const int64_t& key, size_t& v_size, const char* value, int tid) {
            dbs_[tid]->Insert(key, v_size, value);
            return 1;
        }

        virtual int Get(const int64_t  key, int64_t* value, int tid) {
            return dbs_[tid]->Get(key, value);
        }
    private:
        myDB *dbs_[ServerNum];
};

int main() {
    DBTest dbtest;
    Benchmark benchmark(&dbtest);
    benchmark.Run();
}

#else
int main(int argc, char *argv[]){
    //PinCore("main");
    pid_t pid = getpid();
    printf("Process ID %d\n", pid);
    std::string mem_command = "cat /proc/" + std::to_string(pid) + "/status >> mem_dump";
    //zExecute(mem_command);
    //cout << "Log entry size is " << LogEntrySize << endl;
    bool create = true;
    if (argc == 2 && argv[1][0] == 'r') create = false;
    if (create) {
        cout << "Creating new DBs." << endl;
        myDB* dbs[ServerNum];
        db_server_param* dbParams[ServerNum];
        thread server_threads[ServerNum];
        std::atomic<bool> start(false);
        std::atomic<int> readyCount(0);
        size_t finishSize = 0;
        for (int i = 0; i < ServerNum; i++) {
            std::string log_path = LOG_DIR_PATH+std::to_string(i)+".log";
            cout << "Log file for server " << i << " is " << log_path << " of size " << EstimateLogSize/1024/1024 << "MB." << endl;
            dbs[i] = new myDB(create, log_path.c_str());
            dbParams[i] = new db_server_param(i, dbs[i], &start, &readyCount);
            server_threads[i] = thread(db_server, dbParams[i]);
        }
        //zExecute(mem_command);
        struct timespec time_start, time_end;//, time_middle;
        double time_span;
        double old_progress = 0, new_progress = 0;
        while (readyCount < ServerNum) {};
        cout << "All servers are ready." << endl;
        {
#ifdef RECORD_WA
        IPMWatcher write_watcher("write");
#endif
        start.store(true, std::memory_order_release);
        for (int i = 0; i < ServerNum; i++) {
            server_threads[i].join();
        }
        vector<double> put_ops(1000, 0.0);
        for (int i = 0; i < ServerNum; ++i) {
            for (unsigned j = 0; j < 1000; ++j) {
                put_ops[j] += dbParams[i]->put_ops[j];
            }
        }
        printf("progress    ops\n");
        for (unsigned i = 0; i < 1000; ++i) {
            printf("   %3.1lf%%  %.0lf\n", i/10.0, put_ops[i]);
        }
        double ops = 0;
        for (int i = 0; i < ServerNum; i++) {
            ops += (dbParams[i]->ops);
        }
        std::cout << "Insert ops: " << ops << std::endl;
#ifdef RECORD_PUT_LAT
        vector<unsigned> wtime = vector<unsigned>(1000, 0);
        for(unsigned i = 0; i < ServerNum; i++){
            for (unsigned j = 0; j < 1000; ++j) {
                wtime[j] += dbParams[i]->put_lats[j];
            }
        }
        std::cout << "Put time CDF." << std::endl;
        for (int i = 0; i < 1000; i++) {
            printf("%d %llu\n", i*10, wtime[i]);
        }
#endif
        }
        std::cout << "Begin get test with " << GetThreadNum << " threads" << std::endl;
        //zExecute(mem_command);
        {
        fflush(stdout);
        unsigned itemstoget = (unsigned)100*1024*1024;
        unsigned itemsperthread = itemstoget/GetThreadNum;
#ifdef RECORD_GET_LAT
        vector<unsigned> rtime = vector<unsigned>(1000, 0);
#endif
        atomic<bool> get_start(false);
        atomic<unsigned> get_thread_ready_count(0);
        db_get_thread_param *get_params[GetThreadNum];
        thread get_threads[GetThreadNum];
        for (unsigned i = 0; i < GetThreadNum; ++i) {
            get_params[i] = new db_get_thread_param(dbs,
                                                 itemsperthread,
                                                 InsertSize,
                                                 &get_start,
                                                 &get_thread_ready_count);
            get_threads[i] = thread(GetTestThread, get_params[i]);
        }
        while (get_thread_ready_count.load(std::memory_order_acquire) < GetThreadNum) {asm("nop");}
        get_start.store(true, std::memory_order_release);
        uint64_t tstart = GetTimeNsec();
        for (unsigned i = 0; i < GetThreadNum; ++i) {
            get_threads[i].join();
        }
        uint64_t tspan = GetTimeNsec() - tstart;
        std::cout << "Get test time span:   " << tspan << "ns" << std::endl
                  << "Get throughput:       " << itemstoget/(tspan/1000000000.0) << "Ops/s" << std::endl;
#ifdef RECORD_GET_LAT
        for(unsigned i = 0; i < GetThreadNum; i++){
            for (unsigned j = 0; j < 1000; ++j) {
                rtime[j] += get_params[i]->get_lats[j];
            }
        }
        std::cout << "Get time CDF." << std::endl;
        for (int i = 0; i < 1000; i++) {
            printf("%d %llu\n", i*10, rtime[i]);
        }
#endif
        }
        //zExecute(mem_command);
        fflush(stdout);
    } else {
        cout << "Reading exiting DBs." << endl;
        myDB **dbs;
        dbs = (myDB **)malloc(ServerNum*sizeof(myDB *));
        string *log_paths[ServerNum];
        db_open_param *openParams[ServerNum];
        thread db_open_threads[ServerNum];
        for (int i = 0; i < ServerNum; i++) {
            log_paths[i] = new string(LOG_DIR_PATH+std::to_string(i)+".log");
            openParams[i] = new db_open_param(dbs+i, log_paths[i]);
        }
        struct timespec time_start, time_end;
        {
        //IPMWatcher write_watcher("write");
        clock_gettime(CLOCK_REALTIME, &time_start);
        for (int i = 0; i < ServerNum; i++) {
            db_open_threads[i] = thread(db_recover, openParams[i]);
        }
        for (int i = 0; i < ServerNum; i++) {
            db_open_threads[i].join();
        }
        clock_gettime(CLOCK_REALTIME, &time_end);
        }
        cout << "Recover time " << ((time_end.tv_sec - time_start.tv_sec) + (time_end.tv_nsec - time_start.tv_nsec)/1000000000.0) << " s." << endl;
        zExecute(mem_command);
        fflush(stdout);
        int wrongget = 0, failedget = 0;
        cout << "Begin get test." << std::endl;
        std::default_random_engine re(time(0));
        //std::uniform_int_distribution<Key_t> u(0, InsertSize-1);
        std::uniform_int_distribution<Key_t> u(InsertSize, InsertSize*10);
        uint64_t get_time_span = 0;
        uint64_t get_time_max = 0, get_time_min = ~0, get_time_this;
        unsigned itemstoget = 100*1024*1024;
        uint64_t rtime[1000];
        for (int i = 0; i < 1000; i++) rtime[i] = 0;
        Key_t key;
        //Value_t value;
        char value[valueSize];
        for(unsigned i=0; i<itemstoget; i++){
            key = u(re);
            clock_gettime(CLOCK_REALTIME, &time_start);
            auto ret = dbs[key%ServerNum]->Get(key, value);
            clock_gettime(CLOCK_REALTIME, &time_end);
            get_time_this = ((time_end.tv_sec - time_start.tv_sec)*1000000000 + (time_end.tv_nsec - time_start.tv_nsec));
            get_time_span += get_time_this;
            if (get_time_this > get_time_max) get_time_max = get_time_this;
            if (get_time_this < get_time_min) get_time_min = get_time_this;
            if (get_time_this >= 10000) {
                rtime[999]++;
            } else {
                rtime[get_time_this/10]++;
            }
            if (ret == 0) {
                failedget++;
            } else if(strcmp(value, ConstValue[((key-key%ServerNum)/ServerNum)%2]) != 0) {
                wrongget++;
                cout << "Wrong value for key " << key << " : " << ret << endl;
            }
            if (i%10000 == 0) {
                printf("\rprogress %u", i);
                fflush(stdout);
            }
        }
        std::cout << std::endl << "Avg Get Lat: " << get_time_span/(double)itemstoget << "ns, max " << get_time_max << "ns, min " << get_time_min << "ns." << std::endl;
        cout << "Wrong get num " << wrongget << endl
            << "Failed get num " << failedget << endl;
        zExecute(mem_command);
        fflush(stdout);
        std::cout << "Get time CDF." << std::endl;
        for (int i = 0; i < 1000; i++) {
            printf("%d %llu\n", i*10, rtime[i]);
        }
    }
    return 0;
}
#endif /*YCSB_TEST*/
	
