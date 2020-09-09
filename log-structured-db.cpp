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
#include "wal.h"
#include "util/concurrentqueue.h"
#include "util/ipmwatcher.h"
#include "src/CCEH.h"
//#include "src/cuckoo_hash.h"
//#include "src/Level_hashing.h"

using namespace std;

#define LOG_DIR_PATH "/mnt/pmem0/zwh_test/logDB/"
//#define LOG_DIR_PATH "/mnt/pmem/zwh_test/logDB/"
mutex cout_lock;
const size_t InsertSize = 1000*1024*1024;
const int BatchSize = 1024;
const int ServerNum = 1;
const size_t InsertSizePerServer = InsertSize/ServerNum;
const Value_t ConstValue[2] = {"VALUE_1", "value_2"};
const size_t LogEntrySize = sizeof(Key_t)+sizeof(size_t)+strlen(ConstValue[0])+1;
const size_t EstimateLogSize = 512+(LogEntrySize*InsertSizePerServer)+512*1024*1024;
//const size_t EstimateLogSize = 512*1024*1024;

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

class myDB
{
    public:
        myDB(bool create, const char *log_path) {
            index = new CCEH();
            //index = new CuckooHash(1024*1024);
            //index = new LevelHashing(10);
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
                    ancho += (sizeof(Key_t)+sizeof(size_t)+value_size+1);
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
            delete log;
            delete index;
            //delete request_queue;
        }
        inline void Insert(Key_t &key, Value_t value) {
            size_t value_size = strlen(value);
            int buffer_size = sizeof(Key_t)+sizeof(size_t)+strlen(value)+1;
            char *buffer = (char *)malloc(buffer_size);
            memcpy(buffer, &key, sizeof(Key_t));
            memcpy(buffer+sizeof(Key_t), &value_size, sizeof(size_t));
            memcpy(buffer+sizeof(Key_t)+sizeof(size_t), value, value_size+1);
            auto pos = log->append(buffer, buffer_size);
            index->Insert(key, reinterpret_cast<Value_t>(pos));
            free(buffer);
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
        inline Value_t Get(Key_t &key) {
            uint64_t offset = reinterpret_cast<uint64_t>(index->Get(key));
            void *data_handler = log->get_data_offset(offset);
            Key_t g_key = *((Key_t *)data_handler);
            if (g_key != key) {
                cout << "Get the wrong key " << g_key << " : " << key << endl;
                exit(1);
            }
            return (Value_t)((char *)data_handler+sizeof(Key_t)+sizeof(size_t));

        }
        inline void print_put_stat() {
            cout << "Insert prepare time " << insert_prepare_time << ", log append time " << insert_log_append_time << ", index insert time " << insert_index_insert_time << endl;
            cout << "    index update time " << index->insert_time << ", rehash time " << index->rehash_time << endl;
        }
    private:
        CCEH* index;
        //CuckooHash* index;
        //LevelHashing* index;
        //Hash* index;
        Wal* log;
        uint64_t insert_prepare_time = 0;
        uint64_t insert_log_append_time = 0;
        uint64_t insert_index_insert_time = 0;
        //moodycamel::ConcurrentQueue<struct Pair *> request_queue;
};

struct db_server_param
{
    int id;
    myDB *db;
    bool *start;
    int *readyCount;
    size_t *finishSize;
    db_server_param(int _id, myDB* _db, bool *_start, int *_readyCount, size_t *_finishSize)
        : id(_id), db(_db), start(_start), readyCount(_readyCount), finishSize(_finishSize) {}
};

void db_server(db_server_param *p)
{
    PinCore("server");
    myDB *db = p->db;
    int id = p->id;
    size_t *finishSize = p->finishSize;
    vector<Pair> pairs_to_put;
    __sync_fetch_and_add(p->readyCount, 1);
    cout_lock.lock();
    cout << "Server " << id << " is ready with db " << db << "." << endl;
    cout_lock.unlock();
    while(!(*(p->start))) {}
    for (unsigned i = 0; i < InsertSizePerServer/BatchSize; i++) {
        pairs_to_put.clear();
        for (unsigned j = 0; j < BatchSize; j++) {
            pairs_to_put.push_back(Pair((i*BatchSize+j)*ServerNum+id, ConstValue[j%2]));
        }
        db->BatchInsert(&pairs_to_put);
        __sync_fetch_and_add(finishSize, BatchSize);
    }
    db->print_put_stat();
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

int main(int argc, char *argv[]){
    PinCore("main");
    pid_t pid = getpid();
    printf("Process ID %d\n", pid);
    std::string mem_command = "cat /proc/" + std::to_string(pid) + "/status >> mem_dump";
    zExecute(mem_command);
    //cout << "Log entry size is " << LogEntrySize << endl;
    bool create = true;
    if (argc == 2 && argv[1][0] == 'r') create = false;
    if (create) {
        cout << "Creating new DBs." << endl;
        myDB* dbs[ServerNum];
        db_server_param* dbParams[ServerNum];
        thread server_threads[ServerNum];
        bool start = false;
        int readyCount = 0;
        size_t finishSize = 0;
        for (int i = 0; i < ServerNum; i++) {
            std::string log_path = LOG_DIR_PATH+std::to_string(i)+".log";
            cout << "Log file for server " << i << " is " << log_path << endl;
            dbs[i] = new myDB(create, log_path.c_str());
            dbParams[i] = new db_server_param(i, dbs[i], &start, &readyCount, &finishSize);
            server_threads[i] = thread(db_server, dbParams[i]);
        }
        zExecute(mem_command);
        struct timespec time_start, time_end;//, time_middle;
        double time_span;
        double old_progress = 0, new_progress = 0;
        while (readyCount < ServerNum) {};
        cout << "All servers are ready." << endl;
        {
        IPMWatcher write_watcher("write");
        uint64_t old_progress_checkpoint, new_progress_checkpoint;
        size_t new_fs, old_fs = 0;
        clock_gettime(CLOCK_REALTIME, &time_start);
        old_progress_checkpoint = time_start.tv_sec*1000000000+time_start.tv_nsec;
        uint64_t put_time_start = old_progress_checkpoint;
        start = true;
        while (finishSize < InsertSize) {
            new_fs = finishSize;
            new_progress = new_fs/(double)InsertSize*100;
            if (new_progress - old_progress >= 1) {
                //printf("\rProgress %2.1lf%%", new_progress);
                //clock_gettime(CLOCK_REALTIME, &time_middle);
                new_progress_checkpoint = GetTimeNsec();
                //double span = (time_middle.tv_sec - time_start.tv_sec) + (time_middle.tv_nsec - time_start.tv_nsec)/1000000000.0;
                printf("Progress %2.1lf%%, ops %.1lf wa %.2lf avg_ops %.1lf\n", new_progress, 
                    (new_fs-old_fs)/(double)((new_progress_checkpoint-old_progress_checkpoint)/1000000000.0), 
                    (double) write_watcher.CheckDataWriteToDIMM()/(new_fs*16.0),
                    new_fs/(double)((new_progress_checkpoint-put_time_start)/1000000000.0));
                //write_watcher.Checkpoint();
                fflush(stdout);
                old_progress = new_progress;
                old_progress_checkpoint = new_progress_checkpoint;
                old_fs = new_fs;
            }
        }
        printf("\n");
        clock_gettime(CLOCK_REALTIME, &time_end);
        }
        for (int i = 0; i < ServerNum; i++) {
            server_threads[i].join();
        }
        time_span = ((time_end.tv_sec - time_start.tv_sec) + (time_end.tv_nsec - time_start.tv_nsec)/1000000000.0);
        double ops = InsertSize/(double)time_span;
        std::cout << "Insert ops: " << ops << std::endl;

        zExecute(mem_command);
        fflush(stdout);
        int failSearch = 0;
        std::default_random_engine re(time(0));
        std::uniform_int_distribution<Key_t> u(0, InsertSize-1);
        uint64_t get_time_span = 0;
        uint64_t get_time_max = 0, get_time_min = ~0, get_time_this;
        unsigned itemstoget = 1000000;
        unsigned wrongget = 0;
        Key_t key;
        {
        IPMWatcher write_watcher("read");
        for(unsigned i=0; i<itemstoget; i++){
            key = u(re);
            clock_gettime(CLOCK_REALTIME, &time_start);
            auto ret = dbs[key%ServerNum]->Get(key);
            clock_gettime(CLOCK_REALTIME, &time_end);
            get_time_this = ((time_end.tv_sec - time_start.tv_sec)*1000000000 + (time_end.tv_nsec - time_start.tv_nsec));
            //cout << "Value for key " << key << " is " << ret << endl;
            if(strcmp(ret, ConstValue[((key-key%ServerNum)/ServerNum)%2]) != 0) {
                wrongget++;
                cout << "Wrong value for key " << key << " : " << ret << endl;
            }
            get_time_span += get_time_this;
            if (get_time_this > get_time_max) get_time_max = get_time_this;
            if (get_time_this < get_time_min) get_time_min = get_time_this;
        }
        }
        std::cout << "Avg Get Lat: " << get_time_span/(double)itemstoget << "ns, max " << get_time_max << "ns, min " << get_time_min << "ns." << std::endl;
        cout << "Wrong get num " << wrongget << endl;
        zExecute(mem_command);
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
        IPMWatcher write_watcher("write");
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
        int failSearch = 0;
        std::default_random_engine re(time(0));
        std::uniform_int_distribution<Key_t> u(0, InsertSize-1);
        uint64_t get_time_span = 0;
        uint64_t get_time_max = 0, get_time_min = ~0, get_time_this;
        unsigned itemstoget = 100*1024*1024;
        uint64_t rtime[1000];
        for (int i = 0; i < 1000; i++) rtime[i] = 0;
        Key_t key;
        for(unsigned i=0; i<itemstoget; i++){
            key = u(re);
            clock_gettime(CLOCK_REALTIME, &time_start);
            auto ret = dbs[key%ServerNum]->Get(key);
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
            if(strcmp(ret, ConstValue[((key-key%ServerNum)/ServerNum)%2]) != 0) {
                failSearch++;
                cout << "Wrong value for key " << key << " : " << ret << endl;
            }
            if (i%10000 == 0) {
                printf("\rprogress %u", i);
                fflush(stdout);
            }
        }
        std::cout << std::endl << "Avg Get Lat: " << get_time_span/(double)itemstoget << "ns, max " << get_time_max << "ns, min " << get_time_min << "ns." << std::endl;
        cout << "Wrong get num " << failSearch << endl;
        zExecute(mem_command);
        fflush(stdout);
        std::cout << "Get time CDF." << std::endl;
        for (int i = 0; i < 1000; i++) {
            printf("%d %llu\n", i*10, rtime[i]);
        }
    }
    return 0;
}

	
