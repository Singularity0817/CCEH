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
#include "wal.h"
#include "util/concurrentqueue.h"
#include "util/ipmwatcher.h"
//#include "src/CCEH.h"
//#include "src/cuckoo_hash.h"
#include "src/Level_hashing.h"

using namespace std;

#define LOG_PATH "/mnt/pmem0/zwh_test/cceh_log"
#define MAX_QUEUE_LENGTH 1024

#define TO_WAL_ACTUAL_POS(x) (x+WAL_HEADER_SIZE)
/*
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
*/

inline uint64_t GetTimeNsec()
{
    struct timespec nowtime;
    clock_gettime(CLOCK_REALTIME, &nowtime);
    return nowtime.tv_sec * 1000000000 + nowtime.tv_nsec;
}

class myDB
{
    public:
        myDB(bool create) {
            //index = new CCEH();
            //index = new CuckooHash(1024*1024);
            index = new LevelHashing(10);
            log = new Wal();
            if (create) {
                log->create(LOG_PATH, LOG_POOL_SIZE);
            } else {
                log->open(LOG_PATH);
                void *log_data_handler = log->get_data_handler();
                uint64_t log_size = log->get_wal_data_size();
                cout << "Log size is " << log_size << endl;
                uint64_t ancho = 0;
                size_t value_size = 0;
                unsigned counter = 0;
                while (ancho < log_size) {
#ifdef DEBUG
                    cout << "Recovering key " << *((Key_t *)((char *)log_data_handler+ancho)) << " in pos " << ancho+WAL_HEADER_SIZE << endl;
#endif
                    index->Insert(*((Key_t *)((char *)log_data_handler+ancho)), reinterpret_cast<Value_t>(ancho+WAL_HEADER_SIZE));
                    value_size = *(size_t *)((char *)log_data_handler+ancho+sizeof(Key_t));
                    ancho += (sizeof(Key_t)+sizeof(size_t)+value_size+1);
                    counter++;
                }
                cout << "Recover " << counter << " items." << endl;
            }
            //request_queue = new moodycamel::ConcurrentQueue<struct Pair *>(ceil(MAX_QUEUE_LENGTH/MOODYCAMEL_BLOCK_SIZE)*MOODYCAMEL_BLOCK_SIZE);
        }
        ~myDB() {
            delete log;
            delete index;
            //delete request_queue;
        }
        /*
        void myDB::Insert(Key_t &key, Value_t value) {
            Pair *p = new Pair(key, value);
            while(!(request_queue->try_enqueue(p))) {}
        }
        */
        void Insert(Key_t &key, Value_t value) {
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
            for(auto it = pairs->begin(); it != pairs->end(); it++) {
                buffer_size += (sizeof(Key_t)+sizeof(size_t)+strlen((*it).value)+1);
            }
            char *buffer = (char *)malloc(buffer_size);
            uint64_t offset = 0;
            vector<uint64_t> offsets;
            size_t value_size = 0;
            //cout << "Begin Batch size " << buffer_size << endl;
            for(auto it = pairs->begin(); it != pairs->end(); it++) {
                //cout << "    " << (*it).key << "    " << (*it).value << endl;
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
        void print_put_stat() {
            cout << "Insert prepare time " << insert_prepare_time << ", log append time " << insert_log_append_time << ", index insert time " << insert_index_insert_time << endl;
        }
    private:
        //CCEH* index;
        //CuckooHash* index;
        LevelHashing* index;
        Wal* log;
        uint64_t insert_prepare_time = 0;
        uint64_t insert_log_append_time = 0;
        uint64_t insert_index_insert_time = 0;
        //moodycamel::ConcurrentQueue<struct Pair *> request_queue;
};

int main(){
    const size_t initialSize = 1024*16*4;
    const size_t insertSize = 100*1024*1024;
    
    int batchSize = 1024;
    pid_t pid = getpid();
    printf("Process ID %d\n", pid);
    std::string mem_command = "cat /proc/" + std::to_string(pid) + "/status >> mem_dump";
    std::cout << " command is :" << mem_command << std::endl;
    zExecute(mem_command);

    bool create = false;
    if (create) {
        //CCEH* HashTable = new CCEH();
        cout << "Creating a new DB." << endl;
        myDB* db = new myDB(create);
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
        Value_t value[2] = {"VALUE_1", "value_2"};
        vector<Pair> pairs_to_put;
        {
        IPMWatcher write_watcher("write");
        double now_progress, old_progress = 0;
        for(unsigned i=0; i<(insertSize/batchSize); i++){
            //HashTable->Insert(keys[i], reinterpret_cast<Value_t>(&keys[i]));
            pairs_to_put.clear();
            for(unsigned j=0; j < batchSize; j++) {
                pairs_to_put.push_back(Pair(i*batchSize+j, value[j%2]));
            }
            clock_gettime(CLOCK_REALTIME, &time_start);
            db->BatchInsert(&pairs_to_put);
            clock_gettime(CLOCK_REALTIME, &time_end);
            time_span += ((time_end.tv_sec - time_start.tv_sec) + (time_end.tv_nsec - time_start.tv_nsec)/1000000000.0);
            now_progress = ((i+1)/(double)(insertSize/batchSize))*100;
            if (now_progress - old_progress >= 1) {
                printf("Progress %2.2lf%%, time %.4lfns, ops %lf\n", now_progress, time_span, ((i+1)*batchSize/(double)time_span));
                old_progress = now_progress;
            }
            /*
            if (i%1000 == 0) {
                printf("\rInserted %2.2lf%%", (i/(double)(insertSize/batchSize))*100);
                fflush(stdout);
            }
            */
            //key = i;
            //db->Insert(key, value);
        }
        }
        //clock_gettime(CLOCK_REALTIME, &time_end);
        //time_span += ((time_end.tv_sec - time_start.tv_sec) + (time_end.tv_nsec - time_start.tv_nsec)/1000000000.0);
        double ops = insertSize/(double)time_span;
        std::cout << "Insert ops: " << ops << std::endl;
        db->print_put_stat();
        zExecute(mem_command);
        fflush(stdout);
        int failSearch = 0;
        std::default_random_engine re(time(0));
        std::uniform_int_distribution<Key_t> u(0, insertSize-1);
        uint64_t get_time_span = 0;
        uint64_t get_time_max = 0, get_time_min = ~0, get_time_this;
        unsigned itemstoget = 1000000;
        {
        IPMWatcher write_watcher("read");
        for(unsigned i=0; i<itemstoget; i++){
        //auto ret = HashTable->Get(keys[i]);
        //key = i;
        key = u(re);
        clock_gettime(CLOCK_REALTIME, &time_start);
        auto ret = db->Get(key);
        clock_gettime(CLOCK_REALTIME, &time_end);
        get_time_this = ((time_end.tv_sec - time_start.tv_sec)*1000000000 + (time_end.tv_nsec - time_start.tv_nsec));
        //cout << "Value for key " << key << " is " << ret << endl;
        get_time_span += get_time_this;
        if (get_time_this > get_time_max) get_time_max = get_time_this;
        if (get_time_this < get_time_min) get_time_min = get_time_this;
        /*
        if(!ret){
            failSearch++;
        }
        */
        }
        }
        std::cout << "Avg Get Lat: " << get_time_span/(double)itemstoget << "ns, max " << get_time_max << "ns, min " << get_time_min << "ns." << std::endl;
        //printf("failedSearch: %d\n", failSearch);
        zExecute(mem_command);
        fflush(stdout);
    } else {
        cout << "Reading an exiting DB." << endl;
        myDB* db;
        struct timespec time_start, time_end;
        {
        IPMWatcher write_watcher("write");
        clock_gettime(CLOCK_REALTIME, &time_start);
        db = new myDB(create);
        clock_gettime(CLOCK_REALTIME, &time_end);
        }
        cout << "Recover time " << ((time_end.tv_sec - time_start.tv_sec) * 1000000000 + (time_end.tv_nsec - time_start.tv_nsec)) << "ns." << endl;
        zExecute(mem_command);
        fflush(stdout);
        int failSearch = 0;
        std::default_random_engine re(time(0));
        std::uniform_int_distribution<Key_t> u(0, insertSize-1);
        uint64_t get_time_span = 0;
        uint64_t get_time_max = 0, get_time_min = ~0, get_time_this;
        unsigned itemstoget = 1000000;
        Key_t key;
        for(unsigned i=0; i<itemstoget; i++){
        //auto ret = HashTable->Get(keys[i]);
        //key = i;
        key = u(re);
        clock_gettime(CLOCK_REALTIME, &time_start);
        auto ret = db->Get(key);
        clock_gettime(CLOCK_REALTIME, &time_end);
        get_time_this = ((time_end.tv_sec - time_start.tv_sec)*1000000000 + (time_end.tv_nsec - time_start.tv_nsec));
        //cout << "Value for key " << key << " is " << ret << endl;
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
        fflush(stdout);
    }
    return 0;
}

	
