rm -f ./a.out
rm -f ./mem_dump
rm -f /mnt/pmem/zwh_test/logDB/*
#g++ ./example.cpp ./src/CCEH_LSB.o -I ./ -L ./ -o a.out
#g++ ./example.cpp ./src/cuckoo_hash.o -std=c++17 -I ./ -L ./ -lpthread -o cuckoo_hash.out
#g++ ./example.cpp ./src/Level_hashing.o -std=c++17 -I ./ -L ./ -lpthread -o level_hashing.out
#g++ ./example_2.cpp ./wal.cc ./src/CCEH_LSB.o -I ./ -L ./ -L /usr/local/lib -lpmem -o a.out
#g++ ./example_3.cpp -std=c++17 ./wal.cc ./src/cuckoo_hash.o -I ./ -L ./ -L /usr/local/lib -lpmem -lpthread -o a.out
#g++ ./log-structured-db.cpp -std=c++17 ./wal.cc ./src/CCEH_LSB.o -I ./ -L ./ -L /usr/local/lib -lpmem -lpthread -o a.out
g++ ./log-structured-db-cuckoo.cpp -std=c++17 ./wal.cc ./src/CCEH_LSB.o -I ./ -L ./ -L /usr/local/lib -lpmem -lpthread -o a.out
./a.out
./a.out r
