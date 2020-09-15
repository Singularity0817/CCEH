rm -f ./cceh_test

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./lib/pmdk
export LD_LIBRARY_PATH

make
#g++ -std=c++17 -g -I./ -L /usr/local/lib/ -L /usr/lib -L ./lib2/pcm/ -I /usr/include/ -I /usr/local/include/ -I ./src2/ -I ./lib2/pcm/ -lrt -lpthread -mavx512f -msse -msse2 -msse3 -lpmem -o cceh_test test.cpp src/CCEH_LSB.cpp -lpmemobj
#g++ -std=c++17 -I./ -L /usr/local/lib/ -L /usr/lib -L ./lib2/pcm/ -I /usr/include/ -I /usr/local/include/ -I ./src2/ -I ./lib2/pcm/ -lrt -lpthread -mavx512f -msse -msse2 -msse3 -lpmem -O3 -o cceh_test test.cpp src/CCEH_LSB.cpp -lpmemobj
g++ -std=c++17 -I./ -L /usr/local/lib/ -L /usr/lib -L ./lib2/pcm/ -I /usr/include/ -I /usr/local/include/ -I ./src2/ -I ./lib2/pcm/ -lrt -lpthread -mavx512f -msse -msse2 -msse3 -lpmem -O3 -o cceh_test CCEHonPmem.cpp src/CCEH_LSB.cpp src2/util/slice.cc -lpmemobj -pthread
#g++ -std=c++17 -I./ -L /usr/local/lib/ -L /usr/lib -L ./lib2/pcm/ -I /usr/include/ -I /usr/local/include/ -I ./src2/ -I ./lib2/pcm/ -lrt -lpthread -mavx512f -msse -msse2 -msse3 -lpmem -g3 -o cceh_test CCEHonPmem.cpp src/CCEH_LSB.cpp src2/util/slice.cc -lpmemobj -pthread
