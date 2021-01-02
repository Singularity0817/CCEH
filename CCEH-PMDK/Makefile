.PHONY: all clean
.DEFAULT_GOAL := all

CC=g++
LIBS=-lrt -lm -lpmemobj -lpthread #-lpmem# -lpthread -mavx512f -msse -msse2 -msse3 -lpmem
CFLAGS=-O3 -std=c++11 -w 
#CFLAGS=-g -std=c++11 -w 
INCLUDES=-I./# -I/usr/include/ -I/usr/local/include/ -I./src2/ -I./lib2/pcm/
LINKS=-L/usr/local/lib/ -L/usr/lib -L./lib2/pcm/

all: main

main: 
	$(CC) $(CFLAGS) -c src/CCEH_LSB.cpp -o src/CCEH_LSB.o $(LIBS) $(INCLUDES)

clean: 
	rm -rf src/*.o
