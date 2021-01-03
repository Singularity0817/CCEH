#include "./src/wal.h"

int main() {
    char path[50] = "/mnt/pmem0/a";
    Wal *log = new Wal();
    log->create(path, 1024);
    return 0;
}