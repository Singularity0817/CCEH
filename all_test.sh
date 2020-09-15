rm -f /mnt/pmem0/zwh_test/CCEH/*
rm -f /mnt/pmem0/zwh_test/ShardedDB/*
rm -f /mnt/pmem0/zwh_test/logDB/*

echo "Testing D-Hash no reserve"
echo "Thread #1"
./d-hash-1-no-reserve.bin > log-d-hash-1-no-reserve.log
rm -f /mnt/pmem0/zwh_test/logDB/*
echo "Thread #2"
./d-hash-2-no-reserve.bin > log-d-hash-2-no-reserve.log
rm -f /mnt/pmem0/zwh_test/logDB/*
echo "Thread #4"
./d-hash-4-no-reserve.bin > log-d-hash-4-no-reserve.log
rm -f /mnt/pmem0/zwh_test/logDB/*
echo "Thread #8"
./d-hash-8-no-reserve.bin > log-d-hash-8-no-reserve.log
rm -f /mnt/pmem0/zwh_test/logDB/*

echo "Testing D-Hash no reserved"
echo "Thread #1"
./d-hash-1-reserved.bin > log-d-hash-1-reserved.log
rm -f /mnt/pmem0/zwh_test/logDB/*
echo "Thread #2"
./d-hash-2-reserved.bin > log-d-hash-2-reserved.log
rm -f /mnt/pmem0/zwh_test/logDB/*
echo "Thread #4"
./d-hash-4-reserved.bin > log-d-hash-4-reserved.log
rm -f /mnt/pmem0/zwh_test/logDB/*
echo "Thread #8"
./d-hash-8-reserved.bin > log-d-hash-8-reserved.log
rm -f /mnt/pmem0/zwh_test/logDB/*