rm -f /mnt/pmem0/zwh_test/CCEH/*
rm -f /mnt/pmem0/zwh_test/logDB/*
rm -f /mnt/pmem0/zwh_test/ShardedDB/*

echo "Testing CCEH no reserved"
echo "#Thread 1"
numactl -N 0 ./run_cceh_1_no_reserve.bin -wr > log_cceh_1_no_reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*
echo "#Thread 2"
numactl -N 0 ./run_cceh_2_no_reserve.bin -wr > log_cceh_2_no_reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*
echo "#Thread 4"
numactl -N 0 ./run_cceh_4_no_reserve.bin -wr > log_cceh_4_no_reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*
echo "#Thread 8"
numactl -N 0 ./run_cceh_8_no_reserve.bin -wr > log_cceh_8_no_reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*

echo "Testing CCEH reserved"
echo "#Thread 1"
numactl -N 0 ./run_cceh_1_reserved.bin -wr > log_cceh_1_reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*
echo "#Thread 2"
numactl -N 0 ./run_cceh_2_reserved.bin -wr > log_cceh_2_reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*
echo "#Thread 4"
numactl -N 0 ./run_cceh_4_reserved.bin -wr > log_cceh_4_reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*
echo "#Thread 8"
numactl -N 0 ./run_cceh_8_reserved.bin -wr > log_cceh_8_reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*