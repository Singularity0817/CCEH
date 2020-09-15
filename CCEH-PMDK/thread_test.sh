rm -f /mnt/pmem0/zwh_test/CCEH/*
./cceh_2_no_reserve -rw > p-hash-2-no-reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*
./cceh_4_no_reserve -rw > p-hash-4-no-reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*
./cceh_2_reserve -rw > p-hash-2-reserve.log
rm -f /mnt/pmem0/zwh_test/CCEH/*
./cceh_4_reserve -rw > p-hash-4-reserve.log
