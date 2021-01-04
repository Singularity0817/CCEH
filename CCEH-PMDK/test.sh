./a.sh

rm -rf /mnt/pmem0/zwh_test/CCEH/*

numactl -N 0 ./cceh_test -wr

numactl -N 0 ./cceh_test -r