./a.sh

rm -rf /mnt/pmem0/zwh_test/CCEH/*

numactl -N 0 ./cceh_test -wr
echo "==========================="
numactl -N 0 ./cceh_test -r