rm -f /mnt/pmem0/zwh_test/CCEH/*
./a.sh
sudo numactl -N 0 ./cceh_test -wr
#echo "Reopen the db"
#sudo numactl -N 0 ./cceh_test -r
