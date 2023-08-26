#!/bin/bash

echo "========== RocksDB with ZNS SSD =========="
echo ""

if [ ! $1 ] 
then
	echo "$0 workload_type (1: random, 2:sequential, 3:zippyDB)"
	exit 1 
fi

if [ $1 -ne 6 ]
then

echo ""
echo "Init filesystem (ZenFS)"
rm -r /zenfs_aux01
blkzone reset /dev/nvme3n2

max_bk_jobs=$2
num=300000000

../plugin/zenfs/util/zenfs mkfs --zbd=/nvme3n2 --aux_path=/zenfs_aux01 --enable_gc=false --max_background_jobs=$max_bk_jobs --force

echo "Complete!"
fi

sleep 1

if [ $1 -eq 1 ]
then	
	echo "FillRand+Overwrite 420KV 300M"
	./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=fillrandom,overwrite,stats --use_direct_io_for_flush_and_compaction \
				--key_size=20 --value_size=800 --num=300000000 --max_background_jobs=$max_bk_jobs 
elif [ $1 -eq 2 ]
then
	echo "Seq"
	./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=fillseq,stats --use_direct_io_for_flush_and_compaction --max_background_jobs=$max_bk_jobs 
elif [ $1 -eq 3 ] 
then
	echo "ZippyDB"
	./db_bench --fs_uri=zenfs://dev:nvme3n2 --benchmarks=fillrandom,mixgraph,stats --use_direct_io_for_flush_and_compaction \
				--keyrange_dist_a=14.18 -keyrange_dist_b=-2.917 -keyrange_dist_c=0.0164 -keyrange_dist_d=-0.08082 -keyrange_num=30 \
				--value_k=0.2615 -value_sigma=25.45 -mix_get_ratio=0 -mix_put_ratio=1 --num=3000000000 -key_size=48 -sine_mix_rate_interval_milliseconds=5000 \
				-sine_a=1000 -sine_b=0.000073 -sine_d=4500 --max_background_jobs=$max_bk_jobs
fi