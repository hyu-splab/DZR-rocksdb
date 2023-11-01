# Dynamic Zone Redistribution (DZR)

This is a ZenFS plugin for optimizing throughput and reducing write amplification on ZNS (Zoned Namespaces SSDs).



## Quick Install Guide :

```bash
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
pushd .
git clone https://github.com/hyu-splab/DZR-rocksdb.git plugin/DZR
cd plugin/DZR
mv zenfs ../ && cp build/* ../../build/ 
cp util/* ../../util/ && cp tools/* ../../tools/ # Optional : If you need YCSB benchmark
popd
```

## Build and install rocksdb with zenfs enabled :
```bash
sudo DEBUG_LEVEL=0 ROCKSDB_PLUGINS=zenfs make -j$(nproc) db_bench install
mv db_bench ./build/db_bench
pushd .
cd plugin/zenfs/util/
make    # Before, you need to set $CPLUS_INCLUDE_PATH and $LD_LIBRARY_PATH 
popd
```

## Run db_bench Benchmark :
```bash
cd build
./zenfs_test.sh    # You can change --aux_path, --zbd, --fs_uri, etc.
```

### Note :
the mq-deadline scheduler must be set manually to ensure that the regular write operations used by btrfs are delivered to the device in sequential order. For a NVMe zoned namespace device /dev/nvmeXnY, this is done with the following command:
```bash
echo mq-deadline > /sys/block/nvmeXnY/queue/scheduler
```
