#make clean
DEBUG_LEVEL=0 ROCKSDB_PLUGINS=zenfs make -j64 db_bench
mv db_bench ./build/db_bench
