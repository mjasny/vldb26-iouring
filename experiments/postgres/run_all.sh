#!/bin/bash
set -e

CONFIG_FILE=~/postgres/src/backend/storage/uring_config.h

iodepths=(1000) # 256
workers=(0 1 3 7)
modes=({0..7})
    
for iodepth in "${iodepths[@]}"; do
for worker in "${workers[@]}"; do
for mode in "${modes[@]}"; do


sed -i "s/^#define RUN_MODE .*/#define RUN_MODE $mode/" "$CONFIG_FILE"

# wait for sync
sleep 1


pushd ~/postgres >/dev/null
meson setup build --reconfigure -Dliburing=enabled --buildtype=release
meson install -C build/ --destdir=./install
popd >/dev/null

TAG="large_$((worker+1))w_${iodepth}io_mode${mode}"

if [ "$iodepth" -eq 1000 ]; then
  TAG="large_$((worker+1))w_1024io_mode${mode}"
fi

for i in {1..10}; do 
    echo "iodepth: $iodepth worker: $((worker+1)) mode: $mode run: $i"
    #./run_pg_experiment.sh sync off $TAG
    #./run_pg_experiment.sh worker off $TAG
    #./run_pg_experiment.sh io_uring off $TAG

    #./run_pg_experiment.sh sync data,wal $TAG
    #./run_pg_experiment.sh worker data,wal $TAG
    ./run_pg_experiment.sh io_uring data,wal $TAG $worker $iodepth
done

done
done
done
