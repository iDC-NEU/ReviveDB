#!/bin/bash
# Copyright (c): 2021-2025, Huawei Tech. Co., Ltd.
set -e
source ../sec_options.rc
mkdir -p $(pwd)/../../output/tbb
export TARGET_PATH=$(pwd)/../../output/tbb
export GCC_INSTALL_HOME=$(gcc -v 2>&1 | grep prefix | awk -F'prefix=' '{print $2}' |awk -F' ' '{print $1}')
export LD_LIBRARY_PATH=$GCC_INSTALL_HOME/lib64:$LD_LIBRARY_PATH
export PATH=$TARGET_PATH:$PATH
tmp_cpus=$(grep -w processor /proc/cpuinfo|wc -l)
SOURCE_FILE=oneTBB-2021.11.0

cd $SOURCE_FILE
rm -rf build
mkdir -p build && cd build
mkdir -p output
tmp_output=$(pwd)/output
cmake .. -DCMAKE_BUILD_TYPE=release -DTBB_TEST=OFF -DCMAKE_INSTALL_PREFIX=$tmp_output
make -sj${tmp_cpus}
make install

cd $tmp_output
cp -R include $TARGET_PATH
cd lib64
mkdir $TARGET_PATH/lib
cp libtbb.so.12.11 $TARGET_PATH/lib
cd $TARGET_PATH/lib
ln -s libtbb.so.12.11 libtbb.so.12
ln -s libtbb.so.12 libtbb.so
