#!/bin/bash
# Copyright (c): 2021-2025, Huawei Tech. Co., Ltd.
set -e
source ../sec_options.rc
mkdir -p $(pwd)/../../output/pmdk
export TARGET_PATH=$(pwd)/../../output/pmdk
export GCC_INSTALL_HOME=$(gcc -v 2>&1 | grep prefix | awk -F'prefix=' '{print $2}' |awk -F' ' '{print $1}')
export LD_LIBRARY_PATH=$GCC_INSTALL_HOME/lib64:$LD_LIBRARY_PATH
export PATH=$TARGET_PATH:$PATH
tmp_cpus=$(grep -w processor /proc/cpuinfo|wc -l)
SOURCE_FILE=pmdk-1.12.0

cd $SOURCE_FILE
make clean
mkdir -p output
tmp_output=$(pwd)/output
make -sj${tmp_cpus}
make install prefix=$tmp_output

cd $tmp_output
cp -R include $TARGET_PATH
cd lib
mkdir $TARGET_PATH/lib
cp libpmem.a libpmemobj.a $TARGET_PATH/lib
