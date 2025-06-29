# ReviveDB: Instant OLTP Recovery by Persisting Lock with NVM

## Building

We tested our build with Centos7 and GCC 10.3.

### Compiling

First, download the source code.

```bash
git clone https://github.com/iDC-NEU/ReviveDB.git
cd ReviveDB
```

Then, download and unzip dependencies.

```bash
wget https://github.com/iDC-NEU/ReviveDB/releases/download/v1.0.0/revivedb-libs.tar.gz
tar -xzvf revivedb-libs.tar.gz
```

Then compile:

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j
```

## Benchmark

All benchmarks are written with gtest. The executable file is `build/tests/ReviveDBTests`.

| WorkLoad | Index |              Source Code File               |       Test Suit        |
| :------: | :---: | :-----------------------------------------: | :--------------------: |
| TPCC-NP  |  NVM  | `tests/benchmarks/tpcc_hash_index_dash.cpp` |   `TPCCHashDashTest`   |
| TPCC-NP  | DRAM  |   `tests/benchmarks/tpcc_hash_index.cpp`    |     `TPCCHashTest`     |
|  YCSB-T  |  NVM  |    `tests/benchmarks/ycsb/ycsb_dash.cpp`    | `YCSBDASHTestWithInit` |
|  YCSB-T  | DRAM  |      `tests/benchmarks/ycsb/ycsb.cpp`       |   `YCSBTestWithInit`   |
