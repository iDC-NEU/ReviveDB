# 在docker容器内操作
cd ~/

#sudo yum -y update
sudo yum -y install epel-release
sudo yum -y install https://packages.endpointdev.com/rhel/7/os/x86_64/endpoint-repo.x86_64.rpm

sudo yum install -y git glibc-devel libaio-devel flex bison ncurses-devel patch lsb_release readline-devel ndctl-devel daxctl-devel numactl-devel ipmctl zlib-devel wget make

wget https://github.com/Kitware/CMake/releases/download/v3.29.3/cmake-3.29.3-linux-x86_64.tar.gz

tar -zxvf cmake-3.29.3-linux-x86_64.tar.gz

# 覆盖bashrc
echo export CMAKE_PATH=$PWD/cmake-3.29.3-linux-x86_64 > ~/.bashrc

tar -zxvf output.tar.gz >/dev/null 2>&1
cd NextGenDB
git checkout -f origin/nvmdb-test
cd ..

echo export GAUSSHOME=$PWD/NextGenDB/mppdb_temp_install >> ~/.bashrc
echo export GCC_PATH=$PWD/output/buildtools/gcc10.3 >> ~/.bashrc
echo export PATH=\$GAUSSHOME/bin:\$GCC_PATH/gcc/bin:\$CMAKE_PATH/bin/:\$PATH >> ~/.bashrc

echo export LD_LIBRARY_PATH=\$GAUSSHOME/lib:\$GCC_PATH/lib64:\$GCC_PATH/isl/lib:\$GCC_PATH/mpc/lib/:\$GCC_PATH/mpfr/lib/:\$GCC_PATH/gmp/lib/:\$LD_LIBRARY_PATH >> ~/.bashrc
source ~/.bashrc

cd NextGenDB
mkdir tmp_build
sh build.sh -3rd ~/output
