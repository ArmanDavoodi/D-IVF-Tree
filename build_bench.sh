#!/bin/bash
ROOT=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CURDIR=$(pwd)
cd $ROOT
source build_hw_configs.sh

rm bench/run.conf
source bench/build_configs.sh

make clean -C bench
rm -r bench/out
mkdir -p bench/out/cores
mkdir -p bench/out/logs
# make -C bench DEFINES="-DTESTING $*"
make -C bench DEFINES="$*"
cd $CURDIR