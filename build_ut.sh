#!/bin/bash
ROOT=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CURDIR=$(pwd)
cd $ROOT
source build_configs.sh
make clean -C ut
rm -r ut/out
make -C ut DEFINES="-DTESTING $*"
mkdir -p ut/out/cores
mkdir -p ut/out/logs
cd $CURDIR