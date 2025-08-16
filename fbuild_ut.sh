#!/bin/bash
ROOT=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
CURDIR=$(pwd)
cd $ROOT
make -C ut DEFINES="-DTESTING $@"
cd $CURDIR