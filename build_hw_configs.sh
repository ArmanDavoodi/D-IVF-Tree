ROOT=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

g++ $ROOT/configs/utils/cpuid.cpp -o $ROOT/configs/utils/bin/cpuid

echo "#ifndef SUPPORT_H_" > $ROOT/configs/support.h
echo "#define SUPPORT_H_" >> $ROOT/configs/support.h
echo "" >> $ROOT/configs/support.h

has_avx=$(./configs/utils/bin/cpuid 1 ECX 28)
if [[ $has_avx -eq 1 ]]; then
  echo "#define __DIVFTREE_ATOMIC128__" >> $ROOT/configs/support.h
else
  echo "WARNING: Atomic load and store for 128bit data is not supported!"
fi

if grep -q cx16 /proc/cpuinfo; then
  echo "#define __DIVFTREE_CMPXCHG128__" >> $ROOT/configs/support.h
else
  echo "WARNING: DWCAS instruction is not supported!"
fi

echo "" >> $ROOT/configs/support.h
echo "#endif" >> $ROOT/configs/support.h