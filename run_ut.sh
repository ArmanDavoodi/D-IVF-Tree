#!/bin/bash
ROOT=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

rm -r $ROOT/ut/out/cores
rm -r $ROOT/ut/out/logs
mkdir -p $ROOT/ut/out/cores
mkdir -p $ROOT/ut/out/logs
# ulimit -c unlimited

# export CORE_DUMP_DIR=ut/out/cores/core.$(date +%Y_%m_%d_%H_%M_%S).$(pidof -s your_executable)

# echo $CORE_DUMP_DIR | sudo tee /proc/sys/kernel/core_pattern

# ./ut/build/bin/$1 > ut/out/logs/$1.log

directory="$ROOT/ut/build/bin"
all_success=1
for file in "$directory"/*; do
  if [ -f "$file" ]; then
    # Check if the file is executable
    if [ -x "$file" ]; then
      file_name=$(basename "$file")
      ./"$file" $* > $ROOT/ut/out/logs/"$file_name".log
      retcode=$?
      if [ $retcode -ne 0 ]; then
        all_success=0
        echo -e "\e[31m$file_name failed with exit code $retcode\e[0m"
      fi
    else
      all_success=0
      echo "File '$file' is not executable."
    fi
  fi
done
echo -e "\n########################################\n"
if [ $all_success -eq 0 ]; then
  echo -e "\e[31mSome tests failed. Check the logs in ut/out/logs/\e[0m\n"
else
  echo -e "\e[32mAll tests passed successfully!\e[0m\n"
fi

# echo "|/usr/share/apport/apport -p%p -s%s -c%c -d%d -P%P -u%u -g%g -- %E\n" | sudo tee /proc/sys/kernel/core_pattern

# unset CORE_DUMP_DIR

# todo run ut all
# todo run ut multiple files