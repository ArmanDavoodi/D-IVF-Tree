ROOT=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/..
CURDIR=$(pwd)
cd $ROOT/bench/datasets

DATASET_NAMES=("BIGANN100M")
DATASET_PATHS=("bigann/raw_data/BIGANN100M.u8bin")
URLS=("https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/learn.100M.u8bin")

LEN=${#DATASET_PATHS[@]}

# Loop through the array using indices
for (( i=0; i<LEN; i++ )); do
    echo "downloading ${DATASET_NAMES[$i]}..."
    wget -O ${DATASET_PATHS[$i]} ${URLS[$i]}
done

cd $CURDIR