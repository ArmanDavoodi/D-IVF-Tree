ROOT=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/..
CURDIR=$(pwd)
cd $ROOT/bench/datasets

DATASET_NAMES=("BIGANN100M" "BIGANN1B" "BIGANNQ10K")
DATASET_PATHS=("bigann/raw_data/BIGANN100M.u8bin" "bigann/raw_data/BIGANN1B.u8bin" "bigann/raw_data/BIGANNQ10K.u8bin")
URLS=("https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/learn.100M.u8bin" "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/base.1B.u8bin" "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/query.public.10K.u8bin")

LEN=${#DATASET_PATHS[@]}

# Loop through the array using indices
for (( i=0; i<LEN; i++ )); do
    if [ ! -f ${DATASET_PATHS[$i]} ]; then
        echo "downloading ${DATASET_NAMES[$i]}..."
        wget -O ${DATASET_PATHS[$i]} ${URLS[$i]}
    fi
done

cd $CURDIR