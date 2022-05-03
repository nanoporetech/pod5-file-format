
benchmark=$1
source_files=$2

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Running benchmark $1 with input files $2:"

docker run --rm -it \
    -v"${script_dir}/tools":/benchmark-tools \
    -v"${source_files}":/input_path \
    -v"${script_dir}/${benchmark}":/benchmark \
    mkr-benchmark-base /benchmark/run.sh

