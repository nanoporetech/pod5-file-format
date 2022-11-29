#!/bin/bash

set -e

input_dir=$(readlink -f "$1")
output_dir="$(pwd)/pod5-benchmark-outputs"
mkdir -p "${output_dir}"

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Running benchmark on input '${input_dir}'"

docker run --rm -it -v"${input_dir}":/input -v"${output_dir}":/outputs -v"${script_dir}"/:/benchmarks pod5-benchmark-base /benchmarks/tools/run_benchmarks_docker_entry.sh
