#!/bin/bash

input_dir=`readlink -f $1`

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Running benchmark on input '${input_dir}'"

${script_dir}/run_benchmark.sh convert "${input_dir}"
${script_dir}/run_benchmark.sh find_all_read_ids "${script_dir}/convert/outputs"
${script_dir}/run_benchmark.sh find_selected_read_ids_read_number "${script_dir}/find_all_read_ids/outputs"
run_benchmark.${script_dir}/sh find_selected_read_ids_sample_count "${script_dir}/find_all_read_ids/outputs"