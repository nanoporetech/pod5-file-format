#!/bin/bash

input_dir=$1
output_dir=$2
full_output_dir=$3

tools/pyslow5_tests.py "${input_dir}"/blow5/*.blow5 "${output_dir}" sample_values --get-column sample_count --select-ids "${full_output_dir}/selected_read_ids.csv"
