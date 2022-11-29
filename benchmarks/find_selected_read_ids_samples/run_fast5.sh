#!/bin/bash

input_dir=$1
output_dir=$2
full_output_dir=$3

tools/find_and_get_fast5.py "${input_dir}/fast5" "${output_dir}" --get-column samples --select-ids "${full_output_dir}/selected_read_ids.csv"
