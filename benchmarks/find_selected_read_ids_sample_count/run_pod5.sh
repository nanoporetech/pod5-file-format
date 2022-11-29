#!/bin/bash

input_dir=$1
type_output_dir=$2
full_output_dir=$3

./tools/find_and_get_pod5.py "${input_dir}/pod5" "${type_output_dir}" --get-column sample_count --select-ids "${full_output_dir}/selected_read_ids.csv"
