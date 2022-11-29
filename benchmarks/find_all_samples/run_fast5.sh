#!/bin/bash

input_dir=$1
output_dir=$2

tools/find_and_get_fast5.py "${input_dir}/fast5" "${output_dir}" --get-column samples
