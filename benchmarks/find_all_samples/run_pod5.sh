#!/bin/bash

input_dir=$1
output_dir=$2

./tools/find_and_get_pod5.py "${input_dir}/pod5" "${output_dir}" --get-column samples
