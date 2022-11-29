#!/bin/bash

input_dir=$1
output_dir=$2

tools/pyslow5_tests.py "${input_dir}"/blow5/*.blow5 "${output_dir}" all_values --get-column samples
