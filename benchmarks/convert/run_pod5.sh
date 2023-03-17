#!/bin/bash

input_dir=$1
output_dir=$2

pod5 convert fast5 "$input_dir" --output "$output_dir"
