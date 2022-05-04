#!/bin/bash

input_path=$1
output_path=$2

temp_dir=/benchmark/outputs/slow5_files_tmp
mkdir -p $temp_dir
mkdir -p $output_path

slow5tools f2s $input_path -d $temp_dir

# Most comparable to have one file for both formats:
slow5tools merge $temp_dir -o $output_path/file.blow5

rm -r $temp_dir

# Index will get generated on first test anyway, we should do it now to give best results later:
slow5tools index $output_path/file.blow5
