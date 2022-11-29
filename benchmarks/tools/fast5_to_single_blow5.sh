#!/bin/bash

input_path=$1
output_path=$2

mkdir -p "$output_path"

temp_dir="${output_path}/tmp"
mkdir -p "$temp_dir"

# specific options (-c zstd -s svb-zd) must be provided to slow5tools to create compression comparable to vbz
# also number of processes/threads must be set to 10 to match with default value in pod5_convert
# however, the svb-zd stream variable byte + zig-zag delta implementation in slow5 mirrors
# ONT's previous 32 bit zigzag delta, where as pod5 is using a newer 16 bit zigzag delta with SIMD optimisations
# so pod5 has the added performance benefit of using the newer zigzag delta
# slow5 compression methods are modular, so we can easily add the new one iff necessary
slow5tools f2s "$input_path" -d "$temp_dir" -p 10 -c zstd -s svb-zd

# Most comparable to have one file for both formats:
slow5tools cat "$temp_dir -o $output_path/file.blow5" || slow5tools merge "$temp_dir" -o "$output_path/file.blow5" -t 10 -c zstd -s svb-zd
#if the files are from the same run ID, slow5tools cat can be used, which is significantly faster
#slow5tools cat $temp_dir -o $output_path/file.blow5

rm -r "$temp_dir"

# Index will get generated on first test anyway, we should do it now to give best results later:
# current slow5tools implementation decompresses the whole record for indexing and is not efficient
# the specification supports partial decompress of the record (also signal chunking if necessary)
slow5tools index "$output_path/file.blow5"
