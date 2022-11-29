#!/bin/bash

input_dir=$1
output_dir=$2

echo "Unpacking builds from $input_dir to $output_dir"

file_regex=".*/lib_pod5-[0-9\.]*-(.*).tar.gz"
for i in "${input_dir}"/lib_pod5*.tar.gz; do

    if [[ $i =~ $file_regex ]]
    then
        sku="${BASH_REMATCH[1]}"
        echo "Extracting for SKU: $sku"
    else
        echo "$i doesn't match expected file pattern" >&2
        exit 1
    fi

    sku_out_dir="$output_dir/$sku/"
    mkdir -p "${sku_out_dir}"

    tmp_dir="$output_dir/tmp"
    mkdir -p "$tmp_dir"
    tar -xzf "$i" --directory "$output_dir/tmp"

    mv "${tmp_dir}"/lib/* "${sku_out_dir}"

    rm -r "$tmp_dir"
done

echo "unpacked skus:"
ls "${output_dir}/"
echo "contents:"
ls "${output_dir}"/*
