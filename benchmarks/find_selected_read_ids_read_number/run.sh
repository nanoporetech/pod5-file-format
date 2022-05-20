#!/bin/bash

ls /input_path

rm -r /benchmark/outputs || true

echo "selecting read ids"
/benchmark-tools/select-random-ids.py /input_path/slow5_files/read_ids.csv /benchmark/outputs/read_ids.csv --select-ratio 0.1

echo "fast5"
/benchmark-tools/time.py "fast5" -- /benchmark-tools/find_and_get_fast5.py /input_path/fast5_files /benchmark/outputs/fast5_files --select-ids /benchmark/outputs/read_ids.csv  --get-column read_number

echo "pod5"
/benchmark-tools/time.py "pod5" -- /benchmark-tools/find_and_get_pod5.py /input_path/pod5_files /benchmark/outputs/pod5_files --select-ids /benchmark/outputs/read_ids.csv  --get-column read_number

echo "slow5"
/benchmark-tools/time.py "slow5" -- /benchmark-tools/find_and_get_slow5.py /input_path/slow5_files /benchmark/outputs/slow5_files --select-ids /benchmark/outputs/read_ids.csv  --get-column read_number

echo "check"
/benchmark-tools/check_csvs_consistent.py /benchmark/outputs/fast5_files/read_ids.csv /benchmark/outputs/slow5_files/read_ids.csv
/benchmark-tools/check_csvs_consistent.py /benchmark/outputs/pod5_files/read_ids.csv /benchmark/outputs/slow5_files/read_ids.csv
