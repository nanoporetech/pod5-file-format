#!/bin/bash

ls /input_path

rm -r /benchmark/outputs || true

echo "selecting read ids"
/benchmark-tools/select-random-ids.py /input_path/slow5_files/read_ids.csv /benchmark/outputs/read_ids.csv --select-ratio 0.1

echo "fast5"
/benchmark-tools/time.py "fast5" -- /benchmark-tools/find_and_get_fast5.py /input_path/fast5_files /benchmark/outputs/fast5_files --select-ids /benchmark/outputs/read_ids.csv  --get-column samples

echo "mkr"
/benchmark-tools/time.py "mkr" -- /benchmark-tools/find_and_get_mkr.py /input_path/mkr_files /benchmark/outputs/mkr_files --select-ids /benchmark/outputs/read_ids.csv  --get-column samples

echo "slow5"
/benchmark-tools/time.py "slow5" -- /benchmark-tools/find_and_get_slow5.py /input_path/slow5_files /benchmark/outputs/slow5_files --select-ids /benchmark/outputs/read_ids.csv  --get-column samples

echo "check"
/benchmark-tools/check_csvs_consistent.py /benchmark/outputs/fast5_files/read_ids.csv /benchmark/outputs/slow5_files/read_ids.csv
/benchmark-tools/check_csvs_consistent.py /benchmark/outputs/mkr_files/read_ids.csv /benchmark/outputs/slow5_files/read_ids.csv