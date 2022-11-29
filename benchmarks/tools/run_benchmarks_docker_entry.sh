#!/bin/bash

# Use taskset to limit benchmarks to specific cores, ensuring a fair test of limited resources:
taskset -c 0-10 /benchmarks/run_benchmarks.py /input /outputs
