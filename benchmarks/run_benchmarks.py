#!/usr/bin/env python3
"""
Example usage:
```
> taskset -c 0-10 ./benchmarks/run_benchmarks.py ./input_files/ \
    ./benchmark-outputs/ --skip-to-benchmark find_all_samples
```
"""

import argparse
import json
import shutil
import subprocess
import time
from collections import namedtuple
from pathlib import Path

import tabulate

Benchmark = namedtuple(
    "Benchmark",
    ["name", "file_types", "checks", "input_benchmark", "pre_run", "post_run_fixup"],
)

BENCHMARK_ROOT = Path(__file__).resolve().parent

POD5_FILE_TYPE = "pod5"
BLOW5_FILE_TYPE = "blow5"
FAST5_FILE_TYPE = "fast5"
ALL_FILE_TYPES = [POD5_FILE_TYPE, BLOW5_FILE_TYPE, FAST5_FILE_TYPE]


def du(path):
    """disk usage in human readable format (e.g. '2,1GB')"""
    return subprocess.check_output(["du", "-sh", path]).split()[0].decode("utf-8")


def generate_report(input_dir, output_dir, timing_results):
    report = ""

    skipped_benchmarks = len(ALL_BENCHMARKS) - len(timing_results)

    skipped = ""
    if skipped_benchmarks != 0:
        skipped = f", skipped {skipped_benchmarks}"
    report += f"Ran {len(timing_results)} benchmarks{skipped}\n\n"
    report += f"Input data was {input_dir}\n\n"

    report += "File sizes\n"
    report += "----------\n\n"

    convert_output_dir = output_dir / "convert"
    sizes = []
    for file_type in ALL_FILE_TYPES:
        file_type_dir = convert_output_dir / file_type
        if file_type_dir.exists:
            sizes.append(du(file_type_dir))
        else:
            sizes.append("Not Run")
    report += (
        tabulate.tabulate([sizes], headers=ALL_FILE_TYPES, tablefmt="github") + "\n\n"
    )

    report += "Timings\n"
    report += "-------\n\n"
    results = []
    for benchmark in ALL_BENCHMARKS:
        row = [benchmark.name.replace("_", " ")]
        results.append(row)

        if benchmark.name in timing_results:
            timings = timing_results[benchmark.name]
            for file_type in ALL_FILE_TYPES:
                if file_type in timings:
                    row.append(f"{timings[file_type]:.1f} secs")
                else:
                    row.append("Not Run")
        else:
            for file_type in ALL_FILE_TYPES:
                row.append("Not Run")

    results_headers = [""] + ALL_FILE_TYPES
    report += (
        tabulate.tabulate(results, headers=results_headers, tablefmt="github") + "\n"
    )
    return report


def check_read_ids(benchmark, file_types, output_dir, only_format):
    if only_format is not None:
        print("Not checking read ids - only one format executed")
        return

    csv_check_files = []
    for file_type in file_types:
        csv_check_files.append(output_dir / file_type / "read_ids.csv")

    for a, b in zip(csv_check_files[1:], csv_check_files):
        subprocess.run(
            [BENCHMARK_ROOT / "tools" / "check_csvs_consistent.py", a, b], check=True
        )


def check_file_sizes(benchmark, file_types, output_dir, only_format):
    print("File sizes for output dir")
    subprocess.run(["du", "-sh"] + list(output_dir.glob("*")), check=True)


def copy_fast5_files(benchmark, input_dir, output_dir):
    shutil.copytree(input_dir, output_dir / "fast5")


def randomly_select_read_ids(benchmark, input_dir, output_dir):
    print("Randomly selecting read ids for benchmark")

    subprocess.run(
        [
            BENCHMARK_ROOT / "tools" / "find_and_get_pod5.py",
            input_dir / "pod5",
            output_dir,
        ],
        check=True,
    )
    subprocess.run(
        [
            BENCHMARK_ROOT / "tools" / "select-random-ids.py",
            output_dir / "read_ids.csv",
            output_dir / "selected_read_ids.csv",
            "--select-ratio",
            "0.1",
        ],
        check=True,
    )


ALL_BENCHMARKS = [
    Benchmark(
        "convert",
        [POD5_FILE_TYPE, BLOW5_FILE_TYPE],
        [check_file_sizes],
        input_benchmark=None,
        post_run_fixup=copy_fast5_files,
        pre_run=None,
    ),
    Benchmark(
        "find_all_read_ids",
        ALL_FILE_TYPES,
        [check_read_ids],
        input_benchmark="convert",
        post_run_fixup=None,
        pre_run=None,
    ),
    Benchmark(
        "find_all_samples",
        ALL_FILE_TYPES,
        [check_read_ids],
        input_benchmark="convert",
        post_run_fixup=None,
        pre_run=None,
    ),
    Benchmark(
        "find_selected_read_ids_read_number",
        ALL_FILE_TYPES,
        [check_read_ids],
        input_benchmark="convert",
        post_run_fixup=None,
        pre_run=randomly_select_read_ids,
    ),
    Benchmark(
        "find_selected_read_ids_sample_count",
        ALL_FILE_TYPES,
        [check_read_ids],
        input_benchmark="convert",
        post_run_fixup=None,
        pre_run=randomly_select_read_ids,
    ),
    Benchmark(
        "find_selected_read_ids_samples",
        ALL_FILE_TYPES,
        [check_read_ids],
        input_benchmark="convert",
        post_run_fixup=None,
        pre_run=randomly_select_read_ids,
    ),
]


def run_benchmark(benchmark, input_dir, output_dir, only_format=None):
    if output_dir.exists():
        print("Removing old output dir")
        shutil.rmtree(output_dir)

    file_types = benchmark.file_types if benchmark.file_types else ALL_FILE_TYPES

    time_results = {}

    if benchmark.pre_run:
        benchmark.pre_run(benchmark, input_dir, output_dir)

    for file_type in file_types:
        if only_format is not None and only_format != file_type:
            print(f"## Skipping for file type {file_type}:")
            continue
        print(f"## Running for file type {file_type}:")
        file_type_output_dir = output_dir / file_type
        file_type_output_dir.mkdir(exist_ok=True, parents=True)
        start = time.time()
        subprocess.run(
            [
                BENCHMARK_ROOT / benchmark.name / f"run_{file_type}.sh",
                input_dir,
                file_type_output_dir,
                output_dir,
            ],
            check=True,
            cwd=BENCHMARK_ROOT,
        )
        end = time.time()
        duration_secs = end - start
        time_results[file_type] = duration_secs
        print(f"## Took {duration_secs:.2f} seconds")

    if benchmark.post_run_fixup:
        benchmark.post_run_fixup(benchmark, input_dir, output_dir)

    if benchmark.checks:
        print("## Running checks")
        for check in benchmark.checks:
            check(benchmark, file_types, output_dir, only_format)

    return time_results


def run_benchmarks(args):
    timing_results = {}

    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()

    skip_list = []
    if args.skip_to_benchmark:
        found = False
        for benchmark in ALL_BENCHMARKS:
            if benchmark.name == args.skip_to_benchmark:
                found = True

            if not found:
                skip_list.append(benchmark.name)

    for benchmark in ALL_BENCHMARKS:
        if benchmark.name in skip_list:
            print(f"# Skipping benchmark {benchmark.name}")
            continue
        print(f"# Running benchmark {benchmark.name}:")
        benchmark_input_dir = input_dir
        if benchmark.input_benchmark:
            benchmark_input_dir = output_dir / benchmark.input_benchmark
        timing_results[benchmark.name] = run_benchmark(
            benchmark,
            benchmark_input_dir,
            output_dir / benchmark.name,
            args.only_format,
        )

    report = generate_report(input_dir, output_dir, timing_results)
    print(report)

    with open(args.output_dir / "timings.json", "w") as f:
        f.write(json.dumps(timing_results, indent=2))
    with open(args.output_dir / "report.md", "w") as f:
        f.write(report)


def main():
    parser = argparse.ArgumentParser("Run Benchmarks for POD5 format")
    parser.add_argument("input_dir", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument(
        "--skip-to-benchmark",
        type=str,
        help="Start benchmarking from a named benchmark",
    )
    parser.add_argument(
        "--only-format",
        type=str,
        help="Only run benchmarks for a single format",
    )

    args = parser.parse_args()

    run_benchmarks(args)


if __name__ == "__main__":
    main()
