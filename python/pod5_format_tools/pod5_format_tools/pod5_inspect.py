"""
Tool for inspecting the contents of pod5 files
"""
from dataclasses import asdict
import os
import sys
import csv
import argparse
from uuid import UUID
from pathlib import Path

import pod5_format as p5


def do_reads_command(combined_reader: p5.CombinedReader):
    keys = [
        "read_id",
        "channel",
        "well",
        "pore_type",
        "read_number",
        "start_sample",
        "end_reason",
        "median_before",
        "sample_count",
        "byte_count",
        "signal_compression_ratio",
    ]

    csv_read_writer = csv.DictWriter(sys.stdout, keys)
    csv_read_writer.writeheader()
    for read in combined_reader.reads():
        fields = {
            "read_id": read.read_id,
            "channel": read.pore.channel,
            "well": read.pore.well,
            "pore_type": read.pore.pore_type,
            "read_number": read.read_number,
            "start_sample": read.start_sample,
            "end_reason": read.end_reason.name,
            "median_before": f"{read.median_before:.1f}",
            "sample_count": read.sample_count,
            "byte_count": read.byte_count,
            "signal_compression_ratio": f"{read.byte_count / float(read.sample_count*2):.3f}",
        }
        try:
            csv_read_writer.writerow(fields)
        except BrokenPipeError:
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
            break


def dump_run_info(run_info: p5.RunInfo):
    tab = "\t"
    for name, value in asdict(run_info).items():
        if isinstance(value, list):
            print(f"{tab}{name}:")
            for k, v in value:
                print(f"{tab*2}{k}: {v}")
        else:
            print(f"{tab}{name}: {value}")


def do_read_command(combined_reader: p5.CombinedReader, read_id_str: str):
    try:
        read_id = UUID(read_id_str)
    except ValueError:
        print(f"Supplied read_id '{read_id_str}' is not a valid UUID")
        return

    for read in combined_reader.reads():
        if read.read_id != read_id:
            continue

        sample_count = read.sample_count
        byte_count = read.byte_count

        pore_data = read.pore
        calibration_data = read.calibration
        end_reason_data = read.end_reason

        print(f"read_id: {read.read_id}")
        print(f"read_number:\t{read.read_number}")
        print(f"start_sample:\t{read.start_sample}")
        print(f"median_before:\t{read.median_before}")
        print("channel data:")
        print(f"\tchannel: {pore_data.channel}")
        print(f"\twell: {pore_data.well}")
        print(f"\tpore_type: {pore_data.pore_type}")
        print("end reason:")
        print(f"\tname: {end_reason_data.name}")
        print(f"\tforced: {end_reason_data.forced}")
        print("calibration:")
        print(f"\toffset: {calibration_data.offset}")
        print(f"\tscale: {calibration_data.scale}")
        print("samples:")
        print(f"\tsample_count: {sample_count}")
        print(f"\tbyte_count: {byte_count}")
        print(
            f"\tcompression ratio: {read.byte_count / float(read.sample_count*2):.3f}"
        )

        print("run info:")
        dump_run_info(read.run_info)
        break


def do_debug_command(combined_reader: p5.CombinedReader):
    batch_count = 0
    batch_sizes = []
    read_count = 0
    sample_count = 0
    byte_count = 0
    min_sample = float("inf")
    max_sample = 0

    run_infos = {}

    for batch in combined_reader.read_batches():
        batch_count += 1

        batch_read_count = 0
        for read in batch.reads():
            batch_read_count += 1
            read_sample_count = read.sample_count
            sample_count += read_sample_count
            byte_count += read.byte_count

            run_info_index = read.run_info_index
            if run_info_index not in run_infos:
                run_infos[run_info_index] = read.run_info

            min_sample = min(min_sample, read.start_sample)
            max_sample = max(max_sample, read.start_sample + read_sample_count)
        batch_sizes.append(batch_read_count)
        read_count += batch_read_count

    print(f"Contains {read_count} reads, in {batch_count} batches: {batch_sizes}")
    print(f"Reads span from sample {min_sample} to {max_sample}")
    print(
        f"{sample_count} samples, {byte_count} bytes: {100*byte_count/float(sample_count*2):.1f} % signal compression ratio"
    )

    for idx, run_info in run_infos.items():
        print(f"Run info {idx}:")
        dump_run_info(run_info)


def do_summary_command(combined_reader: p5.CombinedReader):
    batch_count = 0
    total_read_count = 0

    for batch in combined_reader.read_batches():
        batch_count += 1

        batch_read_count = 0
        for _ in batch.reads():
            batch_read_count += 1

        print(f"Batch {batch_count}, {batch_read_count} reads")
        total_read_count += batch_read_count
    print(f"Found {batch_count} batches, {total_read_count} reads")


def main():
    parser = argparse.ArgumentParser("Inspect the contents of an pod5 file")

    subparser = parser.add_subparsers(title="command", dest="command")
    summary_parser = subparser.add_parser("summary")
    summary_parser.add_argument("input_files", type=Path, nargs="+")

    reads_parser = subparser.add_parser("reads")
    reads_parser.add_argument("input_files", type=Path, nargs="+")

    reads_parser = subparser.add_parser("read")
    reads_parser.add_argument("input_files", type=Path, nargs="+")
    reads_parser.add_argument("read_id", type=str)

    debug_parser = subparser.add_parser("debug")
    debug_parser.add_argument("input_files", type=Path, nargs="+")

    args = parser.parse_args()

    if args.command == None:
        parser.print_help()
        return

    for filename in args.input_files:
        print(f"File: {filename}")
        try:
            combined_reader = p5.CombinedReader(filename)
        except Exception as exc:
            print(f"Failed to open combined pod5 file: {filename}: {exc}")
            continue

        if args.command == "reads":
            do_reads_command(combined_reader)
        if args.command == "read":
            do_read_command(combined_reader, args.read_id)
        elif args.command == "debug":
            do_debug_command(combined_reader)
        elif args.command == "summary":
            do_summary_command(combined_reader)


if __name__ == "__main__":
    main()
