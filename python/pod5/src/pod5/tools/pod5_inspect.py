"""
Tool for inspecting the contents of pod5 files
"""
import csv
import os
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Callable, Dict, List
from uuid import UUID

import pod5 as p5
from pod5.tools.parsers import prepare_pod5_inspect_argparser, run_tool


def format_shift_scale_pair(pair):
    return f"({pair.shift} {pair.scale})"


def format_shift_scale_pair_num(pair):
    return f"({pair.shift:.1f} {pair.scale:.1f})"


def do_reads_command(reader: p5.Reader):
    keys = [
        "read_id",
        "channel",
        "well",
        "pore_type",
        "read_number",
        "start_sample",
        "end_reason",
        "median_before",
        "num_samples",
        "byte_count",
        "signal_compression_ratio",
        "num_minknow_events",
        "tracked_scaling",
        "predicted_scaling",
        "num_reads_since_mux_change",
        "time_since_mux_change",
    ]

    csv_read_writer = csv.DictWriter(sys.stdout, keys)
    csv_read_writer.writeheader()
    for read in reader.reads():
        fields = {
            "read_id": read.read_id,
            "channel": read.pore.channel,
            "well": read.pore.well,
            "pore_type": read.pore.pore_type,
            "read_number": read.read_number,
            "start_sample": read.start_sample,
            "end_reason": read.end_reason.name,
            "median_before": f"{read.median_before:.1f}",
            "num_samples": read.num_samples,
            "byte_count": read.byte_count,
            "signal_compression_ratio": f"{read.byte_count / float(read.sample_count*2):.3f}",
            "num_minknow_events": read.num_minknow_events,
            "tracked_scaling": format_shift_scale_pair_num(read.tracked_scaling),
            "predicted_scaling": format_shift_scale_pair_num(read.predicted_scaling),
            "num_reads_since_mux_change": read.num_reads_since_mux_change,
            "time_since_mux_change": read.time_since_mux_change,
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


def do_read_command(reader: p5.Reader, read_id: str):
    try:
        read_id = UUID(read_id)
    except ValueError:
        print(f"Supplied read_id '{read_id}' is not a valid UUID", file=sys.stderr)
        return

    for read in reader.reads():
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


def do_debug_command(reader: p5.Reader):
    batch_count = 0
    batch_sizes = []
    read_count = 0
    sample_count = 0
    byte_count = 0
    min_sample = float("inf")
    max_sample = 0

    run_infos = {}

    for batch in reader.read_batches():
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
        f"{sample_count} samples, {byte_count}"
        f" bytes: {100*byte_count/float(sample_count*2):.1f} % signal compression ratio"
    )

    for idx, run_info in run_infos.items():
        print(f"Run info {idx}:")
        dump_run_info(run_info)


def do_summary_command(reader: p5.Reader):
    batch_count = 0
    total_read_count = 0

    print(
        f"File version in memory {reader.file_version}, read table version {reader.reads_table_version}."
    )
    print(f"File version on disk {reader.file_version_pre_migration}.")

    for batch in reader.read_batches():
        batch_count += 1

        batch_read_count = 0
        for _ in batch.reads():
            batch_read_count += 1

        print(f"Batch {batch_count}, {batch_read_count} reads")
        total_read_count += batch_read_count
    print(f"Found {batch_count} batches, {total_read_count} reads")


def inspect_pod5(command: str, input_files: List[Path], **kwargs):
    """Determine which inspect command to run from the parsed arguments and run it"""

    commands: Dict[str, Callable] = {
        "reads": do_reads_command,
        "read": do_read_command,
        "summary": do_summary_command,
        "debug": do_debug_command,
    }

    for filename in input_files:
        print(f"File: {filename}")
        try:
            reader = p5.Reader(filename)
        except Exception as exc:
            print(f"Failed to open pod5 file: {filename}: {exc}", file=sys.stderr)
            continue

        kwargs["reader"] = reader
        commands[command](**kwargs)


def main():
    """Run the pod5 inspect tool"""
    run_tool(prepare_pod5_inspect_argparser())


if __name__ == "__main__":
    main()