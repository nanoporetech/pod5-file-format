"""
Tool for converting pod5 files to the legacy fast5 format
"""
import multiprocessing as mp
import time
from collections import namedtuple
from pathlib import Path
from queue import Empty
from typing import List

import h5py
import numpy
import vbz_h5py_plugin  # noqa: F401

import pod5 as p5
from pod5.tools.parsers import pod5_convert_to_fast5_argparser, run_tool

from .utils import iterate_inputs


def format_sample_count(count):
    units = [
        (1000000000000, "T"),
        (1000000000, "G"),
        (1000000, "M"),
        (1000, "K"),
    ]

    for div, unit in units:
        if count > div:
            return f"{count/div:.1f} {unit}Samples"

    return f"{count} Samples"


WriteRequest = namedtuple("WriteRequest", [])
Read = namedtuple(
    "Read",
    [
        "read_id",
        "signal",
        "pore_type",
        "digitisation",
        "offset",
        "range",
        "sampling_rate",
        "channel_number",
        "channel_mux",
        "start_time",
        "duration",
        "read_number",
        "median_before",
        "end_reason",
        "tracking_id",
        "context_tags",
        "num_minknow_events",
        "tracked_scaling_scale",
        "tracked_scaling_shift",
        "predicted_scaling_scale",
        "predicted_scaling_shift",
        "num_reads_since_mux_change",
        "time_since_mux_change",
    ],
)
Fast5FileData = namedtuple("Fast5FileData", ["filename", "reads"])


def do_write_fast5_files(write_request_queue, write_data_queue, exit_queue):

    # Pod5 does not have 'partial' so need to add that back in here.
    fast5_end_reasons = {
        "unknown": 0,
        "partial": 1,  # Do not remove, required by fast5.
        "mux_change": 2,
        "unblock_mux_change": 3,
        "data_service_unblock_mux_change": 4,
        "signal_positive": 5,
        "signal_negative": 6,
    }

    while True:
        # Try to get some data to write:
        try:
            file_data = write_data_queue.get(timeout=5)
        except Empty:
            # Check if we are requested to exit:
            try:
                exit_queue.get(timeout=0.001)
                break
            except Empty:
                pass
            continue

        end_reason_type = h5py.enum_dtype(fast5_end_reasons)

        ascii_string_type = h5py.string_dtype("ascii")

        with h5py.File(file_data.filename, "w") as file:
            file.attrs.create(
                "file_version", "3.0".encode("ascii"), dtype=ascii_string_type
            )
            file.attrs.create(
                "file_type", "multi-read".encode("ascii"), dtype=ascii_string_type
            )

            for read in file_data.reads:
                tracking_id = dict(read.tracking_id)
                read_group = file.create_group(f"read_{read.read_id}")
                read_group.attrs.create(
                    "run_id",
                    tracking_id["run_id"].encode("ascii"),
                    dtype=ascii_string_type,
                )
                read_group.attrs.create(
                    "pore_type", read.pore_type.encode("ascii"), dtype=ascii_string_type
                )

                tracking_id_group = read_group.create_group("tracking_id")
                for k, v in tracking_id.items():
                    tracking_id_group.attrs[k] = v

                context_tags_group = read_group.create_group("context_tags")
                for k, v in read.context_tags:
                    context_tags_group.attrs[k] = v

                channel_id_group = read_group.create_group("channel_id")
                channel_id_group.attrs.create(
                    "digitisation", read.digitisation, dtype=numpy.float64
                )
                channel_id_group.attrs.create(
                    "offset", read.offset, dtype=numpy.float64
                )
                channel_id_group.attrs.create("range", read.range, dtype=numpy.float64)
                channel_id_group.attrs.create(
                    "sampling_rate", read.sampling_rate, dtype=numpy.float64
                )
                channel_id_group.attrs["channel_number"] = str(read.channel_number)

                raw_group = read_group.create_group("Raw")
                raw_group.create_dataset(
                    "Signal",
                    data=read.signal,
                    dtype=numpy.int16,
                    compression=32020,
                    compression_opts=(0, 2, 1, 1),
                )
                raw_group.attrs.create(
                    "start_time", read.start_time, dtype=numpy.uint64
                )
                raw_group.attrs.create("duration", read.duration, dtype=numpy.uint32)
                raw_group.attrs.create(
                    "read_number", read.read_number, dtype=numpy.int32
                )
                raw_group.attrs.create("start_mux", read.channel_mux, dtype=numpy.uint8)
                raw_group.attrs["read_id"] = str(read.read_id)
                raw_group.attrs.create(
                    "median_before", read.median_before, dtype=numpy.float64
                )

                # Lookup the fast5 enumeration values, which should include "partial: 1"
                # This will ensure that the enumeration is valid on a round-trip
                raw_group.attrs.create(
                    "end_reason",
                    fast5_end_reasons[read.end_reason.name],
                    dtype=end_reason_type,
                )

                raw_group.attrs.create(
                    "num_minknow_events", read.num_minknow_events, dtype=numpy.uint64
                )

                raw_group.attrs.create(
                    "tracked_scaling_scale",
                    read.tracked_scaling_scale,
                    dtype=numpy.float32,
                )
                raw_group.attrs.create(
                    "tracked_scaling_shift",
                    read.tracked_scaling_shift,
                    dtype=numpy.float32,
                )
                raw_group.attrs.create(
                    "predicted_scaling_scale",
                    read.predicted_scaling_scale,
                    dtype=numpy.float32,
                )
                raw_group.attrs.create(
                    "predicted_scaling_shift",
                    read.predicted_scaling_shift,
                    dtype=numpy.float32,
                )
                raw_group.attrs.create(
                    "num_reads_since_mux_change",
                    read.num_reads_since_mux_change,
                    dtype=numpy.uint32,
                )
                raw_group.attrs.create(
                    "time_since_mux_change",
                    read.time_since_mux_change,
                    dtype=numpy.float32,
                )

        # Request more writes
        write_request_queue.put(WriteRequest())


def put_write_fast5_file(
    file_reads: Fast5FileData, write_request_queue, write_data_queue
):

    write_request_queue.get()
    write_data_queue.put(file_reads)


def extract_read(read_table_version: p5.reader.ReadTableVersion, read: p5.ReadRecord):
    run_info = read.run_info

    return Read(
        read.read_id,
        read.signal,
        read.pore.pore_type,
        read.calibration_digitisation,
        read.calibration.offset,
        read.calibration_range,
        run_info.sample_rate,
        read.pore.channel,
        read.pore.well,
        read.start_sample,
        read.sample_count,
        read.read_number,
        read.median_before,
        read.end_reason,
        run_info.tracking_id,
        run_info.context_tags,
        read.num_minknow_events,
        read.tracked_scaling.scale,
        read.tracked_scaling.shift,
        read.predicted_scaling.scale,
        read.predicted_scaling.shift,
        read.num_reads_since_mux_change,
        read.time_since_mux_change,
    )


def make_fast5_filename(output_location, file_index):
    output_location.mkdir(parents=True, exist_ok=True)
    return output_location / f"output_{file_index}.fast5"


def convert_to_fast5(
    inputs: List[Path],
    output: Path,
    recursive: bool = False,
    threads: int = 10,
    force_overwrite: bool = False,
    file_read_count: int = 4000,
):

    if output.is_file() and not force_overwrite:
        raise FileExistsError(
            "Output path points to an existing file and --force_overwrite not set"
        )

    ctx = mp.get_context("spawn")
    write_request_queue = ctx.Queue()
    write_data_queue = ctx.Queue()
    write_exit_queue = ctx.Queue()

    active_processes = []

    for _ in range(2 * threads):
        # Preload the write request queue with two requests per writer:
        write_request_queue.put(WriteRequest())

    for _ in range(threads):
        # And kick off the writers waiting for data:
        p = ctx.Process(
            target=do_write_fast5_files,
            args=(
                write_request_queue,
                write_data_queue,
                write_exit_queue,
            ),
        )
        p.start()
        active_processes.append(p)

    print("Converting reads...")
    t_start = t_last_update = time.time()
    update_interval = 5  # seconds

    file_count = 0
    read_count = 0
    sample_count = 0
    mb_total = (sample_count) / (1000 * 1000)

    # Divide up files between readers:
    current_reads_batch = []

    for filename in iterate_inputs(inputs, recursive, "*.pod5"):
        try:
            reader = p5.Reader(filename)
        except Exception as exc:
            print(f"Error opening: {filename}: {exc}")
            continue

        for read in reader.reads(preload={"samples"}):
            now = time.time()
            if t_last_update + update_interval < now:
                t_last_update = now
                mb_total = (sample_count) / (1000 * 1000)
                time_total = t_last_update - t_start
                print(
                    f"{file_count} fast5s\t"
                    f"{read_count} reads\t"
                    f"{format_sample_count(sample_count)}\t"
                    f"{mb_total/time_total:.1f} MB/s"
                )

            extracted_read = extract_read(reader.reads_table_version, read)
            current_reads_batch.append(extracted_read)
            read_count += 1
            sample_count += len(extracted_read.signal)

            # Write a batch of reads to a fast5 file
            if len(current_reads_batch) >= file_read_count:
                put_write_fast5_file(
                    Fast5FileData(
                        make_fast5_filename(output, file_count),
                        current_reads_batch,
                    ),
                    write_request_queue,
                    write_data_queue,
                )
                file_count += 1

                current_reads_batch = []

    # Flush the final batch to file:
    put_write_fast5_file(
        Fast5FileData(make_fast5_filename(output, file_count), current_reads_batch),
        write_request_queue,
        write_data_queue,
    )

    for p in active_processes:
        write_exit_queue.put(None)

    for p in active_processes:
        p.join()

    time_total = now - t_start
    print(
        f"{file_count+1} fast5s\t"
        f"{read_count} reads\t"
        f"{format_sample_count(sample_count)}\t"
        f"{mb_total/time_total:.1f} MB/s"
    )

    print(f"Conversion complete: {sample_count} samples")

    for q in [write_request_queue, write_data_queue, write_exit_queue]:
        q.close()
        q.join_thread()


def main():
    run_tool(pod5_convert_to_fast5_argparser())


if __name__ == "__main__":
    main()
