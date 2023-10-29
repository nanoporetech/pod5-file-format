"""
Tool for converting pod5 files to the legacy fast5 format
"""
import time
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

import h5py
import numpy
import vbz_h5py_plugin  # noqa: F401
from more_itertools import chunked

import pod5 as p5
from pod5.tools.parsers import pod5_convert_to_fast5_argparser, run_tool
from pod5.tools.utils import DEFAULT_THREADS, collect_inputs, limit_threads

# Pod5 does not have 'partial' so need to add that back in here.
FAST5_END_REASONS = {
    "unknown": 0,
    "partial": 1,  # Do not remove, required by fast5.
    "mux_change": 2,
    "unblock_mux_change": 3,
    "data_service_unblock_mux_change": 4,
    "signal_positive": 5,
    "signal_negative": 6,
}

# Fast5 types
FAST5_END_REASON_TYPE = h5py.enum_dtype(FAST5_END_REASONS)
FAST5_STRING_TYPE = h5py.string_dtype("ascii")


class StatusMonitor:
    """Class for monitoring the status / progress of the conversion"""

    def __init__(self, file_count: int):
        self.update_interval = 10

        self.file_count = file_count
        self.files_started = 0
        self.files_ended = 0
        self.read_count = 0
        self.reads_processed = 0
        self.sample_count = 0

        self.time_start = self.time_last_update = time.time()

    @property
    def running(self) -> bool:
        """Return true if not all files have finished processing"""
        return self.files_ended < self.file_count

    def increment(
        self,
        *,
        files_started: int = 0,
        files_ended: int = 0,
        read_count: int = 0,
        reads_processed: int = 0,
        sample_count: int = 0,
    ) -> None:
        """Incremeent the status counters"""
        self.files_started += files_started
        self.files_ended += files_ended
        self.read_count += read_count
        self.reads_processed += reads_processed
        self.sample_count += sample_count

    @property
    def samples_mb(self) -> float:
        """Return the samples count in megabytes"""
        return (self.sample_count * 2) / 1_000_000

    @property
    def time_elapsed(self) -> float:
        """Return the total time elapsed in seconds"""
        return self.time_last_update - self.time_start

    @property
    def sample_rate(self) -> float:
        """Return the time averaged sample rate"""
        return self.samples_mb / self.time_elapsed

    def print_status(self, force: bool = False):
        """Print the status if the update interval has passed or if forced"""
        now = time.time()

        if force or self.time_last_update + self.update_interval < now:
            self.time_last_update = now

            print(
                f"{self.reads_processed} reads,\t",
                f"{self.formatted_sample_count},\t",
                f"{self.files_ended}/{self.file_count} files,\t",
                f"{self.sample_rate:.1f} MB/s",
            )

    @property
    def formatted_sample_count(self) -> str:
        """Return the sample count as a string with leading Metric prefix if necessary"""
        units = [
            (1000000000000, "T"),
            (1000000000, "G"),
            (1000000, "M"),
            (1000, "K"),
        ]

        for div, unit in units:
            if self.sample_count > div:
                return f"{self.sample_count/div:.1f} {unit}Samples"
        return f"{self.sample_count} Samples"


def write_pod5_record_to_fast5(read: p5.ReadRecord, fast5: h5py.File) -> None:
    tracking_id = read.run_info.tracking_id

    read_group = fast5.create_group(f"read_{read.read_id}")
    read_group.attrs.create(
        "run_id",
        tracking_id["run_id"].encode("ascii"),
        dtype=FAST5_STRING_TYPE,
    )
    read_group.attrs.create(
        "pore_type",
        read.pore.pore_type.encode("ascii"),
        dtype=FAST5_STRING_TYPE,
    )

    tracking_id_group = read_group.create_group("tracking_id")
    for k, v in tracking_id.items():
        tracking_id_group.attrs[k] = v

    context_tags_group = read_group.create_group("context_tags")
    for k, v in read.run_info.context_tags.items():
        context_tags_group.attrs[k] = v

    channel_id_group = read_group.create_group("channel_id")
    digitisation = read.run_info.adc_max - read.run_info.adc_min + 1
    channel_id_group.attrs.create("digitisation", digitisation, dtype=numpy.float64)
    channel_id_group.attrs.create(
        "offset", read.calibration.offset, dtype=numpy.float64
    )

    channel_id_group.attrs.create(
        "range", digitisation * read.calibration.scale, dtype=numpy.float64
    )
    channel_id_group.attrs.create(
        "sampling_rate", read.run_info.sample_rate, dtype=numpy.float64
    )
    channel_id_group.attrs["channel_number"] = str(read.pore.channel)

    raw_group = read_group.create_group("Raw")
    raw_group.create_dataset(
        "Signal",
        data=read.signal,
        dtype=numpy.int16,
        compression=32020,
        compression_opts=(0, 2, 1, 1),
    )
    raw_group.attrs.create("start_time", read.start_sample, dtype=numpy.uint64)
    raw_group.attrs.create("duration", read.sample_count, dtype=numpy.uint32)
    raw_group.attrs.create("read_number", read.read_number, dtype=numpy.int32)
    raw_group.attrs.create("start_mux", read.pore.well, dtype=numpy.uint8)
    raw_group.attrs["read_id"] = str(read.read_id).encode("utf-8")
    raw_group.attrs.create("median_before", read.median_before, dtype=numpy.float64)

    # Lookup the fast5 enumeration values, which should include "partial: 1"
    # This will ensure that the enumeration is valid on a round-trip
    raw_group.attrs.create(
        "end_reason",
        FAST5_END_REASONS[read.end_reason.name],
        dtype=FAST5_END_REASON_TYPE,
    )

    raw_group.attrs.create(
        "num_minknow_events", read.num_minknow_events, dtype=numpy.uint64
    )

    raw_group.attrs.create(
        "tracked_scaling_scale",
        read.tracked_scaling.scale,
        dtype=numpy.float32,
    )
    raw_group.attrs.create(
        "tracked_scaling_shift",
        read.tracked_scaling.shift,
        dtype=numpy.float32,
    )
    raw_group.attrs.create(
        "predicted_scaling_scale",
        read.predicted_scaling.scale,
        dtype=numpy.float32,
    )
    raw_group.attrs.create(
        "predicted_scaling_shift",
        read.predicted_scaling.shift,
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


def convert_pod5_to_fast5(
    source: Path, dest: Path, read_ids: List[str]
) -> Tuple[int, int]:
    """
    Open a source pod5 file and write the selected read_ids into the destination fast5
    file target.
    """

    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        dest.unlink()

    total_samples = 0

    with p5.Reader(source) as reader:
        with h5py.File(dest, "w") as f5:
            f5.attrs.create(
                "file_version", "3.0".encode("ascii"), dtype=FAST5_STRING_TYPE
            )
            f5.attrs.create(
                "file_type", "multi-read".encode("ascii"), dtype=FAST5_STRING_TYPE
            )

            # Take the chunk of read ids for this file
            for read in reader.reads(
                selection=read_ids, missing_ok=False, preload={"samples"}
            ):
                write_pod5_record_to_fast5(read, f5)

                total_samples += read.num_samples

    return (len(read_ids), total_samples)


def convert_to_fast5(
    inputs: List[Path],
    output: Path,
    recursive: bool = False,
    threads: int = DEFAULT_THREADS,
    force_overwrite: bool = False,
    file_read_count: int = 4000,
):
    if output.exists() and not output.is_dir():
        raise FileExistsError("Cannot output to a file")

    threads = limit_threads(threads)

    with ProcessPoolExecutor(max_workers=threads) as executor:
        total_reads = 0
        futures: Dict[Future, Path] = {}

        # Enumerate over input pod5 files
        for input_idx, source in enumerate(
            collect_inputs(inputs, recursive, "*.pod5", threads=threads)
        ):
            # Open the inputs to read the read ids
            with p5.Reader(source) as reader:
                for chunk_idx, read_ids in enumerate(
                    chunked(reader.read_ids, file_read_count)
                ):
                    dest = (
                        output / f"{source.stem}.{chunk_idx}_{input_idx}.fast5"
                    ).resolve()

                    if dest.exists() and not force_overwrite:
                        raise FileExistsError(
                            "Output path points to an existing file and --force-overwrite not set"
                        )

                    kwargs = {
                        "source": source,
                        "dest": dest,
                        "read_ids": read_ids,
                    }
                    futures[executor.submit(convert_pod5_to_fast5, **kwargs)] = dest  # type: ignore

                total_reads += len(reader.read_ids)

        print(f"Converting pod5s into {len(futures)} fast5 files. Please wait...")

        status = StatusMonitor(file_count=len(inputs))
        status.increment(files_started=len(inputs), read_count=total_reads)

        for idx, future in enumerate(as_completed(futures)):
            (reads_converted, samples_converted) = future.result()

            status.increment(
                files_ended=1,
                sample_count=samples_converted,
                reads_processed=reads_converted,
            )
            status.print_status()

        status.print_status(force=True)

        print("Conversion complete")


def main():
    run_tool(pod5_convert_to_fast5_argparser())


if __name__ == "__main__":
    main()
