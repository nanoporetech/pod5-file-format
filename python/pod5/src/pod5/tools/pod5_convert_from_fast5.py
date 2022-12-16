"""
Tool for converting fast5 files to the pod5 format
"""

import datetime
import multiprocessing as mp
import sys
import time
import uuid
from pathlib import Path
from queue import Empty
from typing import Dict, Iterable, List, NamedTuple, Optional, Union

import h5py
import iso8601
import more_itertools
import vbz_h5py_plugin  # noqa: F401

import pod5 as p5
from pod5.signal_tools import DEFAULT_SIGNAL_CHUNK_SIZE, vbz_compress_signal_chunked
from pod5.tools.parsers import pod5_convert_from_fast5_argparser, run_tool
from pod5.tools.utils import iterate_inputs

READ_CHUNK_SIZE = 100


def assert_multi_read_fast5(h5_file: h5py.File) -> None:
    """
    Assert that the given h5 handle is a multi-read fast5 file for which
    direct-to-pod5 conversion is supported. Raises an AssertionError
    explaining how to correct this issue when unsupported single-read or bulk-read fast5
    files are given.
    """

    # The "file_type" attribute might be present on supported multi-read fast5 files.
    if h5_file.attrs.get("file_type") == "multi-read":
        return

    # No keys, assume multi-read but there shouldn't be anything to do which would
    # cause an issue so pass silently
    if len(h5_file) == 0:
        return

    # if there are "read_x" keys, this is a multi-read file
    if any(key for key in h5_file if key.startswith("read_")):
        return

    raise AssertionError(
        f"The file provided is not a multi-read fast5 file {h5_file.filename}. "
        "Please use the conversion tools in the nanoporetech/ont_fast5_api project "
        "to convert this file to the supported multi-read fast5 format. "
    )


def decode_str(value: Union[str, bytes]) -> str:
    """Decode a h5py utf-8 byte string to python string"""
    if isinstance(value, str):
        return value
    return value.decode("utf-8")


def convert_fast5_end_reason(fast5_end_reason: int) -> p5.EndReason:
    """
    Return an EndReason instance from the given end_reason integer from a fast5 file.
    This will handle the difference between fast5 and pod5 values for this enumeration
    and set the default "forced" value for each fast5 enumeration value.
    """
    # Expected fast5 enumeration:
    # end_reason_dict = {
    #     "unknown": 0,
    #     "partial": 1, <-- Not used in pod5
    #     "mux_change": 2,  <-- Remaining values are offset by +1
    #     "unblock_mux_change": 3,
    #     "data_service_unblock_mux_change": 4,
    #     "signal_positive": 5,
    #     "signal_negative": 6,
    # }

    # (0:unknown | 1:partial) => pod5 (0:unknown)
    if fast5_end_reason < 2:
        return p5.EndReason.from_reason_with_default_forced(p5.EndReasonEnum.UNKNOWN)

    # Resolve the offset in enumeration values between both files
    p5_scaled_end_reason = fast5_end_reason - 1
    return p5.EndReason.from_reason_with_default_forced(
        p5.EndReasonEnum(p5_scaled_end_reason)
    )


def get_datetime_as_epoch_ms(time_str: Optional[str]) -> datetime.datetime:
    """Convert the fast5 time string to timestamp"""
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    if time_str is None:
        return epoch
    try:
        return iso8601.parse_date(decode_str(time_str))
    except iso8601.iso8601.ParseError:
        return epoch


class RequestQItem(NamedTuple):
    """Enqueued to request more reads"""


class ExceptionQItem(NamedTuple):
    """Enqueued to pass exceptions"""

    exception: Exception
    trace: str
    path: Path


class StartFileQItem(NamedTuple):
    """Enqueued to record the start of a file process"""

    read_count: int


class EndFileQItem(NamedTuple):
    """Enqueued to record the end of a file process"""

    file: Path
    read_count: int


class ReadListQItem(NamedTuple):
    """Enqueued to send list of reads to be written"""

    file: Path
    reads: List[p5.CompressedRead]


def create_run_info(
    acq_id: str,
    adc_max: int,
    adc_min: int,
    sample_rate: int,
    context_tags: Dict[str, str],
    device_type: str,
    tracking_id: Dict[str, str],
) -> p5.RunInfo:
    """Create a Pod5RunInfo instance from parsed fast5 data"""
    return p5.RunInfo(
        acquisition_id=acq_id,
        acquisition_start_time=get_datetime_as_epoch_ms(tracking_id["exp_start_time"]),
        adc_max=adc_max,
        adc_min=adc_min,
        context_tags={
            str(key): decode_str(value) for key, value in context_tags.items()
        },
        experiment_name="",
        flow_cell_id=decode_str(tracking_id.get("flow_cell_id", b"")),
        flow_cell_product_code=decode_str(
            tracking_id.get("flow_cell_product_code", b"")
        ),
        protocol_name=decode_str(tracking_id["exp_script_name"]),
        protocol_run_id=decode_str(tracking_id["protocol_run_id"]),
        protocol_start_time=get_datetime_as_epoch_ms(
            tracking_id.get("protocol_start_time", None)
        ),
        sample_id=decode_str(tracking_id["sample_id"]),
        sample_rate=sample_rate,
        sequencing_kit=decode_str(context_tags.get("sequencing_kit", b"")),
        sequencer_position=decode_str(tracking_id.get("device_id", b"")),
        sequencer_position_type=decode_str(tracking_id.get("device_type", device_type)),
        software="python-pod5-converter",
        system_name=decode_str(tracking_id.get("host_product_serial_number", b"")),
        system_type=decode_str(tracking_id.get("host_product_code", b"")),
        tracking_id={str(key): decode_str(value) for key, value in tracking_id.items()},
    )


def convert_fast5_read(
    fast5_read: h5py.Group,
    run_info_cache: Dict[str, p5.RunInfo],
    signal_chunk_size: int = DEFAULT_SIGNAL_CHUNK_SIZE,
) -> p5.CompressedRead:
    """
    Given a fast5 read parsed from a fast5 file, return a pod5.Read object.
    """
    attrs = fast5_read.attrs
    try:
        channel_id = fast5_read["channel_id"]
        raw = fast5_read["Raw"]
    except KeyError as exc:
        # A KeyError here could be caused by users attempting to convert unsupported
        # single-read fast5 files.
        err = (
            "Supplied hdf5 group doesn't appear to be from a supported fast5 file. "
            f"Expected 'channel_id' and 'Raw' keys but found: {fast5_read.keys()}. "
            "Please ensure that single-read fast5s are converted to multi-read fast5s "
            "using tools available at nanoporetech/ont_fast5_api before conversion "
            "to pod5."
        )
        raise TypeError(err) from exc

    # Get the acquisition id
    if "run_id" in attrs:
        acq_id = decode_str(attrs["run_id"])
    else:
        acq_id = decode_str(fast5_read["tracking_id"].attrs["run_id"])

    # Create new run_info if we've not seen this acquisition id before
    if acq_id not in run_info_cache:
        adc_min = 0
        adc_max = 2047
        device_type_guess = "promethion"
        if channel_id.attrs["digitisation"] == 8192:
            adc_min = -4096
            adc_max = 4095
            device_type_guess = "minion"

        # Add new run_info to cache
        run_info_cache[acq_id] = create_run_info(
            acq_id=acq_id,
            adc_max=adc_max,
            adc_min=adc_min,
            sample_rate=int(channel_id.attrs["sampling_rate"]),
            context_tags=dict(fast5_read["context_tags"].attrs),
            device_type=device_type_guess,
            tracking_id=dict(fast5_read["tracking_id"].attrs),
        )

    # Process attributes unique to this read
    read_id = uuid.UUID(decode_str(raw.attrs["read_id"]))
    pore = p5.Pore(
        channel=int(channel_id.attrs["channel_number"]),
        well=raw.attrs["start_mux"],
        pore_type=decode_str(attrs.get("pore_type", b"not_set")),
    )
    calibration = p5.Calibration.from_range(
        offset=channel_id.attrs["offset"],
        adc_range=channel_id.attrs["range"],
        digitisation=channel_id.attrs["digitisation"],
    )

    end_reason = convert_fast5_end_reason(raw.attrs.get("end_reason", 0))

    # Signal conversion process
    signal = raw["Signal"][()]
    signal_chunks, signal_chunk_lengths = vbz_compress_signal_chunked(
        signal, signal_chunk_size
    )

    return p5.CompressedRead(
        read_id=read_id,
        pore=pore,
        calibration=calibration,
        read_number=raw.attrs["read_number"],
        start_sample=raw.attrs["start_time"],
        median_before=raw.attrs["median_before"],
        num_minknow_events=raw.attrs.get("num_minknow_events", 0),
        tracked_scaling=p5.pod5_types.ShiftScalePair(
            raw.attrs.get("tracked_scaling_shift", float("nan")),
            raw.attrs.get("tracked_scaling_scale", float("nan")),
        ),
        predicted_scaling=p5.pod5_types.ShiftScalePair(
            raw.attrs.get("predicted_scaling_shift", float("nan")),
            raw.attrs.get("predicted_scaling_scale", float("nan")),
        ),
        num_reads_since_mux_change=raw.attrs.get("num_reads_since_mux_change", 0),
        time_since_mux_change=raw.attrs.get("time_since_mux_change", 0.0),
        end_reason=end_reason,
        run_info=run_info_cache[acq_id],
        signal_chunks=signal_chunks,
        signal_chunk_lengths=signal_chunk_lengths,
    )


def await_request(request_queue: mp.Queue):
    """Wait for the next request"""
    while True:
        try:
            request_queue.get(timeout=1)
            return
        except Empty:
            continue


def get_reads_from_files(
    request_queue: mp.Queue,
    data_queue: mp.Queue,
    fast5_files: Iterable[Path],
    signal_chunk_size: int = DEFAULT_SIGNAL_CHUNK_SIZE,
) -> None:
    """
    Main function for converting an iterable of fast5 files.
    The collection of pod5 reads is emplaced on the data_queue for writing in
    the main process.
    """
    for fast5_file in fast5_files:
        count_reads_sent = 0
        try:
            with h5py.File(str(fast5_file), "r") as _f5:

                assert_multi_read_fast5(_f5)

                data_queue.put(StartFileQItem(len(_f5.keys())))

                run_info_cache: Dict[str, p5.RunInfo] = {}
                for read_ids in more_itertools.chunked(_f5.keys(), READ_CHUNK_SIZE):

                    # Allow the out queue to throttle us back if we are too far ahead.
                    await_request(request_queue)

                    reads: List[p5.CompressedRead] = []
                    for read_id in read_ids:
                        read = convert_fast5_read(
                            _f5[read_id],
                            run_info_cache,
                            signal_chunk_size=signal_chunk_size,
                        )
                        reads.append(read)

                    count_reads_sent += len(reads)
                    data_queue.put(ReadListQItem(fast5_file, reads))

        except Exception as exc:
            import traceback

            data_queue.put(ExceptionQItem(exc, traceback.format_exc(), fast5_file))

        data_queue.put(EndFileQItem(fast5_file, count_reads_sent))


class OutputHandler:
    """Class for managing p5.Writer handles"""

    def __init__(
        self,
        output_root: Path,
        one_to_one: Optional[Path],
        force_overwrite: bool,
    ):
        self.output_root = output_root
        self._one_to_one = one_to_one
        self._force_overwrite = force_overwrite
        self._input_to_output_path: Dict[Path, Path] = {}
        self._output_files: Dict[Path, p5.Writer] = {}

    def _open_writer(self, output_path: Path) -> p5.Writer:
        """Get the writer from existing handles or create a new one if unseen"""
        if output_path in self._output_files:
            return self._output_files[output_path]

        if output_path.exists() and self._force_overwrite:
            output_path.unlink()

        writer = p5.Writer(output_path)
        self._output_files[output_path] = writer
        return writer

    def get_writer(self, input_path: Path) -> p5.Writer:
        """Get a Pod5Writer to write data from the input_path"""
        if input_path not in self._input_to_output_path:
            if self._one_to_one is not None:

                try:
                    # find the relative path between the input fast5 file and the
                    # output-one-to-one root
                    relative = input_path.with_suffix(".pod5").relative_to(
                        self._one_to_one
                    )
                except ValueError as exc:
                    raise RuntimeError(
                        f"--output-one-to-one directory must be a relative parent of "
                        f"all input fast5 files. For {input_path} "
                        f"relative to {self._one_to_one}"
                    ) from exc

                # Resolve the new final output path relative to the output directory
                # This path is to a file with the equivalent filename(.pod5)
                out_path = self.output_root / relative

                # Create directory structure if needed
                out_path.parent.mkdir(parents=True, exist_ok=True)
            else:
                if self.output_root.is_dir():
                    # If the output path is a directory, the write the default filename
                    out_path = self.output_root / "output.pod5"
                else:
                    # The provided output path is assumed to be a named file
                    out_path = self.output_root

            self._input_to_output_path[input_path] = out_path

        output_path = self._input_to_output_path[input_path]
        return self._open_writer(output_path=output_path)

    def set_input_complete(self, input_path: Path) -> None:
        """Close the Pod5Writer for associated input_path"""
        if not self._one_to_one:
            return

        if input_path not in self._input_to_output_path:
            return

        output_path = self._input_to_output_path[input_path]
        self._output_files[output_path].close()
        del self._output_files[output_path]

    def close_all(self):
        """Close all open writers"""
        for writer in self._output_files.values():
            writer.close()
            del writer
        self._output_files = {}


class StatusMonitor:
    """Class for monitoring the status / progress of the conversion"""

    def __init__(self, file_count: int):
        self.update_interval = 15

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

    def check_all_reads_processed(self) -> None:
        """Check that all reads have been processed"""
        if self.reads_processed != self.read_count:
            error_message = "!!! Some reads count not be converted due to errors !!!"
            print(error_message, file=sys.stderr)


def convert_from_fast5(
    inputs: List[Path],
    output: Path,
    recursive: bool = False,
    threads: int = 10,
    output_one_to_one: Optional[Path] = None,
    force_overwrite: bool = False,
    signal_chunk_size: int = DEFAULT_SIGNAL_CHUNK_SIZE,
) -> None:
    """
    Convert fast5 files found (optionally recursively) at the given input Paths
    into pod5 file(s). If output_one_to_one is a Path then the new pod5 files are
    created in a new relative directory structure within output relative to the the
    output_one_to_one Path.
    """

    if output.is_file() and not force_overwrite:
        raise FileExistsError(
            "Output path points to an existing file and --force-overwrite not set"
        )

    if len(output.parts) > 1:
        output.parent.mkdir(parents=True, exist_ok=True)

    output_handler = OutputHandler(output, output_one_to_one, force_overwrite)

    ctx = mp.get_context("spawn")
    request_queue: mp.Queue = ctx.Queue()
    data_queue: mp.Queue = ctx.Queue()

    # Divide up files between readers:
    pending_files = list(iterate_inputs(inputs, recursive, "*.fast5"))
    active_processes = []

    if not pending_files:
        raise RuntimeError("Found no fast5 inputs to process - Exiting")

    print(f"Converting {len(pending_files)} fast5 files.. ")

    threads = min(threads, len(pending_files))

    # Create equally sized lists of files to process by each process
    for fast5s in more_itertools.distribute(threads, pending_files):

        # spawn a new process to begin converting fast5 files
        process = ctx.Process(
            target=get_reads_from_files,
            args=(
                request_queue,
                data_queue,
                fast5s,
                signal_chunk_size,
            ),
        )
        process.start()
        active_processes.append(process)

    # start requests for reads, we probably don't need more reads in memory at a time
    for _ in range(threads * 3):
        request_queue.put(RequestQItem())

    status = StatusMonitor(len(pending_files))

    try:
        while status.running:
            status.print_status()

            try:
                item = data_queue.get(timeout=0.5)
            except Empty:
                continue

            if isinstance(item, ReadListQItem):
                # Write the incoming list of converted reads
                writer = output_handler.get_writer(item.file)
                writer.add_reads(item.reads)

                sample_count = sum(r.sample_count for r in item.reads)
                status.increment(
                    reads_processed=len(item.reads), sample_count=sample_count
                )

                # Inform the input queues we can handle another read now:
                request_queue.put(RequestQItem())

            elif isinstance(item, StartFileQItem):
                status.increment(files_started=1, read_count=item.read_count)

            elif isinstance(item, EndFileQItem):
                output_handler.set_input_complete(item.file)
                status.increment(files_ended=1)

            elif isinstance(item, ExceptionQItem):
                print(f"Error processing {item.path}\n", file=sys.stderr)
                print(f"Sub-process trace:\n{item.trace}", file=sys.stderr)
                raise RuntimeError from item.exception

        # Finished running
        status.print_status(force=True)
        status.check_all_reads_processed()

        print(f"Conversion complete: {status.sample_count} samples")

        for proc in active_processes:
            proc.join()
            proc.close()

    except Exception as exc:
        print(f"An unexpected error occurred: {exc}", file=sys.stderr)

        # Kill all child processes if anything has gone wrong
        for proc in active_processes:
            try:
                proc.terminate()
            except ValueError:
                # Catch ValueError raised if proc is already closed
                pass
        raise exc
    finally:
        output_handler.close_all()


def main():
    """Main function for pod5_convert_from_fast5"""
    run_tool(pod5_convert_from_fast5_argparser())


if __name__ == "__main__":
    main()
