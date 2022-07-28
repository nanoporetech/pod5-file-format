"""
Tool for converting fast5 files to the pod5 format
"""

import argparse
import datetime
from pathlib import Path
import sys
import time
import typing
import uuid
import multiprocessing as mp
from queue import Empty

import h5py
import iso8601
import numpy
import more_itertools
from ont_fast5_api.compression_settings import register_plugin

import pod5_format as p5
from pod5_format.reader_utils import make_split_filename

from pod5_format_tools.utils import iterate_inputs


register_plugin()

READ_CHUNK_SIZE = 100


def pod5_convert_from_fast5_argparser() -> argparse.ArgumentParser:
    """
    Create an argument parser for the pod5 convert-from-fast5 tool
    """
    parser = argparse.ArgumentParser("Convert a fast5 file into an pod5 file")

    parser.add_argument("input", type=Path, nargs="+", help="Input path for fast5 file")
    parser.add_argument("output", type=Path, help="Output path for the pod5 file(s)")
    parser.add_argument(
        "-r",
        "--recursive",
        default=False,
        action="store_true",
        help="Search for input files recursively",
    )
    parser.add_argument(
        "-p",
        "--processes",
        default=10,
        type=int,
        help="Set the number of processes to use",
    )
    parser.add_argument(
        "--output-one-to-one",
        action="store_true",
        help="Output files should be 1:1 with input files, written as children of [output] argument.",
    )
    parser.add_argument(
        "--output-split",
        action="store_true",
        help="Output files should use the pod5 split format.",
    )
    parser.add_argument(
        "--force-overwrite", action="store_true", help="Overwrite destination files"
    )
    parser.add_argument(
        "--signal-chunk-size",
        default=102400,
        help="Chunk size to use for signal data set",
    )

    return parser


def h5py_get_str(value):
    if isinstance(value, str):
        return value
    return value.decode("utf-8")


def find_end_reason(end_reason: int) -> p5.EndReason:
    """Return a Pod5EndReason instance from the given end_reason integer"""
    if end_reason is not None:
        if end_reason == 2:
            return p5.EndReason(name=p5.EndReasonEnum.MUX_CHANGE, forced=True)
        if end_reason == 3:
            return p5.EndReason(name=p5.EndReasonEnum.UNBLOCK_MUX_CHANGE, forced=True)
        if end_reason == 4:
            return p5.EndReason(
                name=p5.EndReasonEnum.DATA_SERVICE_UNBLOCK_MUX_CHANGE, forced=True
            )
        if end_reason == 5:
            return p5.EndReason(name=p5.EndReasonEnum.SIGNAL_POSITIVE, forced=False)
        if end_reason == 6:
            return p5.EndReason(name=p5.EndReasonEnum.SIGNAL_NEGATIVE, forced=False)
    return p5.EndReason(name=p5.EndReasonEnum.UNKNOWN, forced=False)


def get_datetime_as_epoch_ms(time_str: typing.Optional[str]) -> datetime.datetime:
    """Convert the fast5 time string to timestamp"""
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    if time_str is None:
        return epoch
    try:
        return iso8601.parse_date(h5py_get_str(time_str))
    except iso8601.iso8601.ParseError:
        return epoch


class RequestQItem(typing.NamedTuple):
    """Enqueued to request more reads"""


class StartFileQItem(typing.NamedTuple):
    """Enqueued to record the start of a file process"""

    read_count: int


class EndFileQItem(typing.NamedTuple):
    """Enqueued to record the end of a file process"""

    file: Path
    read_count: int


class ReadListQItem(typing.NamedTuple):
    """Enqueued to send list of reads to be written"""

    file: Path
    reads: typing.List[p5.Read]


def create_run_info(
    acq_id: str,
    adc_max: int,
    adc_min: int,
    sample_rate: int,
    context_tags: typing.Dict[str, typing.Any],
    device_type: str,
    tracking_id: typing.Dict[str, typing.Any],
) -> p5.RunInfo:
    """Create a Pod5RunInfo instance from parsed fast5 data"""
    return p5.RunInfo(
        acquisition_id=acq_id,
        acquisition_start_time=get_datetime_as_epoch_ms(tracking_id["exp_start_time"]),
        adc_max=adc_max,
        adc_min=adc_min,
        context_tags=context_tags,
        experiment_name="",
        flow_cell_id=h5py_get_str(tracking_id.get("flow_cell_id", b"")),
        flow_cell_product_code=h5py_get_str(
            tracking_id.get("flow_cell_product_code", b"")
        ),
        protocol_name=h5py_get_str(tracking_id["exp_script_name"]),
        protocol_run_id=h5py_get_str(tracking_id["protocol_run_id"]),
        protocol_start_time=get_datetime_as_epoch_ms(
            tracking_id.get("protocol_start_time", None)
        ),
        sample_id=h5py_get_str(tracking_id["sample_id"]),
        sample_rate=sample_rate,
        sequencing_kit=h5py_get_str(context_tags.get("sequencing_kit", b"")),
        sequencer_position=h5py_get_str(tracking_id.get("device_id", b"")),
        sequencer_position_type=h5py_get_str(
            tracking_id.get("device_type", device_type)
        ),
        software="python-pod5-converter",
        system_name=h5py_get_str(tracking_id.get("host_product_serial_number", b"")),
        system_type=h5py_get_str(tracking_id.get("host_product_code", b"")),
        tracking_id=tracking_id,
    )


def convert_fast5_read(
    fast5_read: h5py.Group,
    run_info_cache: typing.Dict[str, p5.RunInfo],
    pre_compress_signal: bool,
    signal_chunk_size: int,
) -> p5.Read:
    """
    Given a fast5 read parsed from a fast5 file, return a pod5_format.Read object.
    """
    attrs = fast5_read.attrs
    channel_id = fast5_read["channel_id"]
    raw = fast5_read["Raw"]

    # Get the acquisition id
    if "run_id" in attrs:
        acq_id = h5py_get_str(attrs["run_id"])
    else:
        acq_id = h5py_get_str(fast5_read["tracking_id"].attrs["run_id"])

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
    read_id = uuid.UUID(h5py_get_str(raw.attrs["read_id"]))
    pore = p5.Pore(
        channel=int(channel_id.attrs["channel_number"]),
        well=raw.attrs["start_mux"],
        pore_type=h5py_get_str(attrs.get("pore_type", b"not_set")),
    )
    calibration = p5.Calibration.from_range(
        offset=channel_id.attrs["offset"],
        adc_range=channel_id.attrs["range"],
        digitisation=channel_id.attrs["digitisation"],
    )
    end_reason = find_end_reason(
        raw.attrs["end_reason"] if "end_reason" in raw.attrs else None
    )

    # Signal conversion process
    signal = raw["Signal"][()]
    sample_count = signal.shape[0]
    signal_arr = signal

    # Compress chunks of the signal if required
    if pre_compress_signal:
        sample_count = []
        signal_arr = []

        # Take slice views of the signal ndarray (non-copying)
        for slice_index in range(0, len(signal), signal_chunk_size):
            signal_slice = signal[slice_index : slice_index + signal_chunk_size]
            signal_arr.append(p5.signal_tools.vbz_compress_signal(signal_slice))
            sample_count.append(len(signal_slice))

    return p5.Read(
        read_id=read_id,
        pore=pore,
        calibration=calibration,
        read_number=raw.attrs["read_number"],
        start_time=raw.attrs["start_time"],
        median_before=raw.attrs["median_before"],
        end_reason=end_reason,
        run_info=run_info_cache[acq_id],
        signal=signal_arr,
        samples_count=sample_count,
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
    fast5_files: typing.Iterable[Path],
    pre_compress_signal: bool,
    signal_chunk_size: int,
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
                data_queue.put(StartFileQItem(len(_f5.keys())))

                run_info_cache: typing.Dict[str, p5.RunInfo] = {}
                for read_ids in more_itertools.chunked(_f5.keys(), READ_CHUNK_SIZE):

                    # Allow the out queue to throttle us back if we are too far ahead.
                    await_request(request_queue)

                    reads: typing.List[p5.Read] = []
                    for read_id in read_ids:
                        read = convert_fast5_read(
                            _f5[read_id],
                            run_info_cache,
                            pre_compress_signal=pre_compress_signal,
                            signal_chunk_size=signal_chunk_size,
                        )
                        reads.append(read)

                    count_reads_sent += len(reads)
                    data_queue.put(ReadListQItem(fast5_file, reads))

        except Exception as exc:
            import traceback

            traceback.print_exc()
            print(f"Error in file {fast5_file}: {exc}", file=sys.stderr)

        data_queue.put(EndFileQItem(fast5_file, count_reads_sent))


def add_reads(
    writer: p5.Writer,
    reads: typing.Iterable[p5.Read],
    pre_compressed_signal: bool,
):
    """
    Write an iterable of Reads to Writer
    """
    pore_types = numpy.array(
        [writer.find_pore(r.pore)[0] for r in reads], dtype=numpy.int16
    )
    calib_types = numpy.array(
        [writer.find_calibration(r.calibration)[0] for r in reads],
        dtype=numpy.int16,
    )
    end_reason_types = numpy.array(
        [writer.find_end_reason(r.end_reason)[0] for r in reads],
        dtype=numpy.int16,
    )
    run_info_types = numpy.array(
        [writer.find_run_info(r.run_info)[0] for r in reads], dtype=numpy.int16
    )

    writer.add_reads(
        numpy.array(
            [numpy.frombuffer(r.read_id.bytes, dtype=numpy.uint8) for r in reads]
        ),
        pore_types,
        calib_types,
        numpy.array([r.read_number for r in reads], dtype=numpy.uint32),
        numpy.array([r.start_time for r in reads], dtype=numpy.uint64),
        numpy.array([r.median_before for r in reads], dtype=numpy.float32),
        end_reason_types,
        run_info_types,
        # Pass an array of arrays here, as we have pre compressed data
        # top level array is per read, then the sub arrays are chunks within the reads.
        # the two arrays here should have the same dimensions, first contains compressed
        # sample array, the second contains the sample counts
        [r.signal for r in reads],
        [numpy.array(r.samples_count, dtype=numpy.uint64) for r in reads],
        pre_compressed_signal=pre_compressed_signal,
    )


class OutputHandler:
    """Class for managing p5.Writer handles"""

    def __init__(
        self,
        output_root: Path,
        one_to_one: bool,
        output_split: bool,
        force_overwrite: bool,
    ):
        self.output_root = output_root
        self._one_to_one = one_to_one
        self._output_split = output_split
        self._force_overwrite = force_overwrite
        self._input_to_output_path: typing.Dict[Path, Path] = {}
        self._output_files: typing.Dict[Path, p5.Writer] = {}

    def _open_writer(self, output_path: Path) -> p5.Writer:
        """Get the writer from existing handles or create a new one if unseen"""
        if output_path in self._output_files:
            return self._output_files[output_path]

        if self._output_split:
            signal_path, reads_path = make_split_filename(output_path)
            for path in [signal_path, reads_path]:
                if self._force_overwrite:
                    path.unlink(missing_ok=True)
            writer = p5.Writer.open_split(signal_path, reads_path)
        else:
            if self._force_overwrite:
                output_path.unlink(missing_ok=True)
            writer = p5.Writer.open_combined(output_path)

        self._output_files[output_path] = writer
        return writer

    def get_writer(self, input_path: Path) -> p5.Writer:
        """Get a Pod5Writer to write data from the input_path"""
        if input_path not in self._input_to_output_path:
            if self._one_to_one:
                out_path = self.output_root / input_path.with_suffix(".pod5").name
            else:
                out_path = self.output_root / "output.pod5"
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
                f"{self.files_ended}/{self.files_started}/{self.file_count} files\t"
                f"{self.reads_processed}/{self.read_count} reads, ",
                f"{self.formatted_sample_count}, " f"{self.sample_rate:.1f} MB/s",
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


def main():
    """Main function for pod5_convert_from_fast5"""
    parser = pod5_convert_from_fast5_argparser()
    args = parser.parse_args()

    if args.output.exists() and args.output.is_file():
        raise FileExistsError("Invalid output location - already exists as file")

    args.output.mkdir(parents=True, exist_ok=True)
    output_handler = OutputHandler(
        args.output, args.output_one_to_one, args.output_split, args.force_overwrite
    )

    ctx = mp.get_context("spawn")
    request_queue: mp.Queue = ctx.Queue()
    data_queue: mp.Queue = ctx.Queue()

    # Always writing compressed files right now.
    pre_compress_signal = True

    # Divide up files between readers:
    pending_files = list(iterate_inputs(args.input, args.recursive, "*.fast5"))
    active_processes: typing.List[mp.Process] = []

    # Create equally sized lists of files to process by each process
    for fast5s in map(list, more_itertools.distribute(args.processes, pending_files)):

        # Skip empty lists if there are more processes than files
        if not fast5s:
            continue

        # spawn a new process to begin converting fast5 files
        process = ctx.Process(
            target=get_reads_from_files,
            args=(
                request_queue,
                data_queue,
                fast5s,
                pre_compress_signal,
                args.signal_chunk_size,
            ),
        )
        process.start()
        active_processes.append(process)

    # start requests for reads, we probably dont need more reads in memory at a time
    for _ in range(args.processes * 3):
        request_queue.put(RequestQItem())

    print("Converting reads...")
    status = StatusMonitor(len(pending_files))

    try:
        while status.running:
            status.print_status()

            try:
                item = data_queue.get(timeout=0.5)
            except Empty:
                continue

            if isinstance(item, ReadListQItem):
                # Write the incomming list of converted reads
                writer = output_handler.get_writer(item.file)
                add_reads(writer, item.reads, pre_compress_signal)

                # samples_count here is a list due to the compression implementation;
                # this will be changed
                sample_count = sum(sum(r.samples_count) for r in item.reads)
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


if __name__ == "__main__":
    main()
