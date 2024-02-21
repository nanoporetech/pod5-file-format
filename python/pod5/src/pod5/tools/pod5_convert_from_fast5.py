"""
Tool for converting fast5 files to the pod5 format
"""

import datetime
import multiprocessing as mp
from multiprocessing.context import SpawnContext
import sys
import warnings
from pod5.pod5_types import CompressedRead
from tqdm.auto import tqdm
import uuid
from pathlib import Path
from queue import Empty
from typing import (
    Any,
    Collection,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import h5py
import iso8601
import more_itertools
import vbz_h5py_plugin  # noqa: F401

import pod5 as p5
from pod5.signal_tools import DEFAULT_SIGNAL_CHUNK_SIZE, vbz_compress_signal_chunked
from pod5.tools.parsers import pod5_convert_from_fast5_argparser, run_tool
from pod5.tools.utils import (
    DEFAULT_THREADS,
    PBAR_DEFAULTS,
    collect_inputs,
    init_logging,
    limit_threads,
    logged,
    logged_all,
    terminate_processes,
)

READ_CHUNK_SIZE = 400
TIMEOUT_SECONDS = 600


logger = init_logging()


class QueueManager:
    def __init__(
        self,
        context: SpawnContext,
        inputs: Collection[Path],
        threads: int,
        timeout: float,
    ) -> None:
        """Manager for balancing work queues"""
        self._requests_size = threads * 2
        self._inputs: mp.Queue = context.Queue(maxsize=len(inputs))
        self._requests: mp.Queue = context.Queue(maxsize=self._requests_size)
        self._data: mp.Queue = context.Queue()
        self._exceptions: mp.Queue = context.Queue()
        self._timeout = timeout

        self._start(inputs=inputs)

    def _await(self, queue: mp.Queue) -> Any:
        """Await the next item on a queue raising TimeoutError if failing"""
        try:
            item = queue.get(timeout=self._timeout)
            return item
        except Empty:
            logger.fatal("Empty queue or timeout ")
            raise TimeoutError(f"No progress in {self._timeout} seconds - quitting")

    def enqueue_request(self) -> None:
        self._requests.put(None, timeout=self._timeout)

    def await_request(self) -> None:
        """Await a request for data"""
        self._await(self._requests)

    @logged()
    def enqueue_data(
        self, path: Optional[Path], reads: Union[List[CompressedRead], int, None]
    ) -> None:
        """
        Enqueues an input path and either a list of compressed reads to be written, or
        the total count of reads converted for that path.
        Otherwise, if path is None, mark the child process as being empty.
        """
        self._data.put((path, reads), timeout=self._timeout)

    @logged(log_time=True)
    def await_data(
        self,
    ) -> Tuple[Optional[Path], Union[List[CompressedRead], int, None]]:
        """
        Await compressed reads or the total count of reads compressed (file end) for
        a input filepath. Enqueues the next request if necessary
        """
        path, item = self._await(self._data)

        # Check for the exhausted process sentinel value
        if path is None:
            return None, None

        # Add another request if we received compressed reads
        if isinstance(item, List):
            self.enqueue_request()

        return path, item

    @logged(log_args=True)
    def enqueue_exception(self, path: Path, exception: Exception, trace: str) -> None:
        self._exceptions.put((path, exception, trace), timeout=self._timeout)

    def get_exception(self) -> Optional[Tuple[Path, Exception, str]]:
        """Promptly get an exception if any"""
        try:
            # Use short timeout instead of get_nowait as we might call this method
            # very shortly after enqueueing an exception
            path, exc, trace = self._exceptions.get(timeout=0.01)
            logger.exception(f"Encountered an exception in {path} - {exc}")
            if trace:
                logger.exception(f"Trace Exception {path}\n{trace}")
            return path, exc, trace
        except Empty:
            pass
        return None

    @logged(log_args=True)
    def enqueue_input(self, path: Path) -> None:
        """Enqueue a request"""
        self._inputs.put(path)

    @logged_all
    def get_input(self) -> Optional[Path]:
        """Promptly get an input if any returning None if queue is empty"""
        try:
            return self._inputs.get(timeout=0.1)
        except Empty:
            pass
        return None

    @logged(log_return=True)
    def _discard_and_close(self, queue: mp.Queue) -> int:
        """
        Discard all remaining enqueued items and close a queue to nicely shutdown the
        queue. Returns the number of discarded items
        """
        count = 0
        while True:
            try:
                queue.get(timeout=0.1)
                count += 1
            except Exception:
                break
        queue.close()
        queue.join_thread()
        return count

    @logged(log_return=True)
    def shutdown(self) -> Tuple[int, int, int, int]:
        """Shutdown all queues returning the counts of all remaining items"""
        n_inputs = self._discard_and_close(self._inputs)
        n_req = self._discard_and_close(self._requests)
        n_data = self._discard_and_close(self._data)
        n_exc = self._discard_and_close(self._exceptions)

        if n_inputs > 0:
            logger.warn("Unfinished inputs found during shutdown!")
        if n_data > 0:
            logger.warn("Unfinished data found during shutdown!")
        if n_exc > 0:
            logger.warn("Unfinished exceptions found during shutdown!")

        return n_inputs, n_req, n_data, n_exc

    @logged(log_args=True)
    def _start(self, inputs: Iterable[Path]) -> None:
        """Enqueue all inputs for child processes to poll and set the requests size"""
        for path in inputs:
            if path.is_file():
                self.enqueue_input(path)

        for _ in range(self._requests_size):
            self.enqueue_request()


class OutputHandler:
    """Class for managing p5.Writer handles"""

    @logged(log_args=True)
    def __init__(
        self,
        output_root: Path,
        one_to_one: Optional[Path],
        force_overwrite: bool,
    ):
        self.output_root = output_root
        self._one_to_one = one_to_one
        self._force_overwrite = force_overwrite
        self._input_to_output: Dict[Path, Path] = {}
        self._open_writers: Dict[Path, p5.Writer] = {}
        self._closed_writers: Dict[Path, bool] = {}

    @logged_all
    def _open_writer(self, output_path: Path) -> Optional[p5.Writer]:
        """Get the writer from existing handles or create a new one if unseen"""
        if output_path in self._open_writers:
            return self._open_writers[output_path]

        if output_path in self._closed_writers:
            had_exception = self._closed_writers[output_path]
            if had_exception:
                return None
            raise FileExistsError(f"Trying to re-open a closed Writer to {output_path}")

        if output_path.exists() and self._force_overwrite:
            output_path.unlink()

        writer = p5.Writer(output_path)
        self._open_writers[output_path] = writer
        return writer

    @logged_all
    def get_writer(self, input_path: Path) -> Optional[p5.Writer]:
        """Get a Pod5Writer to write data from the input_path"""
        if input_path not in self._input_to_output:
            out_path = self.resolve_output_path(
                path=input_path, root=self.output_root, relative_root=self._one_to_one
            )
            self._input_to_output[input_path] = out_path

        output_path = self._input_to_output[input_path]
        return self._open_writer(output_path=output_path)

    @staticmethod
    @logged_all
    def resolve_one_to_one_path(path: Path, root: Path, relative_root: Path):
        """
        Find the relative path between the input path and the relative root
        """
        try:
            relative = path.with_suffix(".pod5").relative_to(relative_root)
        except ValueError as exc:
            raise RuntimeError(
                f"--one-to-one directory must be a relative parent of "
                f"all input fast5 files. For {path} relative to {relative_root}"
            ) from exc

        # Resolve the new final output path relative to the output directory
        # This path is to a file with the equivalent filename(.pod5)
        return root / relative

    @staticmethod
    @logged_all
    def resolve_output_path(
        path: Path, root: Path, relative_root: Optional[Path]
    ) -> Path:
        """
        Resolve the output path. If relative_root is a path, resolve the relative output
        path under root, otherwise, the output is either root or a new file within root
        if root is a directory
        """
        if relative_root is not None:
            # Resolve the relative path to the one_to_one root path
            out_path = OutputHandler.resolve_one_to_one_path(
                path=path,
                root=root,
                relative_root=relative_root,
            )

            # Create directory structure if needed
            out_path.parent.mkdir(parents=True, exist_ok=True)
            return out_path

        if root.is_dir():
            # If the output path is a directory, the write the default filename
            return root / "output.pod5"

        # The provided output path is assumed to be a named file
        return root

    @logged(log_args=True)
    def set_input_complete(self, input_path: Path, is_exception: bool) -> None:
        """Close the Pod5Writer for associated input_path"""
        if not self._one_to_one:
            # Do not close common output file when not in 1-2-1 mode
            return

        if input_path not in self._input_to_output:
            return

        output_path = self._input_to_output[input_path]
        self._open_writers[output_path].close()
        self._closed_writers[output_path] = is_exception
        del self._open_writers[output_path]

    @logged()
    def close_all(self):
        """Close all open writers"""
        for path, writer in self._open_writers.items():
            try:
                writer.close()
                del writer
                # Keep track of closed writers to ensure we don't overwrite our own work
                self._closed_writers[path] = False
            except Exception as exc:
                logger.debug(f"Failed to cleanly close writer to {path} - {exc}")
        self._open_writers = {}


class StatusMonitor:
    """Class for monitoring the status of the conversion"""

    @logged_all
    def __init__(self, paths: Sequence[Path]):
        # Estimate that a fast5 file will have 4k reads
        self.path_reads = {path: 4000 for path in paths}
        self.count_finished = 0

        self.pbar = tqdm(
            total=self.total_reads,
            desc=f"Converting {len(self.path_reads)} Fast5s",
            unit="Reads",
            leave=True,
            **PBAR_DEFAULTS,
        )

    @property
    def total_files(self) -> int:
        return len(self.path_reads)

    @property
    def total_reads(self) -> int:
        return sum(self.path_reads.values())

    @logged(log_args=True)
    def increment_reads(self, n: int) -> None:
        """Increment the reads status by n"""
        self.pbar.update(n)

    @logged(log_args=True)
    def update_reads_total(self, path: Path, total: int) -> None:
        """Increment the reads status by n and update the total reads"""
        self.path_reads[path] = total
        self.pbar.total = self.total_reads
        self.pbar.refresh()

    @logged(log_args=True)
    def write(self, msg: str, file: Any) -> None:
        """Write runtime message to avoid clobbering tqdm pbar"""
        self.pbar.write(msg, file=file)

    @logged()
    def close(self) -> None:
        """Close the progress bar"""
        self.pbar.close()


@logged_all
def is_multi_read_fast5(path: Path) -> bool:
    """
    Assert that the given path points to a a multi-read fast5 file for which
    direct-to-pod5 conversion is supported.
    """
    try:
        with h5py.File(path) as _h5:
            # The "file_type" attribute might be present on supported multi-read fast5 files.
            if _h5.attrs.get("file_type") == "multi-read":
                return True

            # No keys, assume multi-read but there shouldn't be anything to do which would
            # cause an issue so pass silently
            if len(_h5) == 0:
                return True

            # if there are "read_x" keys, this is a multi-read file
            if any(key for key in _h5 if key.startswith("read_")):
                return True

    except Exception:
        pass

    return False


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


def convert_datetime_as_epoch_ms(
    time_str: Union[str, bytes, None]
) -> datetime.datetime:
    """Convert the fast5 time string to timestamp"""
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    if time_str is None:
        return epoch
    try:
        return iso8601.parse_date(decode_str(time_str))
    except iso8601.iso8601.ParseError:
        return epoch


def convert_run_info(
    acq_id: str,
    adc_max: int,
    adc_min: int,
    sample_rate: int,
    context_tags: Dict[str, Union[str, bytes]],
    device_type: str,
    tracking_id: Dict[str, Union[str, bytes]],
) -> p5.RunInfo:
    """Create a Pod5RunInfo instance from parsed fast5 data"""
    return p5.RunInfo(
        acquisition_id=acq_id,
        acquisition_start_time=convert_datetime_as_epoch_ms(
            tracking_id.get("exp_start_time")
        ),
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
        protocol_name=decode_str(tracking_id.get("exp_script_name", b"")),
        protocol_run_id=decode_str(tracking_id.get("protocol_run_id", b"")),
        protocol_start_time=convert_datetime_as_epoch_ms(
            tracking_id.get("protocol_start_time", None)
        ),
        sample_id=decode_str(tracking_id.get("sample_id", b"")),
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
    channel_id = fast5_read["channel_id"]
    raw = fast5_read["Raw"]

    attrs = fast5_read.attrs

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
        run_info_cache[acq_id] = convert_run_info(
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


def get_read_from_fast5(group_name: str, h5_file: h5py.File) -> Optional[h5py.Group]:
    """Read a group from a h5 file ensuring that it's a read"""
    if not group_name.startswith("read_"):
        return None

    try:
        return h5_file[group_name]
    except KeyError as exc:
        # Observed strange behaviour where h5py reports a KeyError with
        # the message "Unable to open object". Report a failed read as warning
        warnings.warn(
            f"Failed to read key {group_name} from {h5_file.filename} : {exc}",
        )
    return None


def convert_fast5_file_chunk(
    queues: QueueManager,
    handle: h5py.File,
    chunk: Iterable[str],
    cache: Dict[str, p5.RunInfo],
    signal_chunk_size: int,
) -> List[CompressedRead]:
    reads: List[p5.CompressedRead] = []

    # Allow request queue to throttle work
    queues.await_request()
    try:
        for group_name in chunk:
            f5_read = get_read_from_fast5(group_name, handle)
            if f5_read is None:
                continue
            read = convert_fast5_read(f5_read, cache, signal_chunk_size)
            reads.append(read)

    except Exception as exc:
        # Ensures that requests aren't exhausted
        queues.enqueue_request()
        raise exc
    return reads


@logged_all
def convert_fast5_file(
    path: Path,
    queues: QueueManager,
    signal_chunk_size: int = DEFAULT_SIGNAL_CHUNK_SIZE,
) -> int:
    """Convert the reads in a fast5 file"""

    run_info_cache: Dict[str, p5.RunInfo] = {}
    total_reads: int = 0

    with h5py.File(str(path), "r") as _f5:
        for chunk in more_itertools.chunked(_f5.keys(), READ_CHUNK_SIZE):
            reads = convert_fast5_file_chunk(
                queues, _f5, chunk, run_info_cache, signal_chunk_size
            )
            queues.enqueue_data(path, reads)
            total_reads += len(reads)

    return total_reads


@logged()
def issue_not_multi_read_exception(path: Path, queues: QueueManager):
    logger.error(f"Input {path.name} is not a multi-read fast5")
    queues.enqueue_exception(
        path=path,
        exception=TypeError(f"{path} is not a multi-read fast5 file."),
        trace="",
    )
    logger.info(f"Enqueueing file end: {path.name} reads: 0")
    queues.enqueue_data(path, 0)


@logged(log_time=True)
def convert_fast5_files(
    queues: QueueManager,
    signal_chunk_size: int = DEFAULT_SIGNAL_CHUNK_SIZE,
) -> None:
    """
    Main function for converting fast5s available in queues.
    Collections of converted reads are emplaced on the data_queue for writing in
    the main process.
    """
    while True:
        path = queues.get_input()

        if path is None:
            logger.info("Inputs exhausted. Closing Process")
            break

        if not is_multi_read_fast5(path):
            issue_not_multi_read_exception(path, queues)
            continue

        try:
            total_reads = convert_fast5_file(path, queues, signal_chunk_size)
            logger.info(f"Enqueueing file end: {path.name} reads: {total_reads}")
            queues.enqueue_data(path, total_reads)

        except Exception as exc:
            import traceback

            logger.error(f"Enqueueing exception: {path.name} {exc}")
            queues.enqueue_exception(path, exc, traceback.format_exc())

    logger.info("Enqueue sentinel")
    queues.enqueue_data(None, None)


@logged(log_args=True)
def handle_exception(
    exception: Tuple[Path, Exception, str],
    output_handler: OutputHandler,
    status: StatusMonitor,
    strict: bool,
) -> None:
    path, exc, trace = exception
    status.write(str(exc), sys.stderr)
    output_handler.set_input_complete(path, is_exception=True)

    if strict:
        status.close()
        logger.fatal("Exception raised and --strict set")
        logger.debug(f"trace: {trace}")
        raise exc


@logged_all
def process_conversion_tasks(
    queues: QueueManager,
    output_handler: OutputHandler,
    status: StatusMonitor,
    strict: bool,
    threads: int = DEFAULT_THREADS,
) -> None:
    """Work through the queues of data until all work is done"""

    count_complete_processes = 0
    while count_complete_processes < threads:
        # Always poll exceptions to ensure they're handled
        exception = queues.get_exception()
        if exception is not None:
            handle_exception(
                exception=exception,
                output_handler=output_handler,
                status=status,
                strict=strict,
            )
            continue

        path, data = queues.await_data()

        # Handle exhausted processes
        if path is None:
            # Processed finished sentinel
            count_complete_processes += 1
            logger.info(
                f"Got process end sentinel {count_complete_processes} of {threads}"
            )
            continue

        # Update the progress bar with the total number of converted reads in the file
        if isinstance(data, int):
            status.update_reads_total(path, data)
            output_handler.set_input_complete(path, is_exception=False)
            continue

        # Write the incoming list of converted reads
        writer = output_handler.get_writer(path)
        if writer is None:
            logger.warn(
                f"Trying to write to {path} writer which was closed by an exception"
            )
        else:
            logger.info(f"Writing {len(data)} reads to {path.name} using {writer}")
            writer.add_reads(data)
            status.increment_reads(len(data))

    status.close()


@logged(log_time=True)
def convert_from_fast5(
    inputs: List[Path],
    output: Path,
    recursive: bool = False,
    threads: int = DEFAULT_THREADS,
    one_to_one: Optional[Path] = None,
    force_overwrite: bool = False,
    signal_chunk_size: int = DEFAULT_SIGNAL_CHUNK_SIZE,
    strict: bool = False,
) -> None:
    """
    Convert fast5 files found (optionally recursively) at the given input Paths
    into pod5 file(s). If one_to_one is a Path then the new pod5 files are
    created in a new relative directory structure within output relative to the the
    one_to_one Path.
    """

    if output.is_file() and not force_overwrite:
        raise FileExistsError(
            "Output path points to an existing file and --force-overwrite not set"
        )

    if len(output.parts) > 1:
        output.parent.mkdir(parents=True, exist_ok=True)

    threads = limit_threads(threads)

    pending_fast5s = collect_inputs(inputs, recursive, "*.fast5", threads=threads)
    if not pending_fast5s:
        logger.fatal(f"Found no *.fast5 files in inputs: {inputs}")
        raise RuntimeError("Found no fast5 inputs to process - Exiting")

    output_handler = OutputHandler(output, one_to_one, force_overwrite)
    status = StatusMonitor(pending_fast5s)

    threads = min(threads, len(pending_fast5s))
    ctx = mp.get_context("spawn")
    queues = QueueManager(
        context=ctx,
        inputs=pending_fast5s,
        threads=threads,
        timeout=TIMEOUT_SECONDS,
    )

    active_processes = []
    for _ in range(threads):
        process = ctx.Process(
            target=convert_fast5_files,
            args=(queues, signal_chunk_size),
            daemon=True,
        )
        process.start()
        active_processes.append(process)

    try:
        process_conversion_tasks(
            queues=queues,
            output_handler=output_handler,
            status=status,
            strict=strict,
            threads=threads,
        )

        queues.shutdown()
        for proc in active_processes:
            proc.join()
            proc.close()

    except Exception as exc:
        status.write(f"An unexpected error occurred: {exc}", file=sys.stderr)
        terminate_processes(active_processes)
        raise exc

    finally:
        output_handler.close_all()
        logger.disabled = True


def main():
    """Main function for pod5_convert_from_fast5"""
    run_tool(pod5_convert_from_fast5_argparser())


if __name__ == "__main__":
    main()
