"""
Tool for converting fast5 files to the pod5 format
"""

from concurrent.futures import (
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
import datetime
from itertools import islice
import os
import sys
import warnings
from pod5.pod5_types import CompressedRead
from tqdm.auto import tqdm
import uuid
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import h5py
import iso8601
from more_itertools import pairwise
import numpy as np
import vbz_h5py_plugin  # noqa: F401

import pod5 as p5
from pod5.signal_tools import DEFAULT_SIGNAL_CHUNK_SIZE, vbz_compress_signal_chunked
from pod5.tools.parsers import pod5_convert_from_fast5_argparser, run_tool
from pod5.tools.utils import iterate_inputs

READS_PER_CHUNK = 250


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
        self._input_to_output: Dict[Path, Path] = {}
        self._open_writers: Dict[Path, p5.Writer] = {}
        self._closed_writers: Set[Path] = set([])

        if output_root.is_file() and not force_overwrite:
            raise FileExistsError(
                "Output path points to an existing file and --force-overwrite not set"
            )

        if len(output_root.parts) > 1:
            output_root.parent.mkdir(parents=True, exist_ok=True)

    def _open_writer(self, output_path: Path) -> p5.Writer:
        """Get the writer from existing handles or create a new one if unseen"""
        if output_path in self._open_writers:
            return self._open_writers[output_path]

        if output_path in self._closed_writers:
            raise FileExistsError(f"Trying to re-open a closed Writer to {output_path}")

        if output_path.exists() and self._force_overwrite:
            output_path.unlink()

        writer = p5.Writer(output_path)
        self._open_writers[output_path] = writer
        return writer

    def get_writer(self, input_path: Path) -> p5.Writer:
        """Get a Pod5Writer to write data from the input_path"""
        if input_path not in self._input_to_output:

            out_path = self.resolve_output_path(
                path=input_path, root=self.output_root, relative_root=self._one_to_one
            )
            self._input_to_output[input_path] = out_path

        output_path = self._input_to_output[input_path]
        return self._open_writer(output_path=output_path)

    def add_reads(self, input_path: Path, reads: List[CompressedRead]) -> None:
        """Add reads to the writer handle stored for the input_path"""
        writer = self.get_writer(input_path)
        writer.add_reads(reads)

    @staticmethod
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

    def set_input_complete(self, input_path: Path) -> None:
        """Close the Pod5Writer for associated input_path"""
        if not self._one_to_one:
            # Do not close common output file when not in 1-2-1 mode
            return

        if input_path not in self._input_to_output:
            return

        output_path = self._input_to_output[input_path]
        self._open_writers[output_path].close()
        self._closed_writers.add(output_path)
        del self._open_writers[output_path]

    def close_all(self):
        """Close all open writers"""
        for path, writer in self._open_writers.items():
            writer.close()
            del writer
            # Keep track of closed writers to ensure we don't overwrite our own work
            self._closed_writers.add(path)
        self._open_writers = {}

    def __del__(self) -> None:
        self.close_all()


class StatusMonitor:
    """Class for monitoring the status of the conversion"""

    def __init__(self, expected: Dict[Path, int]):
        self.expected: Dict[Path, int] = expected
        self.done: Dict[Path, int] = {path: 0 for path in expected}

        disable_pbar = not bool(int(os.environ.get("POD5_PBAR", 1)))
        self.pbar = tqdm(
            total=self.total_expected,
            ascii=True,
            disable=disable_pbar,
            desc=f"Converting {len(self.expected)} Fast5s",
            unit="Reads",
            leave=True,
            dynamic_ncols=True,
        )

    @property
    def total_files(self) -> int:
        return len(self.expected)

    @property
    def total_expected(self) -> int:
        return sum(self.expected.values())

    def increment(self, path: Path, n: int, expected: int) -> None:
        """Increment the reads status by n and update the number of expected"""
        self.expected[path] -= expected - n
        self.pbar.total = self.total_expected

        self.done[path] += n
        self.pbar.update(n)

    def is_input_done(self, path: Path) -> bool:
        """Returns true if the number of done reads equals the number of expected reads"""
        return self.done[path] >= self.expected[path]

    def write(self, msg: str, file: Any) -> None:
        """Write runtime message to avoid clobbering tqdm pbar"""
        self.pbar.write(msg, file=file)

    def close(self) -> None:
        """Close the progress bar"""
        self.pbar.close()


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


def filter_multi_read_fast5s(paths: Iterable[Path], threads: int) -> List[Path]:
    """Filter an iterable of paths returning only multi-read-fast5s"""
    multi_read_fast5s: List[Path] = []
    bad_paths: List[Path] = []

    paths = list(paths)
    pbar = tqdm(
        desc="Checking Fast5 Files",
        total=len(paths),
        disable=not bool(int(os.environ.get("POD5_PBAR", 1))),
        leave=False,
        ascii=True,
        unit="Files",
        dynamic_ncols=True,
    )

    # Speed up the check with multi-processing. Can't use multi-threading because
    # hdf5 might crash
    with ThreadPoolExecutor(max_workers=threads * 2) as exc:
        futures = {
            exc.submit(is_multi_read_fast5, path): path
            for path in paths
            if path.exists()
        }
        for future in as_completed(futures):
            pbar.update()
            path = futures[future]
            if future.result():
                multi_read_fast5s.append(path)
            else:
                bad_paths.append(path)

    if bad_paths:
        skipped_paths = " ".join(path.name for path in bad_paths)
        warnings.warn(
            f"""
Some inputs are not multi-read fast5 files. Please use the conversion
tools in the nanoporetech/ont_fast5_api project to convert this file to the supported
multi-read fast5 format. These files will be ignored.
Ignored files: \"{skipped_paths}\""""
        )

    pbar.close()
    return multi_read_fast5s


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


def convert_datetime_as_epoch_ms(time_str: Optional[str]) -> datetime.datetime:
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
    context_tags: Dict[str, str],
    device_type: str,
    tracking_id: Dict[str, str],
) -> p5.RunInfo:
    """Create a Pod5RunInfo instance from parsed fast5 data"""
    return p5.RunInfo(
        acquisition_id=acq_id,
        acquisition_start_time=convert_datetime_as_epoch_ms(
            tracking_id["exp_start_time"]
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
        protocol_name=decode_str(tracking_id["exp_script_name"]),
        protocol_run_id=decode_str(tracking_id["protocol_run_id"]),
        protocol_start_time=convert_datetime_as_epoch_ms(
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


def plan_chunks(
    path: Path, reads_per_chunk: int = READS_PER_CHUNK
) -> Tuple[Path, Optional[List[Any]]]:
    """Given a fast5 file, return index ranges for each chunk"""
    with h5py.File(str(path), "r") as _f5:
        ranges = chunked_ranges(len(_f5.keys()), reads_per_chunk)
    return path, ranges


def chunked_ranges(
    count: int, reads_per_chunk: int = READS_PER_CHUNK
) -> Optional[List[Any]]:
    """Create index ranges"""
    if count == 0:
        return None

    if reads_per_chunk <= 0:
        raise ValueError(
            f"reads_per_chunk must be greater than zero got {reads_per_chunk}"
        )

    ranges = list(np.arange(0, count, reads_per_chunk))
    ranges.append(count)
    return list(pairwise(ranges))


def convert_fast5_file(
    fast5_file: Path,
    chunk_range: Tuple[int, int],
    signal_chunk_size: int = DEFAULT_SIGNAL_CHUNK_SIZE,
) -> Tuple[List[CompressedRead], int]:
    """Convert the reads within chunk_range indices"""

    with h5py.File(str(fast5_file), "r") as _f5:

        run_info_cache: Dict[str, p5.RunInfo] = {}
        reads: List[p5.CompressedRead] = []

        start, end = chunk_range
        expected_chunk_count = end - start
        for group_name in islice(_f5.keys(), start, end):
            f5_read = get_read_from_fast5(group_name, _f5)
            if f5_read is None:
                continue

            read = convert_fast5_read(
                f5_read,
                run_info_cache,
                signal_chunk_size=signal_chunk_size,
            )
            reads.append(read)
    return reads, expected_chunk_count


def futures_exception(
    path: Path, future: Future, status: StatusMonitor, strict: bool
) -> bool:
    """Check a conversion future for exceptions and raise if strict"""
    exc = future.exception()

    if exc is None:
        return False

    status.write(f"Error processing: {path}", file=sys.stderr)
    status.write(f"Sub-process trace:\n{exc}", file=sys.stderr)

    if strict:
        status.close()
        raise exc

    return True


def write_converted_reads(
    futures: Dict[Future, Path],
    output_handler: OutputHandler,
    status: StatusMonitor,
    strict: bool,
) -> None:
    """Work through the futures until all work is done"""

    for future in as_completed(futures):
        path = futures[future]

        if futures_exception(path, future, status, strict):
            continue

        result: Tuple[List[CompressedRead], int] = future.result()
        reads, expected_chunk_count = result
        output_handler.add_reads(path, reads)
        status.increment(path, len(reads), expected_chunk_count)

        if status.is_input_done(path):
            output_handler.set_input_complete(path)

    status.close()


def submit_conversion_processes(
    executor: ProcessPoolExecutor,
    fast5s: List[Path],
    signal_chunk_size: int = DEFAULT_SIGNAL_CHUNK_SIZE,
) -> Tuple[Dict[Future, Path], Dict[Path, int]]:

    # Track the number of keys in each input fast5 file
    counts: Dict[Path, int] = {}
    futures: Dict[Future, Path] = {}

    # Plan the chunks (index ranges) that will be visited by each future
    for plan in executor.map(plan_chunks, fast5s):
        path, chunk_ranges = plan

        if not chunk_ranges:
            print(f"{path} skipped - nothing to do")
            continue

        counts[path] = chunk_ranges[-1][1]

        # Foreach index range chunk, spawn a subprocess in this executor
        for chunk_range in chunk_ranges:
            future = executor.submit(
                convert_fast5_file,
                fast5_file=path,
                chunk_range=chunk_range,
                signal_chunk_size=signal_chunk_size,
            )
            futures[future] = path

    return futures, counts


def convert_from_fast5(
    inputs: List[Path],
    output: Path,
    recursive: bool = False,
    threads: int = 10,
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

    output_handler = OutputHandler(output, one_to_one, force_overwrite)

    fast5s = filter_multi_read_fast5s(
        iterate_inputs(inputs, recursive, "*.fast5"), threads=threads
    )

    if not fast5s:
        raise RuntimeError("Found no fast5 files to process - Exiting")

    with ProcessPoolExecutor(max_workers=threads) as executor:

        futures, counts = submit_conversion_processes(
            executor=executor, fast5s=fast5s, signal_chunk_size=signal_chunk_size
        )

        # Start the progress bar with the estimated number of reads
        status = StatusMonitor(counts)

        try:
            write_converted_reads(
                futures,
                output_handler=output_handler,
                status=status,
                strict=strict,
            )

        except Exception as exc:
            status.write(f"An unexpected error occurred: {exc}", file=sys.stderr)
            raise exc

        finally:
            output_handler.close_all()


def main():
    """Main function for pod5_convert_from_fast5"""
    run_tool(pod5_convert_from_fast5_argparser())


if __name__ == "__main__":
    main()
