import codecs
import multiprocessing as mp
from multiprocessing.context import SpawnProcess
from multiprocessing.synchronize import Lock
import os
from pathlib import Path
from queue import Empty
import sys
from typing import Dict, Generator, List, NamedTuple, Optional, Set, Tuple

import polars as pl

import pod5 as p5
from pod5.tools.parsers import prepare_pod5_view_argparser, run_tool
from pod5.tools.polars_utils import pl_format_empty_string, pl_format_read_id
from pod5.tools.utils import (
    DEFAULT_THREADS,
    collect_inputs,
    init_logging,
    limit_threads,
    logged,
    logged_all,
    terminate_processes,
)


logger = init_logging()

pl.enable_string_cache(True)


class Field(NamedTuple):
    """Container class for storing the polars expression for a named field"""

    expr: pl.Expr
    docs: str


# This dict defines the order of the fields
FIELDS: Dict[str, Field] = {
    "read_id": Field(pl.col("read_id"), "Read UUID"),
    "filename": Field(pl.col("filename"), "Source pod5 filename"),
    "read_number": Field(pl.col("read_number"), "Read number"),
    "channel": Field(pl.col("channel"), "1-indexed channel"),
    "mux": Field(pl.col("mux"), "1-indexed well"),
    "end_reason": Field(pl.col("end_reason"), "End reason string"),
    "start_time": Field(
        pl.col("start_time"),
        "Seconds since the run start to the first sample of this read",
    ),
    "start_sample": Field(
        pl.col("start").alias("start_sample"),
        "Samples recorded on this channel since run start to the first sample of this read",
    ),
    "duration": Field(pl.col("duration"), "Seconds of sampling for this read"),
    "num_samples": Field(pl.col("num_samples"), "Number of signal samples"),
    "minknow_events": Field(
        pl.col("minknow_events"),
        "Number of minknow events that this read contains",
    ),
    "sample_rate": Field(
        pl.col("sample_rate"), "Number of samples recorded each second"
    ),
    "median_before": Field(
        pl.col("median_before"), "Current level in this well before the read"
    ),
    "predicted_scaling_scale": Field(
        pl.col("predicted_scaling_scale"), "Scale for predicted read scaling"
    ),
    "predicted_scaling_shift": Field(
        pl.col("predicted_scaling_shift"), "Shift for predicted read scaling"
    ),
    "tracked_scaling_scale": Field(
        pl.col("tracked_scaling_scale"), "Scale for tracked read scaling"
    ),
    "tracked_scaling_shift": Field(
        pl.col("tracked_scaling_shift"), "Shift for tracked read scaling"
    ),
    "num_reads_since_mux_change": Field(
        pl.col("num_reads_since_mux_change"),
        "Number of selected reads since the last mux change on this channel",
    ),
    "time_since_mux_change": Field(
        pl.col("time_since_mux_change"),
        "Seconds since the last mux change on this channel",
    ),
    "run_id": Field(pl.col("protocol_run_id").alias("run_id"), "Run UUID"),
    "sample_id": Field(pl.col("sample_id"), "User-supplied name for the sample"),
    "experiment_id": Field(
        pl.col("experiment_id"), "User-supplied name for the experiment"
    ),
    "flow_cell_id": Field(pl.col("flow_cell_id"), "The flow cell id"),
    "pore_type": Field(pl.col("pore_type"), "Name of the pore in this well"),
}


@logged()
def print_fields():
    """Print a list of the available columns"""
    for name, field in FIELDS.items():
        print(f"{name.ljust(28)} {field.docs}")
    print("")


@logged_all
def get_field_or_raise(key: str) -> Field:
    """Get the Field for this key or raise a KeyError"""
    try:
        return FIELDS[key]
    except KeyError:
        raise KeyError(
            f"Field: '{key}' did not match any known fields. "
            "Please check it exists by viewing `-L/--list-fields`"
        )


@logged_all
def select_fields(
    *,
    group_read_id: bool = False,
    include: Optional[str] = None,
    exclude: Optional[str] = None,
) -> Set[str]:
    """Select fields to write"""
    selected: Set[str] = set([])

    # Select only read ids
    if group_read_id:
        selected.add("read_id")
        return selected

    if include:
        for key in include.split(","):
            key = key.strip()
            if not key:
                continue
            get_field_or_raise(key)
            selected.add(key)

    # Default selection - All fields
    if not selected:
        selected.update(FIELDS)

    if exclude:
        for key in exclude.split(","):
            key = key.strip()
            if not key:
                continue
            get_field_or_raise(key)
            try:
                selected.remove(key)
            except KeyError:
                pass

    if not selected:
        raise RuntimeError("Zero Fields selected. Please select at least one field")

    return selected


def format_view_table(
    lazyframe: pl.LazyFrame, path: Path, selected_fields: Set[str]
) -> pl.LazyFrame:
    """Format the view table based on the selected fields"""
    maybe_empty = ["experiment_id", "protocol_run_id", "sample_id", "flow_cell_id"]

    lazyframe = lazyframe.with_columns(
        # Add the source filename
        pl.lit(path.name).alias("filename"),
        pl_format_read_id(pl.col("read_id")),
        # Rename fields to better match legacy sequencing summary
        (pl.col("well").alias("mux")),
        (pl.col("num_minknow_events").alias("minknow_events")),
        pl.col("experiment_name").alias("experiment_id"),
        # Compute the start_time in seconds
        (pl.col("start") / pl.col("sample_rate")).alias("start_time"),
        # Compute the duration of the read in seconds
        (pl.col("num_samples") / pl.col("sample_rate")).alias("duration"),
    )

    # Replace potentially empty fields with "not_set"
    # This can't be done in the above expression due to the behaviour of
    # keep_name()
    lazyframe = lazyframe.with_columns(
        pl_format_empty_string(pl.col(maybe_empty), "not_set").keep_name()
    )

    # Apply the field selection
    lazyframe = lazyframe.select(
        field.expr for key, field in FIELDS.items() if key in selected_fields
    )

    return lazyframe


@logged(log_time=True)
def write(
    ldf: pl.LazyFrame,
    output: Optional[Path],
    separator: str = "\t",
) -> None:
    """Write the polars.LazyFrame"""

    kwargs = dict(
        has_header=False, separator=separator, null_value="", float_precision=8
    )

    # Write to the nominated output path
    if output is not None:
        with output.open("ab") as f:
            ldf.collect().write_csv(f, **kwargs)
        return

    # No output path, collect the table content as a string and print it to stdout
    content = ldf.collect().write_csv(**kwargs)
    try:
        # Do not add additional newline at the end, this ensures consistency with
        # writing to file
        print(content, end="")
    except BrokenPipeError as exc:
        # https://docs.python.org/3/library/signal.html#note-on-sigpipe
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        raise exc


def write_header(
    output: Optional[Path], selected: Set[str], separator: str = "\t"
) -> None:
    """Write the header line"""
    header = separator.join(key for key in FIELDS if key in selected)
    if output is None:
        print(header, file=sys.stdout)
    else:
        output.write_text(header + "\n")


@logged_all
def resolve_output(output: Optional[Path], force_overwrite: bool) -> Optional[Path]:
    """
    Resolve the output path if necessary checking for no accidental overwrite
    and resolving to default output if given a path
    """
    if output is None:
        return None

    # Do not allow accidental overwrite
    if output.is_file():
        if not force_overwrite:
            raise FileExistsError(
                f"{output} points to an existing file and --force-overwrite not set"
            )
        output.unlink()

    # If given a directory, check the default filename is valid
    if output.is_dir():
        default_name = output / "view.txt"
        return resolve_output(default_name, force_overwrite)

    return output


@logged()
def assert_unique_acquisition_id(run_info: pl.LazyFrame, path: Path) -> None:
    """
    Perform a check that the acquisition ids are unique raising AssertionError otherwise
    """
    groups = run_info.collect().group_by(pl.col("acquisition_id"))
    common_acq_ids = [acq_id for acq_id, frame in groups if frame.n_unique() != 1]
    if common_acq_ids:
        raise AssertionError(
            f"Found non-unique run_info acquisition_id in {path.name}: {common_acq_ids}. "
        )


def parse_reads_table_all(reader: p5.Reader) -> pl.LazyFrame:
    """
    Parse all records in the reads table returning a polars LazyFrame
    """
    logger.debug(f"Parsing {reader.path.name} records")
    read_table = reader.read_table.read_all()
    reads = (
        pl.from_arrow(read_table, rechunk=False)
        .drop(["signal"])
        .lazy()
        .with_columns(pl.col("run_info").cast(pl.Utf8))
    )
    return reads


def parse_reads_table_batch(
    reader: p5.Reader, batch_index: int
) -> Tuple[pl.LazyFrame, int]:
    """
    Parse the reads table record batch at `batch_index` from a pod5 file returning a
    polars LazyFrame and the number of records in it
    """
    logger.debug(f"Parsing {reader.path.name} record batch {batch_index}")
    read_table = reader.read_table.get_batch(batch_index)
    reads = (
        pl.from_arrow(read_table, rechunk=False)
        .drop(["signal"])
        .lazy()
        .with_columns(pl.col("run_info").cast(pl.Utf8))
    )
    return reads, read_table.num_rows


@logged_all
def parse_read_table_chunks(
    reader: p5.Reader, approx_size: int = 99_999
) -> Generator[pl.LazyFrame, None, None]:
    """
    Read record batches and yield polars lazyframes of `approx_size` records.
    Records are yielded in units of whole batches of the underlying table
    """
    chunks: List[pl.LazyFrame] = []
    chunk_rows = 0

    for batch_index in range(reader.read_table.num_record_batches):
        reads, n_rows = parse_reads_table_batch(reader, batch_index)

        chunks.append(reads)
        chunk_rows += n_rows

        if chunk_rows > approx_size:
            reads_chunk = pl.concat(chunks)
            logger.debug(f"Emitting chunk of {chunk_rows} rows")
            chunks = []
            chunk_rows = 0
            yield reads_chunk

    if chunk_rows > 0:
        reads_chunk = pl.concat(chunks)
        chunks = []
        logger.debug(f"Emitting final chunk of {chunk_rows} rows")
        yield reads_chunk


@logged()
def parse_run_info_table(reader: p5.Reader) -> pl.LazyFrame:
    """Parse the reads table from a pod5 file returning a polars LazyFrame"""
    run_info_table = reader.run_info_table.read_all().drop(
        ["context_tags", "tracking_id"]
    )
    run_info = pl.from_arrow(run_info_table, rechunk=False).lazy().unique()
    return run_info


@logged()
def join_reads_to_run_info(reads: pl.LazyFrame, run_info: pl.LazyFrame) -> pl.LazyFrame:
    """Join the reads and run_info tables"""
    return reads.join(
        run_info.unique(),
        left_on="run_info",
        right_on="acquisition_id",
    )


def get_reads_tables(
    path: Path, selected_fields: Set[str], threshold: int = 100_000
) -> Generator[pl.LazyFrame, None, None]:
    """
    Generate lazy dataframes from pod5 records. If the number of records
    is greater than `threshold` then yield chunks to limit memory consumption and
    improve overall performance
    """
    with p5.Reader(path) as reader:
        run_info = parse_run_info_table(reader)
        assert_unique_acquisition_id(run_info, path)

        if reader.num_reads <= threshold:
            reads_table = parse_reads_table_all(reader)
            joined = join_reads_to_run_info(reads_table, run_info)
            yield format_view_table(joined, path, selected_fields)
            return

        for reads_chunk in parse_read_table_chunks(reader, approx_size=threshold - 1):
            joined = join_reads_to_run_info(reads_chunk, run_info)
            yield format_view_table(joined, path, selected_fields)


def join_workers(processes: List[SpawnProcess], exceptions: mp.JoinableQueue) -> None:
    """Poll workers checking for exceptions which will likely cause"""
    prcs = {p for p in processes}
    while prcs:
        try:
            exc, path = exceptions.get(timeout=0.1)
            terminate_processes(processes)
            exceptions.task_done()
            if isinstance(exc, BrokenPipeError):
                sys.exit(1)
            else:
                terminate_processes(processes)
                raise RuntimeError(f"Error while processing '{path}'") from exc
        except Empty:
            pass

        done = set()
        for prc in prcs:
            exit_code = prc.exitcode

            if exit_code is None:
                continue

            if exit_code > 0:
                terminate_processes(processes)
                raise mp.ProcessError(
                    f"Unexpected exception ocurrecd in {prc} - exit code: {exit_code}"
                )
            else:
                done.add(prc)
        prcs.difference_update(done)

    for prc in processes:
        prc.join()


@logged_all
def worker_process(
    paths: mp.JoinableQueue,
    exceptions: mp.JoinableQueue,
    lock: Lock,
    output: Path,
    separator: bool,
    selection: Set[str],
) -> None:
    """
    Consume pod5 paths from `paths` queue, parse the records and write to `output` after
    acquiring `lock`.
    Returns `None` when all finish sentinel `None` is received in `paths` queue.
    """
    path: Optional[Path] = None
    try:
        while True:
            path = paths.get()
            if path is None:
                paths.task_done()
                break

            try:
                for table in get_reads_tables(path, selection):
                    with lock:
                        write(ldf=table, output=output, separator=separator)
            finally:
                paths.task_done()
        paths.close()

    except Exception as exc:
        exceptions.put((exc, path))


def launch_view_workers(
    paths: Set[Path],
    output: Path,
    selection: Set[str],
    separator: str,
    num_workers: int,
):
    ctx = mp.get_context("spawn")
    write_lock = ctx.Lock()
    paths_queue = ctx.JoinableQueue(maxsize=len(paths) * 2)
    exceptions_queue = ctx.JoinableQueue(maxsize=len(paths))

    # Prepare the paths queue
    for path in paths:
        paths_queue.put(path)

    processes: List[SpawnProcess] = []
    for _ in range(num_workers):
        worker = ctx.Process(
            target=worker_process,
            kwargs=dict(
                paths=paths_queue,
                exceptions=exceptions_queue,
                lock=write_lock,
                output=output,
                separator=separator,
                selection=selection,
            ),
            daemon=True,
        )
        worker.start()
        processes.append(worker)

        # Enqueue a stop sentinel for each worker
        paths_queue.put(None)

    join_workers(processes, exceptions_queue)

    paths_queue.join()
    paths_queue.close()
    paths_queue.join_thread()


@logged_all
def view_pod5(
    inputs: List[Path],
    output: Path,
    separator: str = "\t",
    recursive: bool = False,
    force_overwrite: bool = False,
    list_fields: bool = False,
    no_header: bool = False,
    threads: int = DEFAULT_THREADS,
    **kwargs,
) -> None:
    """Given a list of POD5 files write a table to view their contents"""

    if list_fields:
        print_fields()
        return

    threads = limit_threads(threads)

    output_path = resolve_output(output, force_overwrite)

    # Decode escaped separator characters e.g. \t
    sep = codecs.decode(separator, "unicode-escape")

    # Parse column selection args
    selection = select_fields(**kwargs)

    collected_paths = collect_inputs(
        inputs, recursive=recursive, pattern="*.pod5", threads=threads
    )
    if not collected_paths:
        raise AssertionError("Found no pod5 files searching inputs")

    num_workers = min(len(collected_paths), threads)

    if not no_header:
        write_header(output=output_path, selected=selection, separator=sep)

    launch_view_workers(
        paths=collected_paths,
        output=output_path,
        selection=selection,
        separator=sep,
        num_workers=num_workers,
    )


def main():
    run_tool(prepare_pod5_view_argparser())


if __name__ == "__main__":
    main()
