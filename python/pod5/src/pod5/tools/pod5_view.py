import codecs
import multiprocessing as mp
from multiprocessing.context import SpawnProcess
from multiprocessing.synchronize import Lock
import os
from pathlib import Path
from queue import Empty
import sys
from typing import Callable, Dict, Generator, List, NamedTuple, Optional, Set, Tuple

from pod5.reader import ArrowTableHandle
import polars as pl
import pyarrow as pa

import pod5 as p5
from pod5.tools.parsers import prepare_pod5_view_argparser, run_tool
from pod5.tools.polars_utils import (
    pl_format_empty_string,
    pl_format_read_id,
    pl_from_arrow,
    pl_from_arrow_batch,
)
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

pl.enable_string_cache()


class Selection(NamedTuple):
    selected: Set[str]  # The set of column names selected
    reads_fields: Set[str]  # The set of read table fields required
    info_fields: Set[str]  # The set of run info table fields required

    def __contains__(self, key):
        return key in self.selected

    def union(self) -> Set[str]:
        return self.reads_fields.union(self.info_fields)


class Field(NamedTuple):
    """Container class for storing the expression for a named field"""

    docs: str
    reads_fields: Optional[List[str]] = None
    info_fields: Optional[List[str]] = None


# This dict defines the order of the fields
FIELDS: Dict[str, Field] = {
    "read_id": Field(
        "Read UUID",
        ["read_id"],
    ),
    "filename": Field(
        "Source pod5 filename",
    ),
    "read_number": Field(
        "Read number",
        ["read_number"],
    ),
    "channel": Field(
        "1-indexed channel",
        ["channel"],
    ),
    "mux": Field(
        "1-indexed well",
        ["well"],
    ),
    "end_reason": Field(
        "End reason string",
        ["end_reason"],
    ),
    "start_time": Field(
        "Seconds since the run start to the first sample of this read",
        ["start"],
        ["sample_rate"],
    ),
    "start_sample": Field(
        "Samples recorded on this channel since run start to the first sample of this read",
        ["start"],
    ),
    "duration": Field(
        "Seconds of sampling for this read",
        ["num_samples", "sample_rate"],
    ),
    "num_samples": Field(
        "Number of signal samples",
        ["num_samples"],
    ),
    "minknow_events": Field(
        "Number of minknow events that this read contains",
        ["num_minknow_events"],
    ),
    "sample_rate": Field(
        "Number of samples recorded each second",
        ["sample_rate"],
    ),
    "median_before": Field(
        "Current level in this well before the read",
        ["median_before"],
    ),
    # DEPRECATED
    "predicted_scaling_scale": Field(
        "Scale for predicted read scaling",
        ["predicted_scaling_scale"],
    ),
    # DEPRECATED
    "predicted_scaling_shift": Field(
        "Shift for predicted read scaling",
        ["predicted_scaling_shift"],
    ),
    # DEPRECATED
    "tracked_scaling_scale": Field(
        "Scale for tracked read scaling",
        ["tracked_scaling_scale"],
    ),
    # DEPRECATED
    "tracked_scaling_shift": Field(
        "Shift for tracked read scaling",
        ["tracked_scaling_shift"],
    ),
    "num_reads_since_mux_change": Field(
        "Number of selected reads since the last mux change on this channel",
        ["num_reads_since_mux_change"],
    ),
    "time_since_mux_change": Field(
        "Seconds since the last mux change on this channel",
        ["time_since_mux_change"],
    ),
    "run_id": Field("Run UUID", None, ["protocol_run_id"]),
    "sample_id": Field(
        "User-supplied name for the sample",
        None,
        ["sample_id"],
    ),
    "experiment_id": Field(
        "User-supplied name for the experiment",
        None,
        ["experiment_name"],
    ),
    "flow_cell_id": Field(
        "The flow cell id",
        None,
        ["flow_cell_id"],
    ),
    "pore_type": Field(
        "Name of the pore in this well",
        ["pore_type"],
    ),
    "open_pore_level": Field(
        "The tracked open pore level for this read",
        ["open_pore_level"],
    ),
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
) -> Selection:
    """Select fields to write"""
    selected: Set[str] = set([])

    # Select only read ids
    if group_read_id:
        selected.add("read_id")
        return Selection(selected, selected, set())

    if include:
        for key in include.split(","):
            key = key.strip()
            if not key:
                continue
            get_field_or_raise(key)
            selected.add(key)

    # Default selection - All fields
    if not selected:
        selected.update(FIELDS.keys())

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

    reads_fields: Set[str] = set()
    info_fields: Set[str] = set()

    for field_name in selected:
        field = FIELDS[field_name]
        if field.reads_fields:
            reads_fields.update(field.reads_fields)
        if field.info_fields:
            info_fields.update(field.info_fields)

    # If we use the anything from run_info - add fields to perform the join
    if info_fields:
        reads_fields.update(["run_info"])
        info_fields.update(["acquisition_id"])

    return Selection(selected, reads_fields, info_fields)


def get_format_view_table_fn(
    path: Path, selection: Selection
) -> Callable[[pl.LazyFrame], pl.LazyFrame]:
    """Format the view table based on the selected fields"""
    drop: Set[str] = set()
    exprs: List[pl.Expr] = []
    if "filename" in selection:
        exprs.append(pl.lit(path.name).alias("filename"))
    if "read_id" in selection:
        exprs.append(pl_format_read_id(pl.col("read_id")))
    if "mux" in selection:
        exprs.append(pl.col("well").alias("mux"))
    if "start_time" in selection:
        exprs.append((pl.col("start") / pl.col("sample_rate")).alias("start_time"))
        if "start" not in selection:
            drop.add("start")
        if "sample_rate" not in selection:
            drop.add("sample_rate")
    if "start_sample" in selection:
        exprs.append(pl.col("start").alias("start_sample"))
    if "duration" in selection:
        exprs.append((pl.col("num_samples") / pl.col("sample_rate")).alias("duration"))
        if "num_samples" not in selection:
            drop.add("num_samples")
        if "sample_rate" not in selection:
            drop.add("sample_rate")
    if "minknow_events" in selection:
        exprs.append(pl.col("num_minknow_events").alias("minknow_events"))
    if "run_id" in selection:
        exprs.append(pl.col("protocol_run_id").alias("run_id"))
    if "experiment_id" in selection:
        exprs.append(pl.col("experiment_name").alias("experiment_id"))

    maybe_empty = ["experiment_id", "protocol_run_id", "sample_id", "flow_cell_id"]
    order = [key for key in FIELDS.keys() if key in selection.selected]

    # All tables are the same so we can compute this work ONCE
    def format_view_table(lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = lf.with_columns(exprs)

        # Replace potentially empty fields with "not_set"
        # This can't be done in the above expression due to the behaviour of
        # name.keep()
        empty_cols = [f for f in maybe_empty if f in lf.collect_schema().names()]
        if empty_cols:
            lf = lf.with_columns(
                pl_format_empty_string(pl.col(empty_cols), "not_set").name.keep()
            )

        # Apply the field selection order
        return lf.select(order)

    return format_view_table


@logged(log_time=True)
def write(
    ldf: pl.LazyFrame,
    output: Optional[Path],
    separator: str = "\t",
) -> None:
    """Write the polars.LazyFrame"""

    kwargs = dict(
        include_header=False, separator=separator, null_value="", float_precision=8
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
    output: Optional[Path], selection: Selection, separator: str = "\t"
) -> None:
    """Write the header line"""
    header = separator.join(key for key in FIELDS if key in selection.selected)
    if output is None:
        print(header, file=sys.stdout, flush=True)
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


def parse_reads_table_all(
    reader: p5.Reader, included_fields: List[int]
) -> pl.LazyFrame:
    """
    Parse all records in the reads table returning a polars LazyFrame
    """
    logger.debug(f"Parsing {reader.path.name} records {included_fields=}")

    options = pa.ipc.IpcReadOptions(included_fields=included_fields)
    with ArrowTableHandle(
        reader.inner_file_reader.get_file_read_table_location(), options=options
    ) as handle:
        reads_table = handle.reader.read_all()
        reads_table = pl_from_arrow(reads_table, rechunk=False).lazy()

    return reads_table


def parse_reads_table_batch(
    reader: p5.Reader, included_fields: List[int], batch_index: int
) -> Tuple[pl.LazyFrame, int]:
    """
    Parse the reads table record batch at `batch_index` from a pod5 file returning a
    polars LazyFrame and the number of records in it
    """
    logger.debug(
        f"Parsing {reader.path.name} record batch {batch_index} {included_fields=}"
    )

    options = pa.ipc.IpcReadOptions(included_fields=included_fields)
    with ArrowTableHandle(
        reader.inner_file_reader.get_file_read_table_location(), options=options
    ) as handle:
        reads_batch = handle.reader.get_record_batch(batch_index)
        num_reads = reads_batch.num_rows
        reads_batch = pl_from_arrow_batch(reads_batch, rechunk=False).lazy()

    return reads_batch, num_reads


@logged_all
def parse_read_table_chunks(
    reader: p5.Reader, included_fields: List[int], approx_size: int = 99_999
) -> Generator[pl.LazyFrame, None, None]:
    """
    Read record batches and yield polars lazyframes of `approx_size` records.
    Records are yielded in units of whole batches of the underlying table
    """
    chunks: List[pl.LazyFrame] = []
    chunk_rows = 0

    for batch_index in range(reader.read_table.num_record_batches):
        reads, n_rows = parse_reads_table_batch(reader, included_fields, batch_index)

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
def parse_run_info_table(
    reader: p5.Reader, selection: Selection
) -> Optional[pl.LazyFrame]:
    """Parse the reads table from a pod5 file returning a polars LazyFrame"""
    included_fields: List[int] = []
    for field_idx, name in enumerate(reader.run_info_table.schema.names):
        if name in selection.info_fields:
            included_fields.append(field_idx)

    if not included_fields:
        return None

    options = pa.ipc.IpcReadOptions(included_fields=included_fields)

    with ArrowTableHandle(
        reader.inner_file_reader.get_file_run_info_table_location(), options=options
    ) as handle:
        table = handle.reader.read_all()
        table = pl_from_arrow(table, rechunk=False).lazy()

    assert_unique_acquisition_id(table, reader.path)
    return table


@logged()
def join_reads_to_run_info(reads: pl.LazyFrame, run_info: pl.LazyFrame) -> pl.LazyFrame:
    """Join the reads and run_info tables"""
    return reads.with_columns(pl.col("run_info").cast(pl.Utf8)).join(
        run_info.unique(),
        left_on="run_info",
        right_on="acquisition_id",
    )


def get_included_reads_table_fields(reader: p5.Reader, selection: Selection):
    included_fields: List[int] = []
    for field_idx, name in enumerate(reader.read_table.schema.names):
        if name in selection.reads_fields:
            included_fields.append(field_idx)

    if not included_fields:
        raise KeyError(
            f"No reads fields set in {selection.selected=} {selection.reads_fields=}"
        )
    return included_fields


def get_reads_tables(
    path: Path, selection: Selection, threshold: int = 100_000
) -> Generator[pl.LazyFrame, None, None]:
    """
    Generate lazy dataframes from pod5 records. If the number of records
    is greater than `threshold` then yield chunks to limit memory consumption and
    improve overall performance
    """

    with p5.Reader(path) as reader:
        included_fields = get_included_reads_table_fields(reader, selection)

        format_view_table_fn: Callable[[pl.LazyFrame], pl.LazyFrame] = (
            get_format_view_table_fn(path, selection)
        )

        run_info = parse_run_info_table(reader, selection)

        if reader.num_reads <= threshold:
            reads_table = parse_reads_table_all(reader, included_fields)
            if run_info is not None:
                reads_table = join_reads_to_run_info(reads_table, run_info)

            yield format_view_table_fn(reads_table)
            return

        for reads_chunk in parse_read_table_chunks(
            reader, included_fields, approx_size=threshold - 1
        ):
            if run_info is not None:
                reads_chunk = join_reads_to_run_info(reads_chunk, run_info)
            yield format_view_table_fn(reads_chunk)


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
    selection: Selection,
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
    selection: Selection,
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
        write_header(output=output_path, selection=selection, separator=sep)

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
