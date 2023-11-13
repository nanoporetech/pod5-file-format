"""
Tool for subsetting pod5 files into one or more outputs
"""

from copy import deepcopy
import multiprocessing as mp
from multiprocessing.context import SpawnContext
from pathlib import Path
from queue import Empty
from string import Formatter
import sys
from time import sleep
from typing import Any, List, Optional, Set, Tuple

import polars as pl
from tqdm.auto import tqdm
import pod5 as p5
import pod5.repack as p5_repack
from pod5.tools.polars_utils import (
    PL_DEST_FNAME,
    PL_READ_ID,
    PL_SRC_FNAME,
    PL_UUID_REGEX,
    pl_format_read_id,
)
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
from pod5.tools.parsers import prepare_pod5_subset_argparser, run_tool


DEFAULT_READ_ID_COLUMN = "read_id"

logger = init_logging()

pl.enable_string_cache()


@logged_all
def get_separator(path: Path) -> str:
    """
    Inspect the first line of the file at path and attempt to determine the field
    separator as either tab or comma, depending on the number of occurrences of each
    Returns "," or "<tab>"
    """
    with path.open("r") as fh:
        line = fh.readline()
    n_tabs = line.count("\t")
    n_comma = line.count(",")
    if n_tabs >= n_comma:
        return "\t"
    return ","


@logged_all
def default_filename_template(subset_columns: List[str]) -> str:
    """Create the default filename template from the subset_columns selected"""
    default = "_".join(f"{col}-{{{col}}}" for col in subset_columns)
    default += ".pod5"
    return default


@logged_all
def column_keys_from_template(template: str) -> List[str]:
    """Get a list of placeholder keys in the template"""
    return [key for _, key, _, _ in Formatter().parse(template) if key]


@logged_all
def fstring_to_polars(
    template: str,
) -> Tuple[str, List[str]]:
    """
    Repalce f-string keyed placeholders with positional ones and return the keys in
    their respective position
    """
    # This is for pl.format positional syntax
    replaced = template
    keys = column_keys_from_template(template)
    for key in keys:
        replaced = replaced.replace(f"{{{key}}}", "{}")
    return replaced, keys


@logged_all
def parse_table_mapping(
    summary_path: Path,
    filename_template: Optional[str],
    subset_columns: List[str],
    read_id_column: str = DEFAULT_READ_ID_COLUMN,
    ignore_incomplete_template: bool = False,
) -> pl.LazyFrame:
    """
    Parse a table using polars to create a mapping of output targets to read ids
    """
    if not subset_columns:
        raise AssertionError("Missing --columns when using --summary / --table")

    if not filename_template:
        filename_template = default_filename_template(subset_columns)

    assert_filename_template(
        filename_template, subset_columns, ignore_incomplete_template
    )

    # Add the destination filename as a column
    pl_template, keys = fstring_to_polars(filename_template)

    columns = deepcopy(subset_columns)
    columns.append(read_id_column)

    targets = (
        pl.read_csv(
            summary_path,
            columns=columns,
            separator=get_separator(summary_path),
            comment_char="#",
        )
        .lazy()
        .with_columns(
            [
                pl.format(pl_template, *keys).cast(pl.Categorical).alias(PL_DEST_FNAME),
                pl.col(read_id_column).alias(PL_READ_ID),
            ]
        )
    )
    return targets


@logged_all
def assert_filename_template(
    template: str, subset_columns: List[str], ignore_incomplete_template: bool
) -> None:
    """
    Get the keys named in the template to assert that they exist in subset_columns
    """
    # Parse the template string to get the keywords
    # "{hello}_world_{name}" -> ["hello", "name"]
    template_keys = set(args[1] for args in Formatter().parse(template) if args[1])
    allowed_keys = set(subset_columns)

    # Assert there are no unexpected keys in the template
    unexpected = template_keys - allowed_keys
    if unexpected:
        raise KeyError(f"--template {template} has unexpected keys: {unexpected}")

    # Assert there are no unused keys in the template
    # This is important as the output would be degenerate on some keys
    if not ignore_incomplete_template:
        unused = allowed_keys - template_keys
        if unused:
            raise KeyError(
                f"--template {template} does not use {unused} keys. "
                "Use --ignore-incomplete-template to suppress this exception."
            )


@logged_all
def create_default_filename_template(subset_columns: List[str]) -> str:
    """Create the default filename template from the subset_columns selected"""
    default = "_".join(f"{col}-{{{col}}}" for col in subset_columns)
    default += ".pod5"
    return default


@logged_all
def parse_csv_mapping(csv_path: Path) -> pl.LazyFrame:
    """Parse the csv direct mapping of output target to read_ids to a targets dataframe"""
    targets = (
        pl.scan_csv(
            csv_path,
            has_header=False,
            comment_char="#",
            new_columns=[PL_DEST_FNAME, PL_READ_ID],
            rechunk=False,
        )
        .drop_nulls()
        .with_columns(
            [
                pl.col(PL_DEST_FNAME).cast(pl.Categorical),
                pl.col(PL_READ_ID).str.contains(PL_UUID_REGEX).alias("is_uuid"),
            ]
        )
        .filter(pl.col("is_uuid"))
        .drop("is_uuid")
    )

    if len(targets.fetch(10)) == 0:
        raise AssertionError(f"Found 0 read_ids in {csv_path}. Nothing to do")

    return targets


@logged_all
def resolve_output_targets(targets: pl.LazyFrame, output: Path) -> pl.LazyFrame:
    """Prepend the output path to the target filename and resolve the complete string"""
    # Add output directory column
    # Concatenate output directory to the output filenmae, drop temporary column
    targets = targets.with_columns(
        pl.concat_str(
            [
                pl.lit(str(output.resolve())).cast(pl.Categorical),
                pl.col(PL_DEST_FNAME).cast(pl.Categorical),
            ],
            separator="/",
        )
        .cast(pl.Categorical)
        .alias(PL_DEST_FNAME),
    )

    return targets


@logged_all
def assert_overwrite_ok(targets: pl.LazyFrame, force_overwrite: bool) -> None:
    """
    Given the target filenames, assert that no unforced overwrite will occur
    unless requested raising an FileExistsError. Unlinks existing files if they exist
    if `force_overwrite` set
    """

    def exists(path: str) -> bool:
        return Path(path).exists()

    DEST_EXISTS = "__dest_exists"
    dests = (
        targets.select(pl.col(PL_DEST_FNAME).unique())
        .with_columns(pl.col(PL_DEST_FNAME).apply(exists).alias(DEST_EXISTS))
        .collect()
    )

    if dests.get_column(DEST_EXISTS).any():
        if not force_overwrite:
            raise FileExistsError(
                "Output files already exists and --force-overwrite not set. "
            )

        def unlinker(item) -> bool:
            path, *_ = item
            Path(path).unlink()
            return True

        dests.select(pl.col(PL_DEST_FNAME).where(pl.col(DEST_EXISTS))).apply(unlinker)


@logged_all
def parse_source_process(paths: mp.JoinableQueue, parsed_sources: mp.Queue):
    """Parse sources until paths queue is consumed"""
    while True:
        path = paths.get(timeout=60)

        # Seninel value of finished work
        if path is None:
            paths.task_done()
            break

        parsed_sources.put(parse_source(path))
        paths.task_done()

    parsed_sources.close()
    paths.close()


@logged_all
def parse_source(path: Path) -> pl.LazyFrame:
    """
    Reads the read ids available in a given pod5 file returning a dataframe
    with the formatted read_ids and the source filename
    """
    with p5.Reader(path) as rdr:
        pa_read_table = rdr.read_table.read_all()

    source = (
        pl.from_arrow(pa_read_table, rechunk=False)
        .with_columns(
            pl_format_read_id(pl.col("read_id")).alias(PL_READ_ID),
            pl.lit(str(path.resolve())).alias(PL_SRC_FNAME),
        )
        .select(PL_READ_ID, PL_SRC_FNAME)
        .lazy()
    )
    return source


@logged_all
def parse_sources(paths: Set[Path], threads: int = DEFAULT_THREADS) -> pl.LazyFrame:
    """Reads all inputs and return formatted lazy dataframe"""

    threads = limit_threads(threads)
    n_proc = min(threads, len(paths))

    ctx = mp.get_context("spawn")
    work: mp.JoinableQueue = ctx.JoinableQueue(maxsize=len(paths) + n_proc)
    parsed_sources = ctx.Queue(maxsize=len(paths))
    for path in paths:
        work.put(path)

    # Spawn worker processes
    active_processes = []
    for _ in range(n_proc):
        process = ctx.Process(
            target=parse_source_process,
            args=(work, parsed_sources),
            daemon=True,
        )
        # Enqueue a sentinel for each process to stop
        work.put(None)

        process.start()
        active_processes.append(process)

    # Wait for all work to be done
    work.join()
    work.close()

    items: List[pl.LazyFrame] = []
    for _ in range(len(paths)):
        # After work is joined we will have len(paths) items in parsed_sources
        # shouldn't need to wait or seconds or ever see Empty.
        items.append(parsed_sources.get(timeout=60))

    parsed_sources.close()
    parsed_sources.join_thread()

    sources = pl.concat(
        items=items,
        how="vertical",
        rechunk=False,
        parallel=True,
    )

    # Shutdown
    for proc in active_processes:
        proc.join()
        proc.close()

    return sources


@logged_all
def calculate_transfers(
    sources: pl.LazyFrame, targets: pl.LazyFrame, missing_ok: bool
) -> pl.LazyFrame:
    """
    Produce the transfers dataframe which maps the read_ids, source and destination
    """
    transfers = targets.join(sources, on=PL_READ_ID, how="left").select(
        PL_READ_ID, PL_SRC_FNAME, PL_DEST_FNAME
    )

    if not missing_ok:
        # Find any records where there there is no source i.e. input is missing
        if (
            transfers.select(pl.col(PL_SRC_FNAME).is_null().any())
            .collect()
            .to_series()
            .any()
        ):
            raise AssertionError(
                "Missing read_ids from inputs but --missing-ok not set"
            )

    # Add filter to transfers query removing missing inputs
    transfers = transfers.filter(pl.col(PL_SRC_FNAME).is_not_null())
    return transfers


class WorkQueue:
    def __init__(
        self,
        context: SpawnContext,
        transfers: pl.LazyFrame,
    ) -> None:
        self.work: mp.JoinableQueue = context.JoinableQueue()
        self.size = 0
        groupby_dest = transfers.collect().group_by(PL_DEST_FNAME)
        for dest, sources in groupby_dest:
            self.work.put((Path(dest), sources))
            self.size += 1

        self.progress: mp.Queue = context.Queue(maxsize=self.size + 1)
        logger.info(f"WorkQueue size: {self.size}")

    @logged_all
    def join(self) -> None:
        """Call join on the work queue waiting for all tasks to be done"""
        self.work.join()

    @logged_all
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

    @logged_all
    def shutdown(self) -> int:
        """Shutdown all queues returning the counts of all remaining items"""
        n_work = self._discard_and_close(self.work)

        if n_work > 0:
            print("Unfinished work remaining during shutdown!", file=sys.stderr)

        return n_work


@logged_all
def overall_progress(queue: WorkQueue):
    pbar = tqdm(
        total=queue.size,
        desc="Subsetting",
        unit="Files",
        leave=True,
        position=0,
        **PBAR_DEFAULTS,
    )

    count = 0
    while count < queue.size:
        try:
            queue.progress.get(timeout=1)
            count += 1
            pbar.update()
        except Empty:
            continue


@logged_all
def launch_subsetting(
    transfers: pl.LazyFrame, duplicate_ok: bool, threads: int = DEFAULT_THREADS
) -> None:
    """
    Iterate over the transfers dataframe subsetting reads from sources to destinations
    """
    threads = limit_threads(threads)
    assert {PL_READ_ID, PL_SRC_FNAME, PL_DEST_FNAME}.issubset(set(transfers.columns))

    ctx = mp.get_context("spawn")
    work = WorkQueue(ctx, transfers)

    active_processes = []
    try:
        # Spawn worker processes
        for idx in range(min(threads, work.size)):
            process = ctx.Process(
                target=process_subset_tasks,
                args=(work, idx + 1, duplicate_ok),
                daemon=True,
            )
            # Enqueue a sentinel for each process to stop
            work.work.put(None)
            process.start()
            active_processes.append(process)

        # Spawn progressbar process
        progress_proc = ctx.Process(
            target=overall_progress,
            args=(work,),
            daemon=True,
        )
        progress_proc.start()
        active_processes.append(progress_proc)

        # Wait for all work to be done
        work.join()
        work.shutdown()

        # Shutdown
        for proc in active_processes:
            proc.join()
            proc.close()

    except Exception as exc:
        terminate_processes(active_processes)
        raise exc


@logged(log_time=True)
def process_subset_tasks(queue: WorkQueue, process: int, duplicate_ok: bool):
    """Consumes work from the queue and launches subsetting tasks"""
    while True:
        task = queue.work.get(timeout=60)
        if task is None:
            queue.work.task_done()
            break

        target, sources = task
        try:
            subset_reads(target, sources, process, duplicate_ok)
        finally:
            queue.work.task_done()
            queue.progress.put(True)


@logged(log_time=True)
def subset_reads(
    dest: Path, sources: pl.DataFrame, process: int, duplicate_ok: bool
) -> None:
    """Copy the reads in `sources` into a new pod5 file at `dest`"""
    # Count the total number of reads expected
    total_reads = 0
    for source, reads in sources.group_by(PL_SRC_FNAME):
        total_reads += len(reads.get_column(PL_READ_ID))

    pbar = tqdm(
        total=total_reads,
        desc=dest.name,
        unit="Reads",
        leave=False,
        position=process,
        **PBAR_DEFAULTS,
    )

    repacker = p5_repack.Repacker()
    with p5.Writer(dest) as writer:
        output = repacker.add_output(writer, not duplicate_ok)

        active_limit = 5
        # Copy selected reads from one file at a time
        for source, reads in sources.group_by(PL_SRC_FNAME):
            while repacker.currently_open_file_reader_count >= active_limit:
                pbar.update(repacker.reads_completed - pbar.n)
                sleep(0.2)

            read_ids = reads.get_column(PL_READ_ID).unique().to_list()
            logger.debug(f"Subsetting: {source} - n_reads: {len(read_ids)}")

            with p5.Reader(Path(source)) as reader:
                repacker.add_selected_reads_to_output(output, reader, read_ids)

        repacker.set_output_finished(output)
        while repacker.currently_open_file_reader_count > 0:
            pbar.update(repacker.reads_completed - pbar.n)
            sleep(0.1)

        pbar.update(total_reads - pbar.n)

        # Finish the pod5 file and close source handles
        repacker.finish()

    pbar.close()

    return


@logged(log_time=True)
def subset_pod5s_with_mapping(
    inputs: Set[Path],
    output: Path,
    targets: pl.LazyFrame,
    threads: int = DEFAULT_THREADS,
    missing_ok: bool = False,
    duplicate_ok: bool = False,
    force_overwrite: bool = False,
) -> None:
    """
    Given an iterable of input pod5 paths and an output directory, create output pod5
    files containing the read_ids specified in the given mapping of output filename to
    set of read_id.
    """

    if not output.exists():
        output.mkdir(parents=True, exist_ok=True)

    targets = resolve_output_targets(targets, output)
    assert_overwrite_ok(targets, force_overwrite)

    print(f"Parsed {len(targets.collect())} targets")
    sources_df = parse_sources(inputs, threads)

    transfers = calculate_transfers(
        sources=sources_df,
        targets=targets,
        missing_ok=missing_ok,
    )

    print(f"Calculated {len(transfers.collect())} transfers")
    launch_subsetting(transfers=transfers, duplicate_ok=duplicate_ok, threads=threads)

    print("Done")
    return None


@logged(log_time=True)
def subset_pod5(
    inputs: List[Path],
    output: Path,
    columns: List[str],
    csv: Optional[Path] = None,
    table: Optional[Path] = None,
    threads: int = DEFAULT_THREADS,
    template: str = "",
    read_id_column: str = DEFAULT_READ_ID_COLUMN,
    missing_ok: bool = False,
    duplicate_ok: bool = False,
    ignore_incomplete_template: bool = False,
    force_overwrite: bool = False,
    recursive: bool = False,
) -> Any:
    """Prepare the subsampling mapping and run the repacker"""

    if csv:
        targets = parse_csv_mapping(csv)

    elif table:
        targets = parse_table_mapping(
            table, template, columns, read_id_column, ignore_incomplete_template
        )

    else:
        raise RuntimeError(
            "Arguments provided could not be used to generate a subset mapping."
        )

    if not output.exists():
        output.mkdir(parents=True)

    _inputs = collect_inputs(
        inputs, recursive=recursive, pattern="*.pod5", threads=threads
    )
    if len(_inputs) == 0:
        raise ValueError("Found no input pod5 files")

    subset_pod5s_with_mapping(
        inputs=_inputs,
        output=output,
        targets=targets,
        threads=threads,
        missing_ok=missing_ok,
        duplicate_ok=duplicate_ok,
        force_overwrite=force_overwrite,
    )


@logged()
def main():
    """pod5 subsample main"""
    run_tool(prepare_pod5_subset_argparser())


if __name__ == "__main__":
    main()
