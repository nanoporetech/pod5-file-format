"""
Tool for subsetting pod5 files into one or more outputs using a list of read ids
"""


from pathlib import Path
from typing import List
from pod5.tools.polars_utils import PL_DEST_FNAME, PL_READ_ID, PL_UUID_REGEX
from pod5.tools.utils import (
    DEFAULT_THREADS,
    PBAR_DEFAULTS,
    collect_inputs,
    init_logging,
    logged_all,
)

import polars as pl

from tqdm.auto import tqdm

import pod5 as p5
from pod5.repack import Repacker

from pod5.tools.parsers import prepare_pod5_filter_argparser, run_tool
from pod5.tools.pod5_subset import (
    calculate_transfers,
    parse_sources,
)
from pod5.tools.polars_utils import PL_SRC_FNAME
from pod5.tools.utils import logged


logger = init_logging()


@logged_all
def parse_read_id_targets(ids: Path, output: Path) -> pl.LazyFrame:
    """Parse the list of read_ids checking all are valid uuids"""
    read_ids = (
        pl.scan_csv(
            ids,
            has_header=False,  # Any header will be filtered out by is_uuid
            comment_char="#",
            new_columns=[PL_READ_ID],
            rechunk=False,
        )
        .drop_nulls()
        .unique()
        .with_columns(
            [
                pl.lit(str(output.resolve())).alias(PL_DEST_FNAME),
                pl.col(PL_READ_ID).str.contains(PL_UUID_REGEX).alias("is_uuid"),
            ]
        )
        .filter(pl.col("is_uuid"))
        .drop("is_uuid")
    )

    if len(read_ids.fetch(10)) == 0:
        raise AssertionError(f"Found 0 read_ids in {ids}. Nothing to do")

    return read_ids


@logged(log_time=True)
def filter_reads(dest: Path, sources: pl.DataFrame) -> None:
    """Copy the reads in `sources` into a new pod5 file at `dest`"""
    prev = 0
    repacker = Repacker()
    with p5.Writer(dest) as writer:
        output = repacker.add_output(writer)

        # Count the total number of reads expected
        total_reads = 0
        for source, reads in sources.groupby(PL_SRC_FNAME):
            total_reads += len(reads.get_column(PL_READ_ID))

        pbar = tqdm(
            total=total_reads,
            unit="Read",
            desc="Filtering",
            leave=True,
            **PBAR_DEFAULTS,
        )

        # Copy selected reads from one file at a time
        for source, reads in sources.groupby(PL_SRC_FNAME):
            src = Path(source)
            read_ids = reads.get_column(PL_READ_ID).unique().to_list()
            logger.debug(f"Filtering: {src} - n_reads: {len(read_ids)}")

            if len(read_ids) == 0:
                logger.debug(f"Skipping: {src}")
                continue

            with p5.Reader(src) as reader:
                repacker.add_selected_reads_to_output(output, reader, read_ids)
                for n_written in repacker.waiter(interval=0.1):
                    pbar.update(n_written - prev)
                    prev = n_written

        pbar.update(total_reads - prev)
        pbar.close()
        # Finish the pod5 file and close source handles
        repacker.finish()

    return


@logged_all
def filter_pod5(
    inputs: List[Path],
    output: Path,
    ids: Path,
    missing_ok: bool = False,
    duplicate_ok: bool = False,
    force_overwrite: bool = False,
    recursive: bool = False,
    threads: int = DEFAULT_THREADS,
) -> None:
    """Prepare the pod5 filter mapping and run the repacker"""
    # Remove output file
    if output.exists():
        if not force_overwrite:
            raise FileExistsError(
                f"Output file already exists and --force-overwrite not set - {output}"
            )
        else:
            output.unlink()

    # Create parent directories if they do not exist
    if not output.parent.exists():
        output.parent.mkdir(parents=True, exist_ok=True)

    targets = parse_read_id_targets(ids, output=output)
    print(f"Parsed {len(targets.collect())} reads_ids from: {ids.name}")

    _inputs = collect_inputs(inputs, recursive, "*.pod5", threads=threads)
    sources = parse_sources(_inputs, duplicate_ok=duplicate_ok, threads=threads)
    print(f"Found {len(sources.collect())} read_ids from {len(_inputs)} inputs")

    # Map the target outputs to which source read ids they're comprised of
    transfers = calculate_transfers(
        sources=sources,
        targets=targets,
        missing_ok=missing_ok,
    )
    print(f"Calculated {len(transfers.collect())} transfers")

    # There will only one output from this
    groupby_dest = transfers.collect().groupby(PL_DEST_FNAME)
    for dest, sources in groupby_dest:
        filter_reads(dest=dest, sources=sources)

    return


@logged_all
def main():
    """pod5 filter main"""
    run_tool(prepare_pod5_filter_argparser())


if __name__ == "__main__":
    main()
