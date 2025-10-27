"""
Tool for subsetting pod5 files into one or more outputs using a list of read ids
"""

from pathlib import Path
from typing import List
from pod5.tools.polars_utils import PL_DEST_FNAME, PL_READ_ID, PL_UUID_REGEX
from pod5.tools.utils import (
    DEFAULT_THREADS,
    collect_inputs,
    init_logging,
    limit_threads,
    logged_all,
)

import polars as pl

from pod5.tools.parsers import prepare_pod5_filter_argparser, run_tool
from pod5.tools.pod5_subset import build_targets_dict
import lib_pod5 as p5b


logger = init_logging()

pl.enable_string_cache()


@logged_all
def parse_read_id_targets(ids: Path, output: Path) -> pl.LazyFrame:
    """Parse the list of read_ids checking all are valid uuids"""
    read_ids = (
        pl.scan_csv(
            ids,
            has_header=False,  # Any header will be filtered out by is_uuid
            comment_prefix="#",
            new_columns=[PL_READ_ID],
            rechunk=False,
        )
        .drop_nulls()
        .unique()
        .with_columns(
            [
                pl.lit(str(output.resolve())).cast(pl.Categorical).alias(PL_DEST_FNAME),
                pl.col(PL_READ_ID).str.contains(PL_UUID_REGEX).alias("is_uuid"),
            ]
        )
        .filter(pl.col("is_uuid"))
        .drop("is_uuid")
    )

    if len(read_ids.fetch(10)) == 0:
        raise AssertionError(f"Found 0 read_ids in {ids}. Nothing to do")

    return read_ids


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

    threads = limit_threads(threads)

    _inputs = collect_inputs(inputs, recursive, "*.pod5", threads=threads)

    targets_dict = build_targets_dict(targets)

    p5b.subset_pod5s_with_mapping(
        list(_inputs),
        output,
        targets_dict,
        # threads=threads,
        missing_ok,
        duplicate_ok,
        force_overwrite,
    )
    return


@logged_all
def main():
    """pod5 filter main"""
    run_tool(prepare_pod5_filter_argparser())


if __name__ == "__main__":
    main()
