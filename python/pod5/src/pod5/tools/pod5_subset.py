"""
Tool for subsetting pod5 files into one or more outputs
"""

from copy import deepcopy
from pathlib import Path
from string import Formatter
from typing import Any, List, Optional, Tuple

import polars as pl
from pod5.tools.polars_utils import (
    PL_DEST_FNAME,
    PL_READ_ID,
    PL_UUID_REGEX,
)
from pod5.tools.utils import (
    DEFAULT_THREADS,
    collect_inputs,
    init_logging,
    logged,
    logged_all,
)
from pod5.tools.parsers import prepare_pod5_subset_argparser, run_tool
import lib_pod5 as p5b


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
    Replace f-string keyed placeholders with positional ones and return the keys in
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
            comment_prefix="#",
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
            comment_prefix="#",
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


def build_targets_dict(
    targets: pl.LazyFrame,
) -> dict[str, list[str]]:
    """Build a dictionary of output filename to read_ids from the targets dataframe"""
    targets_dict: dict[str, list[str]] = {}
    for row in targets.select([PL_READ_ID, PL_DEST_FNAME]).collect().iter_rows():
        read_id = row[0]
        fname = row[1]
        if fname not in targets_dict:
            targets_dict[fname] = list()
        targets_dict[fname].append(read_id)
    return targets_dict


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

    targets_dict = build_targets_dict(targets)

    if not output.exists():
        output.mkdir(parents=True)

    _inputs = collect_inputs(
        inputs, recursive=recursive, pattern="*.pod5", threads=threads
    )
    if len(_inputs) == 0:
        raise ValueError("Found no input pod5 files")

    p5b.subset_pod5s_with_mapping(
        list(_inputs),
        output,
        targets_dict,
        # threads=threads,
        missing_ok,
        duplicate_ok,
        force_overwrite,
    )


@logged()
def main():
    """pod5 subsample main"""
    run_tool(prepare_pod5_subset_argparser())


if __name__ == "__main__":
    main()
