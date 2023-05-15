import codecs
import os
from pathlib import Path
import sys
from typing import Dict, List, NamedTuple, Optional, Set
from more_itertools import chunked

import polars as pl

from pod5.tools.parsers import prepare_pod5_view_argparser, run_tool
from pod5.tools.polars_utils import pl_format_empty_string, pl_format_read_id
from pod5.tools.utils import collect_inputs

import pod5 as p5


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


def print_fields():
    """Print a list of the available columns"""
    for name, field in FIELDS.items():
        print(f"{name.ljust(28)} {field.docs}")
    print("")


def assert_unique_acquisition_id(run_info: pl.LazyFrame, path: Path) -> None:
    """
    Perform a check that the acquisition ids are unique raising AssertionError otherwise
    """
    groups = run_info.collect().groupby(pl.col("acquisition_id"))
    common_acq_ids = [acq_id for acq_id, frame in groups if frame.n_unique() != 1]
    if common_acq_ids:
        raise AssertionError(
            f"Found non-unique run_info acquisition_id(s) in {path.name}: {common_acq_ids}. "
        )


def parse_reads_table(reader: p5.Reader) -> pl.LazyFrame:
    """Parse the reads table from a pod5 file returning a polars LazyFrame"""
    read_table = reader.read_table.read_all().drop(["signal"])
    reads = (
        pl.from_arrow(read_table, rechunk=False)
        .lazy()
        .with_columns(pl.col("run_info").cast(pl.Utf8))
    )
    return reads


def parse_run_info_table(reader: p5.Reader) -> pl.LazyFrame:
    """Parse the reads table from a pod5 file returning a polars LazyFrame"""
    run_info_table = reader.run_info_table.read_all().drop(
        ["context_tags", "tracking_id"]
    )
    run_info = pl.from_arrow(run_info_table, rechunk=False).lazy().unique()
    return run_info


def join_reads_to_run_info(reads: pl.LazyFrame, run_info: pl.LazyFrame) -> pl.LazyFrame:
    """Join the reads and run_info tables"""
    return reads.join(
        run_info.unique(),
        left_on="run_info",
        right_on="acquisition_id",
    )


def get_table(path: Path, selected_fields: Set[str]) -> pl.LazyFrame:
    """
    Returns pl.DataFrame of a POD5 read table.
    """
    with p5.Reader(path) as reader:
        reads = parse_reads_table(reader)
        run_info = parse_run_info_table(reader)

    assert_unique_acquisition_id(run_info, path)
    joined = join_reads_to_run_info(reads, run_info)

    maybe_empty = ["experiment_id", "protocol_run_id", "sample_id", "flow_cell_id"]

    joined = joined.with_columns(
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
    joined = joined.with_columns(
        pl_format_empty_string(pl.col(maybe_empty), "not_set").keep_name()
    )

    # Apply the field selection
    joined = joined.select(
        field.expr for key, field in FIELDS.items() if key in selected_fields
    )

    return joined


def write(
    ldf: pl.LazyFrame,
    output: Optional[Path],
    separator: str = "\t",
    has_header: bool = True,
) -> None:
    """Write the polars.LazyFrame"""

    kwargs = dict(
        has_header=has_header, separator=separator, null_value="", float_precision=8
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
    except BrokenPipeError:
        # https://docs.python.org/3/library/signal.html#note-on-sigpipe
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1)  # Python exits with error code 1 on EPIPE


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
        default_name = output / "output.csv"
        return resolve_output(default_name, force_overwrite)

    return output


def get_field_or_raise(key: str) -> Field:
    """Get the Field for this key or raise a KeyError"""
    try:
        return FIELDS[key]
    except KeyError:
        raise KeyError(
            f"Field: '{key}' did not match any known fields. "
            "Please check it exists by viewing `-L/--list-fields`"
        )


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


def view_pod5(
    inputs: List[Path],
    output: Path,
    separator: str = "\t",
    recursive: bool = False,
    force_overwrite: bool = False,
    list_fields: bool = False,
    no_header: bool = False,
    **kwargs,
) -> None:
    """Given a list of POD5 files write a table to view their contents"""

    if list_fields:
        print_fields()
        return

    output_path = resolve_output(output, force_overwrite)

    # Decode escaped separator characters e.g. \t
    sep = codecs.decode(separator, "unicode-escape")

    # Parse content args
    selection = select_fields(**kwargs)

    # Chunk the inputs to limit number of open file handles / memory
    inputs_chunks = chunked(
        collect_inputs(inputs, recursive=recursive, pattern="*.pod5"), 50
    )

    # Potentially chunking inputs, keep a cache of categorical strings
    with pl.StringCache():
        for chunk_idx, inputs_chunk in enumerate(inputs_chunks):
            table = pl.concat(get_table(path, selection) for path in inputs_chunk)

            # Write header only once if at all
            has_header = chunk_idx == 0 and not no_header
            write(table, output_path, has_header=has_header, separator=sep)


def main():
    run_tool(prepare_pod5_view_argparser())


if __name__ == "__main__":
    main()
