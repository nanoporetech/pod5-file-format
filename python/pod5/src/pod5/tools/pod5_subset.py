"""
Tool for subsetting pod5 files into one or more outputs
"""

import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from json import load as json_load
from pathlib import Path
from string import Formatter
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set

import jsonschema
import pandas as pd
from tqdm import tqdm

import pod5 as p5
import pod5.repack as p5_repack
from pod5.tools.parsers import prepare_pod5_subset_argparser, run_tool

# Json Schema used to validate json mapping
JSON_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "subset.schema.json",
    "title": "subset",
    "description": "pod5_subset json schema",
    "type": "object",
    # Allow any not-empty filename to array of non-empty strings (read_ids)
    "patternProperties": {
        "^[\\w\\-.]+$": {
            "description": "output targets",
            "type": "array",
            "items": {"type": "string", "description": "read_ids", "minLength": 1},
            "minItems": 1,
            "uniqueItems": True,
            "minLength": 1,
        },
    },
    # Refuse any keys which do not match the pattern
    "additionalProperties": False,
}

DEFAULT_READ_ID_COLUMN = "read_id"


def parse_table_mapping(
    summary_path: Path,
    filename_template: Optional[str],
    subset_columns: List[str],
    read_id_column: str = DEFAULT_READ_ID_COLUMN,
    ignore_incomplete_template: bool = False,
) -> Dict[str, Set[str]]:
    """
    Parse a table using pandas to create a mapping of output targets to read ids
    """
    if not subset_columns:
        raise AssertionError("Missing --columns when using --summary / --table")

    if not filename_template:
        filename_template = create_default_filename_template(subset_columns)

    assert_filename_template(
        filename_template, subset_columns, ignore_incomplete_template
    )

    # Parse the summary using pandas asserting that usecols exists
    # Use python engine to dynamically detect separator
    summary = pd.read_table(
        summary_path,
        sep=None,
        engine="python",
        usecols=subset_columns + [read_id_column],
    )

    def group_name_to_dict(group_name: Any, columns: List[str]) -> Dict[str, str]:
        """Split group_name tuple/str into a dict by column name key"""
        if not isinstance(group_name, (str, int, float)):
            return {col: str(value).strip() for col, value in zip(columns, group_name)}
        return {columns[0]: str(group_name).strip()}

    mapping: Dict[str, Set[str]] = {}

    # Convert length 1 list to string to silence pandas warning
    subset_group_keys = subset_columns if len(subset_columns) > 1 else subset_columns[0]

    # Create groups by unique sets of subset_columns values
    for group_name, df_group in summary.groupby(subset_group_keys):
        # Create filenames from the unique keys
        group_dict = group_name_to_dict(group_name, subset_columns)
        filename = filename_template.format(**group_dict)

        # Add all the read_ids to the mapping
        mapping[filename] = set(df_group[read_id_column].to_list())

    return mapping


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
                "Use --ignore_incomplete_template to suppress this exception."
            )


def create_default_filename_template(subset_columns: List[str]) -> str:
    """Create the default filename template from the subset_columns selected"""
    default = "_".join(f"{col}-{{{col}}}" for col in subset_columns)
    default += ".pod5"
    return default


def parse_direct_mapping_targets(
    csv_path: Optional[Path] = None,
    json_path: Optional[Path] = None,
) -> Dict[str, Set[str]]:
    """
    Parse either the csv or json direct mapping of output target to read_ids

    Returns
    -------
    dictionary mapping of output target to read_ids
    """
    # Both inputs are invalid
    if csv_path and json_path:
        raise RuntimeError("Given both --csv and --json when only one is required.")

    if csv_path:
        return parse_csv_mapping(csv_path)

    if json_path:
        return parse_json_mapping(json_path)

    raise RuntimeError("Either --csv or --json is required.")


def parse_csv_mapping(csv_path: Path) -> Dict[str, Set[str]]:
    """Parse the csv direct mapping of output target to read_ids"""

    mapping = defaultdict(set)

    with csv_path.open("r") as _fh:
        for line_no, line in enumerate(_fh):

            if not line or line.startswith("#") or "," not in line:
                continue

            try:
                target, *read_ids = (split.strip() for split in line.split(","))
            except ValueError as exc:
                raise RuntimeError(f"csv parse error at: {line_no} - {line}") from exc

            if target and len(read_ids):
                mapping[target].update(read_ids)

    return mapping


def parse_json_mapping(json_path: Path) -> Dict[str, Set[str]]:
    """Parse the json direct mapping of output target to read_ids"""

    with json_path.open("r") as _fh:
        json_data = json_load(_fh)
        jsonschema.validate(instance=json_data, schema=JSON_SCHEMA)

    return json_data


def assert_overwrite_ok(
    output: Path, names: Iterable[str], force_overwrite: bool
) -> None:
    """
    Given the output directory path and target filenames, assert that no unforced
    overwrite will occur unless requested raising an FileExistsError if not
    """

    existing = [name for name in names if (output / name).exists()]
    if any(existing):
        if not force_overwrite:
            raise FileExistsError(
                f"Output files already exists and --force_overwrite not set. "
                f"Refusing to overwrite {existing} in {output} path."
            )

        for name in existing:
            (output / name).unlink()


def resolve_targets(output: Path, mapping) -> Dict[str, Set[Path]]:
    """Resolve the targets from the mapping"""
    # Invert the mapping to have constant time lookup of read_id to targets
    resolved_targets: Dict[str, Set[Path]] = defaultdict(set)
    for target, read_ids in mapping.items():
        for read_id in read_ids:
            resolved_targets[read_id].add(output / target)
    return resolved_targets


def calculate_transfers(
    inputs: List[Path],
    read_targets: Dict[str, Set[Path]],
    missing_ok: bool,
    duplicate_ok: bool,
) -> Dict[Path, Dict[Path, Set[str]]]:
    """
    Calculate the transfers which stores the collection of read_ids their source and
    destination.
    """

    read_id_selection = list(read_targets.keys())
    found = set([])

    transfers: Dict[Path, Dict[Path, Set[str]]] = defaultdict(dict)

    for input_path in inputs:
        # Open a FileReader from input_path
        with p5.Reader(input_path) as rdr:

            # Iterate over batches of read_ids. Missing_ok here as selection could
            # be in another file
            for batch in rdr.read_batches(selection=read_id_selection, missing_ok=True):
                for read_id in p5.format_read_ids(batch.read_id_column):

                    if read_id in found and not duplicate_ok:
                        raise AssertionError(
                            f"read_id {read_id} was found in multiple inputs and --duplicate_ok not set"
                        )

                    found.add(read_id)

                    # Add this read_id to the target_mapping.
                    # transfers = {destination: { source: set(read_id) } }
                    for target in read_targets[read_id]:
                        if input_path in transfers[target]:
                            transfers[target][input_path].add(str(read_id))
                        else:
                            transfers[target][input_path] = set([str(read_id)])

    if not transfers:
        raise RuntimeError(
            f"No transfers prepared for supplied mapping and inputs: {inputs}"
        )

    if not missing_ok and found != set(read_id_selection):
        raise AssertionError("Missing read_ids from inputs but --missing_ok not set")

    return transfers


def launch_subsetting(
    transfers: Dict[Path, Dict[Path, Set[str]]], show_pbar: bool = False
) -> None:
    """
    Iterate over the transfers one target at a time, opening sources and copying
    the required read_ids. Wait for the repacker to finish before moving on
    to ensure we don't have too many open file handles.
    """

    def add_reads(repacker, writer, sources) -> List[p5.Reader]:
        """Add reads to the repacker"""
        readers = []
        output = repacker.add_output(writer)
        for source, read_ids in sources.items():
            reader = p5.Reader(source)
            readers.append(reader)
            repacker.add_selected_reads_to_output(output, reader, read_ids)

        return readers

    for (dest, sources) in sorted(transfers.items()):
        repacker = p5_repack.Repacker()
        with p5.Writer(dest) as wrtr:
            readers = add_reads(repacker, wrtr, sources)
            repacker.wait(interval=0.25, show_pbar=show_pbar)

            for reader in readers:
                reader.close()

            repacker.finish()


def subset_pod5s_with_mapping(
    inputs: Iterable[Path],
    output: Path,
    mapping: Mapping[str, Set[str]],
    threads: int = 1,
    missing_ok: bool = False,
    duplicate_ok: bool = False,
    force_overwrite: bool = False,
) -> List[Path]:
    """
    Given an iterable of input pod5 paths and an output directory, create output pod5
    files containing the read_ids specified in the given mapping of output filename to
    set of read_id.
    """

    if not output.exists():
        output.mkdir(parents=True, exist_ok=True)

    assert_overwrite_ok(output, mapping.keys(), force_overwrite)

    requested_count = 0
    for requested_read_ids in mapping.values():
        requested_count += len(requested_read_ids)

    if requested_count == 0:
        raise AssertionError("Selected 0 read_ids. Nothing to do")

    # Invert the mapping to have constant time lookup of read_id to target
    resolved_targets = resolve_targets(output, mapping)

    all_targets = set([])
    for targets in resolved_targets.values():
        all_targets.update(targets)
    n_targets = len(all_targets)
    n_reads = len(resolved_targets)
    n_workers = min(n_targets, threads)
    print(
        f"Subsetting {n_reads} read_ids into {n_targets} outputs using {n_workers} workers"
    )

    # Map the target outputs to which source read ids they're comprised of
    transfers = calculate_transfers(
        inputs=list(inputs),
        read_targets=resolved_targets,
        missing_ok=missing_ok,
        duplicate_ok=duplicate_ok,
    )

    single_transfer = len(transfers) == 1

    disable_pbar = not bool(int(os.environ.get("POD5_PBAR", 1)))
    futures = {}
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        pbar = tqdm(
            total=len(transfers),
            ascii=True,
            disable=(disable_pbar or single_transfer),
            unit="Files",
        )

        # Launch the subsetting jobs
        for target in transfers:
            transfer = {target: transfers[target]}
            futures[
                executor.submit(
                    launch_subsetting, transfers=transfer, show_pbar=single_transfer
                )
            ] = target

        # Collect the jobs as soon as they're complete
        for future in as_completed(futures):
            tqdm.write(f"Finished {futures[future]}")
            if not pbar.disable:
                pbar.update(1)

    if not pbar.disable:
        pbar.close()

    print("Done")

    return list(transfers.keys())


def subset_pod5(
    inputs: List[Path],
    output: Path,
    csv: Optional[Path],
    json: Optional[Path],
    table: Optional[Path],
    columns: List[str],
    threads: int,
    template: str,
    read_id_column: str,
    missing_ok: bool,
    duplicate_ok: bool,
    ignore_incomplete_template: bool,
    force_overwrite: bool,
) -> Any:
    """Prepare the subsampling mapping and run the repacker"""

    if csv or json:
        mapping = parse_direct_mapping_targets(csv, json)

    elif table:
        mapping = parse_table_mapping(
            table, template, columns, read_id_column, ignore_incomplete_template
        )

    else:
        raise RuntimeError(
            "Arguments provided could not be used to generate a subset mapping."
        )

    subset_pod5s_with_mapping(
        inputs=inputs,
        output=output,
        mapping=mapping,
        threads=threads,
        missing_ok=missing_ok,
        duplicate_ok=duplicate_ok,
        force_overwrite=force_overwrite,
    )


def main():
    """pod5 subsample main"""
    run_tool(prepare_pod5_subset_argparser())


if __name__ == "__main__":
    main()
