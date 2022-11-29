"""
Tool for subsetting pod5 files into one or more outputs
"""

import typing
from collections import Counter, defaultdict
from json import load as json_load
from pathlib import Path
from string import Formatter

import jsonschema
import lib_pod5 as p5b
import pandas as pd

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

# Type aliases
OutputMap = typing.Dict[Path, typing.Tuple[p5.Writer, p5b.Pod5RepackerOutput]]
TransferMap = typing.DefaultDict[
    typing.Tuple[p5.Reader, p5b.Pod5RepackerOutput], typing.Set[str]
]


def parse_summary_mapping(
    summary_path: Path,
    filename_template: typing.Optional[str],
    subset_columns: typing.List[str],
    read_id_column: str = DEFAULT_READ_ID_COLUMN,
    ignore_incomplete_template: bool = False,
) -> typing.Dict[str, typing.Set[str]]:
    """
    Parse a summary file (tab-separated table) using pandas to create a mapping of
    output targets to read ids
    """
    if not filename_template:
        filename_template = create_default_filename_template(subset_columns)

    assert_filename_template(
        filename_template, subset_columns, ignore_incomplete_template
    )

    # Parse the summary using pandas asserting that usecols exists
    summary = pd.read_table(summary_path, usecols=subset_columns + [read_id_column])

    def group_name_to_dict(
        group_name: typing.Any, columns: typing.List[str]
    ) -> typing.Dict[str, str]:
        """Split group_name tuple/str into a dict by column name key"""
        if not isinstance(group_name, (str, int, float)):
            return {col: str(value).strip() for col, value in zip(columns, group_name)}
        return {columns[0]: str(group_name).strip()}

    mapping: typing.Dict[str, typing.Set[str]] = {}

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
    template: str, subset_columns: typing.List[str], ignore_incomplete_template: bool
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


def create_default_filename_template(subset_columns: typing.List[str]) -> str:
    """Create the default filename template from the subset_columns selected"""
    default = "_".join(f"{col}-{{{col}}}" for col in subset_columns)
    default += ".pod5"
    return default


def parse_direct_mapping_targets(
    csv_path: typing.Optional[Path] = None,
    json_path: typing.Optional[Path] = None,
) -> typing.Dict[str, typing.Set[str]]:
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


def parse_csv_mapping(csv_path: Path) -> typing.Dict[str, typing.Set[str]]:
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


def parse_json_mapping(json_path: Path) -> typing.Dict[str, typing.Set[str]]:
    """Parse the json direct mapping of output target to read_ids"""

    with json_path.open("r") as _fh:
        json_data = json_load(_fh)
        jsonschema.validate(instance=json_data, schema=JSON_SCHEMA)

    return json_data


def get_total_selection(
    mapping: typing.Dict[str, typing.Iterable[str]], duplicate_ok: bool
) -> typing.Set[str]:
    """
    Create a set of all read_ids from the mapping checking for duplicates if necessary
    """
    # Create a set of the required output read_ids
    total_selection: typing.Set[str] = set()
    for requested_read_ids in mapping.values():
        total_selection.update(requested_read_ids)

    if len(total_selection) == 0:
        raise ValueError("Selected 0 read_ids. Nothing to do")

    if not duplicate_ok:
        assert_no_duplicate_reads(mapping=mapping)

    return total_selection


def assert_no_duplicate_reads(mapping: typing.Dict[str, typing.Iterable[str]]) -> None:
    """
    Raise AssertionError if we detect any duplicate read_ids in the outputs
    """
    # Count the total number of outputs to detect if duplicates were requested
    counter: typing.Counter[str] = Counter()
    for ids in mapping.values():
        counter.update(ids)

    if any(count > 1 for count in counter.values()):
        raise AssertionError("Duplicate outputs detected but --duplicate_ok not set")


def prepare_repacker_outputs(
    repacker: p5_repack.Repacker,
    output: Path,
    mapping: typing.Dict[str, typing.Iterable[str]],
) -> OutputMap:
    """
    Create a dictionary of the output filepath and their and associated
    FileWriter and Pod5RepackerOutput objects.
    """
    outputs: OutputMap = {}
    for target in mapping:
        target_path = output / target
        p5_output = p5.Writer(target_path)
        outputs[target_path] = (p5_output, repacker.add_output(p5_output))

    return outputs


def assert_no_missing_reads(selection: typing.Set[str], transfers: TransferMap) -> None:
    """
    Raise AssertionError if any read_ids in selection do not appear in transfers
    """
    observed = set()
    for read_ids in transfers.values():
        observed.update(read_ids)

    missing_count = len(selection) - len(observed)
    if missing_count > 0:
        raise AssertionError(
            f"Missing {missing_count} read_ids from input but --missing_ok not set"
        )


def assert_overwrite_ok(
    output: Path, names: typing.Iterable[str], force_overwrite: bool
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


def calculate_transfers(
    inputs: typing.List[Path],
    selection: typing.Set[str],
    outputs: OutputMap,
    read_targets: typing.Dict[str, Path],
) -> TransferMap:
    """
    Calculate the transfers which stores the collection of read_ids their source and
    destination.
    """
    transfers: TransferMap = defaultdict(set)

    for input_path in inputs:
        # Open a FileReader from input_path
        p5_reader = p5.Reader(input_path)

        # Iterate over batches of read_ids
        for batch in p5_reader.read_batches(selection=selection, missing_ok=True):
            for read_id in p5.format_read_ids(batch.read_id_column):

                # Get the repacker output destination
                _, repacker_output = outputs[read_targets[read_id]]

                # Add this read_id to the target_mapping
                transfers[(p5_reader, repacker_output)].add(str(read_id))

    if not transfers:
        raise RuntimeError(
            f"No transfers prepared for supplied mapping and inputs: {inputs}"
        )

    return transfers


def launch_repacker(
    repacker: p5_repack.Repacker, transfers: TransferMap, outputs: OutputMap
) -> None:
    """
    Supply the transfer data to the Repacker and wait for the process to complete
    finally closing the output FileWriters.
    """
    # Repack the input reads to the target outputs
    for (reader, repacker_output), read_ids in transfers.items():
        repacker.add_selected_reads_to_output(repacker_output, reader, read_ids)

    # Wait for repacking to complete:
    repacker.wait(interval=5)

    # Close the FileWriters
    for idx, (p5_writer, _) in enumerate(outputs.values()):
        p5_writer.close()
        print(f"Finished writing: {p5_writer.path} - {idx+1}/{len(outputs)}")

    print("Done")


def subset_pod5s_with_mapping(
    inputs: typing.Iterable[Path],
    output: Path,
    mapping: typing.Dict[str, typing.Iterable[str]],
    missing_ok: bool = False,
    duplicate_ok: bool = False,
    force_overwrite: bool = False,
) -> typing.List[Path]:
    """
    Given an iterable of input pod5 paths and an output directory, create output pod5
    files containing the read_ids specified in the given mapping of output filename to
    iterable of read_id.
    """

    if not output.exists():
        output.mkdir(parents=True, exist_ok=True)

    assert_overwrite_ok(output, mapping.keys(), force_overwrite)

    total_selection = get_total_selection(mapping=mapping, duplicate_ok=duplicate_ok)

    # Invert the mapping to have constant time lookup of read_id to target
    read_targets: typing.Dict[str, Path] = {
        _id: output / target for target, read_ids in mapping.items() for _id in read_ids
    }

    # Create a mapping of output filename to (FileWriter, Pod5RepackerOutput)
    p5_repacker = p5_repack.Repacker()
    outputs = prepare_repacker_outputs(p5_repacker, output, mapping)

    print(f"Parsed {len(read_targets)} read_ids from given mapping")
    print(f"Parsed {len(outputs)} output targets from given mapping")

    # Create sets of read_ids for each source and destination transfer pairing
    transfers = calculate_transfers(
        inputs=list(inputs),
        selection=total_selection,
        outputs=outputs,
        read_targets=read_targets,
    )

    print(f"Mapped: {sum(len(rs) for rs in transfers.values())} reads to outputs")

    if not missing_ok:
        assert_no_missing_reads(selection=total_selection, transfers=transfers)

    launch_repacker(repacker=p5_repacker, transfers=transfers, outputs=outputs)

    return list(outputs)


def subset_pod5(
    inputs: typing.List[Path],
    output: Path,
    csv: typing.Optional[Path],
    json: typing.Optional[Path],
    summary: typing.Optional[Path],
    columns: typing.List[str],
    template: str,
    read_id_column: str,
    missing_ok: bool,
    duplicate_ok: bool,
    ignore_incomplete_template: bool,
    force_overwrite: bool,
) -> typing.Any:
    """Prepare the subsampling mapping and run the repacker"""
    if csv or json:
        mapping = parse_direct_mapping_targets(csv, json)

    elif summary:
        mapping = parse_summary_mapping(
            summary, template, columns, read_id_column, ignore_incomplete_template
        )

    else:
        raise RuntimeError(
            "Arguments provided could not be used to generate a subset mapping."
        )

    subset_pod5s_with_mapping(
        inputs=inputs,
        output=output,
        mapping=mapping,
        missing_ok=missing_ok,
        duplicate_ok=duplicate_ok,
        force_overwrite=force_overwrite,
    )


def main():
    """pod5 subsample main"""
    run_tool(prepare_pod5_subset_argparser())


if __name__ == "__main__":
    main()
