"""
Tool for demultiplexing, separating or sub-setting pod5 files into one or more outputs
"""

import argparse
from collections import defaultdict, Counter
from string import Formatter
import json
from pathlib import Path
import typing

import jsonschema
import pandas as pd

import pod5_format as p5
import pod5_format.pod5_format_pybind as p5b
import pod5_format.repack as p5_repack

# Json Schema used to validate json mapping
JSON_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "demux.schema.json",
    "title": "demux",
    "description": "pod5_demux json schema",
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


def prepare_pod5_demux_argparser() -> argparse.ArgumentParser:
    """Create an argument parser for the pod5 demux tool"""

    parser = argparse.ArgumentParser(
        description="Given one or more pod5 input files, demultiplex (separate) reads "
        "into one or more pod5 output files by a user-supplied mapping."
    )

    # Core arguments
    parser.add_argument(
        "inputs", type=Path, nargs="+", help="Pod5 filepaths to use as inputs"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path.cwd(),
        help="Destination directory to write outputs [cwd]",
    )
    parser.add_argument(
        "-f",
        "--force_overwrite",
        action="store_true",
        help="Overwrite destination files",
    )

    mapping_group = parser.add_argument_group("direct mapping")
    mapping_exclusive = mapping_group.add_mutually_exclusive_group(required=False)
    mapping_exclusive.add_argument(
        "--csv",
        type=Path,
        help="CSV file mapping output filename to read ids",
    )
    mapping_exclusive.add_argument(
        "--json", type=Path, help="JSON mapping output filename to array of read ids."
    )
    summary_group = parser.add_argument_group("sequencing summary mapping")
    summary_group.add_argument("--summary", type=Path, help="Sequencing summary path")
    summary_group.add_argument(
        "-r",
        "--read_id_column",
        type=str,
        default=DEFAULT_READ_ID_COLUMN,
        help=f"Name of the read_id column in the summary. [{DEFAULT_READ_ID_COLUMN}]",
    )
    summary_group.add_argument(
        "-d",
        "--demux_columns",
        type=str,
        nargs="+",
        help="Names of summary columns demultiplex read_ids",
    )
    summary_group.add_argument(
        "-t",
        "--template",
        type=str,
        default=None,
        help="template string to generate output filenames "
        '(e.g. "mux-{mux}_barcode-{barcode}.pod5"). '
        "default is to concatenate all columns to values as shown in the example.",
    )
    summary_group.add_argument(
        "-T",
        "--ignore_incomplete_template",
        action="store_true",
        default=None,
        help="Suppress the exception raised if the --template string does not contain "
        "every --demux_columns key",
    )

    content_group = parser.add_argument_group("content settings")
    content_group.add_argument(
        "-M",
        "--missing_ok",
        action="store_true",
        help="Allow missing read_ids",
    )
    content_group.add_argument(
        "-D",
        "--duplicate_ok",
        action="store_true",
        help="Allow duplicate read_ids",
    )

    return parser


def parse_summary_mapping(
    summary_path: Path,
    filename_template: typing.Optional[str],
    demux_columns: typing.List[str],
    read_id_column: str = DEFAULT_READ_ID_COLUMN,
    ignore_incomplete_template: bool = False,
) -> typing.Dict[str, typing.Set[str]]:
    """
    Parse a summary file (tab-separated table) using pandas to create a mapping of
    output targets to read ids
    """
    if not filename_template:
        filename_template = create_default_filename_template(demux_columns)

    assert_filename_template(
        filename_template, demux_columns, ignore_incomplete_template
    )

    # Parse the summary using pandas asserting that usecols exists
    summary = pd.read_table(summary_path, usecols=demux_columns + [read_id_column])

    def group_name_to_dict(
        group_name: typing.Any, columns: typing.List[str]
    ) -> typing.Dict[str, str]:
        """Split group_name tuple/str into a dict by column name key"""
        if not isinstance(group_name, (str, int, float)):
            return {col: str(value).strip() for col, value in zip(columns, group_name)}
        return {columns[0]: str(group_name).strip()}

    mapping: typing.Dict[str, typing.Set[str]] = {}

    # Convert length 1 list to string to silence pandas warning
    demux_group_keys = demux_columns if len(demux_columns) > 1 else demux_columns[0]

    # Create groups by unique sets of demux_column values
    for group_name, df_group in summary.groupby(demux_group_keys):
        # Create filenames from the unique keys
        group_dict = group_name_to_dict(group_name, demux_columns)
        filename = filename_template.format(**group_dict)

        # Add all the read_ids to the mapping
        mapping[filename] = set(df_group[read_id_column].to_list())

    return mapping


def assert_filename_template(
    template: str, demux_columns: typing.List[str], ignore_incomplete_template: bool
) -> None:
    """
    Get the keys named in the template to assert that they exist in demux_columns
    """
    # Parse the template string to get the keywords
    # "{hello}_world_{name}" -> ["hello", "name"]
    template_keys = set(args[1] for args in Formatter().parse(template) if args[1])
    allowed_keys = set(demux_columns)

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


def create_default_filename_template(demux_columns: typing.List[str]) -> str:
    """Create the default filename template from the demux_columns selected"""
    default = "_".join(f"{col}-{{{col}}}" for col in demux_columns)
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
        json_data = json.load(_fh)
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


def demux_pod5s(
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


def main():
    """
    pod5_demux main program
    """
    parser = prepare_pod5_demux_argparser()
    args = parser.parse_args()

    if args.csv or args.json:
        mapping = parse_direct_mapping_targets(args.csv, args.json)

    elif args.summary:
        mapping = parse_summary_mapping(
            args.summary,
            args.template,
            args.demux_columns,
            args.read_id_column,
        )

    else:
        raise RuntimeError(
            "Arguments provided could not be used to generate a demux mapping."
        )

    demux_pod5s(
        inputs=args.inputs,
        output=args.output,
        mapping=mapping,
        missing_ok=args.missing_ok,
        duplicate_ok=args.duplicate_ok,
        force_overwrite=args.force_overwrite,
    )


if __name__ == "__main__":
    main()
