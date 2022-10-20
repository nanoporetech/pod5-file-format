"""
Tool for merging pod5 files
"""

import argparse
from pathlib import Path
import typing

import pod5_format as p5
import pod5_format.repack as p5_repack


def prepare_pod5_merge_argparser() -> argparse.ArgumentParser:
    """Create an argument parser for the pod5 merge tool"""

    parser = argparse.ArgumentParser(description="Merge multiple pod5 files")

    # Core arguments
    parser.add_argument(
        "inputs",
        type=Path,
        nargs="+",
        help="Pod5 filepaths to use as inputs",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output filepath",
    )
    parser.add_argument(
        "-f",
        "--force_overwrite",
        action="store_true",
        help="Overwrite destination file if it already exists",
    )
    parser.add_argument(
        "-D",
        "--duplicate_ok",
        action="store_true",
        help="Allow duplicate read_ids",
    )

    return parser


def assert_no_duplicate_reads(paths: typing.Iterable[Path]) -> None:
    """
    Raise AssertionError if we detect any duplicate read_ids in the pod5 files given.
    """
    read_ids = set()
    for path in paths:
        with p5.Reader(path) as reader:
            for read in reader.reads():
                if read.read_id in read_ids:
                    raise AssertionError(
                        "Duplicate read_id detected but --duplicate_ok not set"
                    )
                read_ids.add(read.read_id)


def merge_pod5s(
    inputs: typing.Iterable[Path],
    output: Path,
    duplicate_ok: bool = False,
    force_overwrite: bool = False,
) -> None:
    """
    Merge the an iterable of input pod5 paths into the specified output path
    """

    if output.exists():
        if force_overwrite:
            output.unlink()
        else:
            raise FileExistsError(
                f"Output files already exists and --force_overwrite not set. "
                f"Refusing to overwrite {output}."
            )

    if not output.parent.exists():
        output.parent.mkdir(parents=True, exist_ok=True)

    # Assert inputs exist
    non_existant_inputs = [path for path in inputs if not path.is_file()]
    if non_existant_inputs:
        raise FileExistsError(f"Some input(s) do not exist: {non_existant_inputs}")

    if not duplicate_ok:
        assert_no_duplicate_reads(inputs)

    # Open the output file writer
    with p5.Writer(output.absolute()) as writer:

        # Attach the writer to the repacker
        repacker = p5_repack.Repacker()
        repacker_output = repacker.add_output(writer)

        # Iterate over all input files opening a reader handle
        readers = [p5.Reader(path) for path in inputs]

        # Submit each reader handle to the repacker
        for reader in readers:
            repacker.add_all_reads_to_output(repacker_output, reader)

        # blocking wait for the repacker to complete merging inputs
        repacker.wait()

        # Close all the input handles
        for reader in readers:
            reader.close()

    return


def main():
    """
    pod5_merge main program
    """
    parser = prepare_pod5_merge_argparser()
    args = parser.parse_args()

    merge_pod5s(
        inputs=args.inputs,
        output=args.output,
        duplicate_ok=args.duplicate_ok,
        force_overwrite=args.force_overwrite,
    )


if __name__ == "__main__":
    main()
