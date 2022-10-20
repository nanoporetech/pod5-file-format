""" 
Tool for updating pod5 files to the latest available version
"""
import argparse
from pathlib import Path

import sys
import typing

import pod5_format as p5
import pod5_format.pod5_format_pybind as p5b


def update(inputs: typing.List[Path], output: Path, force_overwrite: bool):
    print(f"Updating inputs {' '.join(str(i) for i in inputs)} into {output}")

    if not output.exists():
        output.mkdir(parents=True, exist_ok=True)

    for input_path in inputs:
        reader = p5.Reader(input_path)
        dest_path = output / input_path.name
        if dest_path.exists() and not force_overwrite:
            print(f"{dest_path} already exists, not overwriting", file=sys.stderr)
            continue
        p5b.update_file(reader.inner_file_reader, str(dest_path))


def main():
    parser = argparse.ArgumentParser(
        "Update a pod5 files to the latest available version"
    )

    parser.add_argument(
        "input", type=Path, nargs="+", help="Input pod5 file(s) to update"
    )
    parser.add_argument("output", type=Path, help="Output path for pod5 files")

    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite destination files"
    )

    args = parser.parse_args()

    update(args.input, args.output, args.overwrite)


if __name__ == "__main__":
    main()
