"""
Tool for updating pod5 files to the latest available version
"""
import sys
import typing
from pathlib import Path

import lib_pod5 as p5b

import pod5 as p5
from pod5.tools.parsers import prepare_pod5_update_argparser, run_tool


def update_pod5(inputs: typing.List[Path], output: Path, force_overwrite: bool):
    """
    Given a list of pod5 files, update their tables to the most recent version
    """
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
    run_tool(prepare_pod5_update_argparser())


if __name__ == "__main__":
    main()
