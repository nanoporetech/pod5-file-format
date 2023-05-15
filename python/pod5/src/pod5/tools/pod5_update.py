"""
Tool for updating pod5 files to the latest available version
"""
from typing import Iterable
from pathlib import Path

from tqdm.auto import tqdm

import lib_pod5 as p5b

import pod5 as p5
from pod5.tools.parsers import prepare_pod5_update_argparser, run_tool
from pod5.tools.utils import (
    PBAR_DEFAULTS,
    assert_no_duplicate_filenames,
    collect_inputs,
)


def update_pod5(
    inputs: Iterable[Path],
    output: Path,
    force_overwrite: bool = False,
    recursive: bool = False,
):
    """
    Given a list of pod5 files, update their tables to the most recent version
    """
    if not output.exists():
        output.mkdir(parents=True, exist_ok=True)

    paths = collect_inputs(inputs, recursive=recursive, pattern="*.pod5")
    assert_no_duplicate_filenames(paths)

    exists = set(output / p.name for p in paths if Path(output / p.name).exists())

    if not paths.isdisjoint(exists):
        inout = [p.name for p in exists - paths]
        raise AssertionError(f"Cannot update inputs in-place. Found: {inout}")

    if not force_overwrite and exists:
        raise FileExistsError(
            f"{len(exists)} Output files already exists and --force-overwrite not set. "
            f"Found: {exists}"
        )
    else:
        for path in exists:
            path.unlink()

    pbar = tqdm(
        total=len(paths), desc="Updating", unit="File", leave=True, **PBAR_DEFAULTS
    )

    for path in paths:
        dest = output / path.name
        with p5.Reader(path) as reader:
            p5b.update_file(reader.inner_file_reader, str(dest))
        pbar.update()


def main():
    run_tool(prepare_pod5_update_argparser())


if __name__ == "__main__":
    main()
