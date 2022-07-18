"""
Utilities for reading data out of POD5 files.
"""

import typing
from pathlib import Path


def make_split_filename(
    file: Path, assert_exists: bool = False
) -> typing.Tuple[Path, Path]:
    """
    Find a sensible name for a split pod5 file pair, given a single destination filename.

    Parameters
    ----------
    `file` : `Path`
        The name for the file pair.
    `assert_exists` : `bool`
        If true, assert that both paths are existing files. Raises FileExistsError.

    Returns
    -------
        A tuple of Paths, one for the signal and the other the reads file.
    """

    basename = str(Path(file).with_suffix(""))
    signal = Path(basename + "_signal" + file.suffix)
    reads = Path(basename + "_reads" + file.suffix)

    if assert_exists:
        if not signal.is_file() or not reads.is_file():
            raise FileExistsError(
                f"Could not find signal and reads paths from: {file}. Searched: "
                f"{signal} (exists: {signal.is_file()}) and {reads} "
                f"(exists: {reads.is_file()})"
            )

    return signal, reads
