"""
Utilities for reading data out of POD5 files.
"""

from pathlib import Path
from typing import Tuple

from pod5_format.pod5_types import PathOrStr


def make_split_filename(
    file: PathOrStr, assert_exists: bool = False
) -> Tuple[Path, Path]:
    """
    Find a sensible name for a split pod5 file pair, given a single destination filename.

    Parameters
    ----------
    file : os.PathLike, str
        The name for the file pair.
    assert_exists : bool
        If true, assert that both paths are existing files. Raises FileExistsError.

    Returns
    -------
        A tuple of paths, to the signal and read files respectively.
    """
    path = Path(file).absolute()
    basename = str(path.with_suffix(""))
    signal = Path(basename + "_signal" + path.suffix)
    reads = Path(basename + "_reads" + path.suffix)

    if assert_exists:
        if not signal.is_file() or not reads.is_file():
            raise FileExistsError(
                f"Could not find signal and reads paths from: {path}. Searched: "
                f"{signal} (exists: {signal.is_file()}) and {reads} "
                f"(exists: {reads.is_file()})"
            )

    return signal, reads
