from pathlib import Path
import typing


def make_split_filename(file: Path) -> typing.Tuple[Path, Path]:
    """
    Find a sensible name for a split pod5 file pair, given a single destination filename.

    Parameters
    ----------
    file : Path
        The name for the file pair.

    Returns
    -------
        A tuple of names, one for the signal and the other the reads file.
    """

    unsuffixed = Path(file).with_suffix("")
    signal_file = Path(str(unsuffixed) + "_signal" + file.suffix)
    reads_file = Path(str(unsuffixed) + "_reads" + file.suffix)

    return signal_file, reads_file
