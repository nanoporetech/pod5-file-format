"""
Utilities for reading data out of POD5 files.
"""

import typing
from pathlib import Path

import pod5_format.pod5_format_pybind as pod5_bind

from . import reader_pyarrow


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

    basename = str(Path(file).with_suffix(""))
    signal_file = Path(basename + "_signal" + file.suffix)
    reads_file = Path(basename + "_reads" + file.suffix)

    return signal_file, reads_file


def open_combined_file(filename: Path) -> reader_pyarrow.FileReader:
    """
    Open a combined pod5 file for reading.

    Parameters
    ----------
    filename : Path
        The combined POD5 file to open.
    use_c_api : bool
        Use the direct C API to read the data, if false (the default) the pyarrow
        API is used to read the data.

    Returns
    -------
    A FileReader, with the passed paths files opened for reading.
    """
    reader = pod5_bind.open_combined_file(str(filename))
    if not reader:
        raise Exception(f"Failed to open reader: {pod5_bind.get_error_string()}")

    read_reader = reader_pyarrow.ArrowReaderHandle(
        filename, reader.get_combined_file_read_table_location()
    )
    signal_reader = reader_pyarrow.ArrowReaderHandle(
        filename, reader.get_combined_file_signal_table_location()
    )

    return reader_pyarrow.FileReader(reader, read_reader, signal_reader)


def open_split_file(file: Path, reads_file: Path = None) -> reader_pyarrow.FileReader:
    """
    Open a split pair of pod5 files for reading, one for signal data, one for read data.

    Parameters
    ----------
    file : Path
        Either the basename of the split pair - "my_files.pod5" will open
        pair "my_files_signal.pod5" and "my_files_reads.pod5",
        or the direct path to the signal file. if [reads_file] is None, file
        must be the basename for the split pair.
    reads_file : Path
        The name of the reads file in the split file pair.

    Returns
    -------
    A FileReader, with the passed paths files opened for reading.
    """

    signal_file = file
    if not reads_file:
        signal_file, reads_file = make_split_filename(file)

    reader = pod5_bind.open_split_file(str(signal_file), str(reads_file))
    if not reader:
        raise Exception(f"Failed to open reader: {pod5_bind.get_error_string()}")

    read_reader = reader_pyarrow.ArrowReaderHandle(reads_file)
    signal_reader = reader_pyarrow.ArrowReaderHandle(signal_file)

    return reader_pyarrow.FileReader(reader, read_reader, signal_reader)
