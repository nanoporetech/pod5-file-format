"""
Classes for reading data out of POD5 files.
"""


from collections import namedtuple
from datetime import datetime, timezone
import mmap
from pathlib import Path
import typing
from uuid import UUID

import numpy
import pyarrow as pa

import pod5_format.pod5_format_pybind

from . import reader_pyarrow
from .api_utils import EndReason
from .utils import make_split_filename
from .reader_utils import (
    PoreData,
    CalibrationData,
    EndReasonData,
    RunInfoData,
    SignalRowInfo,
)


class SubFileReader:
    """
    Internal util for slicing an open file without copy.
    """

    def __init__(self, filename, location):
        self._file = open(str(filename), "r")
        mapped_data = mmap.mmap(
            self._file.fileno(),
            length=0,
            access=mmap.ACCESS_READ,
        )
        map_view = memoryview(mapped_data)
        sub_file = map_view[location.offset : location.offset + location.length]
        self.reader = pa.ipc.open_file(pa.BufferReader(sub_file))

    def __del__(self):
        if self._file is not None:
            self._file.close()

    def close(self):
        self.reader = None
        if self._file is not None:
            self._file.close()


def open_combined_file(filename: Path, use_c_api=False):
    """
    Open a combined pod5 file for reading.

    Parameters
    ----------
    filename : Path
        The combined POD5 file to open.
    use_c_api : bool
        Use the direct C API to read the data, if false (the default) the pyarrow API is used to read the data.

    Returns
    -------
    A FileReader, with the passed paths files opened for reading.
    """
    reader = pod5_format.pod5_format_pybind.open_combined_file(str(filename))
    if not reader:
        raise Exception(
            "Failed to open reader: " + c_api.pod5_get_error_string().decode("utf-8")
        )

    read_reader = SubFileReader(
        filename, reader.get_combined_file_read_table_location()
    )
    signal_reader = SubFileReader(
        filename, reader.get_combined_file_signal_table_location()
    )

    return reader_pyarrow.FileReader(reader, read_reader, signal_reader)


def open_split_file(file: Path, reads_file: Path = None, use_c_api=False):
    """
    Open a split pair of pod5 files for reading, one for signal data, one for read data.

    Parameters
    ----------
    file : Path
        Either the basename of the split pair - "my_files.pod5" will open pair "my_files_signal.pod5" and "my_files_reads.pod5",
        or the direct path to the signal file. if [reads_file] is None, file must be the basename for the split pair.
    reads_file : Path
        The name of the reads file in the split file pair.
    use_c_api : bool
        Use the direct C API to read the data, if false (the default) the pyarrow API is used to read the data.

    Returns
    -------
    A FileReader, with the passed paths files opened for reading.
    """

    signal_file = file
    if not reads_file:
        signal_file, reads_file = make_split_filename(file)

    reader = pod5_format.pod5_format_pybind.open_split_file(
        str(signal_file), str(reads_file)
    )
    if not reader:
        raise Exception(
            "Failed to open reader: " + c_api.pod5_get_error_string().decode("utf-8")
        )

    class ArrowReader:
        def __init__(self, reader):
            self.reader = pa.ipc.open_file(reader)

        def close(self):
            self.reader = None

    read_reader = ArrowReader(reads_file)
    signal_reader = ArrowReader(signal_file)
    return reader_pyarrow.FileReader(reader, read_reader, signal_reader)
