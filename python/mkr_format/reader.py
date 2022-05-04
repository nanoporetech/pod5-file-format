"""
Classes for reading data out of MKR files.
"""

import ctypes
from collections import namedtuple
from datetime import datetime, timezone
import mmap
from pathlib import Path
import typing
from uuid import UUID

import numpy
import pyarrow as pa

from . import c_api
from . import reader_c_api
from . import reader_pyarrow
from .api_utils import check_error
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

    def __init__(self, filename, reader, getter):
        location = c_api.EmbeddedFileData()
        getter(reader, ctypes.byref(location))

        self._file = open(str(filename), "r")
        map = mmap.mmap(
            self._file.fileno(),
            length=0,
            access=mmap.ACCESS_READ,
        )
        map_view = memoryview(map)
        sub_file = map_view[location.offset : location.offset + location.length]
        self.reader = pa.ipc.open_file(pa.BufferReader(sub_file))

    def __del__(self):
        self._file.close()


def open_combined_file(filename: Path, use_c_api=False):
    """
    Open a combined mkr file for reading.

    Parameters
    ----------
    filename : Path
        The combined MKR file to open.
    use_c_api : bool
        Use the direct C API to read the data, if false (the default) the pyarrow API is used to read the data.

    Returns
    -------
    A FileReader, with the passed paths files opened for reading.
    """
    reader = c_api.mkr_open_combined_file(str(filename).encode("utf-8"))
    if not reader:
        raise Exception(
            "Failed to open reader: " + c_api.mkr_get_error_string().decode("utf-8")
        )

    if use_c_api:
        return reader_c_api.FileReaderCApi(reader)
    else:
        read_reader = SubFileReader(
            filename, reader, c_api.mkr_get_combined_file_read_table_location
        )
        signal_reader = SubFileReader(
            filename, reader, c_api.mkr_get_combined_file_signal_table_location
        )

        check_error(c_api.mkr_close_and_free_reader(reader))
        reader = None

        return reader_pyarrow.FileReader(read_reader, signal_reader)


def open_split_file(file: Path, reads_file: Path = None, use_c_api=False):
    """
    Open a split pair of mkr files for reading, one for signal data, one for read data.

    Parameters
    ----------
    file : Path
        Either the basename of the split pair - "my_files.mkr" will open pair "my_files_signal.mkr" and "my_files_reads.mkr",
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

    reader = c_api.mkr_open_split_file(
        str(signal_file).encode("utf-8"), str(reads_file).encode("utf-8")
    )
    if not reader:
        raise Exception(
            "Failed to open reader: " + c_api.mkr_get_error_string().decode("utf-8")
        )

    if use_c_api:
        return reader_c_api.FileReaderCApi(reader)
    else:
        check_error(c_api.mkr_close_and_free_reader(reader))
        reader = None

        class ArrowReader:
            def __init__(self, reader):
                self.reader = pa.ipc.open_file(reader)

        read_reader = ArrowReader(reads_file)
        signal_reader = ArrowReader(signal_file)
        return reader_pyarrow.FileReader(read_reader, signal_reader)
