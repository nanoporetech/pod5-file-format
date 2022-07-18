"""
Utilities for managing file handles for the `Reader`
"""

import mmap
from pathlib import Path
import typing

import pyarrow as pa
import pod5_format.pod5_format_pybind as p5b
from pod5_format.api_utils import Pod5ApiException


class ReaderHandle:
    """Class for handling arrow file handles and memory view mapping"""

    def __init__(
        self, path: Path, location: typing.Optional[p5b.EmbeddedFileData] = None
    ) -> None:
        """
        Open an arrow file at the given path. If location is given perform the
        table splitting of combined arrow files into separate read and signal readers.
        """
        if not path.is_file():
            raise FileNotFoundError(f"Failed to open path: {path}")

        self._path = path
        self._location = location

        self._reader: typing.Optional[pa.ipc.RecordBatchFileReader] = None
        self._fh: typing.Optional[typing.IO] = None

        self._open_arrow_reader()

    @property
    def reader(self) -> pa.ipc.RecordBatchFileReader:
        """Return the pyarrow file reader object"""
        if self._reader is None:
            self._open_arrow_reader()

        if self._reader is not None:
            return self._reader

        raise RuntimeError(f"Could not open pyarrow reader: {p5b.get_error_string()}")

    def _open_arrow_reader(self) -> None:
        """
        Open the arrow file. If location is given take a non-copying slice of the
        file.
        """
        if self._reader is not None:
            return

        if self._location is None:
            self._reader = pa.ipc.open_file(self._path)
        else:
            self._fh = self._path.open("r")
            _mmap = mmap.mmap(self._fh.fileno(), length=0, access=mmap.ACCESS_READ)
            map_view = memoryview(_mmap)
            sub_file = map_view[
                self._location.offset : self._location.offset + self._location.length
            ]
            self._reader = pa.ipc.open_file(pa.BufferReader(sub_file))

    def close(self) -> None:
        """
        Cleanly close the open file handles and memory views.
        """
        if self._reader is not None:
            self._reader = None

        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "ReaderHandle":
        return self

    def __exit__(self, *exc_details) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


class ReaderHandleManager:
    """Class to conveniently manage the file handles used to access Pod5 data"""

    def __init__(
        self,
        file_reader: p5b.Pod5FileReader,
        read_reader: ReaderHandle,
        signal_reader: ReaderHandle,
    ):
        self._file_reader = file_reader
        self._read_reader = read_reader
        self._signal_reader = signal_reader

    @classmethod
    def from_combined(cls, combined_path: Path) -> "ReaderHandleManager":
        """Initialise a ReaderHandleManager from a combined pod5 path"""
        if not combined_path.is_file():
            raise FileNotFoundError(f"Failed to open combined_path: {combined_path}")

        reader = p5b.open_combined_file(str(combined_path))
        if not reader:
            raise Pod5ApiException(f"Failed to open reader: {p5b.get_error_string()}")

        read_reader = ReaderHandle(
            combined_path, reader.get_combined_file_read_table_location()
        )
        signal_reader = ReaderHandle(
            combined_path, reader.get_combined_file_signal_table_location()
        )

        return cls(reader, read_reader, signal_reader)

    @classmethod
    def from_split(cls, signal_path: Path, reads_path: Path) -> "ReaderHandleManager":
        """
        Initialise a ReaderHandleManager from a split pod5 signal and reads paths
        """
        if not signal_path.is_file():
            raise FileNotFoundError(f"Failed to open signal_path: {signal_path}")

        if not reads_path.is_file():
            raise FileNotFoundError(f"Failed to open reads_path: {reads_path}")

        reader = p5b.open_split_file(str(signal_path), str(reads_path))
        if not reader:
            raise Pod5ApiException(f"Failed to open reader: {p5b.get_error_string()}")

        read_reader = ReaderHandle(reads_path)
        signal_reader = ReaderHandle(signal_path)

        return cls(reader, read_reader, signal_reader)

    @property
    def file(self) -> p5b.Pod5FileReader:
        """Get the Pod5FileReader"""
        return self._file_reader

    @property
    def read(self) -> ReaderHandle:
        """Get the ReaderHandle for the read data"""
        return self._read_reader

    @property
    def signal(self) -> ReaderHandle:
        """Get the ReaderHandle for the signal data"""
        return self._signal_reader

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> "ReaderHandleManager":
        return self

    def __exit__(self, *exc_details) -> None:
        self.close()

    def close(self) -> None:
        """Close files handles"""
        if self._signal_reader is not None:
            self._signal_reader.close()
            self._signal_reader = None
        if self._read_reader is not None:
            self._read_reader.close()
            self._read_reader = None
        if self._file_reader is not None:
            self._file_reader.close()
            self._file_reader = None
