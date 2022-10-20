"""
Utilities for managing file handles for the `Reader`
"""

import mmap
from pathlib import Path
from typing import IO, Optional
from pod5_format.pod5_types import PathOrStr

import pyarrow as pa
import pod5_format.pod5_format_pybind as p5b
from pod5_format.api_utils import Pod5ApiException


class ReaderHandle:
    """Class for managing arrow file handles and memory view mapping"""

    def __init__(
        self, path: Path, location: Optional[p5b.EmbeddedFileData] = None
    ) -> None:
        """
        Open an arrow file at the given `path`. If location is given, perform the
        pyarrow table splitting of a combined arrow file into separate read and
        signal readers.

        Parameters
        ----------
        path : pathlib.Path
            Path to an existing file to open for use by a :py:class:`Reader`
        location : Optional, pod5_format.pod5_format_pybind.EmbeddedFileData
            Location data for how a combined pod5 file should be spit in memory

        Raises
        ------
        FileNotFoundError
            If `path` is not an existing file
        RuntimeError
            If the `pyarrow.lib.RecordBatchFileReader` could not be opened
        """
        if not path.is_file():
            raise FileNotFoundError(f"Failed to open path: {path}")

        self._path = path
        self._location = location
        if self._location:
            self._path = Path(location.file_path)

        self._reader: Optional[pa.ipc.RecordBatchFileReader] = None
        self._fh: Optional[IO] = None

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
        run_info_reader: ReaderHandle,
        read_reader: ReaderHandle,
        signal_reader: ReaderHandle,
    ):
        """
        Initialise a handle manager

        Note
        ----
        Use :py:meth:`from_combined` or :py:meth:`from_split` to open
        reader handles for combined or split pod5 files respectively
        """
        self._file_reader: Optional[p5b.Pod5FileReader] = file_reader
        self._read_reader: Optional[ReaderHandle] = read_reader
        self._run_info_reader: Optional[ReaderHandle] = run_info_reader
        self._signal_reader: Optional[ReaderHandle] = signal_reader

    @classmethod
    def from_combined(cls, combined_path: PathOrStr) -> "ReaderHandleManager":
        """
        Factory method to instantiate a `ReaderHandleManager` from a combined
        pod5 path.

        Parameters
        ----------
        combined_path : os.PathLike, str
            Path to an existing combined pod5 file

        Returns
        -------
        :py:class:`ReaderHandleManager`

        Raises
        ------
        FileNotFoundError
            If there is no file at `combined_path`
        Pod5ApiException
            If there is an error opening the file reader
        """
        combined_path = Path(combined_path)

        if not combined_path.is_file():
            raise FileNotFoundError(f"Failed to open combined_path: {combined_path}")

        reader = p5b.open_combined_file(str(combined_path))
        if not reader:
            raise Pod5ApiException(f"Failed to open reader: {p5b.get_error_string()}")

        run_info_reader = ReaderHandle(
            combined_path, reader.get_combined_file_run_info_table_location()
        )
        read_reader = ReaderHandle(
            combined_path, reader.get_combined_file_read_table_location()
        )
        signal_reader = ReaderHandle(
            combined_path, reader.get_combined_file_signal_table_location()
        )

        return cls(reader, run_info_reader, read_reader, signal_reader)

    @classmethod
    def from_split(
        cls, signal_path: PathOrStr, reads_path: PathOrStr
    ) -> "ReaderHandleManager":
        """
        Factory method to instantiate a `ReaderHandleManager` from a pair of split
        pod5 paths.

        Parameters
        ----------
        signal_path : os.PathLike, str
            Path to an existing signal pod5 file
        reads_path : os.PathLike, str
            Path to an existing reads pod5 file

        Returns
        -------
        :py:class:`ReaderHandleManager`

        Raises
        ------
        FileNotFoundError
            If either `signal_path` or `reads_path` do not exist
        Pod5ApiException
            If there is an error opening the file reader
        """
        signal_path = Path(signal_path)
        reads_path = Path(reads_path)

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
        """
        Get the c_api `Pod5FileReader`

        Raises
        ------
        Pod5ApiException
            If the reader has been closed
        """
        if self._file_reader is None:
            raise Pod5ApiException("Pod5FileReader has been closed")
        return self._file_reader

    @property
    def run_info(self) -> ReaderHandle:
        """
        Get the :py:class:`ReaderHandle` for the run info data

        Returns
        -------
        :py:class:`ReaderHandle`

        Raises
        ------
        Pod5ApiException
            If the reader has been closed
        """
        if self._run_info_reader is None:
            raise Pod5ApiException("ReadHandle (Run Info) has been closed")
        return self._run_info_reader

    @property
    def read(self) -> ReaderHandle:
        """
        Get the :py:class:`ReaderHandle` for the read data

        Returns
        -------
        :py:class:`ReaderHandle`

        Raises
        ------
        Pod5ApiException
            If the reader has been closed
        """
        if self._read_reader is None:
            raise Pod5ApiException("ReadHandle (Read) has been closed")
        return self._read_reader

    @property
    def signal(self) -> ReaderHandle:
        """
        Get the :py:class:`ReaderHandle` for the signal data

        Returns
        -------
        :py:class:`ReaderHandle`

        Raises
        ------
        Pod5ApiException
            If the reader has been closed
        """
        if self._signal_reader is None:
            raise Pod5ApiException("ReadHandle (Signal) has been closed")
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
        if self._run_info_reader is not None:
            self._run_info_reader.close()
            self._run_info_reader = None
        if self._read_reader is not None:
            self._read_reader.close()
            self._read_reader = None
        if self._file_reader is not None:
            self._file_reader.close()
            self._file_reader = None
