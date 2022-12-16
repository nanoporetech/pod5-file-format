"""
Tools for accessing POD5 data from PyArrow files
"""

import enum
import mmap
from collections import namedtuple
from dataclasses import fields
from functools import total_ordering
from pathlib import Path
from typing import (
    Collection,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
from uuid import UUID

import lib_pod5 as p5b
import numpy as np
import numpy.typing as npt
import packaging.version
import pyarrow as pa

from pod5.pod5_types import (
    Calibration,
    EndReason,
    EndReasonEnum,
    PathOrStr,
    Pore,
    Read,
    RunInfo,
    ShiftScalePair,
)

from .api_utils import Pod5ApiException, pack_read_ids
from .signal_tools import vbz_decompress_signal, vbz_decompress_signal_into

ReadRecordV3Columns = namedtuple(
    "ReadRecordV3Columns",
    [
        "read_id",
        "read_number",
        "start",
        "channel",
        "well",
        "median_before",
        "pore_type",
        "calibration_offset",
        "calibration_scale",
        "end_reason",
        "end_reason_forced",
        "run_info",
        "signal",
        "num_minknow_events",
        "tracked_scaling_scale",
        "tracked_scaling_shift",
        "predicted_scaling_scale",
        "predicted_scaling_shift",
        "num_reads_since_mux_change",
        "time_since_mux_change",
        "num_samples",
    ],
)


@total_ordering
class ReadTableVersion(enum.Enum):
    """Version of read table"""

    V3 = 3

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

    def __eq__(self, other):
        if self.__class__ is other.__class__:
            return self.value == other.value
        return NotImplemented


Signal = namedtuple("Signal", ["signal", "samples"])
SignalRowInfo = namedtuple(
    "SignalRowInfo",
    ["batch_index", "batch_row_index", "sample_count", "byte_count"],
)


class ReadRecord:
    """
    Represents the data for a single read from a pod5 record.
    """

    def __init__(
        self,
        reader: "Reader",
        batch: "ReadRecordBatch",
        row: int,
        batch_signal_cache=None,
        selected_batch_index=None,
    ):
        """ """
        self._reader = reader
        self._batch = batch
        self._row = row
        self._batch_signal_cache = batch_signal_cache
        self._selected_batch_index = selected_batch_index

    @property
    def read_id(self) -> UUID:
        """
        Get the unique read identifier for the read as a `UUID`.
        """
        return UUID(bytes=self._batch.columns.read_id[self._row].as_py())

    @property
    def read_number(self) -> int:
        """
        Get the integer read number of the read.
        """
        return self._batch.columns.read_number[self._row].as_py()

    @property
    def start_sample(self) -> int:
        """
        Get the absolute sample which the read started.
        """
        return self._batch.columns.start[self._row].as_py()

    @property
    def num_samples(self) -> int:
        """
        Get the number of samples in the reads signal data.
        """
        return self._batch.columns.num_samples[self._row].as_py()

    @property
    def median_before(self) -> float:
        """
        Get the median before level (in pico amps) for the read.
        """
        return self._batch.columns.median_before[self._row].as_py()

    @property
    def num_minknow_events(self) -> float:
        """
        Find the number of minknow events in the read.
        """
        return self._batch.columns.num_minknow_events[self._row].as_py()

    @property
    def tracked_scaling(self) -> ShiftScalePair:
        """
        Find the tracked scaling value in the read.
        """
        return ShiftScalePair(
            self._batch.columns.tracked_scaling_shift[self._row].as_py(),
            self._batch.columns.tracked_scaling_scale[self._row].as_py(),
        )

    @property
    def predicted_scaling(self) -> ShiftScalePair:
        """
        Find the predicted scaling value in the read.
        """
        return ShiftScalePair(
            self._batch.columns.predicted_scaling_shift[self._row].as_py(),
            self._batch.columns.predicted_scaling_scale[self._row].as_py(),
        )

    @property
    def num_reads_since_mux_change(self) -> int:
        """
        Number of selected reads since the last mux change on this reads channel.
        """
        return self._batch.columns.num_reads_since_mux_change[self._row].as_py()

    @property
    def time_since_mux_change(self) -> int:
        """
        Time in seconds since the last mux change on this reads channel.
        """
        return self._batch.columns.time_since_mux_change[self._row].as_py()

    @property
    def pore(self) -> Pore:
        """
        Get the pore data associated with the read.
        """
        return Pore(
            self._batch.columns.channel[self._row].as_py(),
            self._batch.columns.well[self._row].as_py(),
            self._batch.columns.pore_type[self._row].as_py(),
        )

    @property
    def calibration(self) -> Calibration:
        """
        Get the calibration data associated with the read.
        """
        return Calibration(
            self._batch.columns.calibration_offset[self._row].as_py(),
            self._batch.columns.calibration_scale[self._row].as_py(),
        )

    @property
    def calibration_digitisation(self) -> int:
        """
        Get the digitisation value used by the sequencer.

        Intended to assist workflows ported from legacy file formats.
        """
        return self.run_info.adc_max - self.run_info.adc_min + 1

    @property
    def calibration_range(self) -> float:
        """
        Get the calibration range value.

        Intended to assist workflows ported from legacy file formats.
        """
        return self.calibration.scale * self.calibration_digitisation

    @property
    def end_reason(self) -> EndReason:
        """
        Get the end reason data associated with the read.
        """
        return EndReason(
            reason=EndReasonEnum[
                self._batch.columns.end_reason[self._row].as_py().upper()
            ],
            forced=self._batch.columns.end_reason_forced[self._row].as_py(),
        )

    @property
    def run_info(self) -> RunInfo:
        """
        Get the run info data associated with the read.
        """
        return self._reader._lookup_run_info(self._batch, self._row)

    @property
    def end_reason_index(self) -> int:
        """
        Get the dictionary index of the end reason data associated with the read.
        This property is the same as the EndReason enumeration value.
        """
        return self._batch.columns.end_reason[self._row].index.as_py()

    @property
    def run_info_index(self) -> int:
        """
        Get the dictionary index of the run info data associated with the read.
        """
        return self._batch.columns.run_info[self._row].index.as_py()

    @property
    def sample_count(self) -> int:
        """
        Get the number of samples in the reads signal data.
        """
        return self.num_samples

    @property
    def byte_count(self) -> int:
        """
        Get the number of bytes used to store the reads data.
        """
        return sum(r.byte_count for r in self.signal_rows)

    @property
    def has_cached_signal(self) -> bool:
        """
        Get if cached signal is available for this read.
        """
        return self._batch_signal_cache

    @property
    def signal(self) -> npt.NDArray[np.int16]:
        """
        Get the full signal for the read.

        Returns
        -------
        numpy.ndarray[int16]
            A numpy array of signal data with int16 type.
        """
        if self.has_cached_signal:
            if self._selected_batch_index is not None:
                return self._batch_signal_cache[self._selected_batch_index]
            return self._batch_signal_cache[self._row]

        rows = self._batch.columns.signal[self._row]
        batch_data = [self._find_signal_row_index(r.as_py()) for r in rows]
        sample_counts = []
        for batch, _, batch_row_index in batch_data:
            sample_counts.append(batch.samples[batch_row_index].as_py())

        output = np.empty(dtype=np.int16, shape=(sum(sample_counts),))
        current_sample_index = 0

        for i, (batch, _, batch_row_index) in enumerate(batch_data):
            signal = batch.signal
            current_row_count = sample_counts[i]
            output_slice = output[
                current_sample_index : current_sample_index + current_row_count
            ]
            if self._reader.is_vbz_compressed:
                vbz_decompress_signal_into(
                    memoryview(signal[batch_row_index].as_buffer()), output_slice
                )
            else:
                output_slice[:] = signal.to_numpy()
            current_sample_index += current_row_count
        return output

    @property
    def signal_pa(self) -> npt.NDArray[np.float32]:
        """
        Get the full signal for the read, calibrated in pico amps.

        Returns
        -------
        numpy.ndarray[float32]
            A numpy array of signal data in pico amps with float32 type.
        """
        return self.calibrate_signal_array(self.signal)

    def signal_for_chunk(self, index: int) -> npt.NDArray[np.int16]:
        """
        Get the signal for a given chunk of the read.

        Returns
        -------
        numpy.ndarray[int16]
            A numpy array of signal data with int16 type for the specified chunk.
        """
        # signal_rows can be used to find details of the signal chunks.
        chunk_abs_row_index = self._batch.columns.signal[self._row][index]
        return self._get_signal_for_row(chunk_abs_row_index.as_py())

    @property
    def signal_rows(self) -> List[SignalRowInfo]:
        """
        Get all signal rows for the read

        Returns
        -------
        list[SignalRowInfo]
            A list of signal row data (as SignalRowInfo) in the read.
        """

        def map_signal_row(sig_row):
            sig_row = sig_row.as_py()

            batch, batch_index, batch_row_index = self._find_signal_row_index(sig_row)
            return SignalRowInfo(
                batch_index,
                batch_row_index,
                batch.samples[batch_row_index].as_py(),
                len(batch.signal[batch_row_index].as_buffer()),
            )

        return [map_signal_row(r) for r in self._batch.columns.signal[self._row]]

    def calibrate_signal_array(
        self, signal_array_adc: npt.NDArray[np.int16]
    ) -> npt.NDArray[np.float32]:
        """
        Transform an array of int16 signal data from ADC space to pA.

        Returns
        -------
        A numpy array of signal data with float32 type.
        """
        offset = np.float32(self.calibration.offset)
        scale = np.float32(self.calibration.scale)
        return (signal_array_adc + offset) * scale

    def _find_signal_row_index(self, signal_row: int) -> Tuple[Signal, int, int]:
        """
        Map from a signal_row to a Signal, batch index and row index within that batch.

        Returns
        -------
        A Tuple containing the `Signal` and its `batch_index` and `row_index`
        """
        sig_row_count: int = self._reader.signal_batch_row_count
        sig_batch_idx: int = signal_row // sig_row_count
        sig_batch = self._reader._get_signal_batch(sig_batch_idx)
        batch_row_idx: int = signal_row - (sig_batch_idx * sig_row_count)

        return sig_batch, sig_batch_idx, batch_row_idx

    def _get_signal_for_row(self, signal_row: int) -> npt.NDArray[np.int16]:
        """
        Get the signal data for a given absolute signal row index

        Returns
        -------
        A numpy array of signal data with int16 type.
        """
        batch, _, batch_row_index = self._find_signal_row_index(signal_row)

        signal = batch.signal
        if self._reader.is_vbz_compressed:
            sample_count = batch.samples[batch_row_index].as_py()
            return vbz_decompress_signal(
                memoryview(signal[batch_row_index].as_buffer()), sample_count
            )

        return signal.to_numpy()

    def to_read(self) -> Read:
        """
        Create a mutable :py:class:`pod5.pod5_types.Read` from this
        :py:class:`ReadRecord` instance.

        Returns
        -------
            :py:class:`pod5.pod5_types.Read`
        """
        return Read(
            read_id=self.read_id,
            pore=self.pore,
            calibration=self.calibration,
            median_before=self.median_before,
            end_reason=self.end_reason,
            read_number=self.read_number,
            run_info=self.run_info,
            start_sample=self.start_sample,
            signal=self.signal,
        )


class ReadRecordBatch:
    """
    Read data for a batch of reads.
    """

    def __init__(self, reader: "Reader", batch: pa.RecordBatch):
        """ """

        self._reader: "Reader" = reader
        self._batch: pa.RecordBatch = batch

        self._signal_cache: Optional[p5b.Pod5SignalCacheBatch] = None
        self._selected_batch_rows: Optional[Iterable[int]] = None
        self._columns: Optional[ReadRecordV3Columns] = None

    @property
    def columns(self) -> ReadRecordV3Columns:
        """Return the data from this batch as a ReadRecordColumns instance"""
        if self._columns is None:
            self._columns = ReadRecordV3Columns(
                *[
                    self._batch.column(name)
                    for name in self._reader._columns_type._fields
                ]
            )
        return self._columns

    def set_cached_signal(self, signal_cache: p5b.Pod5SignalCacheBatch) -> None:
        """Set the signal cache"""
        self._signal_cache = signal_cache

    def set_selected_batch_rows(self, selected_batch_rows: Iterable[int]) -> None:
        """Set the selected batch rows"""
        self._selected_batch_rows = selected_batch_rows

    def reads(self) -> Generator[ReadRecord, None, None]:
        """
        Iterate all reads in this batch.

        Yields
        ------
        ReadRecord
            ReadRecord instances in the file.
        """

        signal_cache = None
        if self._signal_cache and self._signal_cache.samples:
            signal_cache = self._signal_cache.samples

        if self._selected_batch_rows is not None:
            for idx, row in enumerate(self._selected_batch_rows):
                yield ReadRecord(
                    self._reader,
                    self,
                    row,
                    batch_signal_cache=signal_cache,
                    selected_batch_index=idx,
                )
        else:
            for i in range(self.num_reads):
                yield ReadRecord(self._reader, self, i, batch_signal_cache=signal_cache)

    def get_read(self, row: int) -> ReadRecord:
        """Get the ReadRecord at row index"""
        return ReadRecord(self._reader, self, row)

    @property
    def num_reads(self) -> int:
        """Return the number of rows in this RecordBatch"""
        return self._batch.num_rows

    @property
    def read_id_column(self):
        """
        Get the column of read ids for this batch
        """
        if self._selected_batch_rows is not None:
            return self.columns.read_id.take(self._selected_batch_rows)
        return self.columns.read_id

    @property
    def read_number_column(self):
        """
        Get the column of read numbers for this batch
        """
        if self._selected_batch_rows is not None:
            return self.columns.read_number.take(self._selected_batch_rows)
        return self.columns.read_number

    @property
    def cached_sample_count_column(self) -> npt.NDArray[np.uint64]:
        """
        Get the sample counts from the cached signal data
        """
        if not self._signal_cache:
            raise RuntimeError("No cached signal data available")
        return self._signal_cache.sample_count

    @property
    def cached_samples_column(self) -> List[npt.NDArray[np.int16]]:
        """
        Get the samples column from the cached signal data
        """
        if not self._signal_cache:
            raise RuntimeError("No cached signal data available")
        return self._signal_cache.samples


class ArrowTableHandle:
    """Class for managing arrow file handles and memory view mapping of tables"""

    def __init__(self, location: p5b.EmbeddedFileData) -> None:
        """
        Open a pod5 file at the given `path` and use the location data to load
        an arrow table (e.g. signal table)

        Parameters
        ----------
        location : lib_pod5.pod5_format_pybind.EmbeddedFileData
            Location data for how a pod5 file should be spit in memory to read a table.
            This is returned from p5b.Pod5FileReader.get_file_X_location methods

        Raises
        ------
        Pod5ApiException
            If handle could not be opened
        """

        # The location data is passed from the p5b.Pod5FileReader.get_file_X_location
        # methods
        self._location = location
        self._path = Path(self._location.file_path)

        # Open the file
        self._fh = self._path.open("r")

        # Create a memory view of the file and select the region for the table
        _mmap = mmap.mmap(self._fh.fileno(), length=0, access=mmap.ACCESS_READ)
        map_view = memoryview(_mmap)
        arrow_table_view = map_view[
            self._location.offset : self._location.offset + self._location.length
        ]

        # Open the table
        try:
            self._reader = pa.ipc.open_file(pa.BufferReader(arrow_table_view))
        except pa.ArrowInvalid as exc:
            raise Pod5ApiException(f"Failed to open ArrowTable: {self._path}") from exc

    @property
    def reader(self) -> pa.ipc.RecordBatchFileReader:
        """Return the pyarrow file reader object"""
        if self._reader is not None:
            return self._reader

        raise RuntimeError(f"Could not open pyarrow reader: {p5b.get_error_string()}")

    def close(self) -> None:
        """
        Cleanly close the open file handles and memory views.
        """
        self._reader = None
        self._fh.close()

    def __enter__(self) -> "ArrowTableHandle":
        return self

    def __exit__(self, *exc_details) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()


class Reader:
    """
    The base reader for POD5 data
    """

    def __init__(self, path: PathOrStr):
        """
        Open a pod5 filepath for reading
        """

        self._path = Path(path).absolute()

        (
            self._file_reader,
            self._read_handle,
            self._run_info_handle,
            self._signal_handle,
        ) = self._open_arrow_table_handles(self._path)

        schema_metadata = self.read_table.schema.metadata
        self._file_identifier = UUID(
            schema_metadata[b"MINKNOW:file_identifier"].decode("utf-8")
        )
        self._writing_software = schema_metadata[b"MINKNOW:software"].decode("utf-8")
        writing_version_str = schema_metadata[b"MINKNOW:pod5_version"].decode("utf-8")
        writing_version = packaging.version.parse(writing_version_str)

        self._columns_type = ReadRecordV3Columns
        self._reads_table_version = ReadTableVersion.V3

        self._file_version = writing_version
        self._file_version_pre_migration = (
            self._file_reader.get_file_version_pre_migration()
        )

        # Warning: The cached signal maintains an open file handle. So ensure that
        # this dictionary is cleared before closing.
        self._cached_signal_batches: Dict[int, Signal] = {}
        self._cached_run_infos: Dict[str, RunInfo] = {}

        self._is_vbz_compressed: Optional[bool] = None
        self._signal_batch_row_count: Optional[int] = None

    @staticmethod
    def _open_arrow_table_handles(
        path: Path,
    ) -> Tuple[
        p5b.Pod5FileReader, ArrowTableHandle, ArrowTableHandle, ArrowTableHandle
    ]:
        """Open handles to the underlying arrow tables within this pod5 file"""
        if not path.is_file():
            raise FileNotFoundError(f"Failed to open pod5 file at: {path}")

        file_reader = p5b.open_file(str(path))
        if not file_reader:
            raise Pod5ApiException(
                f"Failed to open reader for {path} Reason: {p5b.get_error_string()}"
            )

        read_handle = ArrowTableHandle(file_reader.get_file_read_table_location())
        run_info_handle = ArrowTableHandle(
            file_reader.get_file_run_info_table_location()
        )
        signal_handle = ArrowTableHandle(file_reader.get_file_signal_table_location())

        return file_reader, read_handle, run_info_handle, signal_handle

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, *exc_details) -> None:
        self.close()

    def __iter__(self) -> Generator[ReadRecord, None, None]:
        """Iterate over all reads"""
        yield from self.reads()

    def close(self) -> None:
        """Close files handles"""
        if self._read_handle is not None:
            self._read_handle.close()
            self._read_handle = None

        if self._run_info_handle is not None:
            self._run_info_handle.close()
            self._run_info_handle = None

        if self._signal_handle is not None:
            self._signal_handle.close()
            self._signal_handle = None

        if self._file_reader is not None:
            self._file_reader.close()
            self._file_reader = None

        # Explicitly clear this dictionary to close file handles used in cache
        self._cached_signal_batches = {}

    @property
    def path(self) -> Path:
        """Return the path to this pod5 file"""
        return self._path

    @property
    def inner_file_reader(self) -> p5b.Pod5FileReader:
        """Access the inner c_api Pod5FileReader - use with caution"""
        if self._file_reader is None:
            raise RuntimeError("Pod5FileReader has been closed!")
        return self._file_reader

    @property
    def read_table(self) -> pa.ipc.RecordBatchFileReader:
        """Access the pod5 read table"""
        if self._read_handle is None:
            raise RuntimeError("ArrowTableHandle has been closed!")
        return self._read_handle.reader

    @property
    def run_info_table(self) -> pa.ipc.RecordBatchFileReader:
        """Access the pod5 run_info table"""
        if self._run_info_handle is None:
            raise RuntimeError("ArrowTableHandle has been closed!")
        return self._run_info_handle.reader

    @property
    def signal_table(self) -> pa.ipc.RecordBatchFileReader:
        """Access the pod5 signal table - use with caution"""
        if self._signal_handle is None:
            raise RuntimeError("ArrowTableHandle has been closed!")
        return self._signal_handle.reader

    @property
    def file_version(self) -> packaging.version.Version:
        return self._file_version

    @property
    def file_version_pre_migration(self) -> packaging.version.Version:
        return self._file_version_pre_migration

    @property
    def writing_software(self) -> str:
        return self._writing_software

    @property
    def file_identifier(self) -> UUID:
        return self._file_identifier

    @property
    def reads_table_version(self) -> ReadTableVersion:
        return self._reads_table_version

    @property
    def is_vbz_compressed(self) -> bool:
        """Return if this file's signal is compressed"""
        if self._is_vbz_compressed is None:
            self._is_vbz_compressed = self.signal_table.schema.field(
                "signal"
            ).type.equals(pa.large_binary())
        return self._is_vbz_compressed

    @property
    def signal_batch_row_count(self) -> int:
        """Return signal batch row count"""
        if self._signal_batch_row_count is None:
            if self.signal_table.num_record_batches > 0:
                self._signal_batch_row_count = self.signal_table.get_batch(0).num_rows
            else:
                self._signal_batch_row_count = 0
        return self._signal_batch_row_count

    @property
    def batch_count(self) -> int:
        """
        Find the number of read batches available in the file.
        """
        return self.read_table.num_record_batches

    def get_batch(self, index: int) -> ReadRecordBatch:
        """
        Get a read batch in the file.

        Returns
        -------
        :py:class:`ReadRecordBatch`
            The requested batch as a ReadRecordBatch.
        """
        return ReadRecordBatch(self, self.read_table.get_batch(index))

    def read_batches(
        self,
        selection: Optional[List[str]] = None,
        batch_selection: Optional[Iterable[int]] = None,
        missing_ok: bool = False,
        preload: Optional[Set[str]] = None,
    ) -> Generator[ReadRecordBatch, None, None]:
        """
        Iterate batches in the file, optionally selecting certain rows.

        Parameters
        ----------
        selection : iterable[str]
            The read ids to walk in the file.
        batch_selection : iterable[int]
            The read batches to walk in the file.
        missing_ok : bool
            If selection contains entries not found in the file, an error will be raised.
        preload : set[str]
            Columns to preload - "samples" and "sample_count" are valid values

        Returns
        -------
        An iterable of :py:class:`ReadRecordBatch` in the file.
        """
        if selection is not None:
            assert not batch_selection
            yield from self._select_read_batches(
                selection, missing_ok=missing_ok, preload=preload
            )
        elif batch_selection is not None:
            assert not selection
            yield from self._read_some_batches(batch_selection, preload=preload)
        else:
            yield from self._reads_batches(preload=preload)

    def reads(
        self,
        selection: Optional[Iterable[str]] = None,
        missing_ok: bool = False,
        preload: Optional[Set[str]] = None,
    ) -> Generator[ReadRecord, None, None]:
        """
        Iterate reads in the file, optionally filtering for certain read ids.

        Parameters
        ----------
        selection : iterable[str]
            The read ids to walk in the file.
        missing_ok : bool
            If selection contains entries not found in the file, an error will be raised.
        preload : set[str]
            Columns to preload - "samples" and "sample_count" are valid values

        Returns
        -------
        An iterable of :py:class:`ReadRecord` in the file.
        """
        if selection is None:
            yield from self._reads(preload=preload)
        else:
            yield from self._select_reads(
                list(selection), missing_ok=missing_ok, preload=preload
            )

    def _reads(
        self, preload: Optional[Set[str]] = None
    ) -> Generator[ReadRecord, None, None]:
        """Generate all reads"""
        for batch in self.read_batches(preload=preload):
            for read in batch.reads():
                yield read

    def _select_reads(
        self,
        selection: List[str],
        missing_ok: bool = False,
        preload: Optional[Set[str]] = None,
    ) -> Generator[ReadRecord, None, None]:
        """Generate selected reads"""
        for batch in self._select_read_batches(selection, missing_ok, preload=preload):
            for read in batch.reads():
                yield read

    def _reads_batches(
        self, preload: Optional[Set[str]] = None
    ) -> Generator[ReadRecordBatch, None, None]:
        """Generate the record batches"""
        signal_cache = None
        if preload:
            signal_cache = self.inner_file_reader.batch_get_signal(
                "samples" in preload,
                "sample_count" in preload,
            )

        for idx in range(self.read_table.num_record_batches):
            batch = self.get_batch(idx)
            if signal_cache:
                batch.set_cached_signal(signal_cache.release_next_batch())
            yield batch

    def _read_some_batches(
        self,
        batch_selection: Iterable[int],
        preload: Optional[Set[str]] = None,
    ) -> Generator[ReadRecordBatch, None, None]:
        """Generate the selected record batches"""
        signal_cache = None
        if preload:
            signal_cache = self.inner_file_reader.batch_get_signal_batches(
                "samples" in preload,
                "sample_count" in preload,
                np.array(batch_selection, dtype=np.uint32),
            )

        for i in batch_selection:
            batch = self.get_batch(i)
            if signal_cache:
                batch.set_cached_signal(signal_cache.release_next_batch())
            yield batch

    def _select_read_batches(
        self,
        selection: List[str],
        missing_ok: bool = False,
        preload: Optional[Set[str]] = None,
    ) -> Generator[ReadRecordBatch, None, None]:
        """Generate the selected record batches"""
        successful_finds, per_batch_counts, batch_rows = self._plan_traversal(
            selection, missing_ok=missing_ok
        )

        if not missing_ok and successful_finds != len(selection):
            raise RuntimeError(
                f"Failed to find {len(selection) - successful_finds} requested reads in the file"
            )

        signal_cache: Optional[p5b.Pod5AsyncSignalLoader] = None
        if preload:
            signal_cache = self.inner_file_reader.batch_get_signal_selection(
                "samples" in preload,
                "sample_count" in preload,
                per_batch_counts,
                batch_rows,
            )

        current_offset = 0
        for batch_idx, batch_count in enumerate(per_batch_counts):
            current_batch_rows = batch_rows[
                current_offset : current_offset + batch_count
            ]
            current_offset += batch_count

            batch = self.get_batch(batch_idx)
            batch.set_selected_batch_rows(current_batch_rows)
            if signal_cache:
                batch.set_cached_signal(signal_cache.release_next_batch())
            yield batch

    def _plan_traversal(
        self,
        read_ids: Union[Collection[str], npt.NDArray[np.uint8]],
        missing_ok: bool = False,
    ) -> Tuple[int, npt.NDArray[np.uint32], npt.NDArray[np.uint32]]:
        """
        Query the file reader indexes to return the number of read_ids which
        were found and the batches and rows which are needed to traverse each
        read in the selection.

        Parameters
        ----------
        read_ids : Collection or numpy.ndarray of read_id strings
            The read ids to find in the file

        Returns
        -------
        successful_find_count: int
            The number of reads that were found from the array of read_ids given
        per_batch_counts: numpy.array[uint32]
            The number of rows from the batch row ids to take to form each RecordBatch
        batch_rows: numpy.array[uint32]
            All batch row ids

        """
        if not isinstance(read_ids, np.ndarray):
            read_ids = pack_read_ids(read_ids, invalid_ok=missing_ok)

        assert isinstance(read_ids, np.ndarray)

        batch_rows = np.empty(dtype="u4", shape=read_ids.shape[0])
        per_batch_counts = np.empty(dtype="u4", shape=self.batch_count)

        successful_find_count = self.inner_file_reader.plan_traversal(
            read_ids,
            per_batch_counts,
            batch_rows,
        )

        return successful_find_count, per_batch_counts, batch_rows

    def _get_signal_batch(self, batch_id: int) -> Signal:
        """Get the :py:class:`Signal` from the signal_reader batch at batch_id"""
        if batch_id in self._cached_signal_batches:
            return self._cached_signal_batches[batch_id]

        batch = self.signal_table.get_batch(batch_id)

        signal_batch = Signal(*[batch.column(name) for name in Signal._fields])

        self._cached_signal_batches[batch_id] = signal_batch
        return signal_batch

    def _lookup_run_info(self, batch: ReadRecordBatch, batch_row_id: int) -> RunInfo:
        """Get the :py:class:`RunInfo` from the batch at batch_row_id"""

        acquisition_id = batch.columns.run_info[batch_row_id].as_py()

        if acquisition_id in self._cached_run_infos:
            return self._cached_run_infos[acquisition_id]

        run_info = None
        for idx in range(self.run_info_table.num_record_batches):
            run_info_batch = self.run_info_table.get_batch(idx)
            acquisition_id_col = run_info_batch.column("acquisition_id")
            for row in range(run_info_batch.num_rows):
                if acquisition_id_col[row].as_py() == acquisition_id:
                    values = {}
                    for field in fields(RunInfo):
                        col = run_info_batch.column(field.name)
                        values[field.name] = col[row].as_py()
                    run_info = RunInfo(**values)
                    break

        if not run_info:
            raise Exception(
                f"Failed to find run info '{acquisition_id}' in run info table"
            )

        self._cached_run_infos[acquisition_id] = run_info
        return run_info
