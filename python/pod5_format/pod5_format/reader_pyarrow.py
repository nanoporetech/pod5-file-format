import ctypes
from collections import namedtuple
from datetime import datetime
from uuid import UUID

import numpy
import pyarrow as pa

import pod5_format.pod5_format_pybind
from .api_utils import pack_read_ids
from .signal_tools import vbz_decompress_signal_into, vbz_decompress_signal
from .reader_utils import (
    PoreData,
    CalibrationData,
    EndReasonData,
    RunInfoData,
    SignalRowInfo,
)

Columns = namedtuple(
    "Columns",
    [
        "read_id",
        "read_number",
        "start",
        "median_before",
        "pore",
        "calibration",
        "end_reason",
        "run_info",
        "signal",
    ],
)
SignalColumns = namedtuple("SignalColumns", ["signal", "samples"])


class ReadRowPyArrow:
    """
    Represents the data for a single read.
    """

    def __init__(
        self, reader, batch, row, batch_signal_cache=None, selected_batch_index=None
    ):
        self._reader = reader
        self._batch = batch
        self._row = row
        self._batch_signal_cache = batch_signal_cache
        self._selected_batch_index = selected_batch_index

    @property
    def read_id(self):
        """
        Find the unique read identifier for the read.
        """
        return UUID(bytes=self._batch._columns.read_id[self._row].as_py())

    @property
    def read_number(self):
        """
        Find the integer read number of the read.
        """
        return self._batch._columns.read_number[self._row].as_py()

    @property
    def start_sample(self):
        """
        Find the absolute sample which the read started.
        """
        return self._batch._columns.start[self._row].as_py()

    @property
    def median_before(self):
        """
        Find the median before level (in pico amps) for the read.
        """
        return self._batch._columns.median_before[self._row].as_py()

    @property
    def pore(self):
        """
        Find the pore data associated with the read.

        Returns
        -------
        The pore data (as PoreData).
        """
        return self._reader._lookup_pore(self._batch, self._row)

    @property
    def calibration(self):
        """
        Find the calibration data associated with the read.

        Returns
        -------
        The calibration data (as CalibrationData).
        """
        return self._reader._lookup_calibration(self._batch, self._row)

    @property
    def calibration_digitisation(self):
        """
        Find the digitisation value used by the sequencer.

        Intended to assist workflows ported from legacy file formats.
        """
        return self.run_info.adc_max - self.run_info.adc_min + 1

    @property
    def calibration_range(self):
        """
        Find the calibration range value.

        Intended to assist workflows ported from legacy file formats.
        """
        return self.calibration.scale * self.calibration_digitisation

    @property
    def end_reason(self):
        """
        Find the end reason data associated with the read.

        Returns
        -------
        The end reason data (as EndReasonData).
        """
        return self._reader._lookup_end_reason(self._batch, self._row)

    @property
    def run_info(self):
        """
        Find the run info data associated with the read.

        Returns
        -------
        The run info data (as RunInfoData).
        """
        return self._reader._lookup_run_info(self._batch, self._row)

    @property
    def calibration_index(self):
        """
        Find the dictionary index of the calibration data associated with the read.

        Returns
        -------
        The index of the calibration.
        """
        return self._batch._columns.calibration[self._row].index.as_py()

    @property
    def end_reason_index(self):
        """
        Find the dictionary index of the end reason data associated with the read.

        Returns
        -------
        The end reason index.
        """
        return self._batch._columns.end_reason[self._row].index.as_py()

    @property
    def pore_index(self):
        """
        Find the dictionary index of the pore data associated with the read.

        Returns
        -------
        The pore index.
        """
        return self._batch._columns.pore[self._row].index.as_py()

    @property
    def run_info_index(self):
        """
        Find the dictionary index of the run info data associated with the read.

        Returns
        -------
        The run info index.
        """
        return self._batch._columns.run_info[self._row].index.as_py()

    @property
    def sample_count(self):
        """
        Find the number of samples in the reads signal data.
        """
        return sum(r.sample_count for r in self.signal_rows)

    @property
    def byte_count(self):
        """
        Find the number of bytes used to store the reads data.
        """
        return sum(r.byte_count for r in self.signal_rows)

    @property
    def has_cached_signal(self):
        """
        Find if cached signal is available for this read.
        """
        return self._batch_signal_cache

    @property
    def signal(self):
        """
        Find the full signal for the read.

        Returns
        -------
        A numpy array of signal data with int16 type.
        """
        if self.has_cached_signal:
            if self._selected_batch_index is not None:
                return self._batch_signal_cache[self._selected_batch_index]
            return self._batch_signal_cache[self._row]

        rows = self._batch._columns.signal[self._row]
        batch_data = [self._find_signal_row_index(r.as_py()) for r in rows]
        sample_counts = []
        for batch, batch_index, batch_row_index in batch_data:
            sample_counts.append(batch.samples[batch_row_index].as_py())

        output = numpy.empty(dtype=numpy.int16, shape=(sum(sample_counts),))
        current_sample_index = 0

        for i, (batch, batch_index, batch_row_index) in enumerate(batch_data):
            signal = batch.signal
            current_row_count = sample_counts[i]
            output_slice = output[
                current_sample_index : current_sample_index + current_row_count
            ]
            if self._reader._is_vbz_compressed:
                vbz_decompress_signal_into(
                    memoryview(signal[batch_row_index].as_buffer()), output_slice
                )
            else:
                output_slice[:] = signal.to_numpy()
            current_sample_index += current_row_count
        return output

    @property
    def signal_pa(self):
        """
        Find the full signal for the read, calibrated in pico amps.

        Returns
        -------
        A numpy array of signal data with float type.
        """
        return self.calibrate_signal_array(self.signal)

    def signal_for_chunk(self, i):
        """
        Find the signal for a given chunk of the read.

        #signal_rows can be used to find details of the signal chunks.

        Returns
        -------
        A numpy array of signal data with int16 type.
        """
        output = []
        chunk_abs_row_index = self._batch._columns.signal[self._row][i]
        return self._get_signal_for_row(chunk_abs_row_index.as_py())

    @property
    def signal_rows(self):
        """
        Find all signal rows for the read

        Returns
        -------
        An iterable of signal row data (as SignalRowInfo) in the read.
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

        return [map_signal_row(r) for r in self._batch._columns.signal[self._row]]

    def calibrate_signal_array(self, signal_array_adc):
        """
        Transform an array of int16 signal data from ADC space to pA.
        """
        calibration = self.calibration
        return (signal_array_adc + calibration.offset) * calibration.scale

    def _find_signal_row_index(self, signal_row):
        """
        Map from a signal_row to a batch, batch index and row index within that batch.
        """
        sig_row_count = self._reader._signal_batch_row_count
        sig_batch_idx = signal_row // sig_row_count
        sig_batch = self._reader._get_signal_batch(sig_batch_idx)
        batch_row_idx = signal_row - (sig_batch_idx * sig_row_count)

        return (
            sig_batch,
            signal_row // sig_row_count,
            signal_row - (sig_batch_idx * sig_row_count),
        )

    def _get_signal_for_row(self, r):
        """
        Find the signal data for a given absolute signal row index
        """
        batch, batch_index, batch_row_index = self._find_signal_row_index(r)

        signal = batch.signal
        if self._reader._is_vbz_compressed:
            sample_count = batch.samples[batch_row_index].as_py()
            return vbz_decompress_signal(
                memoryview(signal[batch_row_index].as_buffer()), sample_count
            )
        else:
            return signal.to_numpy()


class ReadBatchPyArrow:
    """
    Read data for a batch of reads.
    """

    def __init__(self, reader, batch):
        self._reader = reader
        self._batch = batch
        self._signal_cache = None
        self._selected_batch_rows = None

        self._columns = Columns(*[self._batch.column(name) for name in Columns._fields])

    def set_cached_siganl(self, signal_cache):
        self._signal_cache = signal_cache

    def set_selected_batch_rows(self, selected_batch_rows):
        self._selected_batch_rows = selected_batch_rows

    def reads(self):
        """
        Iterate all reads in the batch.

        Returns
        -------
        An iterable of reads (as ReadRowPyArrow) in the file.
        """

        signal_cache = None
        if self._signal_cache and self._signal_cache.samples:
            signal_cache = self._signal_cache.samples

        if self._selected_batch_rows is not None:
            for idx, row in enumerate(self._selected_batch_rows):
                yield ReadRowPyArrow(
                    self._reader,
                    self,
                    row,
                    batch_signal_cache=signal_cache,
                    selected_batch_index=idx,
                )
        else:
            for i in range(self._batch.num_rows):
                yield ReadRowPyArrow(
                    self._reader, self, i, batch_signal_cache=signal_cache
                )

    def get_read(self, row):
        return ReadRowPyArrow(self._reader, self, row)

    @property
    def num_reads(self):
        return self._batch.num_rows

    @property
    def read_id_column(self):
        if self._selected_batch_rows is not None:
            return self._columns.read_id.take(self._selected_batch_rows)
        return self._columns.read_id

    @property
    def read_number_column(self):
        if self._selected_batch_rows is not None:
            return self._columns.read_number.take(self._selected_batch_rows)
        return self._columns.read_number

    @property
    def cached_sample_count_column(self):
        if not self._signal_cache:
            raise Exception("No cached signal data available")
        return self._signal_cache.sample_count

    @property
    def cached_samples_column(self):
        if not self._signal_cache:
            raise Exception("No cached signal data available")
        return self._signal_cache.samples


class FileReader:
    """
    A reader for POD5 data, opened using [open_combined_file], [open_split_file].
    """

    def __init__(self, reader, read_reader, signal_reader):
        self._reader = reader
        self._read_reader = read_reader
        self._signal_reader = signal_reader

        self._cached_signal_batches = {}

        self._cached_run_infos = {}
        self._cached_end_reasons = {}
        self._cached_calibrations = {}
        self._cached_pores = {}

        self._is_vbz_compressed = self._signal_reader.reader.schema.field(
            "signal"
        ).type.equals(pa.large_binary())
        if self._signal_reader.reader.num_record_batches > 0:
            self._signal_batch_row_count = self._signal_reader.reader.get_batch(
                0
            ).num_rows

    def __del__(self):
        self._reader.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self._cached_signal_batches = None
        self._read_reader.close()
        self._read_reader = None
        self._signal_reader.close()
        self._signal_reader = None
        self._reader.close()

    @property
    def batch_count(self):
        """
        Find the number of read batches available in the file.

        Returns
        -------
        The number of batches in the file.
        """
        return self._read_reader.reader.num_record_batches

    def get_batch(self, i):
        """
        Get a read batch in the file.

        Returns
        -------
        The requested batch as a ReadBatchPyArrow.
        """
        return ReadBatchPyArrow(self, self._read_reader.reader.get_batch(i))

    def read_batches(
        self, selection=None, batch_selection=None, missing_ok=False, preload=None
    ):
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
        An iterable of batches (as ReadBatchPyArrow) in the file.
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

    def reads(self, selection=None, missing_ok=False, preload=None):
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
        An iterable of reads (as ReadRowPyArrow) in the file.
        """
        if selection is None:
            yield from self._reads(preload=preload)
        else:
            yield from self._select_reads(
                selection, missing_ok=missing_ok, preload=preload
            )

    def _reads(self, preload=None):
        for batch in self.read_batches(preload=preload):
            for read in batch.reads():
                yield read

    def _select_reads(self, selection, missing_ok=False, preload=None):
        for batch in self._select_read_batches(selection, missing_ok, preload=preload):
            for read in batch.reads():
                yield read

    def _reads_batches(self, preload=None):
        signal_cache = None
        if preload:
            signal_cache = self._reader.batch_get_signal(
                "samples" in preload,
                "sample_count" in preload,
            )

        for i in range(self._read_reader.reader.num_record_batches):
            batch = self.get_batch(i)
            if signal_cache:
                batch.set_cached_siganl(signal_cache.release_next_batch())
            yield batch

    def _read_some_batches(self, batch_selection, preload=None):
        signal_cache = None
        if preload:
            signal_cache = self._reader.batch_get_signal_batches(
                "samples" in preload,
                "sample_count" in preload,
                numpy.array(batch_selection, dtype=numpy.uint32),
            )

        for i in batch_selection:
            batch = self.get_batch(i)
            if signal_cache:
                batch.set_cached_siganl(signal_cache.release_next_batch())
            yield batch

    def _select_read_batches(self, selection, missing_ok=False, preload=None):
        successful_finds, per_batch_counts, all_batch_rows = self._plan_traversal(
            selection
        )

        if not missing_ok and successful_finds != len(selection):
            raise Exception(
                f"Failed to find {len(selection) - successful_finds} requested reads in the file"
            )

        signal_cache = None
        if preload:
            signal_cache = self._reader.batch_get_signal_selection(
                "samples" in preload,
                "sample_count" in preload,
                per_batch_counts,
                all_batch_rows,
            )

        current_offset = 0
        for batch_idx, batch_count in enumerate(per_batch_counts):
            current_batch_rows = all_batch_rows[
                current_offset : current_offset + batch_count
            ]

            batch = self.get_batch(batch_idx)
            batch.set_selected_batch_rows(current_batch_rows)
            if signal_cache:
                batch.set_cached_siganl(signal_cache.release_next_batch())
            yield batch

            current_offset += batch_count

    def _plan_traversal(self, read_ids):
        if not isinstance(read_ids, numpy.ndarray):
            read_ids = pack_read_ids(read_ids)

        batch_rows = numpy.empty(dtype="u4", shape=read_ids.shape[0])
        per_batch_counts = numpy.empty(dtype="u4", shape=self.batch_count)

        successful_find_count = self._reader.plan_traversal(
            read_ids,
            per_batch_counts,
            batch_rows,
        )

        return successful_find_count, per_batch_counts, batch_rows

    def _get_signal_batch(self, batch_id):
        if batch_id in self._cached_signal_batches:
            return self._cached_signal_batches[batch_id]

        batch = self._signal_reader.reader.get_batch(batch_id)

        batch_columns = SignalColumns(
            *[batch.column(name) for name in SignalColumns._fields]
        )

        self._cached_signal_batches[batch_id] = batch_columns
        return batch_columns

    def _lookup_run_info(self, batch, batch_row_id):
        return self._lookup_dict_value(
            self._cached_run_infos, "run_info", RunInfoData, batch, batch_row_id
        )

    def _lookup_pore(self, batch, batch_row_id):
        return self._lookup_dict_value(
            self._cached_pores, "pore", PoreData, batch, batch_row_id
        )

    def _lookup_calibration(self, batch, batch_row_id):
        return self._lookup_dict_value(
            self._cached_calibrations,
            "calibration",
            CalibrationData,
            batch,
            batch_row_id,
        )

    def _lookup_end_reason(self, batch, batch_row_id):
        return self._lookup_dict_value(
            self._cached_end_reasons, "end_reason", EndReasonData, batch, batch_row_id
        )

    def _lookup_dict_value(self, storage, field_name, cls_type, batch, batch_row_id):
        field_data = getattr(batch._columns, field_name)
        row_id = field_data.indices[batch_row_id].as_py()
        if row_id in storage:
            return storage[row_id]

        run_info = cls_type(**field_data[batch_row_id].as_py())
        storage[row_id] = run_info
        return run_info
