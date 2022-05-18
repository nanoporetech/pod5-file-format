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
    SearchOrder,
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

    def __init__(self, reader, batch, row):
        self._reader = reader
        self._batch = batch
        self._row = row

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
    def signal(self):
        """
        Find the full signal for the read.

        Returns
        -------
        A numpy array of signal data with int16 type.
        """
        rows = self._batch._columns.signal[self._row]
        batch_data = [self._find_signal_row_index(r.as_py()) for r in rows]
        sample_counts = []
        for batch, batch_index, batch_row_index in batch_data:
            sample_counts.append(batch.samples[batch_row_index].as_py())

        output = numpy.empty(dtype=numpy.int16, shape=(sum(sample_counts),))
        current_sample_index = 0

        for i, (batch, batch_index, batch_row_index) in enumerate(batch_data):
            signal = batch.signal
            output_slice = output[current_sample_index : sample_counts[i]]
            if self._reader._is_vbz_compressed:
                vbz_decompress_signal_into(
                    memoryview(signal[batch_row_index].as_buffer()), output_slice
                )
            else:
                output_slice[:] = signal.to_numpy()
        return output

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

        self._columns = Columns(*[self._batch.column(name) for name in Columns._fields])

    def reads(self):
        """
        Iterate all reads in the batch.

        Returns
        -------
        An iterable of reads (as ReadRowPyArrow) in the file.
        """
        for i in range(self._batch.num_rows):
            yield ReadRowPyArrow(self._reader, self, i)

    def get_read(self, row):
        return ReadRowPyArrow(self._reader, self, row)


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

    def read_batches(self):
        """
        Iterate all read batches in the file.

        Returns
        -------
        An iterable of batches (as ReadBatchPyArrow) in the file.
        """
        for i in range(self._read_reader.reader.num_record_batches):
            yield self.get_batch(i)

    def reads(self, selection=None, missing_ok=False, order=SearchOrder.READ_EFFICIENT):
        if selection is None:
            yield from self._reads()
        else:
            yield from self._select_reads(selection, missing_ok=missing_ok, order=order)

    def _reads(self):
        """
        Iterate all reads in the file.

        Returns
        -------
        An iterable of reads (as ReadRowPyArrow) in the file.
        """
        for batch in self.read_batches():
            for read in batch.reads():
                yield read

    def _select_reads(
        self, selection, missing_ok=False, order=SearchOrder.READ_EFFICIENT
    ):
        """
        Iterate a set of reads in the file

        Parameters
        ----------
        selection : iterable[str]
            The read ids to walk in the file.

        Returns
        -------
        An iterable of reads (as ReadRowPyArrow) in the file.
        """
        steps, successful_finds = self._plan_traversal(selection, order)

        if not missing_ok and successful_finds != len(steps):
            raise Exception(
                f"Failed to find {len(steps) - successful_finds} requested reads in the file"
            )

        batch_index = None
        batch = None
        for item in steps[:successful_finds]:
            batch_idx = item[0]
            batch_row = item[1]
            if batch_index != batch_idx:
                batch = self.get_batch(batch_idx)
                batch_index = batch_idx

            yield batch.get_read(batch_row)

    def _plan_traversal(self, read_ids, order=SearchOrder.READ_EFFICIENT):
        if not isinstance(read_ids, numpy.ndarray):
            read_ids = pack_read_ids(read_ids)

        step_count = read_ids.shape[0]
        traversal_plan = numpy.empty(dtype="u4", shape=(step_count, 3))

        successful_steps = self._reader.plan_traversal(
            read_ids,
            order.value,
            traversal_plan,
        )

        return traversal_plan, successful_steps

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
