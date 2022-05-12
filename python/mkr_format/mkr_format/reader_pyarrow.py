import ctypes
from datetime import datetime
from uuid import UUID

import numpy
import pyarrow as pa

import mkr_format.mkr_format_pybind
from .api_utils import pack_read_ids
from .signal_tools import vbz_decompress_signal
from .reader_utils import (
    PoreData,
    CalibrationData,
    EndReasonData,
    RunInfoData,
    SignalRowInfo,
    SearchOrder,
)


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
        return UUID(bytes=self._batch.column("read_id")[self._row].as_py())

    @property
    def read_number(self):
        """
        Find the integer read number of the read.
        """
        return self._batch.column("read_number")[self._row].as_py()

    @property
    def start_sample(self):
        """
        Find the absolute sample which the read started.
        """
        return self._batch.column("start")[self._row].as_py()

    @property
    def median_before(self):
        """
        Find the median before level (in pico amps) for the read.
        """
        return self._batch.column("median_before")[self._row].as_py()

    @property
    def pore(self):
        """
        Find the pore data associated with the read.

        Returns
        -------
        The pore data (as PoreData).
        """
        return PoreData(**self._batch.column("pore")[self._row].as_py())

    @property
    def calibration(self):
        """
        Find the calibration data associated with the read.

        Returns
        -------
        The calibration data (as CalibrationData).
        """
        return CalibrationData(**self._batch.column("calibration")[self._row].as_py())

    @property
    def end_reason(self):
        """
        Find the end reason data associated with the read.

        Returns
        -------
        The end reason data (as EndReasonData).
        """
        return EndReasonData(**self._batch.column("end_reason")[self._row].as_py())

    @property
    def run_info(self):
        """
        Find the run info data associated with the read.

        Returns
        -------
        The run info data (as RunInfoData).
        """
        return RunInfoData(**self._batch.column("run_info")[self._row].as_py())

    @property
    def run_info_index(self):
        """
        Find the dictionary index of the run info data associated with the read.

        Returns
        -------
        The run info data (as RunInfoData).
        """
        return self._batch.column("run_info")[self._row].index.as_py()

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
        output = []
        for r in self._batch.column("signal")[self._row]:
            output.append(self._get_signal_for_row(r.as_py()))
        return numpy.concatenate(output)

    def signal_for_chunk(self, i):
        """
        Find the signal for a given chunk of the read.

        #signal_rows can be used to find details of the signal chunks.

        Returns
        -------
        A numpy array of signal data with int16 type.
        """
        output = []
        chunk_abs_row_index = self._batch.column("signal")[self._row][i]
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
                batch.column("samples")[batch_row_index].as_py(),
                len(batch.column("signal")[batch_row_index].as_buffer()),
            )

        return [map_signal_row(r) for r in self._batch.column("signal")[self._row]]

    def _find_signal_row_index(self, signal_row):
        """
        Map from a signal_row to a batch, batch index and row index within that batch.
        """
        sig_row_count = self._reader._signal_batch_row_count
        sig_batch_idx = signal_row // sig_row_count
        sig_batch = self._reader._signal_reader.reader.get_record_batch(sig_batch_idx)
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

        signal = batch.column("signal")
        if isinstance(signal, pa.lib.LargeBinaryArray):
            sample_count = batch.column("samples")[batch_row_index].as_py()
            output = numpy.empty(sample_count, dtype=numpy.uint8)
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

    def reads(self):
        """
        Iterate all reads in the batch.

        Returns
        -------
        An iterable of reads (as ReadRowPyArrow) in the file.
        """
        for i in range(self._batch.num_rows):
            yield ReadRowPyArrow(self._reader, self._batch, i)

    def get_read(self, row):
        return ReadRowPyArrow(self._reader, self._batch, row)


class FileReader:
    """
    A reader for MKR data, opened using [open_combined_file], [open_split_file].
    """

    def __init__(self, reader, read_reader, signal_reader):
        self._reader = reader
        self._read_reader = read_reader
        self._signal_reader = signal_reader

        if self._signal_reader.reader.num_record_batches > 0:
            self._signal_batch_row_count = self._signal_reader.reader.get_record_batch(
                0
            ).num_rows

    def __del__(self):
        self._reader.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
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
        return ReadBatchPyArrow(self, self._read_reader.reader.get_record_batch(i))

    def read_batches(self):
        """
        Iterate all read batches in the file.

        Returns
        -------
        An iterable of batches (as ReadBatchPyArrow) in the file.
        """
        for i in range(self._read_reader.reader.num_record_batches):
            yield self.get_batch(i)

    def reads(self):
        """
        Iterate all reads in the file.

        Returns
        -------
        An iterable of reads (as ReadRowPyArrow) in the file.
        """
        for batch in self.read_batches():
            for read in batch.reads():
                yield read

    def select_reads(
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
