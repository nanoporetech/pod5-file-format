from datetime import datetime
from uuid import UUID

import numpy
import pyarrow as pa

from .signal_tools import vbz_decompress_signal
from .reader_utils import (
    PoreData,
    CalibrationData,
    EndReasonData,
    RunInfoData,
    SignalRowInfo,
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
        return UUID(bytes=self._batch.column("read_id")[self._row].as_py())

    @property
    def read_number(self):
        return self._batch.column("read_number")[self._row].as_py()

    @property
    def start_sample(self):
        return self._batch.column("start")[self._row].as_py()

    @property
    def median_before(self):
        return self._batch.column("median_before")[self._row].as_py()

    @property
    def pore(self):
        return PoreData(**self._batch.column("pore")[self._row].as_py())

    @property
    def calibration(self):
        return CalibrationData(**self._batch.column("calibration")[self._row].as_py())

    @property
    def end_reason(self):
        return EndReasonData(**self._batch.column("end_reason")[self._row].as_py())

    @property
    def run_info(self):
        val = self._batch.column("run_info")[self._row]
        return RunInfoData(**self._batch.column("run_info")[self._row].as_py())

    @property
    def sample_count(self):
        return sum(r.sample_count for r in self.signal_rows)

    @property
    def byte_count(self):
        return sum(r.byte_count for r in self.signal_rows)

    @property
    def signal(self):
        output = []
        for r in self._batch.column("signal")[self._row]:
            output.append(self._get_signal_for_row(r.as_py()))

        return numpy.concatenate(output)

    def signal_for_chunk(self, i):
        output = []
        chunk_abs_row_index = self._batch.column("signal")[self._row][i]
        return self._get_signal_for_row(chunk_abs_row_index.as_py())

    @property
    def signal_rows(self):
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
        batch, batch_index, batch_row_index = self._find_signal_row_index(r)

        signal = batch.column("signal")
        if isinstance(signal, pa.lib.LargeBinaryArray):
            sample_count = batch.column("samples")[batch_row_index].as_py()
            output = numpy.empty(sample_count, dtype=numpy.uint8)
            compressed_signal = signal[batch_row_index].as_py()
            return vbz_decompress_signal(compressed_signal, sample_count)
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
        for i in range(self._batch.num_rows):
            yield ReadRowPyArrow(self._reader, self._batch, i)


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

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    @property
    def batch_count(self):
        return self._read_reader.reader.num_record_batches

    def get_batch(self, i):
        return ReadBatchPyArrow(self, self._read_reader.reader.get_record_batch(i))

    def read_batches(self):
        for i in range(self._read_reader.reader.num_record_batches):
            yield self.get_batch(i)

    def reads(self):
        for batch in self.read_batches():
            for read in batch.reads():
                yield read

    def select_reads(self, selection):
        search_selection = set(selection)

        return filter(lambda x: x.read_id in search_selection, self.reads())
