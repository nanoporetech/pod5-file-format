import ctypes
from pathlib import Path
import typing
from uuid import UUID

from . import c_api
from .api_utils import check_error


class ReadRow:
    def __init__(self, batch, row):
        self._batch = batch
        self._row = row
        self._read_id = None
        self._pore_idx = None
        self._calibration_idx = None
        self._read_number = None
        self._start_sample = None
        self._median_before = None
        self._end_reason_idx = None
        self._run_info_idx = None
        self._signal_row_count = None

    def cache_data(self):
        read_id = (ctypes.c_ubyte * 16)()
        pore_idx = ctypes.c_short()
        calibration_idx = ctypes.c_short()
        read_number = ctypes.c_uint()
        start_sample = ctypes.c_ulonglong()
        median_before = ctypes.c_float()
        end_reason_idx = ctypes.c_short()
        run_info_idx = ctypes.c_short()
        signal_row_count = ctypes.c_longlong()

        c_api.mkr_get_read_batch_row_info(
            self._batch,
            self._row,
            read_id,
            ctypes.pointer(pore_idx),
            ctypes.pointer(calibration_idx),
            ctypes.pointer(read_number),
            ctypes.pointer(start_sample),
            ctypes.pointer(median_before),
            ctypes.pointer(end_reason_idx),
            ctypes.pointer(run_info_idx),
            ctypes.pointer(signal_row_count),
        )

        self._read_id = UUID(bytes=bytes(read_id))
        self._pore_idx = pore_idx.value
        self._calibration_idx = calibration_idx.value
        self._read_number = read_number.value
        self._start_sample = start_sample.value
        self._median_before = median_before.value
        self._end_reason_idx = end_reason_idx.value
        self._calibration_idx = end_reason_idx.value
        self._run_info_idx = run_info_idx.value
        self._signal_row_count = signal_row_count.value

    @property
    def read_id(self):
        if not self._read_id:
            self.cache_data()
        return self._read_id

    @property
    def read_number(self):
        if not self._read_number:
            self.cache_data()
        return self._read_number

    @property
    def start_sample(self):
        if not self._start_sample:
            self.cache_data()
        return self._start_sample

    @property
    def median_before(self):
        if not self._median_before:
            self.cache_data()
        return self._median_before

    @property
    def signal_row_count(self):
        if not self._signal_row_count:
            self.cache_data()
        return self._signal_row_count


class ReadBatch:
    def __init__(self, batch):
        self._batch = batch

    def __del__(self):
        check_error(c_api.mkr_free_read_batch(self._batch))

    def iter_reads(self):
        size = ctypes.c_size_t()
        check_error(
            c_api.mkr_get_read_batch_row_count(ctypes.pointer(size), self._batch)
        )

        for i in range(size.value):
            yield ReadRow(self._batch, i)


class FileReader:
    def __init__(self, reader):
        if not reader:
            raise Exception(
                "Failed to open reader: " + c_api.mkr_get_error_string().decode("utf-8")
            )
        self._reader = reader

    def iter_read_batches(self):
        size = ctypes.c_size_t()
        check_error(c_api.mkr_get_read_batch_count(ctypes.pointer(size), self._reader))

        for i in range(size.value):
            batch = ctypes.POINTER(c_api.MkrReadRecordBatch)()

            check_error(
                c_api.mkr_get_read_batch(ctypes.pointer(batch), self._reader, i)
            )

            yield ReadBatch(batch)

    def iter_reads(self):
        for batch in self.iter_read_batches():
            for read in batch.iter_reads():
                yield read


def open_combined_file(filename: Path) -> FileReader:
    return FileReader(c_api.mkr_open_combined_file(str(filename).encode("utf-8")))
