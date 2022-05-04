import ctypes
from datetime import datetime, timezone
import typing
from uuid import UUID

import numpy

from . import c_api
from .api_utils import check_error
from .reader_utils import (
    PoreData,
    CalibrationData,
    EndReasonData,
    RunInfoData,
    SignalRowInfo,
)


def _parse_map(key_value_pairs: c_api.KeyValueData) -> typing.Dict[str, str]:
    """
    Parse a ctypes dict KeyValueData struct into a dict.
    """
    out = []
    for i in range(key_value_pairs.size):
        out.append(
            (
                key_value_pairs.keys[i].decode("utf-8"),
                key_value_pairs.values[i].decode("utf-8"),
            )
        )
    return out


class ReadRowCApi:
    """
    Represents the data for a single read.
    """

    def __init__(self, reader, batch, row):
        self._reader = reader
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
        self._signal_row_info = None
        self._signal_rows = None
        self._pore = None
        self._calibration = None
        self._end_reason = None
        self._run_info = None

    def __del__(self):
        if self._signal_row_info:
            check_error(
                c_api.mkr_free_signal_row_info(
                    self._signal_row_count, self._signal_row_info
                )
            )

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

        check_error(
            c_api.mkr_get_read_batch_row_info(
                self._batch,
                self._row,
                read_id,
                ctypes.byref(pore_idx),
                ctypes.byref(calibration_idx),
                ctypes.byref(read_number),
                ctypes.byref(start_sample),
                ctypes.byref(median_before),
                ctypes.byref(end_reason_idx),
                ctypes.byref(run_info_idx),
                ctypes.byref(signal_row_count),
            )
        )

        self._read_id = UUID(bytes=bytes(read_id))
        self._pore_idx = pore_idx.value
        self._calibration_idx = calibration_idx.value
        self._read_number = read_number.value
        self._start_sample = start_sample.value
        self._median_before = median_before.value
        self._end_reason_idx = end_reason_idx.value
        self._calibration_idx = calibration_idx.value
        self._run_info_idx = run_info_idx.value
        self._signal_row_count = signal_row_count.value

    def cache_signal_row_data(self):
        if not self._signal_row_count:
            self.cache_data()

        signal_rows = (ctypes.c_ulonglong * self._signal_row_count)()

        check_error(
            c_api.mkr_get_signal_row_indices(
                self._batch, self._row, self._signal_row_count, signal_rows
            )
        )

        self._signal_row_info = (
            ctypes.POINTER(c_api.SignalRowInfo) * self._signal_row_count
        )()

        self._signal_rows = []
        check_error(
            c_api.mkr_get_signal_row_info(
                self._reader, self._signal_row_count, signal_rows, self._signal_row_info
            )
        )

        self._signal_rows = []
        for i in range(self._signal_row_count):
            item = self._signal_row_info[i].contents
            self._signal_rows.append(
                SignalRowInfo(
                    item.batch_index,
                    item.batch_row_index,
                    item.stored_sample_count,
                    item.stored_byte_count,
                )
            )

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

    @property
    def signal_rows(self):
        if not self._signal_rows:
            self.cache_signal_row_data()
        return self._signal_rows

    @property
    def sample_count(self):
        return sum(r.sample_count for r in self.signal_rows)

    @property
    def byte_count(self):
        return sum(r.byte_count for r in self.signal_rows)

    @property
    def signal(self):
        if not self._signal_rows:
            self.cache_signal_row_data()
        output = numpy.empty(self.sample_count, dtype=numpy.int16)

        start = 0
        for row in self._signal_row_info:
            sample_count = row.contents.stored_sample_count
            check_error(
                c_api.mkr_get_signal(
                    self._reader,
                    row,
                    sample_count,
                    output[start:].ctypes.data_as(ctypes.POINTER(ctypes.c_short)),
                )
            )
            start += sample_count
        return output

    def signal_for_chunk(self, i):
        if not self._signal_rows:
            self.cache_signal_row_data()

        row = self._signal_row_info[i]
        sample_count = row.contents.stored_sample_count
        output = numpy.empty(self.sample_count, dtype=numpy.int16)
        check_error(
            c_api.mkr_get_signal(
                self._reader,
                row,
                sample_count,
                output.ctypes.data_as(ctypes.POINTER(ctypes.c_short)),
            )
        )
        return output

    def _cache_dict_data(
        self, index, data_type, c_api_type, get, release, map_fields=None
    ):
        data_ptr = ctypes.POINTER(c_api_type)()
        check_error(get(self._batch, index, ctypes.byref(data_ptr)))

        def get_field(data, name, map_fields):
            val = getattr(data, name)
            if isinstance(val, bytes):
                val = val.decode("utf-8")
            elif isinstance(val, c_api.KeyValueData):
                val = _parse_map(val)

            if map_fields and name in map_fields:
                val = map_fields[name](val)

            return val

        data = data_ptr.contents
        tuple_data = data_type(
            *[get_field(data, f, map_fields) for f, _ in data._fields_]
        )

        check_error(release(data))
        return tuple_data

    @property
    def pore(self):
        if not self._pore:
            if not self._pore_idx:
                self.cache_data()
            self._pore = self._cache_dict_data(
                self._pore_idx,
                PoreData,
                c_api.PoreDictData,
                c_api.mkr_get_pore,
                c_api.mkr_release_pore,
            )
        return self._pore

    @property
    def calibration(self):
        if not self._calibration:
            if not self._calibration_idx:
                self.cache_data()
            self._calibration = self._cache_dict_data(
                self._calibration_idx,
                CalibrationData,
                c_api.CalibrationDictData,
                c_api.mkr_get_calibration,
                c_api.mkr_release_calibration,
            )
        return self._calibration

    @property
    def end_reason(self):
        if not self._end_reason:
            if not self._end_reason_idx:
                self.cache_data()
            self._end_reason = self._cache_dict_data(
                self._end_reason_idx,
                EndReasonData,
                c_api.EndReasonDictData,
                c_api.mkr_get_end_reason,
                c_api.mkr_release_end_reason,
            )
        return self._end_reason

    @property
    def run_info_index(self):
        return self._run_info_idx

    @property
    def run_info(self):
        if not self._run_info:
            if not self._run_info_idx:
                self.cache_data()

            def to_datetime(val):
                return datetime.fromtimestamp(val / 1000, timezone.utc)

            self._run_info = self._cache_dict_data(
                self._run_info_idx,
                RunInfoData,
                c_api.RunInfoDictData,
                c_api.mkr_get_run_info,
                c_api.mkr_release_run_info,
                {
                    "acquisition_start_time_ms": to_datetime,
                    "protocol_start_time_ms": to_datetime,
                },
            )
        return self._run_info


class ReadBatchCApi:
    """
    Read data for a batch of reads.
    """

    def __init__(self, reader, batch):
        self._reader = reader
        self._batch = batch

    def __del__(self):
        check_error(c_api.mkr_free_read_batch(self._batch))

    def reads(self):
        size = ctypes.c_size_t()
        check_error(c_api.mkr_get_read_batch_row_count(ctypes.byref(size), self._batch))

        for i in range(size.value):
            yield ReadRowCApi(self._reader, self._batch, i)


class FileReaderCApi:
    """
    A reader for MKR data, opened using [open_combined_file], [open_split_file].
    """

    def __init__(self, reader):
        self._reader = reader

    def __del__(self):
        check_error(c_api.mkr_close_and_free_reader(self._reader))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    @property
    def batch_count(self):
        size = ctypes.c_size_t()
        check_error(c_api.mkr_get_read_batch_count(ctypes.byref(size), self._reader))
        return size.value

    def get_batch(self, i):
        batch = ctypes.POINTER(c_api.MkrReadRecordBatch)()

        check_error(c_api.mkr_get_read_batch(ctypes.byref(batch), self._reader, i))

        return ReadBatchCApi(self._reader, batch)

    def read_batches(self):
        for i in range(self.batch_count):
            yield self.get_batch(i)

    def reads(self):
        for batch in self.read_batches():
            for read in batch.reads():
                yield read

    def select_reads(self, selection):
        search_selection = set(selection)

        return filter(lambda x: x.read_id in search_selection, self.reads())
