import ctypes
from collections import namedtuple
from pathlib import Path
import typing
from uuid import UUID

from . import c_api
from .api_utils import check_error

SignalRowInfo = namedtuple(
    "SignalRowInfo", ["batch_index", "batch_row_index", "sample_count", "byte_count"]
)
PoreData = namedtuple("PoreData", ["channel", "well", "pore_type"])
CalibrationData = namedtuple("CalibrationData", ["offset", "scale"])
EndReasonData = namedtuple("EndReasonData", ["name", "forced"])
RunInfoData = namedtuple(
    "EndReasonData",
    [
        "acquisition_id",
        "acquisition_start_time_ms",
        "adc_max",
        "adc_min",
        "context_tags",
        "experiment_name",
        "flow_cell_id",
        "flow_cell_product_code",
        "protocol_name",
        "protocol_run_id",
        "protocol_start_time_ms",
        "sample_id",
        "sample_rate",
        "sequencing_kit",
        "sequencer_position",
        "sequencer_position_type",
        "software",
        "system_name",
        "system_typacquisie",
        "tracking_id",
    ],
)


def parse_dict(key_value_pairs):
    out = {}
    for i in range(key_value_pairs.size):
        out[key_value_pairs.keys[i].decode("utf-8")] = key_value_pairs.values[i].decode(
            "utf-8"
        )
    return out


class ReadRow:
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
        self._calibration_idx = end_reason_idx.value
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

        """self._signal_rows = []
        for i in range(self._signal_row_count):
            item = self._signal_row_info[i].contents
            self._signal_rows.append(
                SignalRowInfo(
                    item.batch_index,
                    item.batch_row_index,
                    item.stored_sample_count,
                    item.stored_byte_count,
                )
            )"""

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

    def _cache_dict_data(self, index, data_type, c_api_type, get, release):
        data_ptr = ctypes.POINTER(c_api_type)()
        check_error(get(self._batch, index, ctypes.byref(data_ptr)))

        def get_field(data, name):
            val = getattr(data, name)
            if isinstance(val, bytes):
                val = val.decode("utf-8")
            if isinstance(val, c_api.KeyValueData):
                val = parse_dict(val)
            return val

        data = data_ptr.contents
        tuple_data = data_type(*[get_field(data, f) for f, _ in data._fields_])

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
            self._run_info = self._cache_dict_data(
                self._run_info_idx,
                RunInfoData,
                c_api.RunInfoDictData,
                c_api.mkr_get_run_info,
                c_api.mkr_release_run_info,
            )
        return self._run_info


class ReadBatch:
    def __init__(self, reader, batch):
        self._reader = reader
        self._batch = batch

    def __del__(self):
        check_error(c_api.mkr_free_read_batch(self._batch))

    def reads(self):
        size = ctypes.c_size_t()
        check_error(c_api.mkr_get_read_batch_row_count(ctypes.byref(size), self._batch))

        for i in range(size.value):
            yield ReadRow(self._reader, self._batch, i)


class FileReader:
    def __init__(self, reader):
        if not reader:
            raise Exception(
                "Failed to open reader: " + c_api.mkr_get_error_string().decode("utf-8")
            )
        self._reader = reader

    def read_batches(self):
        size = ctypes.c_size_t()
        check_error(c_api.mkr_get_read_batch_count(ctypes.byref(size), self._reader))

        for i in range(size.value):
            batch = ctypes.POINTER(c_api.MkrReadRecordBatch)()

            check_error(c_api.mkr_get_read_batch(ctypes.byref(batch), self._reader, i))

            yield ReadBatch(self._reader, batch)

    def reads(self):
        for batch in self.read_batches():
            for read in batch.reads():
                yield read

    def select_reads(self, selection):
        search_selection = set(selection)

        return filter(lambda x: x.read_id in search_selection, self.reads())


def open_combined_file(filename: Path) -> FileReader:
    return FileReader(c_api.mkr_open_combined_file(str(filename).encode("utf-8")))
