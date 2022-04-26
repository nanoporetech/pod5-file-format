import ctypes
from enum import Enum
from pathlib import Path
import typing

import numpy
from . import c_api
from .api_utils import check_error


def list_to_char_arrays(iterable: typing.Iterable[str]):
    list_bytes = []
    for i in iterable:
        if isinstance(i, str):
            list_bytes.append(i.encode("utf-8"))
        else:
            list_bytes.append(i)

    c_types_array = (ctypes.c_char_p * (len(list_bytes)))()
    c_types_array[:] = list_bytes
    return c_types_array


def tuple_to_char_arrays(tup: typing.Tuple[str, str]):
    return (
        len(tup),
        list_to_char_arrays([t[0] for t in tup]),
        list_to_char_arrays([t[1] for t in tup]),
    )


class EndReason(Enum):
    UNKNOWN = 0
    MUX_CHANGE = 1
    UNBLOCK_MUX_CHANGE = 2
    DATA_SERVICE_UNBLOCK_MUX_CHANGE = 3
    SIGNAL_POSITIVE = 4
    SIGNAL_NEGATIVE = 5


class FileWriter:
    def __init__(self, writer):
        if not writer:
            raise Exception(
                "Failed to open writer: " + c_api.mkr_get_error_string().decode("utf-8")
            )
        self._writer = writer
        self._pore_types = {}
        self._calibration_types = {}
        self._end_reason_types = {}
        self._run_info_types = {}

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        if self._writer:
            c_api.mkr_close_and_free_writer(self._writer)
            self._writer = None

    def find_calibration(
        self,
        offset: float,
        scale: float = None,
        adc_range: float = None,
        digitisation: int = None,
    ) -> typing.Tuple[ctypes.c_short, bool]:
        scale = self.find_scale(
            scale=scale, adc_range=adc_range, digitisation=digitisation
        )
        data = (offset, scale)
        if data in self._calibration_types:
            return self._calibration_types[data], False

        added_idx = self.add_calibration(offset, scale)
        self._calibration_types[data] = added_idx
        return added_idx, True

    def find_pore(
        self, channel: int, well: int, pore_type: str
    ) -> typing.Tuple[ctypes.c_short, bool]:
        data = (channel, well, pore_type)
        if data in self._pore_types:
            return self._pore_types[data], False

        added_idx = self.add_pore(channel, well, pore_type)
        self._pore_types[data] = added_idx
        return added_idx, True

    def find_end_reason(
        self, name: EndReason, forced: bool
    ) -> typing.Tuple[ctypes.c_short, bool]:
        data = (name, forced)
        if data in self._end_reason_types:
            return self._end_reason_types[data], False

        added_idx = self.add_end_reason(name, forced)
        self._end_reason_types[data] = added_idx
        return added_idx, True

    def find_run_info(self, **args) -> typing.Tuple[ctypes.c_short, bool]:
        data = tuple(args.values())
        if data in self._run_info_types:
            return self._run_info_types[data], False

        added_idx = self.add_run_info(**args)
        self._run_info_types[data] = added_idx
        return added_idx, True

    def add_read(
        self,
        read_id: bytes,
        pore: ctypes.c_short,
        calibration: ctypes.c_short,
        read_number: int,
        start_sample: int,
        median_before: float,
        end_reason: ctypes.c_short,
        run_info: ctypes.c_short,
        signal,
        sample_count,
        pre_compressed_signal: bool = False,
    ):
        if pre_compressed_signal:
            signal_bytes = signal.ctypes.data_as(ctypes.c_char_p)
            signal_size = ctypes.c_size_t(signal.shape[0])
            sample_count_arr = ctypes.c_uint(sample_count)
            check_error(
                c_api.mkr_add_read_pre_compressed(
                    self._writer,
                    ctypes.cast(
                        ctypes.c_char_p(read_id), ctypes.POINTER(ctypes.c_ubyte)
                    ),
                    pore,
                    calibration,
                    read_number,
                    start_sample,
                    median_before,
                    end_reason,
                    run_info,
                    ctypes.pointer(signal_bytes),
                    ctypes.pointer(signal_size),
                    ctypes.pointer(sample_count_arr),
                    1,
                )
            )
        else:
            check_error(
                c_api.mkr_add_read(
                    self._writer,
                    ctypes.cast(
                        ctypes.c_char_p(read_id), ctypes.POINTER(ctypes.c_ubyte)
                    ),
                    pore,
                    calibration,
                    read_number,
                    start_sample,
                    median_before,
                    end_reason,
                    run_info,
                    signal.ctypes.data_as(ctypes.POINTER(ctypes.c_int16)),
                    signal.shape[0],
                )
            )

    def flush(self):
        check_error(c_api.mkr_flush_reads_table(self._writer))
        check_error(c_api.mkr_flush_signal_table(self._writer))

    def add_pore(self, channel: int, well: int, pore_type: str) -> ctypes.c_short:
        index_out = ctypes.c_short()
        check_error(
            c_api.mkr_add_pore(
                ctypes.byref(index_out),
                self._writer,
                channel,
                well,
                pore_type.encode("utf-8"),
            )
        )
        return index_out

    def add_calibration(
        self,
        offset: float,
        scale: float = None,
        adc_range: float = None,
        digitisation: int = None,
    ) -> ctypes.c_short:
        scale = self.find_scale(
            scale=scale, adc_range=adc_range, digitisation=digitisation
        )

        index_out = ctypes.c_short()
        check_error(
            c_api.mkr_add_calibration(
                ctypes.byref(index_out), self._writer, offset, scale
            )
        )
        return index_out

    def add_end_reason(self, name: EndReason, forced: bool) -> ctypes.c_short:
        index_out = ctypes.c_short()
        check_error(
            c_api.mkr_add_end_reason(
                ctypes.byref(index_out), self._writer, name.value, forced
            )
        )
        return index_out

    def add_run_info(
        self,
        acquisition_id: str,
        acquisition_start_time_ms: int,
        adc_max: int,
        adc_min: int,
        context_tags: typing.Dict[str, str],
        experiment_name: str,
        flow_cell_id: str,
        flow_cell_product_code: str,
        protocol_name: str,
        protocol_run_id: str,
        protocol_start_time_ms: int,
        sample_id: str,
        sample_rate: int,
        sequencing_kit: str,
        sequencer_position: str,
        sequencer_position_type: str,
        software: str,
        system_name: str,
        system_type: str,
        tracking_id: typing.Dict[str, str],
    ):
        index_out = ctypes.c_short()
        check_error(
            c_api.mkr_add_run_info(
                ctypes.byref(index_out),
                self._writer,
                acquisition_id.encode("utf-8"),
                acquisition_start_time_ms,
                adc_max,
                adc_min,
                *tuple_to_char_arrays(context_tags),
                experiment_name.encode("utf-8"),
                flow_cell_id.encode("utf-8"),
                flow_cell_product_code.encode("utf-8"),
                protocol_name.encode("utf-8"),
                protocol_run_id.encode("utf-8"),
                protocol_start_time_ms,
                sample_id.encode("utf-8"),
                sample_rate,
                sequencing_kit.encode("utf-8"),
                sequencer_position.encode("utf-8"),
                sequencer_position_type.encode("utf-8"),
                software.encode("utf-8"),
                system_name.encode("utf-8"),
                system_type.encode("utf-8"),
                *tuple_to_char_arrays(tracking_id),
            )
        )
        return index_out

    @staticmethod
    def find_scale(
        scale: float = None, adc_range: float = None, digitisation: int = None
    ) -> float:
        if scale:
            if adc_range is not None or digitisation is not None:
                raise RuntimeError(
                    "Expected scale, or adc_range and digitisation to be supplied"
                )
            return scale

        if adc_range is not None and digitisation is not None:
            return adc_range / digitisation

        raise RuntimeError(
            "Expected scale, or adc_range and digitisation to be supplied"
        )


def create_combined_file(
    filename: Path, software_name: str = "Python API"
) -> FileWriter:
    options = None
    return FileWriter(
        c_api.mkr_create_combined_file(
            str(filename).encode("utf-8"), software_name.encode("utf-8"), options
        )
    )


def create_split_file(
    signal_file: Path, reads_file: Path, software_name: str = "Python API"
) -> FileWriter:
    options = None
    return FileWriter(
        c_api.mkr_create_split_file(
            str(signal_file).encode("utf-8"),
            str(reads_file).encode("utf-8"),
            software_name.encode("utf-8"),
            options,
        )
    )


def vbz_compress_signal(signal):
    max_signal_size = c_api.mkr_vbz_compressed_signal_max_size(len(signal))
    signal_bytes = numpy.empty(max_signal_size, dtype="i1")

    signal_size = ctypes.c_size_t(max_signal_size)
    c_api.mkr_vbz_compress_signal(
        signal.ctypes.data_as(ctypes.POINTER(ctypes.c_int16)),
        signal.shape[0],
        signal_bytes.ctypes.data_as(ctypes.c_char_p),
        ctypes.pointer(signal_size),
    )
    numpy.resize(signal_bytes, signal_size.value)
    return signal_bytes
