import itertools
from pathlib import Path
import pytz
import typing

import numpy

import pod5_format.pod5_format_pybind
from .api_utils import EndReason
from .utils import make_split_filename


def map_to_tuple(tup):
    if isinstance(tup, dict):
        return tuple(i for i in tup.items())
    elif isinstance(tup, list):
        return tuple(i for i in tup)
    raise Exception("Unknown input type for context tags")


def timestamp_to_int(ts):
    return int(ts.astimezone(pytz.utc).timestamp() * 1000)


class FileWriter:
    def __init__(self, writer):
        if not writer:
            raise Exception(
                "Failed to open writer: "
                + c_api.pod5_get_error_string().decode("utf-8")
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
            self._writer.close()
            self._writer = None

    def find_calibration(
        self,
        offset: float,
        scale: float = None,
        adc_range: float = None,
        digitisation: int = None,
    ) -> typing.Tuple[int, bool]:
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
    ) -> typing.Tuple[int, bool]:
        data = (channel, well, pore_type)
        if data in self._pore_types:
            return self._pore_types[data], False

        added_idx = self.add_pore(channel, well, pore_type)
        self._pore_types[data] = added_idx
        return added_idx, True

    def find_end_reason(self, name: EndReason, forced: bool) -> typing.Tuple[int, bool]:
        data = (name, forced)
        if data in self._end_reason_types:
            return self._end_reason_types[data], False

        added_idx = self.add_end_reason(name, forced)
        self._end_reason_types[data] = added_idx
        return added_idx, True

    def find_run_info(self, **args) -> typing.Tuple[int, bool]:
        if not isinstance(args["context_tags"], tuple):
            args["context_tags"] = map_to_tuple(args["context_tags"])
        if not isinstance(args["tracking_id"], tuple):
            args["tracking_id"] = map_to_tuple(args["tracking_id"])

        data = tuple(args.values())
        if data in self._run_info_types:
            return self._run_info_types[data], False

        added_idx = self.add_run_info(**args)
        self._run_info_types[data] = added_idx
        return added_idx, True

    def add_read(
        self,
        read_id: bytes,
        pore: int,
        calibration: int,
        read_number: int,
        start_sample: int,
        median_before: float,
        end_reason: int,
        run_info: int,
        signal,
        sample_count,
        pre_compressed_signal: bool = False,
    ):
        return self.add_reads(
            numpy.array([numpy.frombuffer(read_id, dtype=numpy.uint8)]),
            numpy.array([pore], dtype=numpy.int16),
            numpy.array([calibration], dtype=numpy.int16),
            numpy.array([read_number], dtype=numpy.uint32),
            numpy.array([start_sample], dtype=numpy.uint64),
            numpy.array([median_before], dtype=float),
            numpy.array([end_reason], dtype=numpy.int16),
            numpy.array([run_info], dtype=numpy.int16),
            [signal],
            numpy.array([sample_count], dtype=numpy.uint32),
            pre_compressed_signal,
        )

    def add_reads(
        self,
        read_ids,
        pores,
        calibrations,
        read_numbers,
        start_samples,
        median_befores,
        end_reasons,
        run_infos,
        signals,
        sample_counts,
        pre_compressed_signal: bool = False,
    ):
        read_ids = read_ids.astype(numpy.uint8, copy=False)
        pores = pores.astype(numpy.int16, copy=False)
        calibrations = calibrations.astype(numpy.int16, copy=False)
        read_numbers = read_numbers.astype(numpy.uint32, copy=False)
        start_samples = start_samples.astype(numpy.uint64, copy=False)
        median_befores = median_befores.astype(float, copy=False)
        end_reasons = end_reasons.astype(numpy.int16, copy=False)
        run_infos = run_infos.astype(numpy.int16, copy=False)

        if pre_compressed_signal:
            # Find an array of the number of chunks per read
            signal_chunk_counts = numpy.array(
                [len(sample_count) for sample_count in sample_counts],
                dtype=numpy.uint32,
            )
            # Join all read sample counts into one array
            sample_counts = numpy.concatenate(sample_counts).astype(numpy.uint32)

            self._writer.add_reads_pre_compressed(
                read_ids.shape[0],
                read_ids,
                pores,
                calibrations,
                read_numbers,
                start_samples,
                median_befores,
                end_reasons,
                run_infos,
                # Join all signal data into one list
                list(itertools.chain(*signals)),
                sample_counts,
                signal_chunk_counts,
            )
        else:
            self._writer.add_reads(
                read_ids.shape[0],
                read_ids,
                pores,
                calibrations,
                read_numbers,
                start_samples,
                median_befores,
                end_reasons,
                run_infos,
                signals,
            )

    def add_pore(self, channel: int, well: int, pore_type: str) -> int:
        return self._writer.add_pore(
            channel,
            well,
            pore_type,
        )

    def add_calibration(
        self,
        offset: float,
        scale: float = None,
        adc_range: float = None,
        digitisation: int = None,
    ) -> int:
        scale = self.find_scale(
            scale=scale, adc_range=adc_range, digitisation=digitisation
        )

        return self._writer.add_calibration(offset, scale)

    def add_end_reason(self, name: EndReason, forced: bool) -> int:
        return self._writer.add_end_reason(name.value, forced)

    def add_run_info(
        self,
        acquisition_id: str,
        acquisition_start_time,
        adc_max: int,
        adc_min: int,
        context_tags: typing.Dict[str, str],
        experiment_name: str,
        flow_cell_id: str,
        flow_cell_product_code: str,
        protocol_name: str,
        protocol_run_id: str,
        protocol_start_time,
        sample_id: str,
        sample_rate: int,
        sequencing_kit: str,
        sequencer_position: str,
        sequencer_position_type: str,
        software: str,
        system_name: str,
        system_type: str,
        tracking_id: typing.Dict[str, str],
    ) -> int:
        if not isinstance(acquisition_start_time, int):
            acquisition_start_time = timestamp_to_int(acquisition_start_time)
        if not isinstance(protocol_start_time, int):
            protocol_start_time = timestamp_to_int(protocol_start_time)
        if not isinstance(context_tags, tuple):
            context_tags = map_to_tuple(context_tags)
        if not isinstance(tracking_id, tuple):
            tracking_id = map_to_tuple(tracking_id)

        return self._writer.add_run_info(
            acquisition_id,
            acquisition_start_time,
            adc_max,
            adc_min,
            context_tags,
            experiment_name,
            flow_cell_id,
            flow_cell_product_code,
            protocol_name,
            protocol_run_id,
            protocol_start_time,
            sample_id,
            sample_rate,
            sequencing_kit,
            sequencer_position,
            sequencer_position_type,
            software,
            system_name,
            system_type,
            tracking_id,
        )

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
        pod5_format.pod5_format_pybind.create_combined_file(
            str(filename), software_name, options
        )
    )


def create_split_file(
    file: Path, reads_file: Path = None, software_name: str = "Python API"
) -> FileWriter:
    options = None

    signal_file = file
    if reads_file == None:
        signal_file, reads_file = make_split_filename(file)

    return FileWriter(
        pod5_format.pod5_format_pybind.create_split_file(
            str(signal_file),
            str(reads_file),
            software_name,
            options,
        )
    )
