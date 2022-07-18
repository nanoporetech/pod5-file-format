"""

"""
import datetime
import itertools
from pathlib import Path
import typing
from uuid import UUID
from pod5_format.api_utils import deprecation_warning, Pod5ApiException
import pytz

import numpy

import pod5_format.pod5_format_pybind as p5b
from pod5_format.types import (
    Calibration,
    EndReason,
    Pore,
    Read,
    RunInfo,
)
from . import make_split_filename


DEFAULT_SOFTWARE_NAME = "Python API"


def map_to_tuples(info_map) -> typing.List[typing.Tuple[typing.Any, ...]]:
    """Convert a map (e.g. context_tags and tracking_id) to a tuple to pass to c_api"""
    if isinstance(info_map, dict):
        return list((key, value) for key, value in info_map.items())
    elif isinstance(info_map, list):
        return list(tuple(item) for item in info_map)
    raise TypeError(f"Unknown input type for context tags {type(info_map)}")


def timestamp_to_int(time_stamp: typing.Union[datetime.datetime, int]) -> int:
    """Convert a datetime timestamp to an integer if it's not already an integer"""
    if isinstance(time_stamp, int):
        return time_stamp
    return int(time_stamp.astimezone(pytz.utc).timestamp() * 1000)


class Writer:
    """Pod5 File Writer"""

    def __init__(self, writer: p5b.FileWriter):

        if not writer:
            raise Pod5ApiException(f"Failed to open writer: {p5b.get_error_string()}")

        self._writer: p5b.FileWriter = writer
        self._pore_types: typing.Dict[Pore, int] = {}
        self._calibration_types: typing.Dict[Calibration, int] = {}
        self._end_reason_types: typing.Dict[EndReason, int] = {}
        self._run_info_types: typing.Dict[RunInfo, int] = {}

    @classmethod
    def open_combined(
        cls, path: Path, software_name: str = DEFAULT_SOFTWARE_NAME
    ) -> "Writer":
        """Create a new `Writer` instance to write a new combined file at path"""
        return cls(p5b.create_combined_file(str(path), software_name, None))

    @classmethod
    def open_split(
        cls,
        path: Path,
        reads_path: typing.Optional[Path] = None,
        software_name: str = DEFAULT_SOFTWARE_NAME,
    ) -> "Writer":
        """
        Create a new `Writer` instance to write new separate signal and reads
        files. Given only path, a derived pair of split and read filenames are used.
        """
        signal_path = path
        if reads_path is None:
            signal_path, reads_path = make_split_filename(path)

        return cls(
            p5b.create_split_file(
                str(signal_path),
                str(reads_path),
                software_name,
                None,
            )
        )

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()

    def close(self):
        """Close the FileWriter handle"""
        if self._writer:
            self._writer.close()
            self._writer = None

    def find_calibration(self, calibration: Calibration) -> typing.Tuple[int, bool]:
        if calibration in self._calibration_types:
            return self._calibration_types[calibration], False

        added_idx = self.add_calibration(calibration)
        self._calibration_types[calibration] = added_idx
        return added_idx, True

    def find_pore(self, pore_data: Pore) -> typing.Tuple[int, bool]:
        if pore_data in self._pore_types:
            return self._pore_types[pore_data], False

        added_idx = self.add_pore(pore_data)
        self._pore_types[pore_data] = added_idx
        return added_idx, True

    def find_end_reason(self, end_reason: EndReason) -> typing.Tuple[int, bool]:
        if end_reason in self._end_reason_types:
            return self._end_reason_types[end_reason], False

        added_idx = self.add_end_reason(end_reason)
        self._end_reason_types[end_reason] = added_idx
        return added_idx, True

    def find_run_info(self, run_info: RunInfo) -> typing.Tuple[int, bool]:
        if run_info in self._run_info_types:
            return self._run_info_types[run_info], False

        added_idx = self.add_run_info(run_info)
        self._run_info_types[run_info] = added_idx
        return added_idx, True

    def add_read_object(self, read: Read, pre_compressed_signal: bool = False) -> None:

        if pre_compressed_signal:
            self.add_read(
                read_id=read.read_id,
                pore=self.find_pore(read.pore)[0],
                calibration=self.find_calibration(read.calibration)[0],
                read_number=read.read_number,
                start_sample=read.start_time,
                median_before=read.median_before,
                end_reason=self.find_end_reason(read.end_reason)[0],
                run_info=self.find_run_info(read.run_info)[0],
                signal=[read.signal],
                sample_count=[read.samples_count],
                pre_compressed_signal=pre_compressed_signal,
            )
        else:
            self.add_read(
                read_id=read.read_id,
                pore=self.find_pore(read.pore)[0],
                calibration=self.find_calibration(read.calibration)[0],
                read_number=read.read_number,
                start_sample=read.start_time,
                median_before=read.median_before,
                end_reason=self.find_end_reason(read.end_reason)[0],
                run_info=self.find_run_info(read.run_info)[0],
                signal=read.signal,
                sample_count=read.samples_count,
                pre_compressed_signal=pre_compressed_signal,
            )

    def add_read(
        self,
        read_id: UUID,
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
    ) -> None:
        return self.add_reads(
            numpy.array([numpy.frombuffer(read_id.bytes, dtype=numpy.uint8)]),
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

    def add_pore(self, pore: Pore) -> int:
        return self._writer.add_pore(pore.channel, pore.well, pore.pore_type)

    def add_calibration(self, calibration: Calibration) -> int:
        return self._writer.add_calibration(calibration.offset, calibration.scale)

    def add_end_reason(self, end_reason: EndReason) -> int:
        return self._writer.add_end_reason(end_reason.name.value, end_reason.forced)

    def add_run_info(self, run_info: RunInfo) -> int:
        return self._writer.add_run_info(
            run_info.acquisition_id,
            timestamp_to_int(run_info.acquisition_start_time),
            run_info.adc_max,
            run_info.adc_min,
            map_to_tuples(run_info.context_tags),
            run_info.experiment_name,
            run_info.flow_cell_id,
            run_info.flow_cell_product_code,
            run_info.protocol_name,
            run_info.protocol_run_id,
            timestamp_to_int(run_info.protocol_start_time),
            run_info.sample_id,
            run_info.sample_rate,
            run_info.sequencing_kit,
            run_info.sequencer_position,
            run_info.sequencer_position_type,
            run_info.software,
            run_info.system_name,
            run_info.system_type,
            map_to_tuples(run_info.tracking_id),
        )


class CombinedWriter(Writer):
    """
    Pod5 Combined File Writer
    """

    def __init__(
        self, combined_path: Path, software_name: str = DEFAULT_SOFTWARE_NAME
    ) -> None:
        """
        Open a combined pod5 file for WRITING.

        Parameters
        ----------
        `signal_path` : `Path`
           The path to the signal file to create
        `reads_path` : `Path`
           The path to the reads file to create
        `software_name`: `str`
            The name of the application used to create this split pod5 file

        Returns
        -------
        `SplitWriter`
        """
        self._combined_path = combined_path
        self._software_name = software_name

        if not self.combined_path.is_file():
            raise FileExistsError("Input path already exists. Refusing to overwrite.")

        super().__init__(
            p5b.create_combined_file(str(combined_path), software_name, None)
        )

    @property
    def combined_path(self) -> Path:
        """Return the path to the combined pod5 file"""
        return self._combined_path

    @property
    def software_name(self) -> str:
        """Return the software name used to open this file"""
        return self._software_name


class SplitWriter(Writer):
    """
    Pod5 Split File Writer
    """

    def __init__(
        self,
        signal_path: Path,
        reads_path: Path,
        software_name: str = DEFAULT_SOFTWARE_NAME,
    ) -> None:
        """
        Open a split pair of pod5 file for WRITING.

        Parameters
        ----------
        `signal_path` : `Path`
           The path to the signal file to create
        `reads_path` : `Path`
           The path to the reads file to create
        `software_name`: `str`
            The name of the application used to create this split pod5 file

        Returns
        -------
        `SplitWriter`
        """
        self._signal_path = signal_path
        self._reads_path = reads_path
        self._software_name = software_name

        if self._signal_path.is_file() or self._reads_path.is_file():
            raise FileExistsError("Input path already exists. Refusing to overwrite.")

        super().__init__(
            p5b.create_split_file(
                str(signal_path), str(reads_path), software_name, None
            )
        )

    @classmethod
    def from_inferred(
        cls, path: Path, software_name: str = DEFAULT_SOFTWARE_NAME
    ) -> "SplitWriter":
        """
        Open a split pair of pod5 file for WRITING. Given `path`, infer the pair
        of split pod5 filepaths.

        Parameters
        ----------
        `path` : `Path`
           The path to create _signal and _reads paths using `make_split_filename`
        `software_name`: `str`
            The name of the application used to create this split pod5 file

        Returns
        -------
        `SplitWriter`
        """
        signal_path, reads_path = make_split_filename(path, assert_exists=True)
        return cls(signal_path, reads_path, software_name)

    @property
    def reads_path(self) -> Path:
        """Return the path to the reads pod5 file"""
        return self._reads_path

    @property
    def signal_path(self) -> Path:
        """Return the path to the signal pod5 file"""
        return self._signal_path

    @property
    def software_name(self) -> str:
        """Return the software name used to open this file"""
        return self._software_name


def create_combined_file(
    filename: Path, software_name: str = DEFAULT_SOFTWARE_NAME
) -> Writer:
    """
    Return a pod5 `Writer` instance to write a combined pod5 file to `filename` path.
    """
    deprecation_warning(
        "pod5_format.writer.create_combined_file",
        "pod5_format.writer.CombinedWriter",
    )
    return Writer.open_combined(path=Path(filename), software_name=software_name)


def create_split_file(
    file: Path,
    reads_file: typing.Optional[Path] = None,
    software_name: str = DEFAULT_SOFTWARE_NAME,
) -> SplitWriter:
    """
    Deprecated in favour of Writer.open_split
    Return a pod5 `Writer` instance to write a spit pod5 file to `filename` path.
    """
    deprecation_warning(
        "pod5_format.writer.create_split_file",
        "pod5_format.writer.SplitWriter",
    )
    if reads_file is None:
        return SplitWriter.from_inferred(path=Path(file), software_name=software_name)
    return SplitWriter(
        signal_path=Path(file), reads_path=Path(reads_file), software_name=software_name
    )
