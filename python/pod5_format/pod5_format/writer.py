"""
Tools for writing POD5 data 
"""
import datetime
import itertools
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from uuid import UUID
from pod5_format.api_utils import deprecation_warning, Pod5ApiException
import pytz

import numpy as np
import numpy.typing as npt

import pod5_format.pod5_format_pybind as p5b
from pod5_format.pod5_types import (
    BaseRead,
    Calibration,
    CompressedRead,
    EndReason,
    PathOrStr,
    Pore,
    Read,
    RunInfo,
    ShiftScalePair,
)

from pod5_format import make_split_filename

DEFAULT_SOFTWARE_NAME = "Python API"

T = TypeVar("T", bound=Union[Calibration, EndReason, Pore, RunInfo])


def force_type_and_default(value, dtype, count, default_value=None):
    if default_value is not None and value is None:
        value = np.array([default_value] * count, dtype=dtype)
    assert value is not None
    return value.astype(type, copy=False)


def map_to_tuples(info_map) -> List[Tuple[str, str]]:
    """
    Convert a fast5 property map (e.g. context_tags and tracking_id) to a
    tuple or string pairs to pass to pod5 C API
    """
    if isinstance(info_map, dict):
        return list((str(key), str(value)) for key, value in info_map.items())
    if isinstance(info_map, list):
        return list((str(item[0]), str(item[1])) for item in info_map)
    raise TypeError(f"Unknown input type for context tags {type(info_map)}")


def timestamp_to_int(time_stamp: Union[datetime.datetime, int]) -> int:
    """Convert a datetime timestamp to an integer if it's not already an integer"""
    if isinstance(time_stamp, int):
        return time_stamp
    return int(time_stamp.astimezone(pytz.utc).timestamp() * 1000)


class Writer:
    """Pod5 File Writer"""

    def __init__(self, writer: p5b.FileWriter) -> None:
        """
        Initialise a Pod5 file Writer

        Note
        ----
        Do not use this method to instantiate a :py:class:`Writer`.

        Instantiate this class from one of the factory class methods instead:
            :py:meth:`Writer.open_combined`
            :py:meth:`Writer.open_split`

        Parameters
        ----------
        writer : pod5_format.pod5_format_pybind.FileWriter
            An instance of the bound c_api FileWriter
        """

        if not writer:
            raise Pod5ApiException(f"Failed to open writer: {p5b.get_error_string()}")

        self._writer: p5b.FileWriter = writer
        self._calibrations: Dict[Calibration, int] = {}
        self._end_reasons: Dict[EndReason, int] = {}
        self._pores: Dict[Pore, int] = {}
        self._run_infos: Dict[RunInfo, int] = {}

        # Internal lookup of object cache based on their respective type
        self._index_caches: Dict[Type[T], Dict[T, int]] = {
            Calibration: self._calibrations,
            EndReason: self._end_reasons,
            Pore: self._pores,
            RunInfo: self._run_infos,
        }

        # Internal lookup of _add functions based on their respective type
        self._adder_funcs: Dict[Type[T], Callable[[Any], int]] = {
            Calibration: self._add_calibration,
            EndReason: self._add_end_reason,
            Pore: self._add_pore,
            RunInfo: self._add_run_info,
        }

    @classmethod
    def open_combined(
        cls, path: PathOrStr, software_name: str = DEFAULT_SOFTWARE_NAME
    ) -> "Writer":
        """
        Instantiate a :py:class:`Writer` instance to write a new combined file at path

        Note
        ----
        "path" must not point to an existing file object


        Parameters
        ----------
        path : os.PathLike, str
            A path to open a new combined Pod5 file for writing
        software_name : str
            The name of the software used to create this file (Default: "Python API")

        Returns
        -------
        :py:class:`Writer` ready to write a new combined pod5 file

        """
        abs_path = str(Path(path).absolute())
        return cls(p5b.create_combined_file(abs_path, software_name, None))

    @classmethod
    def open_split(
        cls,
        path: PathOrStr,
        reads_path: Optional[PathOrStr] = None,
        software_name: str = DEFAULT_SOFTWARE_NAME,
    ) -> "Writer":
        """
        Instantiate a :py:class:`Writer` instance to write a new split signal and read
        pod5 files. Given only "path", an derived pair of split and read filenames are
        created using :py:meth:`make_split_filename`.

        Note
        ----
        "path" must not point to an existing file object

        Parameters
        ----------
        path : os.PathLike, str
            A path to the signal pod5 file or a basename from which filenames are
            derived
        reads_path : Optional, os.PathLike, str
            Optionally provide the read pod5 file path
        software_name : str
            The name of the software used to create this file (Default: "Python API")

        Returns
        -------
        :py:class:`Writer` ready to write a new split pod5 file

        """
        signal_path = Path(path).absolute()
        if reads_path is None:
            signal_path, reads_path = make_split_filename(path)
        else:
            reads_path = Path(reads_path).absolute()

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

    def close(self) -> None:
        """Close the FileWriter handle"""
        if self._writer:
            self._writer.close()
            self._writer = None

    def add(self, obj: Union[Calibration, EndReason, Pore, RunInfo]) -> int:
        """
        Add a :py:class:`Calibration`, :py:class:`EndReason`, :py:class:`Pore`, or
        :py:class:`RunInfo` object to the Pod5 file (if it doesn't already
        exist) and return the index of this object in the Pod5 file.

        Parameters
        ----------
        obj : :py:class:`Calibration`, :py:class:`EndReason`, :py:class:`Pore`, :py:class:`RunInfo`
            Object to find in this Pod5 file, adding it if it doesn't exist already

        Returns
        -------
        index : int
            The index of the object in the Pod5 file
        """
        # Get the index cache for the type of object given
        index_cache = self._index_caches[type(obj)]

        # Return the index of this object if it exists
        if obj in index_cache:
            return index_cache[obj]

        # Add object using the associated adder function e.g. _add_pore(pore: Pore)
        # and store the new index in the cache for future look-ups avoiding duplication
        added_index = self._adder_funcs[type(obj)](obj)
        index_cache[obj] = added_index

        # Return the newly added index
        return added_index

    def _add_calibration(self, calibration: Calibration) -> int:
        """Add the given Calibration instance to the pod5 file returning its index"""
        return self._writer.add_calibration(calibration.offset, calibration.scale)

    def _add_end_reason(self, end_reason: EndReason) -> int:
        """Add the given EndReason instance to the pod5 file returning its index"""
        return self._writer.add_end_reason(end_reason.name.value, end_reason.forced)

    def _add_pore(self, pore: Pore) -> int:
        """Add the given Pore instance to the pod5 file returning its index"""
        return self._writer.add_pore(pore.channel, pore.well, pore.pore_type)

    def _add_run_info(self, run_info: RunInfo) -> int:
        """Add the given RunInfo instance to the pod5 file returning its index"""
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

    def contains(self, obj: Union[Calibration, EndReason, Pore, RunInfo]) -> bool:
        """
        Test if this Pod5 file contains the given object.

        Parameters
        ----------
        obj: :py:class:`Calibration`, :py:class:`EndReason`, :py:class:`Pore`, :py:class:`RunInfo`
            Object to find in this Pod5 file

        Returns
        -------
        True if obj has already been added to this file
        """
        return obj in self._index_caches[type(obj)]

    def find(self, obj: Union[Calibration, EndReason, Pore, RunInfo]) -> int:
        """
        Returns the index of obj in this Pod5 file raising a KeyError if it is missing.

        Parameters
        ----------
        obj: :py:class:`Calibration`, :py:class:`EndReason`, :py:class:`Pore`, :py:class:`RunInfo`
            Obj instance to find in this Pod5 file

        Returns
        -------
        The index of the object in this Pod5 file

        Raises
        ------
        KeyError
            If the object is not in this file
        """
        try:
            return self._index_caches[type(obj)][obj]
        except KeyError as exc:
            raise KeyError(
                f"Could not find index of {obj} in Pod5 file writer: {self}"
            ) from exc

    def add_read_object(self, read: Union[Read, CompressedRead]) -> None:
        """
        Add a record to the open POD5 file with either compressed or uncompressed
        signal data depending on the given type of Read.

        Parameters
        ----------
        read : :py:class:`Read`, :py:class:`CompressedRead`
            POD5 Read or CompressedRead object to add as a record to the POD5 file.
        """
        if isinstance(read, CompressedRead):
            # Add a pre-compressed read
            self.add_read_objects_pre_compressed([read])
        else:
            # Add an uncompressed read
            self.add_read_objects([read])

    def add_read_objects(self, reads: Iterable[Read]) -> None:
        """
        Add Read objects (with uncompressed signal data) as records in the open POD5
        file.

        Parameters
        ----------
        reads : Iterable[Read]
            Iterable of Read object to be added to this POD5 file
        """

        # Nothing to do
        if not reads:
            return

        self.add_reads(
            **self._arrays_from_read_objects(reads),
            signals=[r.signal for r in reads],
        )

    def add_read_objects_pre_compressed(self, reads: Iterable[CompressedRead]) -> None:
        """
        Add Read objects (with compressed signal data) as records in the open POD5
        file.

        Parameters
        ----------
        reads : Iterable[CompressedRead]
            Iterable of CompressedRead objects to be added to this POD5 file
        """
        # Nothing to do
        if not reads:
            return

        self.add_reads_pre_compressed(
            **self._arrays_from_read_objects(reads),
            signal_chunks=[r.signal_chunks for r in reads],
            signal_chunk_lengths=[r.signal_chunk_lengths for r in reads],
        )

    def _arrays_from_read_objects(self, reads: Iterable[BaseRead]):
        """
        Build a dict of numpy arrays keyed by read field names, from an array of read objects.

        Parameters
        ----------
        reads : Iterable[BaseRead]
            Iterable of BaseRead object to be added to this POD5 file
        """

        return {
            "read_ids": np.array(
                [np.frombuffer(r.read_id.bytes, dtype=np.uint8) for r in reads]
            ),
            "pores": np.array([self.add(r.pore) for r in reads], dtype=np.int16),
            "calibrations": np.array(
                [self.add(r.calibration) for r in reads], dtype=np.int16
            ),
            "read_numbers": np.array([r.read_number for r in reads], dtype=np.uint32),
            "start_samples": np.array([r.start_sample for r in reads], dtype=np.uint64),
            "median_befores": np.array(
                [r.median_before for r in reads], dtype=np.float32
            ),
            "end_reasons": np.array(
                [self.add(r.end_reason) for r in reads], dtype=np.int16
            ),
            "run_infos": np.array(
                [self.add(r.run_info) for r in reads], dtype=np.int16
            ),
            "num_minknow_events": np.array(
                [r.num_minknow_events for r in reads], dtype=np.uint64
            ),
            "tracked_scaling_scale": np.array(
                [r.tracked_scaling.scale for r in reads], dtype=np.float32
            ),
            "tracked_scaling_shift": np.array(
                [r.tracked_scaling.shift for r in reads], dtype=np.float32
            ),
            "predicted_scaling_scale": np.array(
                [r.predicted_scaling.scale for r in reads], dtype=np.float32
            ),
            "predicted_scaling_shift": np.array(
                [r.predicted_scaling.shift for r in reads], dtype=np.float32
            ),
            "num_reads_since_mux_change": np.array(
                [r.num_reads_since_mux_change for r in reads], dtype=np.uint32
            ),
            "time_since_mux_change": np.array(
                [r.time_since_mux_change for r in reads], dtype=np.float32
            ),
        }

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
        signal: npt.NDArray[np.int16],
        **kwargs,
    ) -> None:
        """
        Add a record to the open POD5 file with uncompressed signal data.

        Note
        ----
        This method expects dictionary array indices (not instances) of metadata
        classes (e.g. Calibration, Pore, ...)

        Parameters
        ---------
        read_id: UUID
            numpy array of read_ids as uint8
        pore: int
            pore dictionary array index
        calibration: int
            calibration dictionary array index
        read_number: int
            read number
        start_sample: int
            start samples value
        median_before: float
            median before value
        end_reasons: int
            end reason dictionary array index
        run_infos: int
            run info dictionary array index
        signal: npt.NDArray[np.int16]
            Signal data as a numpy array of int16
        kwargs:
            Additional per read values that can be passed (see _map_single_read_kwargs_arguments_to_np_array)

        """
        return self.add_reads(
            np.array([np.frombuffer(read_id.bytes, dtype=np.uint8)]),
            np.array([pore], dtype=np.int16),
            np.array([calibration], dtype=np.int16),
            np.array([read_number], dtype=np.uint32),
            np.array([start_sample], dtype=np.uint64),
            np.array([median_before], dtype=np.float32),
            np.array([end_reason], dtype=np.int16),
            np.array([run_info], dtype=np.int16),
            [signal],
            **self._map_single_read_kwargs_arguments_to_np_array(**kwargs),
        )

    def add_reads(
        self,
        read_ids: npt.NDArray[np.uint8],
        pores: npt.NDArray[np.int16],
        calibrations: npt.NDArray[np.int16],
        read_numbers: npt.NDArray[np.uint32],
        start_samples: npt.NDArray[np.uint64],
        median_befores: npt.NDArray[np.float32],
        end_reasons: npt.NDArray[np.int16],
        run_infos: npt.NDArray[np.int16],
        signals: List[npt.NDArray[np.int16]],
        **kwargs,
    ):
        """
        Add records to the open POD5 file with uncompressed signal data.

        Note
        ----
        This method expects dictionary array indices (not instances) of metadata
        classes (e.g. Calibration, Pore, ...)

        Parameters
        ---------
        read_ids: npt.NDArray[np.uint8]
            numpy array of read_ids as uint8
        pores: npt.NDArray[np.int16]
            numpy array of pore dictionary array indices as int16
        calibrations: npt.NDArray[np.int16]
            numpy array of calibration dictionary array indices as int16
        read_numbers: npt.NDArray[np.uint32]
            numpy array of read numbers as int32
        start_samples: npt.NDArray[np.uint64]
            numpy array of start samples as int64
        median_befores: npt.NDArray[np.float32]
            numpy array of median before values as float32
        end_reasons: npt.NDArray[np.int16]
            numpy array of end reason dictionary array indices as int16
        run_infos: npt.NDArray[np.int16]
            numpy array of run info dictionary array indices as int16
        signals: List[npt.NDArray[np.int16]]
            List of signal data as numpy array as int16
        kwargs:
            Additional per read values that can be passed.
        """

        self._writer.add_reads(
            *self._prepare_add_reads_args(
                read_ids,
                pores,
                calibrations,
                read_numbers,
                start_samples,
                median_befores,
                end_reasons,
                run_infos,
                **kwargs,
            ),
            signals,
        )

    def add_read_pre_compressed(
        self,
        read_id: UUID,
        pore: int,
        calibration: int,
        read_number: int,
        start_sample: int,
        median_before: float,
        end_reason: int,
        run_info: int,
        signal_chunks: List[npt.NDArray[np.uint8]],
        signal_chunk_lengths: List[int],
        **kwargs,
    ):
        """
        Add a record to the open POD5 file.

        Note
        ----
        This method expects dictionary array indices (not instances) of metadata
        classes (e.g. Calibration, Pore, ...)

        Parameters
        ---------
        read_id: UUID
            numpy array of read_ids as uint8
        pore: int
            pore dictionary array index
        calibration: int
            calibration dictionary array index
        read_number: int
            read number
        start_sample: int
            start samples value
        median_before: float
            median before value
        end_reasons: int
            end reason dictionary array index
        run_infos: int
            run info dictionary array index
        signal_chunks: List[npt.NDArray[np.uint8]]
            List of chunks of compressed signal data as uint8.
        signal_chunk_lengths: List[List[int]]
            List of the number of **original** signal data samples in each
            chunk **before compression**.
        kwargs:
            Additional per read values that can be passed (see _map_single_read_kwargs_arguments_to_np_array)
        """
        return self.add_reads_pre_compressed(
            np.array([np.frombuffer(read_id.bytes, dtype=np.uint8)]),
            np.array([pore], dtype=np.int16),
            np.array([calibration], dtype=np.int16),
            np.array([read_number], dtype=np.uint32),
            np.array([start_sample], dtype=np.uint64),
            np.array([median_before], dtype=np.float32),
            np.array([end_reason], dtype=np.int16),
            np.array([run_info], dtype=np.int16),
            [signal_chunks],
            [signal_chunk_lengths],
            **self._map_single_read_kwargs_arguments_to_np_array(**kwargs),
        )

    def _map_single_read_kwargs_arguments_to_np_array(
        self,
        num_minknow_events: int = 0,
        tracked_scaling: ShiftScalePair = ShiftScalePair(),
        predicted_scaling: ShiftScalePair = ShiftScalePair(),
        num_reads_since_mux_change: int = 0,
        time_since_mux_change: float = 0.0,
    ):
        """
        Map individual read values to a dict of numpy arrays to be used for

        Parameters
        ---------
        num_minknow_events: int
            Number of minknow events
        tracked_scaling: ShiftScalePair
            Tracked scaling values for the read
        predicted_scaling: ShiftScalePair
            Predicted scaling values for the read
        num_reads_since_mux_change: int
            Number of selected reads since the last mux change on this reads channel
        time_since_mux_change: float
            Time in seconds since the last mux change on this reads channel
        """
        return {
            "num_minknow_events": np.array([num_minknow_events], dtype=np.uint64),
            "tracked_scaling_scale": np.array(
                [tracked_scaling.scale], dtype=np.float32
            ),
            "tracked_scaling_shift": np.array(
                [tracked_scaling.shift], dtype=np.float32
            ),
            "predicted_scaling_scale": np.array(
                [predicted_scaling.scale], dtype=np.float32
            ),
            "predicted_scaling_shift": np.array(
                [predicted_scaling.shift], dtype=np.float32
            ),
            "num_reads_since_mux_change": np.array(
                [num_reads_since_mux_change], dtype=np.uint32
            ),
            "time_since_mux_change": np.array(
                [time_since_mux_change], dtype=np.float32
            ),
        }

    def add_reads_pre_compressed(
        self,
        read_ids: npt.NDArray[np.uint8],
        pores: npt.NDArray[np.int16],
        calibrations: npt.NDArray[np.int16],
        read_numbers: npt.NDArray[np.uint32],
        start_samples: npt.NDArray[np.uint64],
        median_befores: npt.NDArray[np.float32],
        end_reasons: npt.NDArray[np.int16],
        run_infos: npt.NDArray[np.int16],
        signal_chunks: List[List[npt.NDArray[np.uint8]]],
        signal_chunk_lengths: List[List[int]],
        **kwargs,
    ):
        """
        Add records to the open POD5 file with pre-compressed signal data.

        Note
        ----
        This method expects dictionary array indices (not instances) of metadata
        classes (e.g. Calibration, Pore, ...)

        Parameters
        ---------
        read_ids: npt.NDArray[np.uint8]
            numpy array of read_ids as uint8
        pores: npt.NDArray[np.int16]
            numpy array of pore dictionary array indices as int16
        calibrations: npt.NDArray[np.int16]
            numpy array of calibration dictionary array indices as int16
        read_numbers: npt.NDArray[np.uint32]
            numpy array of read numbers as int32
        start_samples: npt.NDArray[np.uint64]
            numpy array of start samples as int64
        median_befores: npt.NDArray[np.float32]
            numpy array of median before values as float32
        end_reasons: npt.NDArray[np.int16]
            numpy array of end reason dictionary array indices as int16
        run_infos: npt.NDArray[np.int16]
            numpy array of run info dictionary array indices as int16
        signal_chunks: List[List[npt.NDArray[np.uint8]]]
            List of lists of chunked and compressed signal data as uint8.
            Each top-level list is a complete read, each sub-list is chunk of compressed
            signal data.
        signal_chunk_lengths: List[List[int]]
            List of lists of the number of **original** signal data samples in each
            chunk **before compression**.
        kwargs:
            Additional per read values that can be passed.
        """

        # Nothing to do
        row_count = read_ids.shape[0]
        if row_count == 0:
            return

        # Array containing the number of chunks for each signal
        signal_chunk_counts = np.array(
            [len(samples_per_chunk) for samples_per_chunk in signal_chunk_lengths],
            dtype=np.uint32,
        )

        self._writer.add_reads_pre_compressed(
            *self._prepare_add_reads_args(
                read_ids,
                pores,
                calibrations,
                read_numbers,
                start_samples,
                median_befores,
                end_reasons,
                run_infos,
                **kwargs,
            ),
            # Join all signal data into one list
            list(itertools.chain(*signal_chunks)),
            # Join all read sample counts into one array
            np.concatenate(signal_chunk_lengths).astype(np.uint32),
            signal_chunk_counts,
        )

    def _prepare_add_reads_args(
        self,
        read_ids: npt.NDArray[np.uint8],
        pores: npt.NDArray[np.int16],
        calibrations: npt.NDArray[np.int16],
        read_numbers: npt.NDArray[np.uint32],
        start_samples: npt.NDArray[np.uint64],
        median_befores: npt.NDArray[np.float32],
        end_reasons: npt.NDArray[np.int16],
        run_infos: npt.NDArray[np.int16],
        **kwargs,
    ):
        row_count = read_ids.shape[0]
        return [
            read_ids.shape[0],
            force_type_and_default(read_ids, np.uint8, row_count),
            force_type_and_default(pores, np.int16, row_count),
            force_type_and_default(calibrations, np.int16, row_count),
            force_type_and_default(read_numbers, np.uint32, row_count),
            force_type_and_default(start_samples, np.uint64, row_count),
            force_type_and_default(median_befores, np.float32, row_count),
            force_type_and_default(end_reasons, np.int16, row_count),
            force_type_and_default(run_infos, np.int16, row_count),
            force_type_and_default(
                kwargs.get("num_minknow_events", None), np.uint64, row_count, 0
            ),
            force_type_and_default(
                kwargs.get("tracked_scaling_scale", None),
                np.float32,
                row_count,
                float("nan"),
            ),
            force_type_and_default(
                kwargs.get("tracked_scaling_shift", None),
                np.float32,
                row_count,
                float("nan"),
            ),
            force_type_and_default(
                kwargs.get("predicted_scaling_scale", None),
                np.float32,
                row_count,
                float("nan"),
            ),
            force_type_and_default(
                kwargs.get("predicted_scaling_shift", None),
                np.float32,
                row_count,
                float("nan"),
            ),
            force_type_and_default(
                kwargs.get("num_reads_since_mux_change", None),
                np.uint32,
                row_count,
                0,
            ),
            force_type_and_default(
                kwargs.get("time_since_mux_change", None),
                np.float32,
                row_count,
                0.0,
            ),
        ]


class CombinedWriter(Writer):
    """
    Pod5 Combined File Writer
    """

    def __init__(
        self, combined_path: PathOrStr, software_name: str = DEFAULT_SOFTWARE_NAME
    ) -> None:
        """
        Open a combined pod5 file for Writing.

        Parameters
        ----------
        combined_path : os.PathLike, str
           The path to the combined pod5 file to create
        software_name : str
            The name of the application used to create this split pod5 file
        """
        self._combined_path = Path(combined_path).absolute()
        self._software_name = software_name

        if self.combined_path.is_file():
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
        signal_path: PathOrStr,
        reads_path: PathOrStr,
        software_name: str = DEFAULT_SOFTWARE_NAME,
    ) -> None:
        """
        Open a split pair of pod5 file for Writing.

        Parameters
        ----------
        signal_path : os.PathLike, str
           The path to the signal file to create
        reads_path : os.PathLike, str
           The path to the reads file to create
        software_name : str
            The name of the application used to create this split pod5 file

        Raises
        ------
        FileExistsError
            If either of the signal_path or reads_path already exist
        """
        self._signal_path = Path(signal_path).absolute()
        self._reads_path = Path(reads_path).absolute()
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
        cls, path: PathOrStr, software_name: str = DEFAULT_SOFTWARE_NAME
    ) -> "SplitWriter":
        """
        Open a split pair of pod5 file for Writing. Given only "path", infer the pair
        of split pod5 filepaths using :py:meth:`make_split_filename`.

        Parameters
        ----------
        path : os.PathLike, str
           The base path to create _signal and _reads paths using make_split_filename
        software_name`: os.PathLike, str
            The name of the application used to create this split pod5 file

        Raises
        ------
        FileExistsError
            If either of the inferred signal_path or reads_path already exist
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
    filename: PathOrStr, software_name: str = DEFAULT_SOFTWARE_NAME
) -> CombinedWriter:
    """
    Returns a :py:class:`CombinedWriter` instance

    Parameters
    ----------
    filename : os.PathLike, str
        The path used to create a new combined pod5 file
    software_name : str
        The name of the application used to create this split pod5 file
    Warns
    -----
    FutureWarning
        This method is deprecated in favour of :py:class:`CombinedWriter`
    """
    deprecation_warning(
        "pod5_format.writer.create_combined_file",
        "pod5_format.writer.CombinedWriter",
    )
    return CombinedWriter(combined_path=filename, software_name=software_name)


def create_split_file(
    file: Path,
    reads_file: Optional[Path] = None,
    software_name: str = DEFAULT_SOFTWARE_NAME,
) -> SplitWriter:
    """
    Returns a :py:class:`SplitWriter` instance

    Parameters
    ----------
    file : os.PathLike, str
        The base path to create _signal and _reads paths using make_split_filename
        unless reads_file is also given
    read_file : Optional, os.PathLike, str
        If given, use "file" as _signal and this as the _reads paths when opening
        split pod5 files
    software_name : str
        The name of the application used to create this split pod5 file

    Raises
    ------
    FileExistsError
        If either of the inferred signal_path or reads_path already exist

    Warns
    -----
    FutureWarning
        This method is deprecated in favour of :py:class:`SplitWriter`
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
