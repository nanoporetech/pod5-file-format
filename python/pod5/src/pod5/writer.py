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
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import lib_pod5 as p5b
import numpy as np
import pytz

from pod5.api_utils import Pod5ApiException
from pod5.pod5_types import (
    BaseRead,
    CompressedRead,
    EndReason,
    PathOrStr,
    Read,
    RunInfo,
)

DEFAULT_SOFTWARE_NAME = "Python API"

PoreType = str
T = TypeVar("T", bound=Union[EndReason, PoreType, RunInfo])


def force_type_and_default(value, dtype, count, default_value=None):
    if default_value is not None and value is None:
        value = np.array([default_value] * count, dtype=dtype)
    assert value is not None
    return value.astype(type, copy=False)


def map_to_tuples(info_map: Any) -> List[Tuple[str, str]]:
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

    def __init__(self, path: PathOrStr, software_name: str = DEFAULT_SOFTWARE_NAME):
        """
        Open a pod5 file for Writing.

        Parameters
        ----------
        path : os.PathLike, str
            The path to the pod5 file to create
        software_name : str
            The name of the application used to create this pod5 file
        """
        self._path = Path(path).absolute()
        self._software_name = software_name

        if self._path.is_file():
            raise FileExistsError("Input path already exists. Refusing to overwrite.")

        self._writer: Optional[p5b.FileWriter] = p5b.create_file(
            str(self._path), software_name, None
        )
        if not self._writer:
            raise Pod5ApiException(
                f"Failed to open writer at {self._path} : {p5b.get_error_string()}"
            )

        self._end_reasons: Dict[EndReason, int] = {}
        self._pores: Dict[PoreType, int] = {}
        self._run_infos: Dict[RunInfo, int] = {}

        # Internal lookup of object cache based on their respective type
        self._index_caches: Dict[Type, Dict[Any, int]] = {
            EndReason: self._end_reasons,
            PoreType: self._pores,
            RunInfo: self._run_infos,
        }

        # Internal lookup of _add functions based on their respective type
        self._adder_funcs: Dict[Type, Callable[[Any], int]] = {
            EndReason: self._add_end_reason,
            PoreType: self._add_pore_type,
            RunInfo: self._add_run_info,
        }

    def __enter__(self) -> "Writer":
        return self

    def __exit__(self, *exc_details) -> None:
        self.close()

    def close(self) -> None:
        """Close the FileWriter handle"""
        if self._writer:
            self._writer.close()
            self._writer = None

    @property
    def path(self) -> Path:
        """Return the path to the pod5 file"""
        return self._path

    @property
    def software_name(self) -> str:
        """Return the software name used to open this file"""
        return self._software_name

    def add(self, obj: Union[EndReason, PoreType, RunInfo]) -> int:
        """
        Add a :py:class:`EndReason`, :py:class:`PoreType`, or
        :py:class:`RunInfo` object to the Pod5 file (if it doesn't already
        exist) and return the index of this object in the Pod5 file.

        Parameters
        ----------
        obj : :py:class:`EndReason`, :py:class:`PoreType`, :py:class:`RunInfo`
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

    def _add_end_reason(self, end_reason: EndReason) -> int:
        """Add the given EndReason instance to the pod5 file returning its index"""
        if self._writer is None:
            raise Pod5ApiException("Writer handle has been closed")
        return self._writer.add_end_reason(end_reason.reason.value)

    def _add_pore_type(self, pore_type: PoreType) -> int:
        """Add the given PoreType instance to the pod5 file returning its index"""
        if self._writer is None:
            raise Pod5ApiException("Writer handle has been closed")
        return self._writer.add_pore(pore_type)

    def _add_run_info(self, run_info: RunInfo) -> int:
        """Add the given RunInfo instance to the pod5 file returning its index"""
        if self._writer is None:
            raise Pod5ApiException("Writer handle has been closed")

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

    def contains(self, obj: Union[EndReason, RunInfo]) -> bool:
        """
        Test if this Pod5 file contains the given object.

        Parameters
        ----------
        obj: :py:class:`EndReason`, :py:class:`RunInfo`
            Object to find in this Pod5 file

        Returns
        -------
        True if obj has already been added to this file
        """
        return obj in self._index_caches[type(obj)]

    def find(self, obj: Union[EndReason, RunInfo]) -> int:
        """
        Returns the index of obj in this Pod5 file raising a KeyError if it is missing.

        Parameters
        ----------
        obj: :py:class:`EndReason`, :py:class:`RunInfo`
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

    def add_read(self, read: Union[Read, CompressedRead]) -> None:
        """
        Add a record to the open POD5 file with either compressed or uncompressed
        signal data depending on the given type of Read.

        Parameters
        ----------
        read : :py:class:`Read`, :py:class:`CompressedRead`
            POD5 Read or CompressedRead object to add as a record to the POD5 file.
        """
        self.add_reads([read])

    def add_reads(self, reads: Sequence[Union[Read, CompressedRead]]) -> None:
        """
        Add Read objects (with uncompressed signal data) as records in the open POD5
        file.

        Parameters
        ----------
        reads : Sequence of :py:class:`Read` or :py:class:`CompressedRead` exclusively
            List of Read object to be added to this POD5 file
        """

        # Nothing to do
        if not reads:
            return

        if self._writer is None:
            raise Pod5ApiException("Writer handle has been closed")

        if isinstance(reads[0], Read):
            return self._writer.add_reads(  # type: ignore [call-arg]
                *self._prepare_add_reads_args(reads),
                [r.signal for r in reads],  # type: ignore
            )
        elif isinstance(reads[0], CompressedRead):
            signal_chunks = [r.signal_chunks for r in reads]  # type: ignore
            signal_chunk_lengths = [r.signal_chunk_lengths for r in reads]  # type: ignore

            # Array containing the number of chunks for each signal
            signal_chunk_counts = np.array(
                [len(samples_per_chunk) for samples_per_chunk in signal_chunk_lengths],
                dtype=np.uint32,
            )

            return self._writer.add_reads_pre_compressed(  # type: ignore [call-arg]
                *self._prepare_add_reads_args(reads),
                # Join all signal data into one list
                list(itertools.chain(*signal_chunks)),
                # Join all read sample counts into one array
                np.concatenate(signal_chunk_lengths).astype(np.uint32),  # type: ignore [no-untyped-call]
                signal_chunk_counts,
            )

    def _prepare_add_reads_args(self, reads: Sequence[BaseRead]) -> List[Any]:
        """
        Converts the List of reads into the list of ctypes arrays of data to be supplied
        to the c api.
        """
        read_id = np.array(
            [np.frombuffer(read.read_id.bytes, dtype=np.uint8) for read in reads]
        )
        read_number = np.array([read.read_number for read in reads], dtype=np.uint32)
        start_sample = np.array([read.start_sample for read in reads], dtype=np.uint64)
        channel = np.array([read.pore.channel for read in reads], dtype=np.uint16)
        well = np.array([read.pore.well for read in reads], dtype=np.uint8)
        pore_type = np.array(
            [self.add(PoreType(read.pore.pore_type)) for read in reads], dtype=np.int16
        )
        calib_offset = np.array(
            [read.calibration.offset for read in reads], dtype=np.float32
        )
        calib_scale = np.array(
            [read.calibration.scale for read in reads], dtype=np.float32
        )
        median_before = np.array(
            [read.median_before for read in reads], dtype=np.float32
        )
        end_reason = np.array(
            [self.add(read.end_reason) for read in reads], dtype=np.int16
        )
        end_reason_forced = np.array(
            [read.end_reason.forced for read in reads], dtype=np.bool_
        )
        run_info = np.array([self.add(read.run_info) for read in reads], dtype=np.int16)
        num_minknow_events = np.array(
            [read.num_minknow_events for read in reads], dtype=np.uint64
        )
        tracked_scaling_scale = np.array(
            [read.tracked_scaling.scale for read in reads], dtype=np.float32
        )
        tracked_scaling_shift = np.array(
            [read.tracked_scaling.shift for read in reads], dtype=np.float32
        )
        predicted_scaling_scale = np.array(
            [read.predicted_scaling.scale for read in reads], dtype=np.float32
        )
        predicted_scaling_shift = np.array(
            [read.predicted_scaling.shift for read in reads], dtype=np.float32
        )
        num_reads_since_mux_change = np.array(
            [read.num_reads_since_mux_change for read in reads], dtype=np.uint32
        )
        time_since_mux_change = np.array(
            [read.time_since_mux_change for read in reads], dtype=np.float32
        )

        return [
            read_id.shape[0],
            read_id,
            read_number,
            start_sample,
            channel,
            well,
            pore_type,
            calib_offset,
            calib_scale,
            median_before,
            end_reason,
            end_reason_forced,
            run_info,
            num_minknow_events,
            tracked_scaling_scale,
            tracked_scaling_shift,
            predicted_scaling_scale,
            predicted_scaling_shift,
            num_reads_since_mux_change,
            time_since_mux_change,
        ]
