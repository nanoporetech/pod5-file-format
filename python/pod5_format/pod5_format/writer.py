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
from pod5_format.api_utils import deprecation_warning, Pod5ApiException
import pytz

import numpy as np

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
)

from pod5_format import make_split_filename

DEFAULT_SOFTWARE_NAME = "Python API"

PoreType = str
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
        self._pores: Dict[PoreType, int] = {}
        self._run_infos: Dict[RunInfo, int] = {}

        # Internal lookup of object cache based on their respective type
        self._index_caches: Dict[Type[T], Dict[T, int]] = {
            Calibration: self._calibrations,
            EndReason: self._end_reasons,
            PoreType: self._pores,
            RunInfo: self._run_infos,
        }

        # Internal lookup of _add functions based on their respective type
        self._adder_funcs: Dict[Type[T], Callable[[Any], int]] = {
            EndReason: self._add_end_reason,
            PoreType: self._add_pore_type,
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

    def add(self, obj: Union[Calibration, EndReason, PoreType, RunInfo]) -> int:
        """
        Add a :py:class:`Calibration`, :py:class:`EndReason`, :py:class:`PoreType`, or
        :py:class:`RunInfo` object to the Pod5 file (if it doesn't already
        exist) and return the index of this object in the Pod5 file.

        Parameters
        ----------
        obj : :py:class:`Calibration`, :py:class:`EndReason`, :py:class:`PoreType`, :py:class:`RunInfo`
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
        return self._writer.add_end_reason(end_reason.name.value)

    def _add_pore_type(self, pore_type: PoreType) -> int:
        """Add the given PoreType instance to the pod5 file returning its index"""
        return self._writer.add_pore(pore_type)

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

        self._writer.add_reads(
            *self._prepare_add_reads_args(
                reads,
            ),
            [r.signal for r in reads],
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

        signal_chunks = [r.signal_chunks for r in reads]
        signal_chunk_lengths = [r.signal_chunk_lengths for r in reads]

        # Array containing the number of chunks for each signal
        signal_chunk_counts = np.array(
            [len(samples_per_chunk) for samples_per_chunk in signal_chunk_lengths],
            dtype=np.uint32,
        )

        self._writer.add_reads_pre_compressed(
            *self._prepare_add_reads_args(reads),
            # Join all signal data into one list
            list(itertools.chain(*signal_chunks)),
            # Join all read sample counts into one array
            np.concatenate(signal_chunk_lengths).astype(np.uint32),
            signal_chunk_counts,
        )

    def _prepare_add_reads_args(self, reads: List[BaseRead]):
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
