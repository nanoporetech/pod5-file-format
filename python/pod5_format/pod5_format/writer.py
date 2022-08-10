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
    Calibration,
    EndReason,
    PathOrStr,
    Pore,
    Read,
    RunInfo,
)
from pod5_format import make_split_filename


DEFAULT_SOFTWARE_NAME = "Python API"

T = TypeVar("T", bound=Union[Calibration, EndReason, Pore, RunInfo])


def map_to_tuples(info_map) -> List[Tuple[Any, ...]]:
    """Convert a map (e.g. context_tags and tracking_id) to a tuple to pass to c_api"""
    if isinstance(info_map, dict):
        return list((key, value) for key, value in info_map.items())
    elif isinstance(info_map, list):
        return list(tuple(item) for item in info_map)
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
        return cls(p5b.create_combined_file(str(path), software_name, None))

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

    def add_read_object(self, read: Read, pre_compressed_signal: bool = False) -> None:
        """
        Add the given :py:class:`Read` and all of its members to the Pod5 file
        with a flag indicating if the signal data has been compressed or not

        Parameters
        ----------
        read : :py:class:`Read`
            Pod5 Read object to add to the Pod5 file
        pre_compressed_signal: bool
            Flag indicating if the signal data has been compressed
        """
        if pre_compressed_signal:
            self.add_read(
                read_id=read.read_id,
                pore=self.add(read.pore),
                calibration=self.add(read.calibration),
                read_number=read.read_number,
                start_sample=read.start_time,
                median_before=read.median_before,
                end_reason=self.add(read.end_reason),
                run_info=self.add(read.run_info),
                signal=[read.signal],
                sample_count=[read.samples_count],
                pre_compressed_signal=pre_compressed_signal,
            )
        else:
            self.add_read(
                read_id=read.read_id,
                pore=self.add(read.pore),
                calibration=self.add(read.calibration),
                read_number=read.read_number,
                start_sample=read.start_time,
                median_before=read.median_before,
                end_reason=self.add(read.end_reason),
                run_info=self.add(read.run_info),
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
        """
        Add a new read record to the Pod5 file with a flag indicating if the signal
        data has been compressed or not.

        Note
        ----
        Parameters to dictionary types such as calibration expect their Pod5 index which
        is returned when calling :py:meth:`add` or can be recovered using
        :py:meth:`find`.
        """
        return self.add_reads(
            np.array([np.frombuffer(read_id.bytes, dtype=np.uint8)]),
            np.array([pore], dtype=np.int16),
            np.array([calibration], dtype=np.int16),
            np.array([read_number], dtype=np.uint32),
            np.array([start_sample], dtype=np.uint64),
            np.array([median_before], dtype=float),
            np.array([end_reason], dtype=np.int16),
            np.array([run_info], dtype=np.int16),
            [signal],
            np.array([sample_count], dtype=np.uint32),
            pre_compressed_signal,
        )

    def add_reads(
        self,
        read_ids: npt.NDArray[np.uint8],
        pores: npt.NDArray[np.int16],
        calibrations: npt.NDArray[np.int16],
        read_numbers: npt.NDArray[np.uint32],
        start_samples: npt.NDArray[np.uint64],
        median_befores: npt.NDArray[np.float64],
        end_reasons: npt.NDArray[np.int16],
        run_infos: npt.NDArray[np.int16],
        signals,
        sample_counts,
        pre_compressed_signal: bool = False,
    ):
        """
        Add a new read records to the Pod5 file with a flag indicating if the signal
        data has been compressed or not. The parameters of this function are
        all numpy.ndarrays of various types
        """
        read_ids = read_ids.astype(np.uint8, copy=False)
        pores = pores.astype(np.int16, copy=False)
        calibrations = calibrations.astype(np.int16, copy=False)
        read_numbers = read_numbers.astype(np.uint32, copy=False)
        start_samples = start_samples.astype(np.uint64, copy=False)
        median_befores = median_befores.astype(np.float64, copy=False)
        end_reasons = end_reasons.astype(np.int16, copy=False)
        run_infos = run_infos.astype(np.int16, copy=False)

        if pre_compressed_signal:
            # Find an array of the number of chunks per read
            signal_chunk_counts = np.array(
                [len(sample_count) for sample_count in sample_counts],
                dtype=np.uint32,
            )
            # Join all read sample counts into one array
            sample_counts = np.concatenate(sample_counts).astype(np.uint32)

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
        self._combined_path = Path(combined_path)
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
        self._signal_path = Path(signal_path)
        self._reads_path = Path(reads_path)
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
