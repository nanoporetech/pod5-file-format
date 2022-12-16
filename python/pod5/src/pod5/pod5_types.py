"""
Container class for a pod5 Read object
"""
import datetime
import enum
import os
from dataclasses import dataclass, field
from typing import Dict, List, Union
from uuid import UUID

import numpy as np
import numpy.typing as npt

from pod5.signal_tools import vbz_decompress_signal_chunked

PathOrStr = Union[os.PathLike, str]


class EndReasonEnum(enum.Enum):
    """EndReason Enumeration"""

    UNKNOWN = 0
    MUX_CHANGE = 1
    UNBLOCK_MUX_CHANGE = 2
    DATA_SERVICE_UNBLOCK_MUX_CHANGE = 3
    SIGNAL_POSITIVE = 4
    SIGNAL_NEGATIVE = 5


_END_REASON_FORCED_DEFAULTS: Dict[EndReasonEnum, bool] = {
    EndReasonEnum.UNKNOWN: False,
    EndReasonEnum.MUX_CHANGE: True,
    EndReasonEnum.UNBLOCK_MUX_CHANGE: True,
    EndReasonEnum.DATA_SERVICE_UNBLOCK_MUX_CHANGE: True,
    EndReasonEnum.SIGNAL_POSITIVE: False,
    EndReasonEnum.SIGNAL_NEGATIVE: False,
}


@dataclass(frozen=True)
class EndReason:
    """
    Data on why the Read ended.

    Parameters
    ----------

    reason: EndReasonEnum
        The end reason enumeration.
    forced: bool
        True if it is a 'forced' read break.
    """

    #: The end reason enumeration
    reason: EndReasonEnum
    #: True if it is a 'forced' read break (e.g. mux_change, unblock), False otherwise.
    forced: bool

    @property
    def name(self) -> str:
        """Return the reason name as a lower string"""
        return self.reason.name.lower()

    @classmethod
    def from_reason_with_default_forced(cls, reason: EndReasonEnum) -> "EndReason":
        """
        Return a new EndReason instance with the 'forced' flag set to the expected
        default for the given reason
        """
        return cls(reason=reason, forced=_END_REASON_FORCED_DEFAULTS[reason])


@dataclass()
class Calibration:
    """
    Parameters to convert the signal data to picoamps.

    Parameters
    ----------

    offset: float
        Calibration offset used to convert raw ADC data into pA readings.
    scale: float
        Calibration scale factor used to convert raw ADC data into pA readings.
    """

    #: Calibration offset used to convert raw ADC data into pA readings.
    offset: float
    #: Calibration scale factor used to convert raw ADC data into pA readings.
    scale: float

    @classmethod
    def from_range(cls, offset: float, adc_range: float, digitisation: float):
        """Create a Calibration instance from offset, adc_range and digitisation"""
        return cls(offset, adc_range / digitisation)


@dataclass()
class Pore:
    """
    Data for the pore that the Read was acquired on

    Parameters
    ----------

    channel: int
        1-indexed channel.
    well: int
        1-indexed well.
    pore_type: PoreType
        The pore type present in the well.
    """

    #: 1-indexed channel.
    channel: int
    #: 1-indexed well.
    well: int
    #: Name of the pore type present in the well.
    pore_type: str


@dataclass(frozen=True)
class RunInfo:
    """
    Higher-level information about the Reads that correspond to a part of an
    experiment, protocol or acquisition

    Parameters
    ----------

    acquisition_id : str
        A unique identifier for the acquisition.
    acquisition_start_time : datetime.datetime
        This is the clock time for sample 0
    adc_max : int
        The maximum ADC value that might be encountered.
    adc_min : int
        The minimum ADC value that might be encountered.
    context_tags : Dict[str, str]
        The context tags for the run. (For compatibility with fast5).
    experiment_name : str
        The user-supplied name for the experiment being run.
    flow_cell_id : str
        Uniquely identifies the flow cell the data was captured on.
    flow_cell_product_code : str
        Identifies the type of flow cell the data was captured on.
    protocol_name : str
        The name of the protocol that was run.
    protocol_run_id : str
        The unique identifier for the protocol run that produced this data.
    protocol_start_time : datetime.datetime
         When the protocol that the acquisition was part of started.
    sample_id : str
        A user-supplied name for the sample being analysed.
    sample_rate : int
        The number of samples acquired each second on each channel.
    sequencing_kit : str
        The type of sequencing kit used to prepare the sample.
    sequencer_position : str
        The sequencer position the data was collected on.
    sequencer_position_type : str
        The type of sequencing hardware the data was collected on.
    software : str
        A description of the software that acquired the data.
    system_name : str
        The name of the system the data was collected on.
    system_type : str
        The type of system the data was collected on.
    tracking_id : Dict[str, str]
        The tracking id for the run. (For compatibility with fast5).

    """

    #: A unique identifier for the acquisition - note that readers should not
    #: depend on this uniquely determining the other fields in the run_info, or being
    #: unique among the dictionary keys.
    acquisition_id: str
    #: This is the clock time for sample 0
    acquisition_start_time: datetime.datetime
    #: The maximum ADC value that might be encountered. This is a hardware constraint.
    adc_max: int
    #: The minimum ADC value that might be encountered. This is a hardware constraint.
    adc_min: int
    #: The context tags for the run. (For compatibility with fast5).
    context_tags: Dict[str, str] = field(hash=False, compare=True)
    #: The user-supplied name for the experiment being run.
    experiment_name: str
    #: Uniquely identifies the flow cell the data was captured on.
    #: This is written on the flow cell case.
    flow_cell_id: str
    #: Identifies the type of flow cell the data was captured on.
    flow_cell_product_code: str
    #: The name of the protocol that was run.
    protocol_name: str
    #: The unique identifier for the protocol run that produced this data.
    protocol_run_id: str
    #:  When the protocol that the acquisition was part of started.
    protocol_start_time: datetime.datetime
    #: A user-supplied name for the sample being analysed.
    sample_id: str
    #: The number of samples acquired each second on each channel.
    sample_rate: int
    #: The type of sequencing kit used to prepare the sample.
    sequencing_kit: str
    #: The sequencer position the data was collected on. For removable positions,
    #: like MinION Mk1Bs, this is unique (e.g. 'MN12345'), while for integrated
    #: positions it is not (e.g. 'X1' on a GridION).
    sequencer_position: str
    #: The type of sequencing hardware the data was collected on. For example:
    #: 'MinION Mk1B' or 'GridION' or 'PromethION'.
    sequencer_position_type: str
    #: A description of the software that acquired the data. For example:
    #: 'MinKNOW 21.05.12 (Bream 5.1.6, Configurations 16.2.1, Core 5.1.9, Guppy 4.2.3)'.
    software: str
    #: The name of the system the data was collected on. This might be a sequencer
    #: serial (eg: 'GXB1234') or a host name (e.g. 'Lab PC').
    system_name: str
    #: The type of system the data was collected on. For example, 'GridION Mk1' or
    #: 'PromethION P48'. If the system is not a Nanopore sequencer with built-in
    #: compute, this will be a description of the operating system
    #: (e.g. 'Ubuntu 20.04').
    system_type: str
    #: The tracking id for the run. (For compatibility with fast5).
    tracking_id: Dict[str, str] = field(hash=False, compare=True)


@dataclass()
class ShiftScalePair:
    """A pair of floating point shift and scale values."""

    shift: float = field(default=float("nan"))
    scale: float = field(default=float("nan"))


@dataclass()
class BaseRead:
    """
    Base class for POD5 Read Data

    Parameters
    ----------

    read_id : UUID
        The read_id of this read as UUID.
    pore : Pore
        Pore data.
    calibration : Calibration
        Calibration data.
    read_number : int
        The read number on channel. This is increasing but typically
        not necessarily consecutive.
    start_sample : int
        The number samples recorded on this channel before the read started.
    median_before : float
        The level of current in the well before this read.
    end_reason : EndReason
        EndReason data.
    run_info : RunInfo
        RunInfo data.
    num_minknow_events: int
        Number of minknow events that the read contains
    tracked_scaling: ShiftScalePair
        Shift and Scale for tracked read scaling values (based on previous reads shift)
    predicted_scaling: ShiftScalePair
        Shift and Scale for predicted read scaling values (based on this read's raw signal)
    num_reads_since_mux_change: int
        Number of selected reads since the last mux change on this reads channel
    time_since_mux_change: float
        Time in seconds since the last mux change on this reads channel
    """

    #: The read_id of this read as UUID
    read_id: UUID
    #: Pore metadata
    pore: Pore
    #: Calibration metadata
    calibration: Calibration
    #: The read number on channel. This is increasing but typically
    #: not necessarily consecutive.
    read_number: int
    #: The number samples recorded on this channel before the read started.
    start_sample: int
    #: The level of current in the well before this read.
    median_before: float
    #: EndReason data.
    end_reason: EndReason
    #: RunInfo data.
    run_info: RunInfo
    #: Number of minknow events that the read contains
    num_minknow_events: int = field(default=0)
    #: Shift and Scale for tracked read scaling values (based on previous reads shift)
    tracked_scaling: ShiftScalePair = field(
        default=ShiftScalePair(float("nan"), float("nan"))
    )
    #: Shift and Scale for predicted read scaling values (based on this read's raw signal)
    predicted_scaling: ShiftScalePair = field(
        default=ShiftScalePair(float("nan"), float("nan"))
    )
    #: Number of selected reads since the last mux change on this reads channel
    num_reads_since_mux_change: int = field(default=0)
    #: Time in seconds since the last mux change on this reads channel
    time_since_mux_change: float = field(default=0.0)


@dataclass()
class Read(BaseRead):
    """
    POD5 Read Data with an uncompressed signal

    Parameters
    ----------

    read_id : UUID
        The read_id of this read as UUID.
    pore : Pore
        Pore data.
    calibration : Calibration
        Calibration data.
    read_number : int
        The read number on channel. This is increasing but typically
        not necessarily consecutive.
    start_sample : int
        The number samples recorded on this channel before the read started.
    median_before : float
        The level of current in the well before this read.
    end_reason : EndReason
        EndReason data.
    run_info : RunInfo
        RunInfo data.
    signal : numpy.array[int16]
        Uncompressed signal data.
    """

    #: Uncompressed signal data.
    signal: npt.NDArray[np.int16] = field(default=np.array([], dtype=np.int16))

    @property
    def sample_count(self) -> int:
        """Return the total number of samples in the uncompressed signal."""
        return len(self.signal)


@dataclass()
class CompressedRead(BaseRead):
    """
    POD5 Read Data with a compressed signal.

    Parameters
    ----------

    read_id : UUID
        The read_id of this read as UUID.
    pore : Pore
        Pore data.
    calibration : Calibration
        Calibration data.
    read_number : int
        The read number on channel. This is increasing but typically
        not necessarily consecutive.
    start_sample : int
        The number samples recorded on this channel before the read started.
    median_before : float
        The level of current in the well before this read.
    end_reason : EndReason
        EndReason data.
    run_info : RunInfo
        RunInfo data.
    signal_chunks : List[numpy.array[uint8]]
        Compressed signal data in chunks.
    signal_chunk_lengths : List[int]
        Chunk lengths (number of samples) of signal data **before** compression.
    """

    #: Compressed signal data in chunks.
    signal_chunks: List[npt.NDArray[np.uint8]] = field(default_factory=lambda: [])

    #: Chunk lengths (number of samples) of signal data **before** compression.
    signal_chunk_lengths: List[int] = field(default_factory=lambda: [])

    @property
    def sample_count(self) -> int:
        """Return the total number of samples in the uncompressed signal."""
        return sum(self.signal_chunk_lengths)

    @property
    def decompressed_signal(self) -> npt.NDArray[np.int16]:
        """
        Decompress and return the chunked signal data as a contiguous numpy array.

        Returns
        -------
        decompressed_signal : numpy.array[int16]
            Decompressed signal data
        """
        return vbz_decompress_signal_chunked(
            self.signal_chunks, self.signal_chunk_lengths
        )
