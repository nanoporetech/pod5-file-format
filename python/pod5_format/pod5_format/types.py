"""
Container class for a pod5 Read object
"""

import datetime
import enum
import typing
from dataclasses import dataclass, field
from uuid import UUID

import numpy.typing


class EndReasonEnum(enum.Enum):
    """EndReason Enumeration"""

    UNKNOWN = 0
    MUX_CHANGE = 1
    UNBLOCK_MUX_CHANGE = 2
    DATA_SERVICE_UNBLOCK_MUX_CHANGE = 3
    SIGNAL_POSITIVE = 4
    SIGNAL_NEGATIVE = 5


@dataclass(frozen=True)
class EndReason:
    """Class for containing pod5 end reason data"""

    name: EndReasonEnum
    forced: bool


@dataclass(frozen=True)
class Calibration:
    """Class for containing pod5 calibration data"""

    offset: float
    scale: float

    @classmethod
    def from_range(cls, offset: float, adc_range: float, digitisation: float):
        """Create a Pod5Calibration instance from adc_range and digitisation"""
        return cls(offset, adc_range / digitisation)


@dataclass(frozen=True)
class Pore:
    """Class for containing pod5 pore data"""

    channel: int
    well: int
    pore_type: str


@dataclass(frozen=True)
class RunInfo:
    """Class for containing pod5 run information"""

    acquisition_id: str
    acquisition_start_time: datetime.datetime
    adc_max: int
    adc_min: int
    context_tags: typing.Dict[str, typing.Any] = field(hash=False, compare=True)
    experiment_name: str
    flow_cell_id: str
    flow_cell_product_code: str
    protocol_name: str
    protocol_run_id: str
    protocol_start_time: datetime.datetime
    sample_id: str
    sample_rate: int
    sequencing_kit: str
    sequencer_position: str
    sequencer_position_type: str
    software: str
    system_name: str
    system_type: str
    tracking_id: typing.Dict[str, typing.Any] = field(hash=False, compare=True)


@dataclass(frozen=True)
class Read:
    """Class for containing pod5 read data"""

    read_id: UUID
    pore: Pore
    calibration: Calibration
    read_number: int
    start_time: int
    median_before: float
    end_reason: EndReason
    run_info: RunInfo
    signal: numpy.typing.NDArray[numpy.int16]  # typing.List[int]
    samples_count: int
