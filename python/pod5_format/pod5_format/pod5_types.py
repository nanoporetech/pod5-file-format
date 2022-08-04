"""
Container class for a pod5 Read object
"""
import datetime
import enum
import os
from typing import (
    Any,
    Dict,
    Union,
)
from dataclasses import dataclass, field
from uuid import UUID

import numpy as np
import numpy.typing as npt

PathOrStr = Union[os.PathLike, str]


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
    """POD5 End Reason"""

    name: EndReasonEnum
    forced: bool


@dataclass(frozen=True)
class Calibration:
    """POD5 Read Calibration Data"""

    offset: float
    scale: float

    @classmethod
    def from_range(cls, offset: float, adc_range: float, digitisation: float):
        """Create a Calibration instance from offset, adc_range and digitisation"""
        return cls(offset, adc_range / digitisation)


@dataclass(frozen=True)
class Pore:
    """POD5 Read Pore Data"""

    channel: int
    well: int
    pore_type: str


@dataclass(frozen=True)
class RunInfo:
    """POD5 Run Information"""

    acquisition_id: str
    acquisition_start_time: datetime.datetime
    adc_max: int
    adc_min: int
    context_tags: Dict[str, Any] = field(hash=False, compare=True)
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
    tracking_id: Dict[str, Any] = field(hash=False, compare=True)


@dataclass(frozen=True)
class Read:
    """POD5 Read Data"""

    read_id: UUID
    pore: Pore
    calibration: Calibration
    read_number: int
    start_time: int
    median_before: float
    end_reason: EndReason
    run_info: RunInfo
    signal: npt.NDArray[np.int16]
    samples_count: int
