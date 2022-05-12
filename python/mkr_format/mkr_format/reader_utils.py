from collections import namedtuple
from enum import Enum

SignalRowInfo = namedtuple(
    "SignalRowInfo", ["batch_index", "batch_row_index", "sample_count", "byte_count"]
)
PoreData = namedtuple("PoreData", ["channel", "well", "pore_type"])
CalibrationData = namedtuple("CalibrationData", ["offset", "scale"])
EndReasonData = namedtuple("EndReasonData", ["name", "forced"])
RunInfoData = namedtuple(
    "RunInfoData",
    [
        "acquisition_id",
        "acquisition_start_time",
        "adc_max",
        "adc_min",
        "context_tags",
        "experiment_name",
        "flow_cell_id",
        "flow_cell_product_code",
        "protocol_name",
        "protocol_run_id",
        "protocol_start_time",
        "sample_id",
        "sample_rate",
        "sequencing_kit",
        "sequencer_position",
        "sequencer_position_type",
        "software",
        "system_name",
        "system_type",
        "tracking_id",
    ],
)


class SearchOrder(Enum):
    # Sort the output so the reader can skip around in the file as little as possible.
    READ_EFFICIENT = 0
    # Sort the output traversal as the input data is.
    ORIGINAL_ORDER = 1
