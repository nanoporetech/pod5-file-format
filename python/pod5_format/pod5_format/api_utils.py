from enum import Enum

import numpy
import pyarrow as pa

from pod5_format.pod5_format_pybind import load_read_id_iterable, format_read_id_to_str


def pack_read_ids(read_ids):
    read_id_data = numpy.empty(shape=(len(read_ids), 16), dtype=numpy.uint8)
    load_read_id_iterable(read_ids, read_id_data)
    return read_id_data


def format_read_ids(read_ids):
    if isinstance(read_ids, pa.lib.FixedSizeBinaryArray):
        read_ids = read_ids.buffers()[1]
    return format_read_id_to_str(read_ids)


class EndReason(Enum):
    UNKNOWN = 0
    MUX_CHANGE = 1
    UNBLOCK_MUX_CHANGE = 2
    DATA_SERVICE_UNBLOCK_MUX_CHANGE = 3
    SIGNAL_POSITIVE = 4
    SIGNAL_NEGATIVE = 5
