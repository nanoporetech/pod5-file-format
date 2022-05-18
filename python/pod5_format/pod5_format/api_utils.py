from enum import Enum

import numpy

from pod5_format.pod5_format_pybind import load_read_id_iterable


def pack_read_ids(read_ids):
    read_id_data = numpy.empty(shape=(len(read_ids), 16), dtype=numpy.uint8)
    load_read_id_iterable(read_ids, read_id_data)
    return read_id_data


class EndReason(Enum):
    UNKNOWN = 0
    MUX_CHANGE = 1
    UNBLOCK_MUX_CHANGE = 2
    DATA_SERVICE_UNBLOCK_MUX_CHANGE = 3
    SIGNAL_POSITIVE = 4
    SIGNAL_NEGATIVE = 5
