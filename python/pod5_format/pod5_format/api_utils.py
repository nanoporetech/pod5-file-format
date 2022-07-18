import numpy
import pyarrow as pa

from warnings import warn

from pod5_format.pod5_format_pybind import load_read_id_iterable, format_read_id_to_str


class Pod5ApiException(Exception):
    """Generic Pod5 API Exception"""


def pack_read_ids(read_ids):
    read_id_data = numpy.empty(shape=(len(read_ids), 16), dtype=numpy.uint8)
    load_read_id_iterable(read_ids, read_id_data)
    return read_id_data


def format_read_ids(read_ids):
    if isinstance(read_ids, pa.lib.FixedSizeBinaryArray):
        read_ids = read_ids.buffers()[1]
    return format_read_id_to_str(read_ids)


def deprecation_warning(deprecated: str, instead: str) -> None:
    """Issue a FutureWarning"""
    warn(
        f"{deprecated} is deprecated. Please use {instead}",
        FutureWarning,
        stacklevel=2,
    )
