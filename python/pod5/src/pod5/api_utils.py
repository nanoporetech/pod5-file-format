"""
Utility functions for the pod5 API
"""


import warnings
from typing import Any, Collection, List, Union

import numpy as np
import numpy.typing as npt
import pyarrow as pa
from lib_pod5 import format_read_id_to_str, load_read_id_iterable


class Pod5ApiException(Exception):
    """Generic Pod5 API Exception"""


def pack_read_ids(
    read_ids: Collection[str], invalid_ok: bool = False
) -> npt.NDArray[np.uint8]:
    """
    Convert a `Collection` of `read_id` strings to a `numpy.ndarray`
    in preparation for writing to pod5 files.

    Parameters
    ----------
    read_ids : Collection[str]
        Collection of well-formatted read_id strings

    Returns
    -------
    packed_read_ids : numpy.ndarray[uint8]
        Repacked read_ids ready for writing to pod5 files.
    """
    read_id_data = np.empty(shape=(len(read_ids), 16), dtype=np.uint8)
    count = load_read_id_iterable(read_ids, read_id_data)
    if invalid_ok is False and count != len(read_ids):
        raise RuntimeError("Invalid read id passed")

    return read_id_data


def format_read_ids(
    read_ids: Union[npt.NDArray[np.uint8], pa.lib.FixedSizeBinaryArray]
) -> List[str]:
    """
    Convert a packed array of read_ids and convert them to a list of strings.

    Parameters
    ----------
    read_ids : numpy.ndarray[uint8], pa.lib.FixedSizeBinaryArray
        Packed read_ids from a numpy.ndarray or read directly from pod5 file

    Returns
    -------
    read_ids : list[str]
        A list of converted read_ids as strings
    """
    if isinstance(read_ids, pa.lib.FixedSizeBinaryArray):
        read_ids = read_ids.buffers()[1]
    return format_read_id_to_str(read_ids)


def deprecation_warning(deprecated: str, alternate: str) -> None:
    """
    Issue a `FutureWarning` warning that `deprecated` has been deprecated in favour of
    `alternate`.

    Parameters
    ----------
    deprecated : str
        The module path to the deprecated item
    alternate : str
        The module path to the alternate item
    """
    warnings.warn(
        f"{deprecated} is deprecated. Please use {alternate}",
        DeprecationWarning,
        stacklevel=2,
    )


def safe_close(obj: Any, attr: str) -> None:
    """
    Try to close() an object's attribute ignoring any exceptions raised.
    This is used to safely handle closing potentially unassigned attributes
    while calling close() in __del__()
    """
    if not hasattr(obj, attr):
        return

    try:
        getattr(obj, attr).close()
    except Exception:
        pass
