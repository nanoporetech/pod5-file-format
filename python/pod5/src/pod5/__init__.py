"""POD5 Format

Bindings for the POD5 file format
"""


# Pull the version from the pyproject.toml
import sys

if sys.version_info >= (3, 8):
    from importlib import metadata
else:
    import importlib_metadata as metadata

__version__ = metadata.version("pod5")

from .api_utils import (
    format_read_id_to_str,
    format_read_ids,
    load_read_id_iterable,
    pack_read_ids,
)
from .pod5_types import (
    Calibration,
    CompressedRead,
    EndReason,
    EndReasonEnum,
    Pore,
    Read,
    RunInfo,
)
from .reader import Reader, ReadRecord, ReadRecordBatch
from .dataset import DatasetReader
from .signal_tools import (
    vbz_compress_signal,
    vbz_decompress_signal,
    vbz_decompress_signal_chunked,
    vbz_decompress_signal_into,
)
from .writer import SignalType, Writer

__all__ = (
    "__version__",
    "format_read_id_to_str",
    "format_read_ids",
    "load_read_id_iterable",
    "pack_read_ids",
    "DatasetReader",
    "Calibration",
    "CompressedRead",
    "EndReason",
    "EndReasonEnum",
    "Pore",
    "Read",
    "RunInfo",
    "Reader",
    "ReadRecord",
    "ReadRecordBatch",
    "SignalType",
    "vbz_compress_signal",
    "vbz_decompress_signal",
    "vbz_decompress_signal_chunked",
    "vbz_decompress_signal_into",
    "Writer",
)
