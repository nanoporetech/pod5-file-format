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
    load_read_id_iterable,
    pack_read_ids,
    format_read_id_to_str,
    format_read_ids,
)
from .pod5_types import (
    EndReasonEnum,
    CompressedRead,
    Calibration,
    EndReason,
    Pore,
    Read,
    RunInfo,
)
from .reader import (
    ReadRecord,
    Reader,
    ReadRecordBatch,
)
from .writer import Writer
from .signal_tools import (
    vbz_compress_signal,
    vbz_decompress_signal_chunked,
    vbz_decompress_signal,
    vbz_decompress_signal_into,
)
