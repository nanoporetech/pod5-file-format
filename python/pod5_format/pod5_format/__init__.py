"""POD5 Format

Bindings for the POD5 file format
"""

from ._version import __version__

from . import pod5_format_pybind
from .api_utils import (
    load_read_id_iterable,
    pack_read_ids,
    format_read_id_to_str,
    format_read_ids,
)
from .types import (
    EndReasonEnum,
    Calibration,
    EndReason,
    Pore,
    Read,
    RunInfo,
)
from .reader import (
    ReadRecord,
    Reader,
    SplitReader,
    CombinedReader,
    ReadRecordBatch,
    open_combined_file,
    open_split_file,
)
from .reader_utils import make_split_filename
from .writer import Writer, create_combined_file, create_split_file
from .signal_tools import (
    vbz_compress_signal,
    vbz_decompress_signal,
    vbz_decompress_signal_into,
)
