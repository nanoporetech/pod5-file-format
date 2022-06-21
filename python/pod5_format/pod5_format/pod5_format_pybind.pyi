"""
c++ bindings for pod5_format
"""

# created with mypy.stubgen for code completion
# > pip install mypy
# > stubgen -m pod5_format.pod5_format_pybind

from typing import Any, Iterable, List, Tuple

import numpy

class EmbeddedFileData:
    def __init__(self, *args, **kwargs) -> None: ...
    @property
    def length(self) -> int: ...
    @property
    def offset(self) -> int: ...

class FileWriter:
    def __init__(self, *args, **kwargs) -> None: ...
    def add_calibration(self, arg0: float, arg1: float) -> int: ...
    def add_end_reason(self, arg0: int, arg1: bool) -> int: ...
    def add_pore(self, arg0: int, arg1: int, arg2: str) -> int: ...
    def add_reads(
        self,
        arg0: int,
        arg1: numpy.ndarray[numpy.uint8],
        arg2: numpy.ndarray[numpy.int16],
        arg3: numpy.ndarray[numpy.int16],
        arg4: numpy.ndarray[numpy.uint32],
        arg5: numpy.ndarray[numpy.uint64],
        arg6: numpy.ndarray[numpy.float32],
        arg7: numpy.ndarray[numpy.int16],
        arg8: numpy.ndarray[numpy.int16],
        arg9: list,
    ) -> None: ...
    def add_reads_pre_compressed(
        self,
        arg0: int,
        arg1: numpy.ndarray[numpy.uint8],
        arg2: numpy.ndarray[numpy.int16],
        arg3: numpy.ndarray[numpy.int16],
        arg4: numpy.ndarray[numpy.uint32],
        arg5: numpy.ndarray[numpy.uint64],
        arg6: numpy.ndarray[numpy.float32],
        arg7: numpy.ndarray[numpy.int16],
        arg8: numpy.ndarray[numpy.int16],
        arg9: list,
        arg10: numpy.ndarray[numpy.uint32],
        arg11: numpy.ndarray[numpy.uint32],
    ) -> None: ...
    def add_run_info(
        self,
        arg0: str,
        arg1: int,
        arg2: int,
        arg3: int,
        arg4: List[Tuple[str, str]],
        arg5: str,
        arg6: str,
        arg7: str,
        arg8: str,
        arg9: str,
        arg10: int,
        arg11: str,
        arg12: int,
        arg13: str,
        arg14: str,
        arg15: str,
        arg16: str,
        arg17: str,
        arg18: str,
        arg19: List[Tuple[str, str]],
    ) -> int: ...
    def close(self) -> None: ...

class FileWriterOptions:
    max_signal_chunk_size: int
    read_table_batch_size: int
    signal_compression_type: Any
    signal_table_batch_size: int
    def __init__(self, *args, **kwargs) -> None: ...

class Pod5AsyncSignalLoader:
    def __init__(self, *args, **kwargs) -> None: ...
    def release_next_batch(self) -> Pod5SignalCacheBatch: ...

class Pod5FileReader:
    def __init__(self, *args, **kwargs) -> None: ...
    def batch_get_signal(self, arg0: bool, arg1: bool) -> Pod5AsyncSignalLoader: ...
    def batch_get_signal_batches(
        self, arg0: bool, arg1: bool, arg2: numpy.ndarray[numpy.uint32]
    ) -> Pod5AsyncSignalLoader: ...
    def batch_get_signal_selection(
        self,
        arg0: bool,
        arg1: bool,
        arg2: numpy.ndarray[numpy.uint32],
        arg3: numpy.ndarray[numpy.uint32],
    ) -> Pod5AsyncSignalLoader: ...
    def close(self) -> None: ...
    def get_combined_file_read_table_location(self) -> EmbeddedFileData: ...
    def get_combined_file_signal_table_location(self) -> EmbeddedFileData: ...
    def plan_traversal(
        self,
        arg0: numpy.ndarray[numpy.uint8],
        arg1: numpy.ndarray[numpy.uint32],
        arg2: numpy.ndarray[numpy.uint32],
    ) -> int: ...

class Pod5RepackerOutput:
    def __init__(self, *args, **kwargs) -> None: ...

class Pod5SignalCacheBatch:
    def __init__(self, *args, **kwargs) -> None: ...
    @property
    def batch_index(self) -> int: ...
    @property
    def sample_count(self) -> numpy.ndarray[numpy.uint64]: ...
    @property
    def samples(self) -> list: ...

class Repacker:
    def __init__(self) -> None: ...
    def add_all_reads_to_output(
        self, arg0: Pod5RepackerOutput, arg1: Pod5FileReader
    ) -> None: ...
    def add_output(self, arg0: FileWriter) -> Pod5RepackerOutput: ...
    def add_selected_reads_to_output(
        self,
        arg0: Pod5RepackerOutput,
        arg1: Pod5FileReader,
        arg2: numpy.ndarray[numpy.uint32],
        arg3: numpy.ndarray[numpy.uint32],
    ) -> None: ...
    def finish(self) -> None: ...
    @property
    def batches_completed(self) -> int: ...
    @property
    def batches_requested(self) -> int: ...
    @property
    def is_complete(self) -> bool: ...
    @property
    def pending_batch_writes(self) -> int: ...
    @property
    def reads_completed(self) -> int: ...
    @property
    def reads_sample_bytes_completed(self) -> int: ...

def compress_signal(
    arg0: numpy.ndarray[numpy.int16], arg1: numpy.ndarray[numpy.uint8]
) -> int: ...
def create_combined_file(
    filename: str, writer_name: str, options: FileWriterOptions = ...
) -> FileWriter: ...
def create_split_file(
    signal_filename: str,
    reads_filename: str,
    writer_name: str,
    options: FileWriterOptions = ...,
) -> FileWriter: ...
def decompress_signal(
    arg0: numpy.ndarray[numpy.uint8], arg1: numpy.ndarray[numpy.int16]
) -> None: ...
def format_read_id_to_str(arg0: numpy.ndarray[numpy.uint8]) -> list: ...
def get_error_string() -> str: ...
def load_read_id_iterable(arg0: Iterable, arg1: numpy.ndarray[numpy.uint8]) -> None: ...
def open_combined_file(arg0: str) -> Pod5FileReader: ...
def open_split_file(arg0: str, arg1: str) -> Pod5FileReader: ...
def vbz_compressed_signal_max_size(arg0: int) -> int: ...
