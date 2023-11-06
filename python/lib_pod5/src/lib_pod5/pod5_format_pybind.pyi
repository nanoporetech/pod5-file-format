"""
c++ bindings for pod5_format
"""

# pylint: skip-file

# created with mypy.stubgen for code completion
# > pip install mypy
# > stubgen -m lib_pod5.pod5_format_pybind

from typing import Any, Iterable, List, Optional, Tuple, Union

import numpy as np
import numpy.typing as npt

class EmbeddedFileData:
    def __init__(self, *args, **kwargs) -> None: ...
    @property
    def length(self) -> int: ...
    @property
    def offset(self) -> int: ...
    @property
    def file_path(self) -> str: ...

class FileWriter:
    def __init__(self, *args, **kwargs) -> None: ...
    def add_end_reason(self, end_reason_enum: int) -> int: ...
    def add_pore(self, pore_type: str) -> int: ...
    def add_reads(
        self,
        count: int,
        read_ids: npt.NDArray[np.uint8],
        read_numbers: npt.NDArray[np.uint32],
        start_samples: npt.NDArray[np.uint64],
        channels: npt.NDArray[np.uint16],
        wells: npt.NDArray[np.uint8],
        pore_types: npt.NDArray[np.int16],
        calibration_offsets: npt.NDArray[np.float32],
        calibration_scales: npt.NDArray[np.float32],
        median_befores: npt.NDArray[np.float32],
        end_reasons: npt.NDArray[np.int16],
        end_reason_forceds: npt.NDArray[np.bool_],
        run_infos: npt.NDArray[np.int16],
        num_minknow_events: npt.NDArray[np.uint64],
        tracked_scaling_scales: npt.NDArray[np.float32],
        tracked_scaling_shifts: npt.NDArray[np.float32],
        predicted_scaling_scales: npt.NDArray[np.float32],
        predicted_scaling_shifts: npt.NDArray[np.float32],
        num_reads_since_mux_changes: npt.NDArray[np.uint32],
        time_since_mux_changes: npt.NDArray[np.float32],
        signals: List[npt.NDArray[np.int16]],
    ) -> None: ...
    def add_reads_pre_compressed(
        self,
        count: int,
        read_ids: npt.NDArray[np.uint8],
        read_numbers: npt.NDArray[np.uint32],
        start_samples: npt.NDArray[np.uint64],
        channels: npt.NDArray[np.uint16],
        wells: npt.NDArray[np.uint8],
        pore_types: npt.NDArray[np.int16],
        calibration_offsets: npt.NDArray[np.float32],
        calibration_scales: npt.NDArray[np.float32],
        median_befores: npt.NDArray[np.float32],
        end_reasons: npt.NDArray[np.int16],
        end_reason_forceds: npt.NDArray[np.bool_],
        run_infos: npt.NDArray[np.int16],
        num_minknow_events: npt.NDArray[np.uint64],
        tracked_scaling_scales: npt.NDArray[np.float32],
        tracked_scaling_shifts: npt.NDArray[np.float32],
        predicted_scaling_scales: npt.NDArray[np.float32],
        predicted_scaling_shifts: npt.NDArray[np.float32],
        num_reads_since_mux_changes: npt.NDArray[np.uint32],
        time_since_mux_changes: npt.NDArray[np.float32],
        signal_chunks: List[npt.NDArray[np.uint8]],
        signal_chunk_lengths: npt.NDArray[np.uint32],
        signal_chunk_counts: npt.NDArray[np.uint32],
    ) -> None: ...
    def add_run_info(
        self,
        acquisition_id: str,
        acquisition_start_time: int,
        adc_max: int,
        adc_min: int,
        context_tags: List[Tuple[str, str]],
        experiment_name: str,
        flow_cell_id: str,
        flow_cell_product_code: str,
        protocol_name: str,
        protocol_run_id: str,
        protocol_start_time: int,
        sample_id: str,
        sample_rate: int,
        sequencing_kit: str,
        sequencer_position: str,
        sequencer_position_type: str,
        software: str,
        system_name: str,
        system_type: str,
        tracking_id: List[Tuple[str, str]],
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
    def batch_get_signal(
        self, get_samples: bool, get_sample_count: bool
    ) -> Pod5AsyncSignalLoader: ...
    def batch_get_signal_batches(
        self,
        get_samples: bool,
        get_samples_count: bool,
        batches: npt.NDArray[np.uint32],
    ) -> Pod5AsyncSignalLoader: ...
    def batch_get_signal_selection(
        self,
        get_samples: bool,
        get_sample_count: bool,
        batch_counts: npt.NDArray[np.uint32],
        batch_rows: npt.NDArray[np.uint32],
    ) -> Pod5AsyncSignalLoader: ...
    def close(self) -> None: ...
    def get_file_read_table_location(self) -> EmbeddedFileData: ...
    def get_file_run_info_table_location(self) -> EmbeddedFileData: ...
    def get_file_signal_table_location(self) -> EmbeddedFileData: ...
    def get_file_version_pre_migration(self) -> str: ...
    def plan_traversal(
        self,
        read_id_data: npt.NDArray[np.uint8],
        batch_counts: npt.NDArray[np.uint32],
        batch_rows: npt.NDArray[np.uint32],
    ) -> int: ...

class Pod5RepackerOutput:
    def __init__(self, *args, **kwargs) -> None: ...

class Pod5SignalCacheBatch:
    def __init__(self, *args, **kwargs) -> None: ...
    @property
    def batch_index(self) -> int: ...
    @property
    def sample_count(self) -> npt.NDArray[np.uint64]: ...
    @property
    def samples(self) -> List[npt.NDArray[np.int16]]: ...

class Repacker:
    def __init__(self) -> None: ...
    def add_all_reads_to_output(
        self, output: Pod5RepackerOutput, input: Pod5FileReader
    ) -> None: ...
    def add_output(
        self, output: FileWriter, check_duplicate_read_ids: bool
    ) -> Pod5RepackerOutput: ...
    def add_selected_reads_to_output(
        self,
        output: Pod5RepackerOutput,
        input: Pod5FileReader,
        batch_counts: npt.NDArray[np.uint32],
        all_batch_rows: npt.NDArray[np.uint32],
    ) -> None: ...
    def finish(self) -> None: ...
    @property
    def is_complete(self) -> bool: ...
    @property
    def open_file_readers(self) -> int: ...
    @property
    def reads_completed(self) -> int: ...

def compress_signal(
    signal: npt.NDArray[np.int16], compressed_signal_out: npt.NDArray[np.uint8]
) -> int: ...
def create_file(
    src_filename: str, writer_name: str, options: Optional[FileWriterOptions]
) -> FileWriter: ...
def recover_file(src_filename: str, dst_filename: str) -> FileWriter: ...
def decompress_signal(
    compressed_signal: Union[npt.NDArray[np.uint8], memoryview],
    signal_out: npt.NDArray[np.int16],
) -> None: ...
def format_read_id_to_str(
    read_id_data_out: npt.NDArray[np.uint8],
) -> List[str]: ...
def get_error_string() -> str: ...
def load_read_id_iterable(
    read_ids_str: Iterable, read_id_data_out: npt.NDArray[np.uint8]
) -> int: ...
def open_file(filename: str) -> Pod5FileReader: ...
def update_file(reader: Pod5FileReader, output: str): ...
def vbz_compressed_signal_max_size(sample_count: int) -> int: ...
