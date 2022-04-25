from pathlib import Path
import ctypes


class MkrFileWriter(ctypes.Structure):
    _fields_ = []


class MkrFileReader(ctypes.Structure):
    _fields_ = []


class MkrReadRecordBatch(ctypes.Structure):
    _fields_ = []


class MkrWriterOptions(ctypes.Structure):
    _fields_ = [
        ("max_signal_chunk_size", ctypes.c_uint),
        ("signal_compression_type", ctypes.c_byte),
    ]


class SignalRowInfo(ctypes.Structure):
    _fields_ = [
        ("batch_index", ctypes.c_size_t),
        ("batch_row_index", ctypes.c_size_t),
        ("stored_sample_count", ctypes.c_uint),
        ("stored_byte_count", ctypes.c_size_t),
    ]


class PoreDictData(ctypes.Structure):
    _fields_ = [
        ("channel", ctypes.c_ushort),
        ("well", ctypes.c_ubyte),
        ("pore_type", ctypes.c_char_p),
    ]


class CalibrationDictData(ctypes.Structure):
    _fields_ = [
        ("offset", ctypes.c_float),
        ("scale", ctypes.c_float),
    ]


class EndReasonDictData(ctypes.Structure):
    _fields_ = [
        ("name", ctypes.c_char_p),
        ("forced", ctypes.c_bool),
    ]


class KeyValueData(ctypes.Structure):
    _fields_ = [
        ("size", ctypes.c_size_t),
        ("keys", ctypes.POINTER(ctypes.c_char_p)),
        ("values", ctypes.POINTER(ctypes.c_char_p)),
    ]


class RunInfoDictData(ctypes.Structure):
    _fields_ = [
        ("acquisition_id", ctypes.c_char_p),
        ("acquisition_start_time_ms", ctypes.c_longlong),
        ("adc_max", ctypes.c_short),
        ("adc_min", ctypes.c_short),
        ("context_tags", KeyValueData),
        ("experiment_name", ctypes.c_char_p),
        ("flow_cell_id", ctypes.c_char_p),
        ("flow_cell_product_code", ctypes.c_char_p),
        ("protocol_name", ctypes.c_char_p),
        ("protocol_run_id", ctypes.c_char_p),
        ("protocol_start_time_ms", ctypes.c_longlong),
        ("sample_id", ctypes.c_char_p),
        ("protocol_start_time_ms", ctypes.c_ushort),
        ("sequencing_kit", ctypes.c_char_p),
        ("sequencer_position", ctypes.c_char_p),
        ("sequencer_position_type", ctypes.c_char_p),
        ("software", ctypes.c_char_p),
        ("system_name", ctypes.c_char_p),
        ("system_type", ctypes.c_char_p),
        ("tracking_id", KeyValueData),
    ]


# ----------------------------------------------------------------------------------------------------------------------
REPO_ROOT = Path(__file__).parent.parent.parent
mkr_format = ctypes.cdll.LoadLibrary(REPO_ROOT / "build" / "c++" / "libmkr_format.so")

# Init the MKR library
mkr_format.mkr_init()


class Unloader:
    def __del__(self):
        print("Unloading")
        mkr_format.mkr_terminate()


ERROR_TYPE = ctypes.c_int
WRITER_OPTIONS_PTR = ctypes.POINTER(MkrWriterOptions)
FILE_READER_PTR = ctypes.POINTER(MkrFileReader)
FILE_WRITER_PTR = ctypes.POINTER(MkrFileWriter)
READ_RECORD_BATCH_POINTER = ctypes.POINTER(MkrReadRecordBatch)

# ----------------------------------------------------------------------------------------------------------------------
mkr_get_error_string = mkr_format.mkr_get_error_string
mkr_get_error_string.restype = ctypes.c_char_p

# ----------------------------------------------------------------------------------------------------------------------
mkr_create_combined_file = mkr_format.mkr_create_combined_file
mkr_create_combined_file.argtypes = [
    ctypes.c_char_p,  # filename
    ctypes.c_char_p,  # software name
    WRITER_OPTIONS_PTR,  # options
]
mkr_create_combined_file.restype = FILE_WRITER_PTR

mkr_close_and_free_writer = mkr_format.mkr_close_and_free_writer
mkr_close_and_free_writer.argtypes = [
    FILE_WRITER_PTR,  # writer
]
mkr_close_and_free_writer.restype = ERROR_TYPE

# ----------------------------------------------------------------------------------------------------------------------

mkr_open_combined_file = mkr_format.mkr_open_combined_file
mkr_open_combined_file.argtypes = [
    ctypes.c_char_p,  # filename
]
mkr_open_combined_file.restype = FILE_READER_PTR


# ----------------------------------------------------------------------------------------------------------------------
mkr_add_pore = mkr_format.mkr_add_pore
mkr_add_pore.restype = ERROR_TYPE
mkr_add_pore.argtypes = [
    ctypes.POINTER(ctypes.c_short),
    FILE_WRITER_PTR,
    ctypes.c_ushort,
    ctypes.c_ubyte,
    ctypes.c_char_p,
]

mkr_add_calibration = mkr_format.mkr_add_calibration
mkr_add_calibration.restype = ERROR_TYPE
mkr_add_calibration.argtypes = [
    ctypes.POINTER(ctypes.c_short),
    FILE_WRITER_PTR,
    ctypes.c_float,
    ctypes.c_float,
]

mkr_add_end_reason = mkr_format.mkr_add_end_reason
mkr_add_end_reason.restype = ERROR_TYPE
mkr_add_end_reason.argtypes = [
    ctypes.POINTER(ctypes.c_short),
    FILE_WRITER_PTR,
    ctypes.c_int,
    ctypes.c_bool,
]

mkr_add_run_info = mkr_format.mkr_add_run_info
mkr_add_run_info.restype = ERROR_TYPE
mkr_add_run_info.argtypes = [
    ctypes.POINTER(ctypes.c_short),
    FILE_WRITER_PTR,
    ctypes.c_char_p,
    ctypes.c_longlong,
    ctypes.c_short,
    ctypes.c_short,
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_char_p),
    ctypes.POINTER(ctypes.c_char_p),
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_longlong,
    ctypes.c_char_p,
    ctypes.c_ushort,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_char_p,
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_char_p),
    ctypes.POINTER(ctypes.c_char_p),
]


mkr_add_read = mkr_format.mkr_add_read
mkr_add_read.restype = ERROR_TYPE
mkr_add_read.argtypes = [
    FILE_WRITER_PTR,
    ctypes.POINTER(ctypes.c_ubyte),
    ctypes.c_short,
    ctypes.c_short,
    ctypes.c_uint,
    ctypes.c_ulonglong,
    ctypes.c_float,
    ctypes.c_short,
    ctypes.c_short,
    ctypes.POINTER(ctypes.c_short),
    ctypes.c_size_t,
]

mkr_flush_signal_table = mkr_format.mkr_flush_signal_table
mkr_flush_signal_table.restype = ERROR_TYPE
mkr_flush_signal_table.argtypes = [FILE_WRITER_PTR]

mkr_flush_reads_table = mkr_format.mkr_flush_reads_table
mkr_flush_reads_table.restype = ERROR_TYPE
mkr_flush_reads_table.argtypes = [FILE_WRITER_PTR]

# ----------------------------------------------------------------------------------------------------------------------
mkr_get_read_batch_count = mkr_format.mkr_get_read_batch_count
mkr_get_read_batch_count.restype = ERROR_TYPE
mkr_get_read_batch_count.argtypes = [
    ctypes.POINTER(ctypes.c_size_t),
    FILE_READER_PTR,
]
mkr_free_read_batch = mkr_format.mkr_free_read_batch
mkr_free_read_batch.restype = ERROR_TYPE
mkr_free_read_batch.argtypes = [
    READ_RECORD_BATCH_POINTER,
]

mkr_get_read_batch = mkr_format.mkr_get_read_batch
mkr_get_read_batch.restype = ERROR_TYPE
mkr_get_read_batch.argtypes = [
    ctypes.POINTER(READ_RECORD_BATCH_POINTER),
    FILE_READER_PTR,
    ctypes.c_size_t,
]

mkr_get_read_batch_row_count = mkr_format.mkr_get_read_batch_row_count
mkr_get_read_batch_row_count.restype = ERROR_TYPE
mkr_get_read_batch_row_count.argtypes = [
    ctypes.POINTER(ctypes.c_size_t),
    READ_RECORD_BATCH_POINTER,
]

mkr_get_read_batch_row_info = mkr_format.mkr_get_read_batch_row_info
mkr_get_read_batch_row_info.restype = ERROR_TYPE
mkr_get_read_batch_row_info.argtypes = [
    READ_RECORD_BATCH_POINTER,
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_ubyte),
    ctypes.POINTER(ctypes.c_short),
    ctypes.POINTER(ctypes.c_short),
    ctypes.POINTER(ctypes.c_uint),
    ctypes.POINTER(ctypes.c_ulonglong),
    ctypes.POINTER(ctypes.c_float),
    ctypes.POINTER(ctypes.c_short),
    ctypes.POINTER(ctypes.c_short),
    ctypes.POINTER(ctypes.c_longlong),
]

mkr_get_signal_row_indices = mkr_format.mkr_get_signal_row_indices
mkr_get_signal_row_indices.restype = ERROR_TYPE
mkr_get_signal_row_indices.argtypes = [
    READ_RECORD_BATCH_POINTER,
    ctypes.c_size_t,
    ctypes.c_longlong,
    ctypes.POINTER(ctypes.c_ulonglong),
]

mkr_get_signal_row_info = mkr_format.mkr_get_signal_row_info
mkr_get_signal_row_info.restype = ERROR_TYPE
mkr_get_signal_row_info.argtypes = [
    FILE_READER_PTR,
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_ulonglong),
    ctypes.POINTER(SignalRowInfo),
]

mkr_get_pore = mkr_format.mkr_get_pore
mkr_get_pore.restype = ERROR_TYPE
mkr_get_pore.argtypes = [
    READ_RECORD_BATCH_POINTER,
    ctypes.c_short,
    ctypes.POINTER(ctypes.POINTER(PoreDictData)),
]

mkr_release_pore = mkr_format.mkr_release_pore
mkr_release_pore.restype = ERROR_TYPE
mkr_release_pore.argtypes = [
    ctypes.POINTER(PoreDictData),
]

mkr_get_calibration = mkr_format.mkr_get_calibration
mkr_get_calibration.restype = ERROR_TYPE
mkr_get_calibration.argtypes = [
    READ_RECORD_BATCH_POINTER,
    ctypes.c_short,
    ctypes.POINTER(ctypes.POINTER(CalibrationDictData)),
]

mkr_release_calibration = mkr_format.mkr_release_calibration
mkr_release_calibration.restype = ERROR_TYPE
mkr_release_calibration.argtypes = [
    ctypes.POINTER(CalibrationDictData),
]

mkr_get_end_reason = mkr_format.mkr_get_end_reason
mkr_get_end_reason.restype = ERROR_TYPE
mkr_get_end_reason.argtypes = [
    READ_RECORD_BATCH_POINTER,
    ctypes.c_short,
    ctypes.POINTER(ctypes.POINTER(EndReasonDictData)),
]

mkr_release_end_reason = mkr_format.mkr_release_end_reason
mkr_release_end_reason.restype = ERROR_TYPE
mkr_release_end_reason.argtypes = [
    ctypes.POINTER(EndReasonDictData),
]

mkr_get_run_info = mkr_format.mkr_get_run_info
mkr_get_run_info.restype = ERROR_TYPE
mkr_get_run_info.argtypes = [
    READ_RECORD_BATCH_POINTER,
    ctypes.c_short,
    ctypes.POINTER(ctypes.POINTER(RunInfoDictData)),
]

mkr_release_run_info = mkr_format.mkr_release_run_info
mkr_release_run_info.restype = ERROR_TYPE
mkr_release_run_info.argtypes = [
    ctypes.POINTER(RunInfoDictData),
]
