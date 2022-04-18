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
