import ctypes

import numpy

from . import c_api
from .api_utils import check_error


def vbz_decompress_signal(compressed_signal, sample_count):
    """
    Decompress a numpy array of compressed signal data

    Parameters
    ----------
    compressed_signal : numpy.array
        The array of compressed signal data to decompress.
    sample_count : int
        The sample count of the decompressed data.

    Returns
    -------
    A decompressed numpy int16 array
    """
    signal = numpy.empty(sample_count, dtype="i2")

    compressed_signal_data = compressed_signal
    if isinstance(compressed_signal, numpy.ndarray):
        print("denumpify")
        compressed_signal_data = compressed_signal.ctypes.data_as(ctypes.c_char_p)

    check_error(
        c_api.mkr_vbz_decompress_signal(
            compressed_signal_data,
            len(compressed_signal),
            sample_count,
            signal.ctypes.data_as(ctypes.POINTER(ctypes.c_short)),
        )
    )
    return signal


def vbz_compress_signal(signal):
    """
    Compress a numpy array of signal data

    Parameters
    ----------
    signal : numpy.array
        The array of signal data to compress.

    Returns
    -------
    A compressed numpy byte array
    """
    max_signal_size = c_api.mkr_vbz_compressed_signal_max_size(len(signal))
    signal_bytes = numpy.empty(max_signal_size, dtype="i1")

    signal_size = ctypes.c_size_t(max_signal_size)
    check_error(
        c_api.mkr_vbz_compress_signal(
            signal.ctypes.data_as(ctypes.POINTER(ctypes.c_int16)),
            signal.shape[0],
            signal_bytes.ctypes.data_as(ctypes.c_char_p),
            ctypes.pointer(signal_size),
        )
    )
    signal_bytes = numpy.resize(signal_bytes, signal_size.value)
    return signal_bytes
