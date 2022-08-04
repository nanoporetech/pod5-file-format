"""
Tools for handling pod5 signals
"""

import numpy as np
import numpy.typing as npt
import pod5_format.pod5_format_pybind as p5b


def vbz_decompress_signal(
    compressed_signal: npt.NDArray[np.uint8], sample_count: int
) -> npt.NDArray[np.int16]:
    """
    Decompress a numpy array of compressed signal data

    Parameters
    ----------
    compressed_signal : numpy.ndarray[uint8]
        The array of compressed signal data to decompress.
    sample_count : int
        The sample count of the decompressed data.

    Returns
    -------
    A decompressed signal array numpy.ndarray[int16]
    """
    signal = np.empty(sample_count, dtype="i2")

    p5b.decompress_signal(compressed_signal, signal)
    return signal


def vbz_decompress_signal_into(
    compressed_signal: npt.NDArray[np.uint8],
    output_array: npt.NDArray[np.int16],
) -> npt.NDArray[np.int16]:
    """
    Decompress a numpy array of compressed signal data into the destination
    "output_array"

    Parameters
    ----------
    compressed_signal : numpy.ndarray[uint8]
        The array of compressed signal data to decompress.
    output_array : numpy.ndarray[int16]
        The destination location for signal

    Returns
    -------
    A decompressed signal array numpy.ndarray[int16]
    """
    p5b.decompress_signal(compressed_signal, output_array)
    return output_array


def vbz_compress_signal(signal: npt.NDArray[np.int16]) -> npt.NDArray[np.uint8]:
    """
    Compress a numpy array of signal data

    Parameters
    ----------
    signal : numpy.ndarray[int16]
        The array of signal data to compress.

    Returns
    -------
    A compressed numpy byte array numpy.ndarray[uint8]
    """
    max_signal_size = p5b.vbz_compressed_signal_max_size(len(signal))
    signal_bytes = np.zeros(max_signal_size, dtype="u1")

    size = p5b.compress_signal(signal, signal_bytes)

    signal_bytes = np.resize(signal_bytes, size)
    return signal_bytes
