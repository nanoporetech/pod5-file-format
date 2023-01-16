"""
Tools for handling pod5 signals
"""

from typing import List, Tuple, Union

import lib_pod5 as p5b
import numpy as np
import numpy.typing as npt

DEFAULT_SIGNAL_CHUNK_SIZE = 102400


def vbz_decompress_signal(
    compressed_signal: Union[npt.NDArray[np.uint8], memoryview], sample_count: int
) -> npt.NDArray[np.int16]:
    """
    Decompress a contiguous (not-chunked) numpy array of compressed signal data

    Parameters
    ----------
    compressed_signal : numpy.ndarray[uint8]
        The array of compressed signal data to decompress.
    sample_count : int
        The number of samples in the original signal

    Returns
    -------
    A decompressed signal array numpy.ndarray[int16]
    """
    if len(compressed_signal) == 0:
        return np.array([], dtype=np.int16)

    signal = np.empty(sample_count, dtype="i2")
    p5b.decompress_signal(compressed_signal, signal)
    return signal


def vbz_decompress_signal_chunked(
    compressed_signal_chunks: List[npt.NDArray[np.uint8]], sample_counts: List[int]
) -> npt.NDArray[np.int16]:
    """
    Decompress a chunks of numpy array of compressed signal data

    Parameters
    ----------
    compressed_signal_chunks : List[numpy.ndarray[uint8]]
        A list of compressed signal data chunks to decompress.
    sample_counts : List[int]
        The number of samples in the original signal chunks

    Returns
    -------
    A decompressed signal array numpy.ndarray[int16]

    Raises
    ------
    ValueError
        Inconsistent parameter lengths
    """
    if len(compressed_signal_chunks) != len(sample_counts):
        raise ValueError(
            f"Inconsistent number of chunks to decompress - "
            f"signals: {len(compressed_signal_chunks)}, counts: {len(sample_counts)}"
        )

    if len(compressed_signal_chunks) == 0:
        return np.array([], dtype=np.int16)

    decompressed_signal: npt.NDArray[
        np.int16
    ] = np.concatenate(  # type:ignore [no-untyped-call]
        [
            vbz_decompress_signal(signal_chunk, sample_count)
            for signal_chunk, sample_count in zip(
                compressed_signal_chunks, sample_counts
            )
        ]
    )
    return decompressed_signal


def vbz_decompress_signal_into(
    compressed_signal: Union[npt.NDArray[np.uint8], memoryview],
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
    if len(compressed_signal) == 0:
        return np.array([], dtype=np.int16)

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
    compressed_signal : numpy.array[uint8]
        The compressed signal data as a numpy.ndarray[uint8] (byte array)
    """
    if signal.size == 0:
        return np.array([], dtype=np.uint8)

    max_signal_size = p5b.vbz_compressed_signal_max_size(len(signal))
    compressed_signal = np.zeros(max_signal_size, dtype="u1")

    size = p5b.compress_signal(signal, compressed_signal)

    return np.resize(compressed_signal, size)


def vbz_compress_signal_chunked(
    signal: npt.NDArray[np.int16], signal_chunk_size: int = DEFAULT_SIGNAL_CHUNK_SIZE
) -> Tuple[List[npt.NDArray[np.uint8]], List[int]]:
    """
    Compress a numpy array of signal data into chunks

    Parameters
    ----------
    signal : numpy.ndarray[int16]
        The array of signal data to compress.
    signal_chunk_size : int
        The number of signal samples in a chunk

    Returns
    -------
    compressed_signal_chunks : List[numpy.array[uint8]]
        A List of chunks of compressed signal data as numpy.ndarray[uint8] (byte arrays)
    signal_chunk_lengths : List[int]
        The number of uncompressed signal samples in each chunk
    """
    signal_chunks: List[npt.NDArray[np.uint8]] = []
    signal_chunk_lengths: List[int] = []

    # Take slice views of the signal ndarray (non-copying)
    for slice_index in range(0, len(signal), signal_chunk_size):
        signal_slice = signal[slice_index : slice_index + signal_chunk_size]
        signal_chunks.append(vbz_compress_signal(signal_slice))
        signal_chunk_lengths.append(len(signal_slice))

    return signal_chunks, signal_chunk_lengths
