"""
Testing signal_tools
"""
import random

import numpy as np
import numpy.typing as npt
import pytest

from pod5.signal_tools import (
    vbz_compress_signal,
    vbz_compress_signal_chunked,
    vbz_decompress_signal,
    vbz_decompress_signal_chunked,
)

TEST_SEEDS = range(10)


class TestPod5SignalTools:
    """Test the POD5 signal_tools module"""

    @pytest.mark.parametrize("random_signal", TEST_SEEDS, indirect=True)
    def test_round_trip(self, random_signal: npt.NDArray[np.int16]) -> None:
        """Test compression and decompression round-trip"""

        sample_count = random_signal.shape[0]
        round_trip_signal = vbz_decompress_signal(
            vbz_compress_signal(random_signal), sample_count
        )
        assert np.array_equal(round_trip_signal, random_signal)

    def test_round_trip_empty(self) -> None:
        """Test compression and decompression round-trip of empty signal data"""
        empty_signal = np.array([], dtype=np.int16)
        sample_count = empty_signal.shape[0]
        round_trip_signal = vbz_decompress_signal(
            vbz_compress_signal(empty_signal), sample_count
        )
        assert np.array_equal(round_trip_signal, empty_signal)

    @pytest.mark.parametrize("random_signal", TEST_SEEDS, indirect=True)
    def test_round_trip_chunked(self, random_signal: npt.NDArray[np.int16]) -> None:
        """Test compression and decompression round-trip for chunked data"""

        sample_count = random_signal.shape[0]
        chunk_size = random.randint(1, 1000)

        compressed_signal_chunked, signal_chunk_lengths = vbz_compress_signal_chunked(
            random_signal, chunk_size
        )

        assert len(compressed_signal_chunked) == len(signal_chunk_lengths)
        assert sample_count == sum(signal_chunk_lengths)

        uncompressed_signal = vbz_decompress_signal_chunked(
            compressed_signal_chunked, signal_chunk_lengths
        )

        assert np.array_equal(uncompressed_signal, random_signal)

    def test_round_trip_chunked_empty(self) -> None:
        """Test compression and decompression round-trip for empty chunked data"""
        empty_signal = np.array([], dtype=np.int16)
        sample_count = empty_signal.shape[0]
        chunk_size = random.randint(1, 1000)

        compressed_signal_chunked, signal_chunk_lengths = vbz_compress_signal_chunked(
            empty_signal, chunk_size
        )

        assert len(compressed_signal_chunked) == len(signal_chunk_lengths)
        assert sample_count == sum(signal_chunk_lengths)

        uncompressed_signal = vbz_decompress_signal_chunked(
            compressed_signal_chunked, signal_chunk_lengths
        )

        assert np.array_equal(uncompressed_signal, empty_signal)
