"""
Testing Pod5Reader
"""
from typing import Type
from uuid import UUID

import numpy
import numpy.typing
import pytest

import pod5 as p5
from pod5.pod5_types import Calibration, EndReason, RunInfo
from pod5.reader import SignalRowInfo


class TestPod5Reader:
    """Test the Pod5Reader from a pod5 file"""

    def test_reader_fixture(self, reader: p5.Reader) -> None:
        """Basic assertions on the reader fixture"""
        assert isinstance(reader, p5.Reader)
        assert isinstance(reader.batch_count, int)
        assert reader.is_vbz_compressed is True
        assert reader.batch_count > 0

    @pytest.mark.parametrize(
        "attribute,expected_type",
        [
            ("calibration", Calibration),
            ("calibration_digitisation", int),
            ("calibration_range", float),
            ("end_reason", EndReason),
            ("read_id", UUID),
            ("read_number", int),
            ("start_sample", int),
            ("median_before", float),
            ("run_info", RunInfo),
            ("num_minknow_events", int),
            ("num_reads_since_mux_change", int),
            ("num_samples", int),
        ],
    )
    def test_reader_reads_types(
        self, reader: p5.Reader, attribute: str, expected_type: Type
    ) -> None:
        """Assert the types returned for reads are consistent with expectations"""
        minimum_reads = 5
        for pod5_read in reader.reads():
            assert isinstance(pod5_read, p5.ReadRecord)
            assert isinstance(getattr(pod5_read, attribute), expected_type)
            minimum_reads -= 1
            if minimum_reads <= 0:
                break
        else:
            assert False, "did not test minimum reads!"

    @pytest.mark.parametrize(
        "attribute,collection_type,dtype",
        [
            ("signal", numpy.ndarray, numpy.int16),
            ("signal_pa", numpy.ndarray, numpy.float32),
        ],
    )
    def test_reader_reads_numpy_types(
        self,
        reader: p5.Reader,
        attribute: str,
        collection_type: Type,
        dtype: numpy.typing.DTypeLike,
    ) -> None:
        """Assert the types returned for reads are consistent with expectations"""
        minimum_reads = 5
        for pod5_read in reader.reads():
            assert isinstance(pod5_read, p5.ReadRecord)
            collection = getattr(pod5_read, attribute)
            assert isinstance(collection, collection_type)
            assert collection.dtype == dtype

            minimum_reads -= 1
            if minimum_reads <= 0:
                break
        else:
            assert False, "did not test minimum reads!"

    @pytest.mark.parametrize(
        "attribute,collection_type,leaf_type",
        [
            ("signal_rows", list, SignalRowInfo),
        ],
    )
    def test_reader_reads_container_types(
        self,
        reader: p5.Reader,
        attribute: str,
        collection_type: Type,
        leaf_type: Type,
    ) -> None:
        """Assert the types returned for reads are consistent with expectations"""
        minimum_reads = 5
        for pod5_read in reader.reads():
            assert isinstance(pod5_read, p5.ReadRecord)
            collection = getattr(pod5_read, attribute)
            assert isinstance(collection, collection_type)
            assert isinstance(collection[0], leaf_type)
            assert isinstance(collection[-1], leaf_type)

            minimum_reads -= 1
            if minimum_reads <= 0:
                break
        else:
            assert False, "did not test minimum reads!"
