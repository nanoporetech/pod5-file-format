"""
Testing Pod5Reader
"""
from uuid import UUID
from typing import Type
import numpy
import numpy.typing
from pod5_format.reader import SignalRowInfo
import pytest

import pod5_format as p5
from pod5_format.types import Calibration, EndReason, Pore, RunInfo


class TestPod5ReaderCombined:
    """Test the Pod5Reader from a combined pod5 file"""

    def test_combined_reader_fixture(self, combined_reader: p5.Reader) -> None:
        """Basic assertions on the combined_reader fixture"""
        assert isinstance(combined_reader, p5.Reader)
        assert isinstance(combined_reader.batch_count, int)
        assert combined_reader.is_vbz_compressed is True
        assert combined_reader.batch_count > 0

    @pytest.mark.parametrize(
        "attribute,expected_type",
        [
            ("calibration", Calibration),
            ("calibration_digitisation", int),
            ("calibration_index", int),
            ("calibration_range", float),
            ("end_reason", EndReason),
            ("end_reason_index", int),
            ("median_before", float),
            ("pore", Pore),
            ("pore_index", int),
            ("read_id", UUID),
            ("run_info", RunInfo),
            ("run_info_index", int),
            ("read_number", int),
            ("start_sample", int),
        ],
    )
    def test_combined_reader_reads_types(
        self, combined_reader: p5.Reader, attribute: str, expected_type: Type
    ) -> None:
        """Assert the types returned for reads are consistent with expectations"""
        minimum_reads = 5
        for pod5_read in combined_reader.reads():
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
            ("signal_pa", numpy.ndarray, numpy.float64),
        ],
    )
    def test_combined_reader_reads_numpy_types(
        self,
        combined_reader: p5.Reader,
        attribute: str,
        collection_type: Type,
        dtype: numpy.typing.DTypeLike,
    ) -> None:
        """Assert the types returned for reads are consistent with expectations"""
        minimum_reads = 5
        for pod5_read in combined_reader.reads():
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
    def test_combined_reader_reads_container_types(
        self,
        combined_reader: p5.Reader,
        attribute: str,
        collection_type: Type,
        leaf_type: Type,
    ) -> None:
        """Assert the types returned for reads are consistent with expectations"""
        minimum_reads = 5
        for pod5_read in combined_reader.reads():
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
