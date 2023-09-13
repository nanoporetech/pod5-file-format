"""
Testing Pod5Reader
"""
import random
from typing import Type
from unittest import mock
from uuid import UUID, uuid4

import numpy
import numpy.typing
import packaging
from pathlib import Path
import pyarrow as pa

import pytest
import lib_pod5 as p5b

import pod5 as p5
from pod5.api_utils import format_read_ids
from pod5.pod5_types import Calibration, EndReason, RunInfo
from pod5.reader import ArrowTableHandle, ReadRecordBatch, SignalRowInfo
from tests.conftest import POD5_PATH


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

    def test_attribute_types(self) -> None:
        with p5.Reader(POD5_PATH) as reader:
            assert isinstance(reader.path, Path)
            assert reader.path == POD5_PATH
            assert reader.reads_table_version == 3

            # File handles
            assert isinstance(reader.inner_file_reader, p5b.Pod5FileReader)
            assert isinstance(reader.read_table, pa.ipc.RecordBatchFileReader)
            assert isinstance(reader.run_info_table, pa.ipc.RecordBatchFileReader)
            assert isinstance(reader.signal_table, pa.ipc.RecordBatchFileReader)

            assert isinstance(reader.file_version, packaging.version.Version)
            assert isinstance(
                reader.file_version_pre_migration, packaging.version.Version
            )
            assert isinstance(reader.writing_software, str)
            assert isinstance(reader.file_identifier, UUID)
            assert isinstance(reader.reads_table_version, int)
            assert isinstance(reader.is_vbz_compressed, bool)
            assert isinstance(reader.signal_batch_row_count, int)
            assert isinstance(reader.batch_count, int)
            assert isinstance(reader.num_reads, int)

            assert isinstance(reader.read_ids_raw, pa.ChunkedArray)
            assert isinstance(reader.read_ids, list)
            assert all(isinstance(r, str) for r in reader.read_ids)

            assert isinstance(reader.get_batch(0), ReadRecordBatch)

    def test_without_mmap(self) -> None:
        """Test the file load without mmap for low-memory devices"""
        pod5_file_reader = p5b.open_file(str(POD5_PATH))
        read_table_location = pod5_file_reader.get_file_read_table_location()

        # Raise OSerror when loading with mmap
        mocked = ArrowTableHandle
        mocked._open_reader_with_mmap = mock.Mock(side_effect=OSError)  # type: ignore
        ath = ArrowTableHandle(read_table_location)

        # Assert no handles are opened promptly
        assert ath.reader is not None
        assert ath.reader.num_record_batches > 0

        # Clean reader resources
        del pod5_file_reader

    def test_iter_selection_in_file_order(self, reader: p5.Reader) -> None:
        """Tests iteration order is on-disk order"""
        shuffled = reader.read_ids
        random.shuffle(shuffled)
        observed_count = 0
        for record, read_id in zip(reader.reads(selection=shuffled), reader.read_ids):
            assert str(record.read_id) == read_id
            observed_count += 1
        assert observed_count == len(reader.read_ids)


class TestRecordBatch:
    def test_get_read(self, pod5_factory) -> None:
        n_reads = 10
        path = pod5_factory(n_reads)
        with p5.Reader(path) as reader:
            reads = list(reader.reads())

            assert reader.batch_count == 1
            batch = reader.get_batch(0)
            assert batch.num_reads == n_reads

            for idx, read in enumerate(reads):
                assert read.read_id == batch.get_read(idx).read_id
            assert n_reads == idx + 1

    def test_column_selection(self, pod5_factory) -> None:
        n_reads = 10
        path = pod5_factory(n_reads)
        with p5.Reader(path) as reader:
            batch = reader.get_batch(0)

            assert len(batch.read_id_column) == n_reads
            assert len(batch.read_number_column) == n_reads

            select_idxs = [3, 4, 8]
            batch.set_selected_batch_rows(select_idxs)

            assert type(batch.read_id_column) == pa.FixedSizeBinaryArray
            assert len(batch.read_id_column) == len(select_idxs)
            ids = [reader.read_ids[idx] for idx in select_idxs]
            assert format_read_ids(batch.read_id_column) == ids

            assert type(batch.read_number_column) == pa.UInt32Array
            assert len(batch.read_number_column) == len(select_idxs)
            reads = list(reader.reads())
            rnums = [reads[idx].read_number for idx in select_idxs]

            # assert type(batch.read_number_column.to_numpy().tolist()) == list
            assert batch.read_number_column.to_numpy().tolist() == rnums

    def test_read_batches(self, pod5_factory) -> None:
        n_reads = 1100
        path = pod5_factory(n_reads)
        with p5.Reader(path) as reader:
            rrb = reader.read_batches
            assert len(list(rrb())) == 2
            assert len(list(rrb(batch_selection=[0]))) == 1
            assert len(list(rrb(batch_selection=[0, 1]))) == 2

    def test_read_batches_raises(self, pod5_factory) -> None:
        n_reads = 1100
        path = pod5_factory(n_reads)
        with p5.Reader(path) as reader:
            # with pytest.raises(AssertionError):
            first, last = reader.read_ids[0], reader.read_ids[-1]
            with pytest.raises(ValueError, match="mutually exclusive"):
                list(reader.read_batches(selection=[first, last], batch_selection=[0]))

            with pytest.raises(RuntimeError, match="Failed to find"):
                list(reader.read_batches(selection=[str(uuid4())]))

    def test_cache_exceptions(self, pod5_factory) -> None:
        n_reads = 10
        path = pod5_factory(n_reads)
        with p5.Reader(path) as reader:
            batch = reader.get_batch(0)

            # No cache set
            with pytest.raises(RuntimeError, match="No cached signal data available"):
                batch.cached_sample_count_column
            with pytest.raises(RuntimeError, match="No cached signal data available"):
                batch.cached_samples_column
