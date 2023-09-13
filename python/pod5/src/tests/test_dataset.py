import random
import shutil
from uuid import uuid4
from pathlib import Path

import pytest

import pod5 as p5
from pod5.api_utils import Pod5ApiException
from pod5.reader import ReadRecord
from tests.conftest import (
    POD5_PATH,
    assert_no_leaked_handles,
    assert_no_leaked_handles_win,
)

POD5_PATH_EXPECTED_NUM_READS = 10

# NO LINKS ON WINDOWS
# nested_dataset/
# ./root/root_10.pod5
# ./root/subdir/subdir_11.pod5
# ./root/subdir/symbolic_9.pod5 --> ../../outer/symbolic_9.pod5
# ./root/subdir/subsubdir/subsubdir_12.pod5
# ./root/subdir/subsubdir/empty.txt
# ./root/linked/ --> ../linked/

# ./outer/symbolic_9.pod5
# ./linked/linked_8.pod5

EXPECT_FILE_COUNT_RECURSIVE = 5
EXPECT_READ_COUNT_RECURSIVE = 8 + 9 + 10 + 11 + 12
EXPECT_FILE_COUNT_ROOT = 1
EXPECT_READ_COUNT_ROOT = 10


class TestDatasetReader:
    """
    Test the DatasetReader
    """

    def test_bad_file_num_reads(self, tmp_path: Path) -> None:
        empty = tmp_path / "empty.pod5"
        empty.touch()

        with p5.DatasetReader(empty) as dataset:
            with pytest.raises(Pod5ApiException, match="DatasetReader error reading:"):
                dataset.num_reads

    def test_len_single(self) -> None:
        p5.Reader(POD5_PATH).num_reads == len(
            p5.DatasetReader(POD5_PATH)
        ) == POD5_PATH_EXPECTED_NUM_READS

    def test_iter_single(self) -> None:
        observed = set()
        for record in p5.DatasetReader(POD5_PATH):
            assert isinstance(record, ReadRecord)
            observed.add(record.read_id)
        assert len(observed) == POD5_PATH_EXPECTED_NUM_READS

    def test_no_recursive(self, nested_dataset: Path) -> None:
        """Test root directory file discovery only"""
        with p5.DatasetReader(nested_dataset) as dataset:
            expected_path = nested_dataset / "root_10.pod5"
            assert dataset._paths == [expected_path]
            assert len(dataset) == 10

            reader = dataset.get_reader(expected_path)
            observed_count = 0
            for reader_read, dataset_read in zip(reader, dataset):
                observed_count += 1
                assert reader_read.read_id == dataset_read.read_id
            assert observed_count == len(dataset)

    def test_recursive(self, nested_dataset: Path) -> None:
        """Test recursive file discovery"""
        dataset = p5.DatasetReader(nested_dataset, recursive=True)

        assert len(dataset.paths) == EXPECT_FILE_COUNT_RECURSIVE
        assert len(dataset) == EXPECT_READ_COUNT_RECURSIVE

        assert dataset._index is None

        # Extremely unlikely that there will be a  collision in 40 UUIDs
        assert not dataset.has_duplicate()
        assert dataset._index is not None
        assert dataset.num_reads == len(dataset._index)

        observed_count = 0
        for path in dataset.paths:
            reader = dataset.get_reader(path)

            for read_id in reader.read_ids:
                observed_count += 1
                read_record = dataset.get_read(read_id=read_id)
                assert read_record is not None
                read_record.read_id == read_id
                isinstance(read_record, ReadRecord)

        assert observed_count == len(dataset)

    def test_get_reader_is_cached(self, nested_dataset: Path) -> None:
        """Tests that a reader is cached"""
        dataset = p5.DatasetReader(nested_dataset)
        root_path = nested_dataset / "root_10.pod5"
        reader = dataset.get_reader(root_path)
        reader_clone = dataset.get_reader(root_path)
        assert id(reader) == id(reader_clone)
        assert isinstance(reader, p5.Reader)
        assert reader.path == root_path

        cache_info = dataset._get_reader.cache_info()  # type: ignore[attr-defined]
        assert cache_info.hits == 1

    def test_reader_all_cache(self, nested_dataset: Path) -> None:
        """Tests that readers are cached and do not leak handles on close"""
        REPEATS = 5000
        with assert_no_leaked_handles():
            ds = p5.DatasetReader(
                nested_dataset, recursive=True, max_cached_readers=None
            )
            paths = ds.paths
            list(ds.get_reader(p) for p in paths * REPEATS)
            cache_info = ds._get_reader.cache_info()  # type: ignore[attr-defined]
            assert cache_info.maxsize is None
            assert cache_info.currsize == len(ds.paths)
            assert cache_info.misses == len(ds.paths)
            assert cache_info.hits == (REPEATS - 1) * len(ds.paths)
            del ds

        for p in paths:
            assert_no_leaked_handles_win(p)

    def test_reader_all_cache_context(self, nested_dataset: Path) -> None:
        """Tests that readers are cached and do not leak handles on close"""
        REPEATS = 5000
        with assert_no_leaked_handles():
            with p5.DatasetReader(
                nested_dataset, recursive=True, max_cached_readers=None
            ) as ds:
                paths = ds.paths
                list(ds.get_reader(p) for p in paths * REPEATS)
                cache_info = ds._get_reader.cache_info()  # type: ignore[attr-defined]
                assert cache_info.maxsize is None
                assert cache_info.currsize == len(ds.paths)
                assert cache_info.misses == len(ds.paths)
                assert cache_info.hits == (REPEATS - 1) * len(ds.paths)

        for p in paths:
            assert_no_leaked_handles_win(p)

    def test_reader_no_cache(self, nested_dataset: Path) -> None:
        """Tests that no reader cache is used if set"""
        REPEATS = 500
        with assert_no_leaked_handles():
            ds = p5.DatasetReader(nested_dataset, recursive=True, max_cached_readers=0)
            paths = ds.paths
            for p in paths * REPEATS:
                ds.get_reader(p)
            cache_info = ds._get_reader.cache_info()  # type: ignore[attr-defined]
            assert cache_info.maxsize == 0
            assert cache_info.currsize == 0
            assert cache_info.misses == REPEATS * len(ds.paths)
            assert cache_info.hits == 0

            # No call to del here tests that the no handles are kept open in the ds

        for p in paths:
            assert_no_leaked_handles_win(p)

    def test_reader_clear_readers(self, nested_dataset: Path) -> None:
        """Tests that the cache is reader cache cleared without leaking handles"""
        REPEATS = 500
        with assert_no_leaked_handles():
            ds = p5.DatasetReader(
                nested_dataset, recursive=True, max_cached_readers=None
            )
            list(ds.get_reader(p) for p in ds.paths * REPEATS)
            cache_info_before = ds._get_reader.cache_info()  # type: ignore[attr-defined]
            assert cache_info_before.currsize > 0
            ds.clear_readers()
            cache_info_after = ds._get_reader.cache_info()  # type: ignore[attr-defined]
            assert cache_info_after.currsize == 0

            # No call to del here tests that the no handles are kept open in the ds

        for p in ds.paths:
            assert_no_leaked_handles_win(p)

    def test_reader_cache_delete(self, nested_dataset: Path) -> None:
        with assert_no_leaked_handles():
            with p5.DatasetReader(
                nested_dataset, recursive=True, max_cached_readers=1
            ) as ds:
                list(ds.get_reader(p) for p in ds.paths)
                cache_info_before = ds._get_reader.cache_info()  # type: ignore[attr-defined]
                assert cache_info_before.currsize == 1
                reader = ds.get_reader(ds.paths[0])
                reader_id = id(reader)
                del reader
                assert reader_id == id(ds.get_reader(ds.paths[0]))

                paths = ds.paths

        for p in paths:
            assert_no_leaked_handles_win(p)

    def test_random_read_indexing(self, nested_dataset: Path) -> None:
        """Test randomly selecting by read id"""
        dataset = p5.DatasetReader(nested_dataset, recursive=True)

        observed_count = 0
        for read_id in sorted(dataset.read_ids):
            observed_count += 1
            read_record = dataset.get_read(read_id)
            assert read_record is not None
            assert str(read_record.read_id) == read_id

        assert observed_count == len(dataset) == EXPECT_READ_COUNT_RECURSIVE

        assert dataset.get_read("") is None
        assert dataset.get_read("foo") is None

    def test_prompt_read_indexing(self, nested_dataset: Path) -> None:
        """Test prompt indexing"""
        with p5.DatasetReader(nested_dataset, recursive=True, index=True) as ds:
            assert ds._index is not None
            assert len(ds._index) == EXPECT_READ_COUNT_RECURSIVE
            assert set(ds._index.keys()) == set(ds.read_ids)

    def test_iter_multi(self, nested_dataset: Path) -> None:
        """Test __iter__ yields all records"""
        dataset = p5.DatasetReader(nested_dataset, recursive=True)

        observed_count = 0
        for read_record in dataset:
            observed_count += 1

            assert read_record is not None
            assert str(read_record.read_id)

        assert observed_count == len(dataset) == EXPECT_READ_COUNT_RECURSIVE

    def test_iter_multi_single_thread(self, nested_dataset: Path) -> None:
        """
        Test __iter__ yields selected records while threads is one (no multi-threading)
        """
        dataset = p5.DatasetReader(nested_dataset, recursive=True, threads=1)

        expected_count = int(len(dataset) // 1.4)
        sample = random.sample(list(dataset.read_ids), expected_count)

        observed_count = 0
        observed_read_ids = set()
        for read_record in dataset.reads(selection=sample):
            observed_count += 1
            observed_read_ids.add(str(read_record.read_id))

        assert observed_count == expected_count == len(observed_read_ids)
        assert observed_read_ids == set(sample)

    def test_iter_multi_multi_thread(self, nested_dataset: Path) -> None:
        """Test __iter__ yields selected records using multi-threading"""
        dataset = p5.DatasetReader(nested_dataset, recursive=True, threads=4)

        expected_count = int(len(dataset) // 1.3)
        sample = random.sample(list(dataset.read_ids), expected_count)

        observed_count = 0
        observed_read_ids = set()
        for read_record in dataset.reads(selection=sample):
            observed_count += 1
            observed_read_ids.add(str(read_record.read_id))

        assert observed_count == expected_count == len(observed_read_ids)
        assert observed_read_ids == set(sample)

    def test_iter_multi_multi_thread_no_cache(self, nested_dataset: Path) -> None:
        """
        Test __iter__ yields selected records from multiple files while
        multi-threading without caching readers
        """
        dataset = p5.DatasetReader(
            nested_dataset, recursive=True, threads=4, max_cached_readers=0
        )

        expected_count = int(len(dataset) // 1.5)
        sample = random.sample(list(dataset.read_ids), expected_count)

        observed_count = 0
        observed_read_ids = set()
        for read_record in dataset.reads(selection=sample):
            observed_count += 1
            observed_read_ids.add(str(read_record.read_id))

        assert observed_count == expected_count == len(observed_read_ids)
        assert observed_read_ids == set(sample)

    def test_mixed_load(self, nested_dataset: Path) -> None:
        """Test passing file and directory paths"""
        dataset = p5.DatasetReader(
            [nested_dataset, nested_dataset, POD5_PATH, POD5_PATH], recursive=True
        )
        assert len(dataset) == EXPECT_READ_COUNT_RECURSIVE + 10
        assert len(dataset.paths) == EXPECT_FILE_COUNT_RECURSIVE + 1

    def test_iter_selection(self, nested_dataset: Path) -> None:
        """Test read_id selection"""
        dataset = p5.DatasetReader([nested_dataset], recursive=True)

        sample_size = 20
        read_ids = set(random.sample(list(dataset.read_ids), sample_size))
        observed_count = 0
        for read_record in dataset.reads(selection=read_ids):
            observed_count += 1
            assert str(read_record.read_id) in read_ids

        assert observed_count == sample_size

    def test_iter_duplicate(self, nested_dataset: Path) -> None:
        """Selecting duplicate read_ids yields multiple copies"""
        dataset = p5.DatasetReader([nested_dataset], recursive=True)

        sample_size = 5
        multiplier = 2
        duplicated_read_ids = (
            random.sample(list(dataset.read_ids), sample_size) * multiplier
        )
        observed_count = 0
        for read_record in dataset.reads(selection=duplicated_read_ids):
            observed_count += 1
            assert str(read_record.read_id) in set(duplicated_read_ids)

        assert observed_count == sample_size * multiplier

    def test_duplicate(self, tmp_path: Path) -> None:
        """Selecting from duplicate files yields each copy"""
        pod5_copy = tmp_path / "copy.pod5"
        shutil.copyfile(POD5_PATH, pod5_copy)
        dataset = p5.DatasetReader([POD5_PATH, pod5_copy])

        assert dataset.has_duplicate()

        unique_ids = set(dataset.read_ids)
        observed_count = 0
        for read_record in dataset.reads(selection=unique_ids):
            observed_count += 1
            assert str(read_record.read_id) in unique_ids
        assert observed_count == len(unique_ids) * 2

    def test_duplicate_index_warns(self, tmp_path: Path) -> None:
        """Indexing from duplicate files warns of consequences and can be suppressed"""
        pod5_copy = tmp_path / "copy.pod5"
        shutil.copyfile(POD5_PATH, pod5_copy)
        dataset = p5.DatasetReader([POD5_PATH, pod5_copy])

        assert dataset.has_duplicate()
        with pytest.warns(Warning, match="read_ids found in dataset"):
            dataset.get_path(next(dataset.read_ids))

        with pytest.warns(Warning, match="read_ids found in dataset"):
            dataset.get_read(next(dataset.read_ids))

        dataset.warn_duplicate_indexing = False
        with pytest.warns(None) as record:
            dataset.get_path(next(dataset.read_ids))
            dataset.get_read(next(dataset.read_ids))

        assert len(record) == 0

    def test_iter_missing(self, nested_dataset: Path) -> None:
        """Missing read_ids are not found"""
        dataset = p5.DatasetReader([nested_dataset], recursive=True)

        sample_size = 5
        read_ids = random.sample(list(dataset.read_ids), sample_size)
        missing_ids = [str(uuid4()) for _ in range(50)]

        observed_count = 0
        for read_record in dataset.reads(selection=read_ids + missing_ids):
            observed_count += 1
            assert str(read_record.read_id) in set(read_ids)

        assert observed_count == sample_size

    def test_get_path(self, nested_dataset: Path) -> None:
        """Get the path to underlying file from read_id"""
        dataset = p5.DatasetReader([nested_dataset], recursive=True)

        assert dataset.get_read(str(uuid4())) is None

        for path in dataset.paths:
            read_id = dataset.get_reader(path).read_ids[0]
            assert dataset.get_path(read_id) == path
