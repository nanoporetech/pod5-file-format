from pathlib import Path
from random import sample
from typing import List
from uuid import UUID
from pod5.tools.pod5_view import view_pod5

import polars as pl

import pod5 as p5
from pod5.tools.pod5_filter import parse_read_id_targets, filter_pod5
from pod5.tools.pod5_subset import PL_DEST_FNAME, PL_READ_ID

from tests.conftest import skip_if_windows
import pytest


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"
READ_IDS_PATH = TEST_DATA_PATH / "subset_mapping_examples/read_ids.txt"

EXPECTED_READ_IDS = [
    "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
    "00925f34-6baf-47fc-b40c-22591e27fb5c",
]


class TestFilterParseIds:
    """Test that pod5 filter parse ids"""

    def _read_ids_path(self, tmp_path: Path, ids: List[str]) -> Path:
        rids = tmp_path / "read_ids.txt"
        rids.write_text("\n".join(ids))
        return rids

    def _assert_columns(self, df: pl.LazyFrame) -> None:
        assert PL_READ_ID in df.columns
        assert PL_DEST_FNAME in df.columns

    def _assert_all_expected(self, df: pl.LazyFrame) -> None:
        read_ids = df.select(PL_READ_ID).collect().to_series().to_list()
        assert len(read_ids) > 0
        assert len(read_ids) == len(EXPECTED_READ_IDS)
        assert set(read_ids) == set(EXPECTED_READ_IDS)

    def test_example(self, tmp_path: Path) -> None:
        """Test known good example to filter"""
        path = self._read_ids_path(tmp_path, EXPECTED_READ_IDS)
        assert len(path.read_text().splitlines()) == 2
        df = parse_read_id_targets(path, Path.cwd())
        self._assert_columns(df)
        self._assert_all_expected(df)

    def test_example_with_header(self, tmp_path: Path) -> None:
        """Test known good example to filter"""
        data = ["read_id"]
        data.extend(EXPECTED_READ_IDS)
        path = self._read_ids_path(tmp_path, data)
        assert len(path.read_text().splitlines()) == 3

        df = parse_read_id_targets(path, Path.cwd())
        self._assert_columns(df)
        self._assert_all_expected(df)

    def test_example_with_comments(self, tmp_path: Path) -> None:
        """Test known good example to filter"""
        data = ["read_id", "#", "# A comment read_id"]
        data.extend(EXPECTED_READ_IDS)
        data.extend(["# Comment"])
        path = self._read_ids_path(tmp_path, data)
        assert len(path.read_text().splitlines()) == 6

        df = parse_read_id_targets(path, Path.cwd())
        self._assert_columns(df)
        self._assert_all_expected(df)

    def test_example_with_whitespace(self, tmp_path: Path) -> None:
        """Test known good example to filter"""
        data = ["read_id", "#", "# A comment read_id", " "]
        data.extend(EXPECTED_READ_IDS)
        data.extend(["# Comment", " "])
        path = self._read_ids_path(tmp_path, data)
        assert len(path.read_text().splitlines()) == 8

        df = parse_read_id_targets(path, Path.cwd())
        self._assert_columns(df)
        self._assert_all_expected(df)

    def test_no_ids(self, tmp_path: Path) -> None:
        """Test known good example to filter"""
        data = ["read_id", "#", "# A comment read_id"]
        path = self._read_ids_path(tmp_path, data)
        assert len(path.read_text().splitlines()) == 3

        with pytest.raises(AssertionError, match="Found 0 read_ids"):
            parse_read_id_targets(path, Path.cwd())


class TestFilter:
    def test_all_in_out(self, tmp_path: Path) -> None:
        """Parse a pod5 file for it's all read_ids and filter expecting all in output"""
        with p5.Reader(POD5_PATH) as reader:
            all_ids = reader.read_ids

        output = tmp_path / "output.pod5"
        read_ids = tmp_path / "read_ids.txt"
        with read_ids.open("w") as _fh:
            _fh.write("\n".join(all_ids))

        assert len(all_ids) > 0
        targets = parse_read_id_targets(read_ids, output)
        assert isinstance(targets, pl.LazyFrame)
        targets = targets.collect()
        assert len(targets) == len(all_ids) == 10
        assert set(targets.get_column(PL_READ_ID)) == set(all_ids)

        filter_pod5(
            [POD5_PATH],
            output,
            read_ids,
            missing_ok=False,
            duplicate_ok=False,
            force_overwrite=False,
            recursive=False,
        )

        assert output.is_file()
        with p5.Reader(output) as reader:
            assert all_ids == reader.read_ids
            assert set(reader.read_ids) == set(targets.get_column(PL_READ_ID))
            assert reader.num_reads > 0

    def test_no_duplicates(self, tmp_path: Path) -> None:
        """Provide duplicate read_ids in the input checking no duplicates are written"""
        with p5.Reader(POD5_PATH) as reader:
            first_id = reader.read_ids[0]

        output = tmp_path / "output.pod5"
        read_ids = tmp_path / "read_ids.txt"
        with read_ids.open("w") as _fh:
            _fh.write("\n".join([first_id, first_id]))

        targets = parse_read_id_targets(read_ids, output)
        assert isinstance(targets, pl.LazyFrame)
        targets = targets.collect()
        assert len(targets) == 1
        assert set(targets.get_column(PL_READ_ID)) == set([first_id])

        filter_pod5(
            [POD5_PATH],
            output,
            read_ids,
            missing_ok=False,
            duplicate_ok=False,
            force_overwrite=False,
            recursive=False,
        )

        assert output.is_file()
        with p5.Reader(output) as reader:
            assert reader.read_ids[0] == first_id
            assert reader.num_reads == 1

    def test_missing_read_ids(self, tmp_path: Path) -> None:
        """Assert that missing read_ids are detected"""
        with p5.Reader(POD5_PATH) as reader:
            all_ids = reader.read_ids

        # Write a known missing read_id
        read_ids = tmp_path / "read_ids.txt"
        with read_ids.open("w") as _fh:
            _fh.write("\n".join(all_ids))
            _fh.write(f"\n{UUID(bytes=b'0'*16)}\n")

        output = tmp_path / "output.pod5"
        with pytest.raises(AssertionError):
            filter_pod5(
                [POD5_PATH],
                output,
                read_ids,
                missing_ok=False,
                duplicate_ok=False,
                force_overwrite=False,
                recursive=False,
            )
        assert not output.exists()

        filter_pod5(
            [POD5_PATH],
            output,
            read_ids,
            missing_ok=True,
            duplicate_ok=False,
            force_overwrite=False,
            recursive=False,
        )
        assert output.exists()

    def test_force_overwrite(self, tmp_path: Path) -> None:
        """Assert we cannot overwrite outputs without force"""

        output = tmp_path / "output.pod5"
        output.touch()
        with pytest.raises(FileExistsError, match="--force-overwrite not set"):
            filter_pod5(
                [POD5_PATH],
                output,
                READ_IDS_PATH,
                missing_ok=False,
                duplicate_ok=False,
                force_overwrite=False,
                recursive=False,
            )

        assert output.exists()
        filter_pod5(
            [POD5_PATH],
            output,
            READ_IDS_PATH,
            missing_ok=False,
            duplicate_ok=False,
            force_overwrite=True,
            recursive=False,
        )
        assert output.exists()
        with p5.Reader(output) as reader:
            assert reader.num_reads

    def test_empty_input_fails(self, tmp_path: Path) -> None:
        """Request zero reads"""

        empty_file = tmp_path / "empty.txt"
        empty_file.touch()
        output = tmp_path / "output.pod5"

        with pytest.raises(pl.NoDataError, match="empty CSV"):
            filter_pod5(
                [POD5_PATH],
                output,
                empty_file,
                missing_ok=False,
                duplicate_ok=False,
                force_overwrite=False,
                recursive=False,
            )

    @pytest.mark.parametrize("n_reads", [10, 50])
    def test_random_inputs(self, pod5_factory, tmp_path: Path, n_reads: int) -> None:
        p25 = pod5_factory(25)
        p50 = pod5_factory(50)
        p100 = pod5_factory(100)
        pod5s = [p25, p50, p100]
        view_tsv = tmp_path / "view.tsv"
        view_pod5(
            pod5s, view_tsv, include="read_id", no_header=True, force_overwrite=True
        )

        read_ids = sample(list(set(view_tsv.read_text().splitlines())), k=n_reads)
        filter_path = tmp_path / "filter.txt"
        filter_path.write_text("\n".join(read_ids))
        output = tmp_path / "output.pod5"
        filter_pod5(
            pod5s,
            output,
            ids=filter_path,
            missing_ok=False,
            duplicate_ok=False,
            force_overwrite=False,
            recursive=False,
        )

        with p5.Reader(output) as reader:
            assert len(reader.read_ids) == n_reads

    def test_input_directory(self, tmp_path: Path) -> None:
        """Take inputs from directory"""
        output = tmp_path / "output.pod5"
        copy_of = tmp_path / "input.pod5"
        copy_of.write_bytes(POD5_PATH.read_bytes())

        filter_pod5(
            [tmp_path],
            output,
            READ_IDS_PATH,
            missing_ok=False,
            duplicate_ok=False,
            force_overwrite=False,
            recursive=False,
        )

        assert output.exists()
        with p5.Reader(output) as reader:
            assert reader.num_reads

    @skip_if_windows
    def test_recursive_inputs_symlink(self, tmp_path: Path) -> None:
        """Take inputs from directory"""
        output = tmp_path / "output.pod5"

        subdir = tmp_path / "one/two"
        subdir.mkdir(parents=True)
        subdir_symlink = subdir / "input.pod5"
        subdir_symlink.symlink_to(POD5_PATH)

        filter_pod5(
            [tmp_path],
            output,
            READ_IDS_PATH,
            missing_ok=False,
            duplicate_ok=False,
            force_overwrite=False,
            recursive=True,
        )

        assert output.exists()
        with p5.Reader(output) as reader:
            assert reader.num_reads
