from pathlib import Path
import packaging.version
import pod5 as p5
from pod5.tools.pod5_update import update_pod5
import pytest


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_V1_PATH = TEST_DATA_PATH / "multi_fast5_zip_v1.pod5"
POD5_V2_PATH = TEST_DATA_PATH / "multi_fast5_zip_v2.pod5"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v4.pod5"


def assert_v5_read_table_fields_are_physical(reader: p5.Reader) -> None:
    read_table_fields = set(reader.read_table.schema.names)
    assert "open_pore_level" in read_table_fields
    assert "expected_open_pore_level" in read_table_fields
    assert "selected_read_level" in read_table_fields


def assert_v4_read_table_fields_are_physical(reader: p5.Reader) -> None:
    read_table_fields = set(reader.read_table.schema.names)
    assert "open_pore_level" in read_table_fields
    assert "expected_open_pore_level" not in read_table_fields
    assert "selected_read_level" not in read_table_fields


class TestUpdate:
    """Test that pod5 update runs"""

    def test_detect_inplace_update(self, tmp_path: Path) -> None:
        """detect input is output and raise AssertionError"""
        example = tmp_path / "my.pod5"
        example.touch()

        with pytest.raises(AssertionError, match="in-place"):
            update_pod5([tmp_path], tmp_path, force_overwrite=False, recursive=True)

    def test_update(self, tmp_path: Path) -> None:
        """
        Test update updates files and doesn't overwrite existing files unless forced
        """
        inputs = tmp_path / "data"
        inputs.mkdir(parents=True)
        v1 = inputs / "v1.pod5"
        v1.write_bytes(POD5_V2_PATH.read_bytes())

        with p5.Reader(v1) as reader:
            assert reader.original_file_version < packaging.version.Version("0.1")

        exists = tmp_path / "v1.pod5"
        exists.touch()

        # Test no overwrite
        with pytest.raises(FileExistsError, match="--force-overwrite"):
            update_pod5(
                [inputs],
                tmp_path,
                force_overwrite=False,
                recursive=True,
            )

        update_pod5(
            [inputs],
            tmp_path,
            force_overwrite=True,
            recursive=True,
        )
        assert exists.is_file()
        with p5.Reader(exists) as reader:
            assert reader.original_file_version > packaging.version.Version("0.1")
            assert_v5_read_table_fields_are_physical(reader)
            assert reader.read_ids

    def test_update_materialises_latest_read_table_fields(self, tmp_path: Path) -> None:
        """
        Test update physically migrates v4 read tables to latest
        """
        inputs = tmp_path / "data"
        inputs.mkdir(parents=True)
        v4 = inputs / "example.pod5"
        v4.write_bytes(POD5_PATH.read_bytes())

        with p5.Reader(v4) as reader:
            assert_v4_read_table_fields_are_physical(reader)

        update_pod5(
            [inputs],
            tmp_path,
            force_overwrite=True,
            recursive=True,
        )

        updated = tmp_path / "example.pod5"
        assert updated.is_file()
        with p5.Reader(updated) as reader:
            assert_v5_read_table_fields_are_physical(reader)
            assert reader.read_ids
