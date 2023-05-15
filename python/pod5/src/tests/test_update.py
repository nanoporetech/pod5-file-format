from pathlib import Path
import packaging.version
import pod5 as p5
from pod5.tools.pod5_update import update_pod5
import pytest


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_V1_PATH = TEST_DATA_PATH / "multi_fast5_zip_v1.pod5"
POD5_V2_PATH = TEST_DATA_PATH / "multi_fast5_zip_v2.pod5"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"


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
            assert reader.file_version_pre_migration < packaging.version.Version("0.1")

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
            assert reader.file_version_pre_migration > packaging.version.Version("0.1")
            assert reader.read_ids
