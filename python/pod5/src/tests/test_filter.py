from pathlib import Path
from uuid import UUID

import pod5 as p5
from pod5.tools.pod5_filter import parse_ids, filter_pod5
import pytest


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"
READ_IDS_PATH = TEST_DATA_PATH / "demux_mapping_examples/read_ids.txt"

EXPECTED_READ_IDS = [
    "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
    "00925f34-6baf-47fc-b40c-22591e27fb5c",
]


class TestFilterParseIds:
    """Test that pod5 filter parse ids"""

    def test_example(self) -> None:
        """Test known good example to filter"""
        assert set(parse_ids(READ_IDS_PATH)) == set(EXPECTED_READ_IDS)

    def test_all_in_out(self, tmp_path: Path) -> None:
        """Parse a pod5 file for it's all read_ids and filter expecting all in output"""
        with p5.Reader(POD5_PATH) as reader:
            all_ids = reader.read_ids

        read_ids = tmp_path / "read_ids.txt"
        with read_ids.open("w") as _fh:
            _fh.write("\n".join(all_ids))

        assert len(all_ids) > 0
        assert parse_ids(read_ids) == set(all_ids)

        output = tmp_path / "output.pod5"
        filter_pod5(
            [POD5_PATH],
            output,
            read_ids,
            missing_ok=False,
            duplicate_ok=False,
            force_overwrite=False,
        )

        assert output.is_file()
        with p5.Reader(output) as reader:
            assert all_ids == reader.read_ids
            assert reader.num_reads > 0

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
            )
        assert not output.exists()

        filter_pod5(
            [POD5_PATH],
            output,
            read_ids,
            missing_ok=True,
            duplicate_ok=False,
            force_overwrite=False,
        )
        assert output.exists()

    def test_force_overwrite(self, tmp_path: Path) -> None:
        """Assert we cannot overwrite outputs without force"""

        output = tmp_path / "output.pod5"
        output.touch()
        with pytest.raises(FileExistsError, match="--force_overwrite not set"):
            filter_pod5(
                [POD5_PATH],
                output,
                READ_IDS_PATH,
                missing_ok=False,
                duplicate_ok=False,
                force_overwrite=False,
            )

        assert output.exists()
        filter_pod5(
            [POD5_PATH],
            output,
            READ_IDS_PATH,
            missing_ok=False,
            duplicate_ok=False,
            force_overwrite=True,
        )
        assert output.exists()
        with p5.Reader(output) as reader:
            assert reader.num_reads

    def test_empty_input_fails(self, tmp_path: Path) -> None:
        """Request zero reads"""

        empty_file = tmp_path / "empty.txt"
        empty_file.touch()
        output = tmp_path / "output.pod5"

        with pytest.raises(AssertionError, match="Nothing to do"):
            filter_pod5(
                [POD5_PATH],
                output,
                empty_file,
                missing_ok=False,
                duplicate_ok=False,
                force_overwrite=False,
            )
