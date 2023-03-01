from pathlib import Path

import pod5 as p5
from pod5.tools.pod5_filter import parse_ids, filter_pod5


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"
READ_IDS_PATH = TEST_DATA_PATH / "demux_mapping_examples/read_ids.txt"


class TestFilterParseIds:
    """Test that pod5 filter parse ids"""

    def test_example(self) -> None:
        """Test known good example to filter"""
        parse_ids(READ_IDS_PATH)

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
