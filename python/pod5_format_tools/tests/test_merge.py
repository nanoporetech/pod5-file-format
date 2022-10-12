from pathlib import Path
import pytest
import pod5_format as p5
from pod5_format_tools.pod5_merge import merge_pod5s

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent / "test_data"


class TestMerge:
    """Test merge application"""

    def test_merge_runs(self, tmp_path: Path):
        """Test that the merge tool runs a trivial example"""

        # Test all pod5 inputs in test data, which will likely contain duplicates
        inputs = list(TEST_DATA_PATH.glob("*pod5"))
        output = tmp_path / "test.pod5"
        merge_pod5s(inputs[:2], output, duplicate_ok=True)

        assert output.exists()

        with p5.CombinedReader(output) as reader:
            reads = list(reader.reads())
            assert reads

    def test_merge_duplicate_stopped(self, tmp_path: Path):
        """Test that the merge tool prevents duplicate reads being merged"""

        # Test all pod5 inputs in test data, which will likely contain duplicates
        inputs = list(TEST_DATA_PATH.glob("*pod5"))
        output = tmp_path / "test.pod5"

        with pytest.raises(AssertionError):
            merge_pod5s(inputs, output)
