from pathlib import Path

import pytest

import pod5 as p5
from pod5.tools.pod5_merge import merge_pod5

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"


class TestMerge:
    """Test merge application"""

    def test_merge_runs(self, tmp_path: Path):
        """Test that the merge tool runs a trivial example"""

        # Test all pod5 inputs in test data, which will likely contain duplicates
        inputs = list(TEST_DATA_PATH.glob("*pod5"))
        output = tmp_path / "test.pod5"
        merge_pod5(
            inputs[:2],
            output,
            duplicate_ok=True,
            recursive=False,
            force_overwrite=False,
        )

        assert output.exists()

        with p5.Reader(output) as reader:
            reads = list(reader.reads())
            assert reads

    def test_merge_duplicate_stopped(self, tmp_path: Path):
        """Test that the merge tool prevents duplicate reads being merged"""

        # Test all pod5 inputs in test data, which will likely contain duplicates
        inputs = list(TEST_DATA_PATH.glob("*pod5"))
        output = tmp_path / "test.pod5"

        with pytest.raises(RuntimeError, match="Duplicate read id"):
            merge_pod5(inputs, output)
