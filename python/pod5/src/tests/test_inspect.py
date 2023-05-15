from pathlib import Path
import pytest

from pod5.tools.pod5_inspect import inspect_pod5


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"


class TestReads:
    def test_reads_header_written_once(
        self, capsys: pytest.CaptureFixture, pod5_factory
    ) -> None:
        """Assert that the header line in pod5 inspect reads is only written once"""
        paths = [pod5_factory(10), pod5_factory(25)]

        inspect_pod5("reads", paths)

        lines = str(capsys.readouterr().out).splitlines()
        assert len(lines) == 1 + 10 + 25
        assert sum("read_id" in line for line in lines) == 1
