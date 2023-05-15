from pathlib import Path

import numpy as np

import pytest

import pod5 as p5
from pod5.tools.pod5_recover import recover_pod5

from tests.conftest import _random_read

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"


class TestRecover:
    """Test recover application"""

    def _generate_recoverable_file(self, dest_path: Path, read_count=1200):
        reads = []
        with p5.Writer(dest_path) as writer:
            for _ in range(read_count):
                read = _random_read()
                reads.append(read)
                writer.add_read(read)

            # Prevent close being called, by keeping a ref to the writer
            self._tmp_file_ref = writer._writer
            # And preventing the p5.Writer from closing it:
            writer._writer = None

        # Check the file is left as tmp:
        assert dest_path.exists()
        assert len(list(dest_path.parent.glob(".*.tmp-run-info"))) == 1
        assert len(list(dest_path.parent.glob(".*.tmp-reads"))) == 1
        return reads

    def test_recover_runs(self, tmp_path: Path):
        """Test that the recover tool runs a trivial example"""

        recoverable_path = tmp_path / "recoverable.tmp"
        added_reads = self._generate_recoverable_file(recoverable_path)

        with pytest.raises(RuntimeError):
            p5.Reader(recoverable_path)

        recover_pod5([recoverable_path], recursive=False, force_overwrite=False)

        expected_recovered_path = recoverable_path.parent / (
            recoverable_path.stem + "_recovered.pod5"
        )

        with p5.Reader(expected_recovered_path) as reader:
            count_recovered = 0
            for read_record, expected_read in zip(reader.reads(), added_reads):
                assert read_record.read_id == expected_read.read_id
                assert read_record.end_reason == expected_read.end_reason
                assert read_record.pore == expected_read.pore
                assert read_record.run_info == expected_read.run_info
                assert np.array_equal(read_record.signal, expected_read.signal)
                count_recovered += 1

            # Only recover in whole batches, so whole 1000 read counts
            expected_recovered_count = (len(added_reads) // 1000) * 1000
            assert count_recovered == expected_recovered_count
