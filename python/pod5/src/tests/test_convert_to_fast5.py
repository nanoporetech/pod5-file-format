"""
Test for the convert_from_fast5 tool
"""
from pathlib import Path

import numpy as np
import pytest

import pod5 as p5
from pod5.tools.pod5_convert_from_fast5 import convert_from_fast5
from pod5.tools.pod5_convert_to_fast5 import convert_to_fast5

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
FAST5_PATH = TEST_DATA_PATH / "multi_fast5_zip.fast5"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"


class TestPod5ConversionRoundTrip:
    """Test the pod5 to fast5 conversion and back again to assert consistency"""

    def test_convert_pod5_to_fast5_and_back(self, tmp_path: Path) -> None:
        """
        Test known good pod5 file converts to a fast5 file and back again
        """

        # Convert to fast5
        convert_to_fast5([POD5_PATH], tmp_path, False)

        fast5_paths = list(tmp_path.glob("*.fast5"))
        assert len(fast5_paths) == 1

        # Expected filename has input filename with some extra indexing data
        expected_fast5_name = "multi_fast5_zip_v3.0_0.fast5"
        assert fast5_paths[0].name == expected_fast5_name

        # Convert back to pod5
        convert_from_fast5(fast5_paths, tmp_path)

        pod5_paths = list(tmp_path.glob("*.pod5"))
        assert len(pod5_paths) == 1

        # Ensure we aren't cheating
        assert POD5_PATH != pod5_paths[0]

        expected_tested_reads, count_tested_reads = (10, 0)
        with p5.Reader(POD5_PATH) as original:
            with p5.Reader(pod5_paths[0]) as converted:
                for original_record, converted_record in zip(original, converted):
                    # Assert fields are identical
                    assert original_record.read_id == converted_record.read_id
                    assert original_record.read_number == converted_record.read_number
                    assert (
                        original_record.num_minknow_events
                        == converted_record.num_minknow_events
                    )
                    assert original_record.calibration == converted_record.calibration
                    assert original_record.end_reason == converted_record.end_reason
                    assert original_record.pore == converted_record.pore
                    assert original_record.run_info == converted_record.run_info
                    assert original_record.read_number == converted_record.read_number
                    assert original_record.start_sample == converted_record.start_sample
                    assert original_record.num_samples == converted_record.num_samples
                    assert np.array_equal(
                        original_record.signal, converted_record.signal
                    )

                    count_tested_reads += 1

        # Assert we didn't miss any reads
        assert count_tested_reads == expected_tested_reads


class TestConvertBehaviour:
    """Test the runtime behaviour of the conversion tool based on the cli arguments"""

    def test_no_unforced_overwrite(self, tmp_path: Path):
        """Assert that the conversion tool will not overwrite existing files"""

        existing = tmp_path / "multi_fast5_zip_v3.0_0.fast5"
        existing.touch()
        with pytest.raises(FileExistsError):
            convert_to_fast5(inputs=[POD5_PATH], output=tmp_path, force_overwrite=False)

    def test_forced_overwrite(self, tmp_path: Path):
        """Assert that the conversion tool will overwrite existing file if forced"""

        existing = tmp_path / "multi_fast5_zip_v3.0_0.fast5"
        existing.touch()
        created_time = existing.stat().st_mtime_ns
        convert_to_fast5(inputs=[POD5_PATH], output=tmp_path, force_overwrite=True)

        # Assert the file has been replaces
        assert existing.stat().st_mtime_ns > created_time

    def test_recursive_input(self, tmp_path: Path):
        """Assert that the conversion finds pod5s in subdirs"""

        subdir = tmp_path / "sub/subsub/"
        subdir.mkdir(parents=True)
        src = subdir / "input.pod5"
        src.write_bytes(POD5_PATH.read_bytes())

        convert_to_fast5(inputs=[tmp_path], output=tmp_path, recursive=True)
        assert len(list(tmp_path.glob("*.fast5")))

    def test_multiple_outputs(self, tmp_path: Path):
        """
        Assert that the conversion tool will write multiple files where the
        files-read-count is low
        """
        expect = [
            tmp_path / "multi_fast5_zip_v3.0_0.fast5",
            tmp_path / "multi_fast5_zip_v3.1_0.fast5",
        ]

        assert len(list(tmp_path.rglob("*"))) == 0

        # input reads == 10 so expect 2 files
        convert_to_fast5(inputs=[POD5_PATH], output=tmp_path, file_read_count=5)
        fast5s_found = list(tmp_path.rglob("*.fast5"))
        assert len(fast5s_found) == 2

        for fast5 in fast5s_found:
            assert fast5 in expect
