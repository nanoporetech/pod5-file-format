"""
Test for the convert_from_fast5 tool
"""
from concurrent.futures import Future
import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from unittest.mock import MagicMock
from uuid import UUID

import h5py
import numpy as np
from pod5.tools.utils import iterate_inputs
import pytest

import pod5 as p5
from pod5.tools.pod5_convert_from_fast5 import (
    OutputHandler,
    StatusMonitor,
    convert_fast5_end_reason,
    convert_fast5_file,
    convert_fast5_read,
    convert_from_fast5,
    filter_multi_read_fast5s,
    futures_exception,
    get_read_from_fast5,
    is_multi_read_fast5,
    plan_chunks,
)

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
FAST5_PATH = TEST_DATA_PATH / "multi_fast5_zip.fast5"

SINGLE_READ_FAST5_PATH = (
    TEST_DATA_PATH
    / "single_read_fast5/fe85b517-62ee-4a33-8767-41cab5d5ab39.fast5.single-read"
)

EXPECTED_POD5_RESULTS = {
    "0000173c-bf67-44e7-9a9c-1ad0bc728e74": p5.Read(
        read_id=UUID("0000173c-bf67-44e7-9a9c-1ad0bc728e74"),
        pore=p5.Pore(
            channel=109,
            well=4,
            pore_type="not_set",
        ),
        calibration=p5.Calibration.from_range(
            offset=21.0,
            adc_range=1437.6976318359375,
            digitisation=8192.0,
        ),
        read_number=1093,
        start_sample=4534321,
        median_before=183.1077423095703,
        end_reason=p5.EndReason(
            p5.EndReasonEnum.UNKNOWN,
            False,
        ),
        run_info=p5.RunInfo(
            acquisition_id="a08e850aaa44c8b56765eee10b386fc3e516a62b",
            acquisition_start_time=datetime.datetime(
                2019, 5, 13, 11, 11, 43, tzinfo=datetime.timezone.utc
            ),
            adc_max=4095,
            adc_min=-4096,
            context_tags={
                "basecall_config_filename": "dna_r9.4.1_450bps_fast.cfg",
                "experiment_duration_set": "180",
                "experiment_type": "genomic_dna",
                "package": "bream4",
                "package_version": "4.0.6",
                "sample_frequency": "4000",
                "sequencing_kit": "sqk-lsk108",
            },
            experiment_name="",
            flow_cell_id="",
            flow_cell_product_code="",
            protocol_name="c449127e3461a521e0865fe6a88716f6f6b0b30c",
            protocol_run_id="df049455-3552-438c-8176-d4a5b1dd9fc5",
            protocol_start_time=datetime.datetime(
                1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
            ),
            sample_id="TEST_SAMPLE",
            sample_rate=4000,
            sequencing_kit="sqk-lsk108",
            sequencer_position="MS00000",
            sequencer_position_type="minion",
            software="python-pod5-converter",
            system_name="",
            system_type="",
            tracking_id={
                "asic_id": "131070",
                "asic_id_eeprom": "0",
                "asic_temp": "35.043102",
                "asic_version": "IA02C",
                "auto_update": "0",
                "auto_update_source": "https://mirror.oxfordnanoportal.com/software/MinKNOW/",
                "bream_is_standard": "0",
                "device_id": "MS00000",
                "device_type": "minion",
                "distribution_status": "modified",
                "distribution_version": "unknown",
                "exp_script_name": "c449127e3461a521e0865fe6a88716f6f6b0b30c",
                "exp_script_purpose": "sequencing_run",
                "exp_start_time": "2019-05-13T11:11:43Z",
                "flow_cell_id": "",
                "guppy_version": "3.0.3+7e7b7d0",
                "heatsink_temp": "35.000000",
                "hostname": "happy_fish",
                "installation_type": "prod",
                "local_firmware_file": "1",
                "operating_system": "ubuntu 16.04",
                "protocol_group_id": "TEST_EXPERIMENT",
                "protocol_run_id": "df049455-3552-438c-8176-d4a5b1dd9fc5",
                "protocols_version": "4.0.6",
                "run_id": "a08e850aaa44c8b56765eee10b386fc3e516a62b",
                "sample_id": "TEST_SAMPLE",
                "usb_config": "MinION_fx3_1.1.1_ONT#MinION_fpga_1.1.0#ctrl#Auto",
                "version": "3.4.0-rc3",
            },
        ),
        # Values are not checked but the length here is from manual inspection
        signal=np.array([1] * 123627, dtype=np.int16),
    ),
    "008468c3-e477-46c4-a6e2-7d021a4ebf0b": p5.Read(
        read_id=UUID("008468c3-e477-46c4-a6e2-7d021a4ebf0b"),
        pore=p5.Pore(channel=2, well=2, pore_type="not_set"),
        calibration=p5.Calibration.from_range(
            offset=4.0,
            adc_range=1437.6976318359375,
            digitisation=8192.0,
        ),
        read_number=411,
        start_sample=2510647,
        median_before=219.04641723632812,
        end_reason=p5.EndReason(reason=p5.EndReasonEnum.UNKNOWN, forced=False),
        run_info=p5.RunInfo(
            acquisition_id="a08e850aaa44c8b56765eee10b386fc3e516a62b",
            acquisition_start_time=datetime.datetime(
                2019, 5, 13, 11, 11, 43, tzinfo=datetime.timezone.utc
            ),
            adc_max=4095,
            adc_min=-4096,
            context_tags={
                "basecall_config_filename": "dna_r9.4.1_450bps_fast.cfg",
                "experiment_duration_set": "180",
                "experiment_type": "genomic_dna",
                "package": "bream4",
                "package_version": "4.0.6",
                "sample_frequency": "4000",
                "sequencing_kit": "sqk-lsk108",
            },
            experiment_name="",
            flow_cell_id="",
            flow_cell_product_code="",
            protocol_name="c449127e3461a521e0865fe6a88716f6f6b0b30c",
            protocol_run_id="df049455-3552-438c-8176-d4a5b1dd9fc5",
            protocol_start_time=datetime.datetime(
                1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
            ),
            sample_id="TEST_SAMPLE",
            sample_rate=4000,
            sequencing_kit="sqk-lsk108",
            sequencer_position="MS00000",
            sequencer_position_type="minion",
            software="python-pod5-converter",
            system_name="",
            system_type="",
            tracking_id={
                "asic_id": "131070",
                "asic_id_eeprom": "0",
                "asic_temp": "35.043102",
                "asic_version": "IA02C",
                "auto_update": "0",
                "auto_update_source": "https://mirror.oxfordnanoportal.com/software/MinKNOW/",
                "bream_is_standard": "0",
                "device_id": "MS00000",
                "device_type": "minion",
                "distribution_status": "modified",
                "distribution_version": "unknown",
                "exp_script_name": "c449127e3461a521e0865fe6a88716f6f6b0b30c",
                "exp_script_purpose": "sequencing_run",
                "exp_start_time": "2019-05-13T11:11:43Z",
                "flow_cell_id": "",
                "guppy_version": "3.0.3+7e7b7d0",
                "heatsink_temp": "35.000000",
                "hostname": "happy_fish",
                "installation_type": "prod",
                "local_firmware_file": "1",
                "operating_system": "ubuntu 16.04",
                "protocol_group_id": "TEST_EXPERIMENT",
                "protocol_run_id": "df049455-3552-438c-8176-d4a5b1dd9fc5",
                "protocols_version": "4.0.6",
                "run_id": "a08e850aaa44c8b56765eee10b386fc3e516a62b",
                "sample_id": "TEST_SAMPLE",
                "usb_config": "MinION_fx3_1.1.1_ONT#MinION_fpga_1.1.0#ctrl#Auto",
                "version": "3.4.0-rc3",
            },
        ),
        # Values are not checked but the length here is from manual inspection
        signal=np.array([1] * 206976, dtype=np.int16),
    ),
}


class TestFast5Conversion:
    """Test the fast5 to pod5 conversion"""

    def test_convert_fast5_read(self) -> None:
        """
        Test known good fast5 reads
        """
        run_info_cache: Dict[str, p5.RunInfo] = {}

        with h5py.File(str(FAST5_PATH), "r") as _f5:

            for read_id, expected_read in EXPECTED_POD5_RESULTS.items():
                read = convert_fast5_read(
                    _f5[f"read_{read_id}"],
                    run_info_cache,
                )

                assert expected_read.end_reason == read.end_reason
                assert expected_read.calibration == read.calibration
                assert expected_read.pore == read.pore
                assert expected_read.run_info == read.run_info
                assert expected_read.read_number == read.read_number
                assert expected_read.start_sample == read.start_sample
                assert expected_read.median_before == read.median_before

                signal = read.decompressed_signal
                assert expected_read.signal.shape[0] == signal.shape[0]
                assert signal.dtype == np.int16

    def test_convert_fast5_file(self, tmp_path: Path) -> None:
        """Assert read conversion works and the result is writable"""
        reads, expected_count = convert_fast5_file(FAST5_PATH, chunk_range=(0, 2))
        assert len(reads) == expected_count == 2
        for read in reads:
            assert isinstance(read, p5.CompressedRead)
            assert read.signal_chunks
            assert read.read_id

        test_write = tmp_path / "output.pod5"
        with p5.Writer(test_write) as writer:
            writer.add_reads(reads)

        assert test_write.exists()

    def test_futures_exception_not_strict(self, tmp_path: Path, mocker) -> None:
        """Test the handling of exceptions when not strict"""
        path = tmp_path / "input.fast5"
        status: MagicMock = mocker.MagicMock()
        future: Future = Future()
        assert future.set_running_or_notify_cancel()
        future.set_exception(KeyError("example"))

        result = futures_exception(path, future, status, strict=False)
        assert result is True

        called_args = status.write.call_args_list
        assert called_args[0][0][0].startswith("Error processing")
        assert called_args[1][0][0].startswith("Sub-process trace")
        assert status.close.call_count == 0

    def test_futures_exception_strict(self, tmp_path: Path, mocker) -> None:
        """Test the handling of exceptions when strict"""
        path = tmp_path / "input.fast5"
        status: MagicMock = mocker.MagicMock()
        future: Future = Future()
        assert future.set_running_or_notify_cancel()
        future.set_exception(KeyError("example"))

        with pytest.raises(KeyError, match="example"):
            futures_exception(path, future, status, strict=True)

        called_args = status.write.call_args_list
        assert called_args[0][0][0].startswith("Error processing")
        assert called_args[1][0][0].startswith("Sub-process trace")
        assert status.close.call_count == 1

    def test_futures_exception_none(self, tmp_path: Path, mocker) -> None:
        """Test the handling of exceptions with no exception"""
        path = tmp_path / "input.fast5"
        status: MagicMock = mocker.MagicMock()
        future: Future = Future()
        assert future.set_running_or_notify_cancel()
        future.set_result(([], 0))
        result = futures_exception(path, future, status, strict=True)
        assert result is False


class TestFast5Detection:
    def test_single_read_fast5_detection(self):
        """Test single-read fast5 files are detected raising an assertion error"""
        assert not is_multi_read_fast5(SINGLE_READ_FAST5_PATH)

    def test_multi_read_fast5_detection(self):
        """Test multi-read fast5 files are detected not raising an error"""
        assert is_multi_read_fast5(FAST5_PATH)

    def test_read_id_keys_detected(self) -> None:
        """Test that only read_id groups are returned from a known good file"""
        with h5py.File(str(FAST5_PATH), "r") as _f5:
            for group_key in _f5.keys():
                assert get_read_from_fast5(group_key, _f5) is not None

    def test_unknown_keys_ignored(self) -> None:
        """Test that non-read_id keys are ignored"""
        with h5py.File(str(FAST5_PATH), "r") as _f5:
            assert get_read_from_fast5("bad_key", _f5) is None

    def test_bad_keys_skipped_with_warning(self) -> None:
        """Test that read_id keys which are bad are skipped raising a warning"""
        with h5py.File(str(FAST5_PATH), "r") as _f5:
            with pytest.warns(UserWarning, match="Failed to read key"):
                # Good reads should start with read_ prefix. This will cause a key error
                assert get_read_from_fast5("read_bad_key", _f5) is None

    def test_non_fast5s_ignored(self, tmp_path: Path) -> None:
        """Test that only multi-read fast5s are passed to conversion"""

        for name in ["ignore.txt", "skip.png", "hide.fasta"]:
            (tmp_path / name).touch()

        (tmp_path / "single.fast5").write_bytes(SINGLE_READ_FAST5_PATH.read_bytes())
        (tmp_path / "multi.fast5").write_bytes(FAST5_PATH.read_bytes())

        with pytest.warns(UserWarning, match='Ignored files: "single.fast5"'):
            pending_fast5s = filter_multi_read_fast5s(
                iterate_inputs([tmp_path], False, "*.fast5"), threads=1
            )

        assert len(pending_fast5s) == 1
        assert pending_fast5s[0] == tmp_path / "multi.fast5"

    def test_non_existent_files_ignored(self, tmp_path: Path, recwarn) -> None:
        """Test that no non-existent files are passed to conversion"""

        no_exist = tmp_path / "no_exist"
        assert not no_exist.exists()

        pending_fast5s = filter_multi_read_fast5s([no_exist], threads=1)

        assert len(pending_fast5s) == 0
        assert len(recwarn) == 0


class TestConvertBehaviour:
    """Test the runtime behaviour of the conversion tool based on the cli arguments"""

    def test_no_unforced_overwrite(self, tmp_path: Path) -> None:
        """Assert that the conversion tool will not overwrite existing files"""

        existing = tmp_path / "exists.pod5"
        existing.touch()
        with pytest.raises(FileExistsError):
            convert_from_fast5(inputs=[FAST5_PATH], output=existing)

    def test_forced_overwrite(self, tmp_path: Path):
        """Assert that the conversion tool will overwrite existing file if forced"""

        existing = tmp_path / "exists.pod5"
        existing.touch()
        convert_from_fast5(inputs=[FAST5_PATH], output=existing, force_overwrite=True)

    def test_directory_output(self, tmp_path: Path):
        """
        Assert that the conversion tool will write to a output directory creating
        a default named output.pod5 file
        """

        assert len(list(tmp_path.rglob("*"))) == 0
        convert_from_fast5(inputs=[FAST5_PATH], output=tmp_path)
        assert len(list(tmp_path.rglob("*.pod5"))) == 1
        assert (tmp_path / "output.pod5").exists()

    def test_single_file_output(self, tmp_path: Path):
        """Assert that the conversion tool will write to a specified file path"""

        output = tmp_path / "filename.pod5"
        assert len(list(tmp_path.rglob("*"))) == 0
        convert_from_fast5(inputs=[FAST5_PATH], output=output)
        assert len(list(tmp_path.rglob("*"))) == 1
        assert output.exists()

    def test_output_121_relative(self, tmp_path: Path):
        """
        Assert that the conversion tool will not write one-to-one files as expected
        """

        clone_1 = tmp_path / "clone1.fast5"
        clone_1.write_bytes(FAST5_PATH.read_bytes())

        clone_2 = tmp_path / "subdir/clone2.fast5"
        clone_2.parent.mkdir(parents=True, exist_ok=False)
        clone_2.write_bytes(FAST5_PATH.read_bytes())

        output = tmp_path / "output"
        output.mkdir(parents=True, exist_ok=True)

        convert_from_fast5(
            inputs=[clone_1, clone_2],
            output=output,
            one_to_one=tmp_path,
            strict=True,
        )

        assert (output / "clone1.pod5").exists()
        assert (output / "subdir/clone2.pod5").exists()

    def test_output_121_relative_no_parents(self, tmp_path: Path):
        """
        Assert that the conversion tool will not write one-to-one files outside of the
        desired output folder
        """

        clone_1 = tmp_path / "relative_parent.fast5"
        clone_1.write_bytes(FAST5_PATH.read_bytes())

        clone_2 = tmp_path / "subdir/clone2.fast5"
        clone_2.parent.mkdir(parents=True, exist_ok=False)
        clone_2.write_bytes(FAST5_PATH.read_bytes())

        output = tmp_path / "output"
        output.mkdir(parents=True, exist_ok=True)

        with pytest.raises(RuntimeError):
            convert_from_fast5(
                inputs=[clone_1, clone_2],
                output=output,
                one_to_one=tmp_path / "subdir",
                strict=True,
            )


class TestOutputHandler:
    def test_writer(self, tmp_path: Path) -> None:
        """Assert that a pod5 writer is created"""
        handler = OutputHandler(tmp_path, None, True)
        input_path = tmp_path / "input.fast5"
        writer = handler.get_writer(input_path)
        assert isinstance(writer, p5.Writer)
        assert writer.path == tmp_path / "output.pod5"

    def test_writer_no_reopen(self, tmp_path: Path) -> None:
        """Assert that a writer cannot be re-opened"""
        handler = OutputHandler(tmp_path, None, True)
        input_path = tmp_path / "input.fast5"
        _ = handler.get_writer(input_path)
        handler.close_all()
        with pytest.raises(FileExistsError, match="re-open"):
            _ = handler.get_writer(input_path)

    def test_writer_no_close_when_shared(self, tmp_path: Path) -> None:
        """Assert that a writer isn't closed if shared"""
        handler = OutputHandler(tmp_path, None, True)
        input_path = tmp_path / "input.fast5"
        writer = handler.get_writer(input_path)
        handler.set_input_complete(input_path)
        assert writer._writer

    def test_writer_close_when_one_to_one(self, tmp_path: Path) -> None:
        """Assert that a writer is closed when the input is finished"""
        handler = OutputHandler(tmp_path, tmp_path, True)
        input_path = tmp_path / "input.fast5"
        writer = handler.get_writer(input_path)
        handler.set_input_complete(input_path)
        assert writer._writer is None

    def test_no_overwrite_existing_file(self, tmp_path: Path) -> None:
        """Assert no unintentional overwrite for existing files"""
        exists = tmp_path / "existing.pod5"
        exists.touch()
        with pytest.raises(FileExistsError, match="force-overwrite not set"):
            OutputHandler(exists, None, force_overwrite=False)

    def test_overwrite_existing_file(self, tmp_path: Path) -> None:
        """Assert force overwrite unlnks file"""
        exists = tmp_path / "existing.pod5"
        exists.touch()
        OutputHandler(exists, None, force_overwrite=True)

    def test_resolve_one_to_one_root(self, tmp_path: Path) -> None:
        """Test the relative path resolution"""
        example = tmp_path / "example.fast5"
        example.touch()
        expected = example = tmp_path / "example.pod5"
        resolved = OutputHandler.resolve_one_to_one_path(example, tmp_path, tmp_path)
        assert resolved == expected

    def test_resolve_one_to_one_parent(self, tmp_path: Path) -> None:
        """Test the relative path resolution"""
        parent = tmp_path / "parent"
        parent.mkdir(parents=True)
        example = parent / "example.fast5"
        example.touch()
        expected = parent / "parent/example.pod5"
        resolved = OutputHandler.resolve_one_to_one_path(example, parent, tmp_path)
        assert resolved == expected

    def test_resolve_one_to_one_error(self, tmp_path: Path) -> None:
        """Test the relative path resolution raises"""
        example = tmp_path / "example.fast5"
        example.touch()
        non_relative = tmp_path / "non_relative"
        with pytest.raises(RuntimeError, match="relative parent"):
            OutputHandler.resolve_one_to_one_path(example, tmp_path, non_relative)

    def test_default_writer(self, tmp_path: Path):
        """Assert that the OutputHandler creates an output file with default name"""
        handler = OutputHandler(tmp_path, None, False)
        source = tmp_path / "test.fast5"
        writer = handler.get_writer(source)

        assert isinstance(writer, p5.Writer)
        assert writer.path == tmp_path / "output.pod5"

        handler.close_all()
        assert writer._writer is None
        assert len(list(tmp_path.glob("*.pod5"))) == 1

    def test_one_to_one_writer(self, tmp_path: Path):
        """Assert that the OutputHandler creates output name is similar when in 1:1"""
        handler = OutputHandler(tmp_path, tmp_path, False)
        source = tmp_path / "test.fast5"
        writer = handler.get_writer(source)

        assert isinstance(writer, p5.Writer)
        assert writer.path == tmp_path / "test.pod5"

        handler.close_all()
        assert writer._writer is None
        assert len(list(tmp_path.glob("*.pod5"))) == 1

    def test_one_to_one_multiple_writer(self, tmp_path: Path):
        """Assert that the OutputHandler creates output name is similar when in 1:1"""
        handler = OutputHandler(tmp_path, tmp_path, False)

        names = ["test1.fast5", "test2.fast5", "example.fast5"]
        for name in names:
            writer = handler.get_writer(tmp_path / name)

            assert isinstance(writer, p5.Writer)
            assert writer.path == (tmp_path / name).with_suffix(".pod5")

        assert len(list(tmp_path.glob("*.pod5"))) == len(names)
        handler.close_all()

    def test_resolve_path_default_dir(self, tmp_path: Path) -> None:
        """Test default output case for directory"""
        expect = tmp_path / "output.pod5"
        result = OutputHandler.resolve_output_path(tmp_path, tmp_path, None)
        assert result == expect

    def test_resolve_path_default_file(self, tmp_path: Path) -> None:
        """Test default output case for file is the file path given"""
        path = tmp_path / "output.pod5"
        result = OutputHandler.resolve_output_path(path, tmp_path, None)
        assert result == path

    def test_resolve_path_one_to_one(self, tmp_path: Path) -> None:
        """Test resolve path for one_to_one mode creates parent directories"""
        parent = tmp_path / "parent"
        path = parent / "output.pod5"
        result = OutputHandler.resolve_output_path(path, parent, tmp_path)
        expected = tmp_path / "parent/parent/output.pod5"
        assert result == expected
        assert result.parent.exists()


class TestStatusMonitor:
    def test_done_after_expectation_change(self, tmp_path: Path) -> None:
        """Assert a file is detected as done after changing expected count"""
        path = tmp_path / "input.fast5"
        other = tmp_path / "other.fast5"
        expected = {path: 4000, other: 999}
        sm = StatusMonitor(expected)
        assert sm.total_expected == 4999

        assert sm.expected[path] == 4000
        assert sm.done[path] == 0
        assert not sm.is_input_done(path)

        # Converted the expected number of reads
        sm.increment(path, 500, 500)
        assert sm.expected[path] == 4000
        assert sm.done[path] == 500
        assert not sm.is_input_done(path)

        # Converted fewer than the expected number of reads
        sm.increment(path, 950, 2000)
        assert sm.expected[path] == 2950  # Lower expectation 1010 reads
        assert sm.done[path] == 1450
        assert not sm.is_input_done(path)

        sm.increment(path, 1400, 1500)
        assert sm.expected[path] == 2850  # Lower expectation 100 reads
        assert sm.done[path] == 2850

        # this output is now done as expected == done
        assert sm.is_input_done(path)

        # Total expected has been adjusted and still adds up
        assert sm.total_expected == 3849

        # Other output is not done
        assert sm.expected[other] == 999
        assert sm.done[other] == 0
        assert not sm.is_input_done(other)


class TestConvertEndReason:
    @pytest.mark.parametrize(
        "fast5_value,expected_value",
        [
            (0, p5.EndReasonEnum.UNKNOWN),
            (1, p5.EndReasonEnum.UNKNOWN),
            (2, p5.EndReasonEnum.MUX_CHANGE),
            (3, p5.EndReasonEnum.UNBLOCK_MUX_CHANGE),
            (4, p5.EndReasonEnum.DATA_SERVICE_UNBLOCK_MUX_CHANGE),
            (5, p5.EndReasonEnum.SIGNAL_POSITIVE),
            (6, p5.EndReasonEnum.SIGNAL_NEGATIVE),
        ],
    )
    def test_all(self, fast5_value: int, expected_value: p5.EndReasonEnum) -> None:
        """Test convert fast5 end reason"""
        result = convert_fast5_end_reason(fast5_value)
        assert result == p5.EndReason.from_reason_with_default_forced(expected_value)


class TestChunking:
    def test_plan_chunks_core(self) -> None:
        """Assert plan_chunks gives chunked ranges"""
        known_length = 10
        path, chunks = plan_chunks(FAST5_PATH)
        assert path == FAST5_PATH
        assert chunks is not None
        assert chunks[-1][1] == known_length

    def test_plan_chunks_raises_negative(self) -> None:
        """Assert plan_chunks gives chunked ranges"""
        with pytest.raises(ValueError, match="greater than zero"):
            plan_chunks(FAST5_PATH, 0)

    @pytest.mark.parametrize(
        "rpc,expected",
        [
            (4, [(0, 4), (4, 8), (8, 10)]),
            (9, [(0, 9), (9, 10)]),
            (10, [(0, 10)]),
            (11, [(0, 10)]),
        ],
    )
    def test_plan_chunks(self, rpc: int, expected: List[Tuple[int, int]]) -> None:
        """Assert plan_chunks gives chunked ranges"""
        _, chunks = plan_chunks(FAST5_PATH, reads_per_chunk=rpc)
        assert chunks == expected
