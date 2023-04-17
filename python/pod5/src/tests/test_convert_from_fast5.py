"""
Test for the convert_from_fast5 tool
"""
import datetime
from multiprocessing import Queue
from pathlib import Path
from random import randint
from typing import Dict, Tuple
from unittest.mock import MagicMock, PropertyMock
from uuid import UUID

import h5py
import numpy as np
from pod5.tools.utils import iterate_inputs
import pytest

import pod5 as p5
from pod5.tools.pod5_convert_from_fast5 import (
    ExcItem,
    OutputHandler,
    RequestItem,
    discard_and_close,
    convert_fast5_files,
    convert_fast5_read,
    convert_from_fast5,
    filter_multi_read_fast5s,
    get_read_from_fast5,
    is_multi_read_fast5,
    process_conversion_tasks,
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


class TestQueues:
    @staticmethod
    def setup_queues() -> Tuple[Queue, Queue]:
        """Setup request and data queues"""
        return Queue(), Queue()

    def test_runtime_exception(self, mocker) -> None:
        """Test the propagation of a runtime exception"""
        request_q, data_q = self.setup_queues()
        request_q.put(RequestItem())

        mocker.patch(
            "pod5.tools.pod5_convert_from_fast5.convert_fast5_read",
            side_effect=Exception("Boom"),
        )

        convert_fast5_files(
            request_q,
            data_q,
            [FAST5_PATH],
            is_subprocess=False,
        )

        item = data_q.get()
        assert isinstance(item, ExcItem)

        with pytest.raises(Exception, match="Boom"):
            raise item.exception

        discard_and_close(request_q)
        discard_and_close(data_q)

        self.assert_is_closed(request_q)
        self.assert_is_closed(data_q)

    def test_process_queues_raises_when_strict(self, tmp_path: Path, mocker) -> None:
        """When strict=True, test that exception is raised via queues"""
        request_q, data_q = self.setup_queues()
        handler = OutputHandler(tmp_path, None, False)

        status: MagicMock = mocker.MagicMock()
        type(status).running = PropertyMock(side_effect=[True, False])

        data_q.put(ExcItem(Path.cwd(), Exception("inner_exception"), "error"))
        with pytest.raises(Exception, match="inner_exception"):
            process_conversion_tasks(request_q, data_q, handler, status, strict=True)

        called_args = status.write.call_args_list
        assert called_args[0][0][0].startswith("Error processing")
        assert called_args[1][0][0].startswith("Sub-process trace")
        assert status.close.call_count == 1

        discard_and_close(request_q)
        discard_and_close(data_q)

    def test_process_queues_passes_exceptions(self, tmp_path: Path, mocker) -> None:
        """When strict=False, test that no exception is raised via queues"""
        request_q, data_q = self.setup_queues()

        handler = OutputHandler(tmp_path, None, False)
        status: MagicMock = mocker.MagicMock()
        type(status).running = PropertyMock(side_effect=[True, False])

        data_q.put(ExcItem(Path.cwd(), Exception("inner_exception"), "error"))
        process_conversion_tasks(request_q, data_q, handler, status, strict=False)
        assert status.write.call_count == 2

        called_args = status.write.call_args_list
        assert called_args[0][0][0].startswith("Error processing")
        assert called_args[1][0][0].startswith("Sub-process trace")

        assert status.close.call_count == 1

        discard_and_close(request_q)
        discard_and_close(data_q)

    def test_convert_queues_close(self) -> None:
        """Test that convert_fast5_files queues are closed"""
        request_q, data_q = self.setup_queues()
        request_q.put(RequestItem())

        convert_fast5_files(
            request_q,
            data_q,
            [FAST5_PATH],
            is_subprocess=False,
        )

        discard_and_close(request_q)
        discard_and_close(data_q)

        self.assert_is_closed(request_q)
        self.assert_is_closed(data_q)

    def test_discard_and_close(self) -> None:
        """Assert that discard_and_close closes queues"""
        for _ in range(10):
            queue: Queue = Queue()
            expected = randint(30, 150)
            for idx in range(expected):
                queue.put(idx)
            count = discard_and_close(queue)
            assert count == expected
            self.assert_is_closed(queue)

    def test_discard_and_close_empty(self) -> None:
        """Assert that discard_and_close closes queues"""
        queue: Queue = Queue()
        count = discard_and_close(queue)
        assert count == 0

    def assert_is_closed(self, queue: Queue) -> None:
        """Assert that a mp.Queue is closed"""
        with pytest.raises(Exception, match="closed"):
            queue.put("raises")

        with pytest.raises(Exception, match="closed"):
            queue.get(timeout=None)


class TestConvertBehaviour:
    """Test the runtime behaviour of the conversion tool based on the cli arguments"""

    def test_no_unforced_overwrite(self, tmp_path: Path):
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

        with pytest.raises(RuntimeError, match="directory must be a relative parent"):
            convert_from_fast5(
                inputs=[clone_1, clone_2],
                output=output,
                one_to_one=tmp_path / "subdir",
                strict=True,
            )


class TestOutputHandler:
    def test_output_handler_default_writer(self, tmp_path: Path):
        """Assert that the OutputHandler creates an output file with default name"""
        handler = OutputHandler(tmp_path, None, False)
        source = tmp_path / "test.fast5"
        writer = handler.get_writer(source)

        assert isinstance(writer, p5.Writer)
        assert writer.path == tmp_path / "output.pod5"

        handler.close_all()
        assert writer._writer is None
        assert len(list(tmp_path.glob("*.pod5"))) == 1

    def test_output_handler_one_to_one_writer(self, tmp_path: Path):
        """Assert that the OutputHandler creates output name is similar when in 1:1"""
        handler = OutputHandler(tmp_path, tmp_path, False)
        source = tmp_path / "test.fast5"
        writer = handler.get_writer(source)

        assert isinstance(writer, p5.Writer)
        assert writer.path == tmp_path / "test.pod5"

        handler.close_all()
        assert writer._writer is None
        assert len(list(tmp_path.glob("*.pod5"))) == 1

    def test_output_handler_one_to_one_multiple_writer(self, tmp_path: Path):
        """Assert that the OutputHandler creates output name is similar when in 1:1"""
        handler = OutputHandler(tmp_path, tmp_path, False)

        names = ["test1.fast5", "test2.fast5", "example.fast5"]
        for name in names:
            writer = handler.get_writer(tmp_path / name)

            assert isinstance(writer, p5.Writer)
            assert writer.path == (tmp_path / name).with_suffix(".pod5")

        assert len(list(tmp_path.glob("*.pod5"))) == len(names)
        handler.close_all()
