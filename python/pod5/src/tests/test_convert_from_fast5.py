"""
Test for the convert_from_fast5 tool
"""
import datetime
import multiprocessing as mp
from pathlib import Path
import queue
import sys
from typing import Dict
from unittest.mock import MagicMock, Mock, patch
from uuid import UUID

import h5py
import numpy as np

import pytest

import pod5
from pod5.tools.pod5_convert_from_fast5 import (
    OutputHandler,
    QueueManager,
    convert_datetime_as_epoch_ms,
    convert_fast5_end_reason,
    convert_fast5_files,
    convert_fast5_read,
    convert_from_fast5,
    convert_run_info,
    get_read_from_fast5,
    handle_exception,
    is_multi_read_fast5,
    logger,
)


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
FAST5_PATH = TEST_DATA_PATH / "multi_fast5_zip.fast5"

SINGLE_READ_FAST5_PATH = (
    TEST_DATA_PATH
    / "single_read_fast5/fe85b517-62ee-4a33-8767-41cab5d5ab39.fast5.single-read"
)

EXPECTED_POD5_RESULTS = {
    "0000173c-bf67-44e7-9a9c-1ad0bc728e74": pod5.Read(
        read_id=UUID("0000173c-bf67-44e7-9a9c-1ad0bc728e74"),
        pore=pod5.Pore(
            channel=109,
            well=4,
            pore_type="not_set",
        ),
        calibration=pod5.Calibration.from_range(
            offset=21.0,
            adc_range=1437.6976318359375,
            digitisation=8192.0,
        ),
        read_number=1093,
        start_sample=4534321,
        median_before=183.1077423095703,
        end_reason=pod5.EndReason(
            pod5.EndReasonEnum.UNKNOWN,
            False,
        ),
        run_info=pod5.RunInfo(
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
    "008468c3-e477-46c4-a6e2-7d021a4ebf0b": pod5.Read(
        read_id=UUID("008468c3-e477-46c4-a6e2-7d021a4ebf0b"),
        pore=pod5.Pore(channel=2, well=2, pore_type="not_set"),
        calibration=pod5.Calibration.from_range(
            offset=4.0,
            adc_range=1437.6976318359375,
            digitisation=8192.0,
        ),
        read_number=411,
        start_sample=2510647,
        median_before=219.04641723632812,
        end_reason=pod5.EndReason(reason=pod5.EndReasonEnum.UNKNOWN, forced=False),
        run_info=pod5.RunInfo(
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
        run_info_cache: Dict[str, pod5.RunInfo] = {}

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

    @pytest.mark.parametrize(
        "fast5,expected",
        [
            (0, pod5.EndReasonEnum.UNKNOWN),
            (1, pod5.EndReasonEnum.UNKNOWN),
            (2, pod5.EndReasonEnum.MUX_CHANGE),
            (3, pod5.EndReasonEnum.UNBLOCK_MUX_CHANGE),
            (4, pod5.EndReasonEnum.DATA_SERVICE_UNBLOCK_MUX_CHANGE),
            (5, pod5.EndReasonEnum.SIGNAL_POSITIVE),
            (6, pod5.EndReasonEnum.SIGNAL_NEGATIVE),
        ],
    )
    def test_end_reason(self, fast5: int, expected: pod5.EndReasonEnum) -> None:
        exp = pod5.EndReason.from_reason_with_default_forced(expected)
        assert exp == convert_fast5_end_reason(fast5)

    def test_convert_run_info_defaults(self) -> None:
        result = convert_run_info(
            acq_id="acq_id",
            adc_max=1,
            adc_min=1,
            sample_rate=1,
            context_tags={},
            device_type="dev_type",
            tracking_id={},
        )

        epoch = convert_datetime_as_epoch_ms(f"{datetime.datetime.utcfromtimestamp(0)}")
        assert isinstance(result, pod5.RunInfo)
        assert result.acquisition_id == "acq_id"
        assert result.acquisition_start_time == epoch
        assert result.adc_max == 1
        assert result.adc_min == 1
        assert result.context_tags == {}
        assert result.experiment_name == ""
        assert result.flow_cell_id == ""
        assert result.flow_cell_product_code == ""
        assert result.protocol_name == ""
        assert result.protocol_run_id == ""
        assert result.protocol_start_time == epoch
        assert result.sample_id == ""
        assert result.sample_rate == 1
        assert result.sequencing_kit == ""
        assert result.sequencer_position == ""
        assert result.sequencer_position_type == "dev_type"
        assert result.software == "python-pod5-converter"
        assert result.system_name == ""
        assert result.system_type == ""
        assert result.tracking_id == {}

    def test_convert_run_info(self) -> None:
        result = convert_run_info(
            acq_id="_acq_id",
            adc_max=2,
            adc_min=3,
            sample_rate=4,
            context_tags={"sequencing_kit": b"sequencing_kit", "ctag": b"ctag"},
            device_type="_dev_type",
            tracking_id={
                "exp_start_time": f"{datetime.datetime.utcfromtimestamp(1)}",
                "flow_cell_id": b"flow_cell_id",
                "flow_cell_product_code": b"flow_cell_product_code",
                "exp_script_name": b"exp_script_name",
                "protocol_run_id": b"protocol_run_id",
                "protocol_start_time": f"{datetime.datetime.utcfromtimestamp(2)}",
                "sample_id": b"sample_id",
                "sequencing_kit": b"sequencing_kit",
                "device_id": b"device_id",
                "device_type": b"device_type",
                "host_product_serial_number": b"host_product_serial_number",
                "host_product_code": b"host_product_code",
            },
        )

        assert isinstance(result, pod5.RunInfo)
        assert result.acquisition_id == "_acq_id"
        assert result.acquisition_start_time == convert_datetime_as_epoch_ms(
            f"{datetime.datetime.utcfromtimestamp(1)}"
        )
        assert result.adc_max == 2
        assert result.adc_min == 3
        assert result.context_tags == {
            "sequencing_kit": "sequencing_kit",
            "ctag": "ctag",
        }
        assert result.experiment_name == ""
        assert result.flow_cell_id == "flow_cell_id"
        assert result.flow_cell_product_code == "flow_cell_product_code"
        assert result.protocol_name == "exp_script_name"
        assert result.protocol_run_id == "protocol_run_id"
        assert result.protocol_start_time == convert_datetime_as_epoch_ms(
            f"{datetime.datetime.utcfromtimestamp(2)}"
        )
        assert result.sample_id == "sample_id"
        assert result.sample_rate == 4
        assert result.sequencing_kit == "sequencing_kit"
        assert result.sequencer_position == "device_id"
        assert result.sequencer_position_type == "device_type"
        assert result.software == "python-pod5-converter"
        assert result.system_name == "host_product_serial_number"
        assert result.system_type == "host_product_code"


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

        assert isinstance(writer, pod5.Writer)
        assert writer.path == tmp_path / "output.pod5"

        handler.close_all()
        assert writer._writer is None
        assert len(list(tmp_path.glob("*.pod5"))) == 1

    def test_output_handler_one_to_one_writer(self, tmp_path: Path):
        """Assert that the OutputHandler creates output name is similar when in 1:1"""
        handler = OutputHandler(tmp_path, tmp_path, False)
        source = tmp_path / "test.fast5"
        writer = handler.get_writer(source)

        assert isinstance(writer, pod5.Writer)
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

            assert isinstance(writer, pod5.Writer)
            assert writer.path == (tmp_path / name).with_suffix(".pod5")

        assert len(list(tmp_path.glob("*.pod5"))) == len(names)
        handler.close_all()

    def test_no_reopen(self, tmp_path: Path):
        """Assert that the OutputHandler will not overwrite files"""
        handler = OutputHandler(tmp_path, tmp_path, False)
        example = tmp_path / "example"
        handler.get_writer(example)
        handler.set_input_complete(example, is_exception=False)
        with pytest.raises(FileExistsError, match="Trying to re-open"):
            handler.get_writer(example)

    def test_none_if_exception(self, tmp_path: Path):
        """Assert that the OutputHandler will not overwrite files"""
        handler = OutputHandler(tmp_path, tmp_path, False)
        example = tmp_path / "example"
        handler.get_writer(example)
        handler.set_input_complete(example, is_exception=True)
        assert handler.get_writer(example) is None

    def test_no_duplicate_open(self, tmp_path: Path):
        """Assert that the OutputHandler will re-use handles"""
        handler = OutputHandler(tmp_path, tmp_path, False)
        example = tmp_path / "example"
        writer1 = handler.get_writer(example)
        writer2 = handler.get_writer(example)
        assert writer1 == writer2


class TestQueueManager:
    def test_shutdown(self, monkeypatch, caplog: pytest.LogCaptureFixture):
        logger.disabled = False
        monkeypatch.setenv("POD5_DEBUG", "1")
        threads, timeout = 5, 0.05
        ctx = mp.get_context("spawn")
        queues = QueueManager(ctx, [FAST5_PATH], threads, timeout)
        n_inputs, n_req, n_data, n_exc = queues.shutdown()
        assert n_inputs == 1
        assert "Unfinished inputs" in caplog.messages[0]
        assert n_req == threads * 2
        assert n_data == 0
        assert n_exc == 0

        for getter in [
            queues.await_data,
            queues.await_request,
            queues.get_exception,
            queues.get_input,
        ]:
            with pytest.raises((OSError, ValueError), match="is closed"):
                # OSError changed to ValueError in py3.8
                getter()

    def test_shutdown_with_work(self, monkeypatch, caplog: pytest.LogCaptureFixture):
        logger.disabled = False
        monkeypatch.setenv("POD5_DEBUG", "1")
        threads, timeout = 1, 0.05
        ctx = mp.get_context("spawn")
        queues = QueueManager(ctx, [FAST5_PATH], threads, timeout)
        queues.enqueue_data(None, None)
        queues.enqueue_exception(Path.cwd(), Exception("blah"), "text")
        n_inputs, n_req, n_data, n_exc = queues.shutdown()
        assert n_inputs == 1
        assert "Unfinished inputs" in caplog.messages[0]
        assert n_req == threads * 2
        assert n_data == 1
        assert "Unfinished data" in caplog.messages[1]
        assert n_exc == 1
        assert "Unfinished exceptions" in caplog.messages[2]

        for getter in [
            queues.await_data,
            queues.await_request,
            queues.get_exception,
            queues.get_input,
        ]:
            with pytest.raises((OSError, ValueError), match="is closed"):
                # OSError changed to ValueError in py3.8
                getter()

    def test_blocked_by_requests(self) -> None:
        threads, timeout = 3, 0.05
        ctx = mp.get_context("spawn")
        queues = QueueManager(ctx, [FAST5_PATH], threads, timeout)
        # Exhaust the requests queue
        for _ in range(queues._requests_size):
            queues.await_request()

        with pytest.raises(TimeoutError, match="No progress"):
            queues.await_request()

        queues.enqueue_request()
        queues.await_request()

        with pytest.raises(TimeoutError, match="No progress"):
            queues.await_data()

    def test_data_queue(self) -> None:
        threads, timeout = 5, 0.05
        ctx = mp.get_context("spawn")
        queues = QueueManager(ctx, [FAST5_PATH], threads, timeout)

        # Assert initially empty
        with pytest.raises(TimeoutError, match="No progress"):
            queues.await_data()

        queues.enqueue_data(FAST5_PATH, [])
        queues.enqueue_data(FAST5_PATH, 100)
        queues.enqueue_data(None, None)

        queues.await_request()
        assert queues.await_data() == (FAST5_PATH, [])
        assert queues.await_data() == (FAST5_PATH, 100)
        assert queues.await_data() == (None, None)

        # Assert await data for list of reads replaced the request by checking
        # for requests being Full
        queues.enqueue_data(FAST5_PATH, [])
        with pytest.raises(queue.Full):
            queues.await_data()

        # Assert only 1 request taken and replaced
        n_inputs, n_req, n_data, n_exc = queues.shutdown()
        assert n_req == threads * 2

    def test_exception_queue(self) -> None:
        threads, timeout = 5, 0.05
        ctx = mp.get_context("spawn")
        queues = QueueManager(ctx, [FAST5_PATH], threads, timeout)

        # Assert initially empty
        assert queues.get_exception() is None

        queues.enqueue_exception(FAST5_PATH, Exception("foo"), "bar")
        item = queues.get_exception()
        assert item is not None
        path, exc, trace = item
        assert path == FAST5_PATH
        with pytest.raises(Exception, match="foo"):
            raise exc
        assert trace == "bar"

        # Assert only 1 request taken and replaced
        n_inputs, n_req, n_data, n_exc = queues.shutdown()
        assert n_req == threads * 2


class TestConvertLoop:
    def test_convert_fast5_files_file_type_exceptions(self, tmp_path: Path) -> None:
        nf5 = tmp_path / "not_a.fast5"
        nf5.touch()
        threads, timeout = 5, 0.05
        ctx = mp.get_context("spawn")
        queues = QueueManager(ctx, [nf5], threads, timeout)
        convert_fast5_files(queues)

        exception = queues.get_exception()
        assert exception is not None
        path, exc, _ = exception
        assert path == nf5
        with pytest.raises(TypeError, match="not a multi-read fast5"):
            raise exc

    def test_convert_fast5_files_breaks_loop(self) -> None:
        threads, timeout = 5, 0.05
        ctx = mp.get_context("spawn")
        qm = QueueManager(ctx, [], threads, timeout)
        convert_fast5_files(qm)

        exception = qm.get_exception()
        assert exception is None

        # Assert sentinel enqueued
        path, data = qm.await_data()
        assert path is None
        assert data is None

    @patch("pod5.tools.pod5_convert_from_fast5.convert_fast5_file")
    def test_convert_fast5_file_exception(self, mock: Mock) -> None:
        threads, timeout = 5, 0.05
        ctx = mp.get_context("spawn")
        qm = QueueManager(ctx, [FAST5_PATH], threads, timeout)
        mock.side_effect = Exception
        convert_fast5_files(qm)
        exception = qm.get_exception()
        assert exception is not None
        path, exc, _ = exception
        assert path == FAST5_PATH
        with pytest.raises(Exception):
            raise exc

        path, data = qm.await_data()
        assert path is None
        assert data is None

    def test_handle_exception(self) -> None:
        hndlr = MagicMock()
        status = MagicMock()
        exc = (FAST5_PATH, Exception("foo"), "bar")
        with pytest.raises(Exception, match="foo"):
            handle_exception(exc, hndlr, status, True)

        hndlr.set_input_complete.assert_called_once_with(FAST5_PATH, is_exception=True)
        status.write.assert_called_once_with("foo", sys.stderr)
        status.close.assert_called_once()

        assert handle_exception(exc, hndlr, status, False) is None
