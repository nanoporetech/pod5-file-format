"""
Test for the convert_from_fast5 tool
"""
from pathlib import Path
from typing import Dict
import datetime
from uuid import UUID

import h5py
import numpy as np
import pytest

import pod5_format as p5
from pod5_format_tools.pod5_convert_from_fast5 import (
    convert_fast5_read,
    assert_multi_read_fast5,
)


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent / "test_data"
FAST5_PATH = TEST_DATA_PATH / "multi_fast5_zip.fast5"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip.pod5"

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
        end_reason=p5.EndReason(name=p5.EndReasonEnum.UNKNOWN, forced=False),
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

    def test_convert_fast5_read(self):
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

    def test_single_read_fast5_detection(self):
        """Test single-read fast5 files are detected raising an assertion error"""

        with h5py.File(str(SINGLE_READ_FAST5_PATH), "r") as _f5:
            with pytest.raises(AssertionError, match=".*not a multi-read fast5.*"):
                assert_multi_read_fast5(_f5)

    def test_multi_read_fast5_detection(self):
        """Test multi-read fast5 files are detected not raising an error"""

        with h5py.File(str(FAST5_PATH), "r") as _f5:
            assert_multi_read_fast5(_f5)

    def test_missing_key_type_error(self):
        """Test that a TypeError is raised when converting unsupported fast5 files"""

        with h5py.File(str(SINGLE_READ_FAST5_PATH), "r") as _f5:
            with pytest.raises(TypeError, match=".*supported fast5 file.*"):
                for expected_read_id in _f5:
                    cache = {}
                    convert_fast5_read(_f5[expected_read_id], cache)
