"""
Testing Pod5Reader
"""

import datetime
import inspect
import math
from pathlib import Path
from uuid import UUID


import pod5 as p5
from pod5.pod5_types import RunInfo

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"


def count_normal_class_attributes(instance):
    """returns the number of "data" attributes in a class"""
    attrs = inspect.getmembers(instance, lambda a: not (inspect.isroutine(a)))
    normal_attrs = [a for a in attrs if not a[0].startswith("_")]
    return len(normal_attrs)


# Run-info data can be unique to each read however all in this test-data and likely in the case off all real
# files it is the same for each read. The context-tag data and tracking-id data is a part of the run-info data,
# however the expected values are supplied separately below.
run_info_data = {
    "acquisition_id": "a08e850aaa44c8b56765eee10b386fc3e516a62b",
    "acquisition_start_time": datetime.datetime(
        2019, 5, 13, 11, 11, 43, tzinfo=datetime.timezone.utc
    ),
    "adc_min": -4096,
    "adc_max": 4095,
    "experiment_name": "",
    "flow_cell_id": "",
    "flow_cell_product_code": "",
    "protocol_name": "c449127e3461a521e0865fe6a88716f6f6b0b30c",
    "protocol_run_id": "df049455-3552-438c-8176-d4a5b1dd9fc5",
    "protocol_start_time": datetime.datetime(
        1970, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
    ),
    "sample_id": "TEST_SAMPLE",
    "sample_rate": 4000,
    "sequencing_kit": "sqk-lsk108",
    "sequencer_position": "MS00000",
    "sequencer_position_type": "minion",
    "software": "python-pod5-converter",
    "system_name": "",
    "system_type": "",
}

# Context tag data is a "free" set of key-value data that's included in the run-info data. Luckily this is all the
# same with a few exceptions. In one case "sample_frequency" is used in place of "sampling_frequency", this is
# allowed-for during the comparison in TestOldVersions.check_run_info()
context_tag_data = {
    "basecall_config_filename": "dna_r9.4.1_450bps_fast.cfg",
    "experiment_duration_set": "180",
    "experiment_type": "genomic_dna",
    "package": "bream4",
    "package_version": "4.0.6",
    "sampling_frequency": "4000",
    "sequencing_kit": "sqk-lsk108",
}

tracking_id_data = {
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
}

# Expected read-info for reads in the test files.
read_info_data = [
    {
        "read_id": UUID("0000173c-bf67-44e7-9a9c-1ad0bc728e74"),
        "read_number": 1093,
        "start_sample": 4534321,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=109,
            well=4,
        ),
        "calibration_offset": 21.0,
        "calibration_scale": 0.1755,
        "median_before": 183.1077,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 123627,
    },
    {
        "read_id": UUID("002fde30-9e23-4125-9eae-d112c18a81a7"),
        "read_number": 75,
        "start_sample": 122095,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=463,
            well=2,
        ),
        "calibration_offset": 4.0,
        "calibration_scale": 0.1755,
        "median_before": 174.630,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 37440,
    },
    {
        "read_id": UUID("006d1319-2877-4b34-85df-34de7250a47b"),
        "read_number": 1053,
        "start_sample": 4347870,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=489,
            well=4,
        ),
        "calibration_offset": 6.0,
        "calibration_scale": 0.1755,
        "median_before": 193.574,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 337876,
    },
    {
        "read_id": UUID("00728efb-2120-4224-87d8-580fbb0bd4b2"),
        "read_number": 657,
        "start_sample": 7231572,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=147,
            well=2,
        ),
        "calibration_offset": 2.0,
        "calibration_scale": 0.1755,
        "median_before": 194.651,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 161547,
    },
    {
        "read_id": UUID("007cc97e-6de2-4ff6-a0fd-1c1eca816425"),
        "read_number": 1625,
        "start_sample": 7820030,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=126,
            well=2,
        ),
        "calibration_offset": 23.0,
        "calibration_scale": 0.1755,
        "median_before": 198.098,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 505057,
    },
    {
        "read_id": UUID("008468c3-e477-46c4-a6e2-7d021a4ebf0b"),
        "read_number": 411,
        "start_sample": 2510647,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=2,
            well=2,
        ),
        "calibration_offset": 4.0,
        "calibration_scale": 0.1755,
        "median_before": 219.046,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 206976,
    },
    {
        "read_id": UUID("008ed3dc-86c2-452f-b107-6877a473d177"),
        "read_number": 513,
        "start_sample": 4540554,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=474,
            well=4,
        ),
        "calibration_offset": 5.0,
        "calibration_scale": 0.1755,
        "median_before": 189.613,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 14510,
    },
    {
        "read_id": UUID("00919556-e519-4960-8aa5-c2dfa020980c"),
        "read_number": 56,
        "start_sample": 314914,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=199,
            well=4,
        ),
        "calibration_offset": 2.0,
        "calibration_scale": 0.1755,
        "median_before": math.nan,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 9885,
    },
    {
        "read_id": UUID("00925f34-6baf-47fc-b40c-22591e27fb5c"),
        "read_number": 930,
        "start_sample": 4234717,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=53,
            well=4,
        ),
        "calibration_offset": 37.0,
        "calibration_scale": 0.1755,
        "median_before": 181.331,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 136370,
    },
    {
        "read_id": UUID("009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e"),
        "read_number": 195,
        "start_sample": 1171730,
        "pore": p5.Pore(
            pore_type="not_set",
            channel=452,
            well=2,
        ),
        "calibration_offset": 14.0,
        "calibration_scale": 0.1755,
        "median_before": 170.736,
        "end_reason": p5.EndReason(p5.EndReasonEnum.UNKNOWN, False),
        "num_minknow_events": 0,
        "open_pore_level": math.nan,
        "expected_open_pore_level": math.nan,
        "selected_read_level": math.nan,
        "num_samples": 15643,
    },
]

# List of files to check along with expected data. If you add new files to the directory you should add them
# to this list. Historically the various versions of the files have been generated by using this Python
# library to update existing files to the latest version, however that's no a requirement when new files are added.
files_to_check = {
    "multi_fast5_zip_v3.pod5": {
        "run_info": run_info_data,
        "read_info": read_info_data,
    },
    "multi_fast5_zip_v4.pod5": {
        "run_info": run_info_data,
        "read_info": read_info_data,
    },
    "multi_fast5_zip_v5.pod5": {
        "run_info": run_info_data,
        "read_info": read_info_data,
    },
    "multi_fast5_zip_v6.pod5": {
        "run_info": run_info_data,
        "read_info": read_info_data,
    },
}

# List of files to ignore
files_to_ignore = [
    "multi_fast5_zip.fast5",
    "multi_fast5_zip_v0.pod5",
    "multi_fast5_zip_v1.pod5",
    "multi_fast5_zip_v2.pod5",
]


def find_files() -> list[Path]:
    # Find the test pod5 files, check there's the correct number.
    test_files = []
    for child in TEST_DATA_PATH.iterdir():
        if not child.name.startswith("multi_fast5_zip"):
            continue
        if child.name in files_to_ignore:
            continue
        test_files.append(child)

    # New files, update this test.
    assert len(test_files) == len(files_to_check)
    return test_files


class TestOldVersions:
    @staticmethod
    def check_run_info(run_info: RunInfo) -> None:
        # The run info should include the tracking_id and context_tags too.
        assert len(run_info_data) + 2 == count_normal_class_attributes(run_info)

        # Check regular run_info fields.
        for field, expected in run_info_data.items():
            assert hasattr(run_info, field)
            assert getattr(run_info, field) == expected, field

        # Check the tracking-id embedded in the run_info
        assert hasattr(run_info, "tracking_id")
        assert len(tracking_id_data) == len(run_info.tracking_id)
        for field, expected in tracking_id_data.items():
            assert field in run_info.tracking_id
            assert run_info.tracking_id[field] == expected

        assert hasattr(run_info, "context_tags")
        assert len(run_info.context_tags) == len(context_tag_data)
        for field, expected in context_tag_data.items():
            # The context tags are "free text" so there is some variation. here it that
            # is limited to names for the sampling rate. Patch it for comparison.
            if field == "sampling_frequency" and field not in run_info.context_tags:
                field = "sample_frequency"
            assert field in run_info.context_tags
            assert run_info.context_tags[field] == expected

    @staticmethod
    def check_read(physical_version: int, index: int, read: p5.ReadRecord) -> None:
        """Checks that the fields in the read-record match the expected data in read_info_data."""

        def check_attribute(actual, expected, field_name: str) -> None:
            if type(expected) is float:
                # Floats identified by expected data and get compared with a tolerance.
                if math.isnan(expected):
                    assert math.isnan(actual), field_name
                else:
                    assert math.isclose(actual, expected, rel_tol=0.001), field_name
            else:
                assert actual == expected, field_name

        def check_v3_fields() -> None:
            """Check the fields present in V3 files, this includes those added in earlier versions, but does not
            include fields marked a deprecated in the latest version of the read_info.
            """
            v3_fields = [
                # v0
                "read_id",
                "read_number",
                "start_sample",
                "median_before",
                # v1
                "num_minknow_events",
                # v2
                "num_samples",
                # v3
                "pore",  # Covers channel, well and pore_type.
                # calibration - needs decomposition for float comparison
                "end_reason",
                # run_info - checked elsewhere
            ]
            for field in v3_fields:
                if field == "calibration":
                    # Checked below
                    continue

                assert hasattr(read, field)
                check_attribute(
                    actual=getattr(read, field),
                    expected=read_info_data[index][field],
                    field_name=field,
                )

            # Calibration requires decomposition for sensible floating point comparison.
            assert type(read_info_data[index]["calibration_scale"]) is float
            assert math.isclose(
                read.calibration.scale,
                read_info_data[index]["calibration_scale"],  # type: ignore
                rel_tol=0.001,
            )
            assert type(read_info_data[index]["calibration_offset"]) is float
            assert math.isclose(
                read.calibration.offset,
                read_info_data[index]["calibration_offset"],  # type: ignore
                rel_tol=0.001,
            )

        def check_v4_fields() -> None:
            # Check fields present in V4 with fields present in earlier versions of read-info checked by delegation.
            check_v3_fields()
            v4_fields = [
                "open_pore_level",
            ]
            for field in v4_fields:
                assert hasattr(read, field)
                assert hasattr(read, field)
                check_attribute(
                    actual=getattr(read, field),
                    expected=read_info_data[index][field],
                    field_name=field,
                )

        def check_v5_fields() -> None:
            # Check fields present in V5, V6 and V7 with fields present in earlier versions of read-info checked
            # by delegation.
            check_v4_fields()
            v5_fields = [
                "expected_open_pore_level",
                "selected_read_level",
            ]
            for field in v5_fields:
                assert hasattr(read, field)
                assert hasattr(read, field)
                check_attribute(
                    actual=getattr(read, field),
                    expected=read_info_data[index][field],
                    field_name=field,
                )

        if physical_version == 3:
            check_v3_fields()
        elif physical_version == 4:
            check_v4_fields()
        elif physical_version in [5, 6, 7]:
            check_v5_fields()
        else:
            assert False, f"unknown physical version {physical_version}"

    def test_reading_historical_versions(self):
        """Tests that all the multi_fast5_zip_... files in test data can be read.

        For each of the files found it will check the run-info and the reads. The read fields checked will depend on the
        physical version of the file.
        """
        for file in find_files():
            with p5.Reader(file) as reader:
                physical_version = reader.physical_read_table_version
                for index, read in enumerate(reader.reads()):
                    self.check_run_info(read.run_info)
                    self.check_read(
                        physical_version=physical_version,
                        index=index,
                        read=read,
                    )
