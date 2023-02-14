import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import jsonschema
import pytest

import pod5 as p5
from pod5.tools.pod5_inspect import do_reads_command
from pod5.tools.pod5_subset import (
    calculate_transfers,
    create_default_filename_template,
    launch_subsetting,
    parse_csv_mapping,
    parse_json_mapping,
    parse_table_mapping,
    resolve_targets,
)

CSV_CONTENT_1 = """
repeated_name, r1
repeated_name, r2
multi_read, r1, r2, r3
handle_spaces, r2,r3, r5
"""
CSV_RESULT_1 = {
    "repeated_name": {"r1", "r2"},
    "multi_read": {"r1", "r2", "r3"},
    "handle_spaces": {"r2", "r3", "r5"},
}

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"


class TestSubset:
    """Test that pod5 subset subsets files"""

    def test_subset_base(self, tmp_path: Path):
        """Test a known-good basic use case"""
        # Known good mapping
        mapping = {
            "well-2.pod5": {
                "002fde30-9e23-4125-9eae-d112c18a81a7",
            },
            "well-4.pod5": {
                "00919556-e519-4960-8aa5-c2dfa020980c",
                "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
                "00925f34-6baf-47fc-b40c-22591e27fb5c",
            },
        }

        resolved_targets = resolve_targets(tmp_path, mapping)
        transfers = calculate_transfers([POD5_PATH], resolved_targets, False, False)
        launch_subsetting(transfers)

        outnames = ["well-2.pod5", "well-4.pod5"]

        # Assert only the expected files are output
        assert outnames == list(path.name for path in tmp_path.glob("*.pod5"))

        # Check all read_ids are present in their respective files
        for outname in outnames:
            with p5.Reader(tmp_path / outname) as reader:
                assert sorted(reader.read_ids) == sorted(list(mapping[outname]))

    def test_subset_shared_read_id(self, tmp_path: Path):
        """Test subsample with a mapping with duplicates a read_id"""
        # Known good mapping
        mapping = {
            "well-2.pod5": {
                "002fde30-9e23-4125-9eae-d112c18a81a7",
                "00925f34-6baf-47fc-b40c-22591e27fb5c",
            },
            "well-4.pod5": {
                "00919556-e519-4960-8aa5-c2dfa020980c",
                "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
                "00925f34-6baf-47fc-b40c-22591e27fb5c",
            },
        }

        resolved_targets = resolve_targets(tmp_path, mapping)
        transfers = calculate_transfers([POD5_PATH], resolved_targets, False, False)
        launch_subsetting(transfers)

        outnames = list(mapping.keys())

        # Assert only the expected files are output
        assert outnames == list(path.name for path in tmp_path.glob("*.pod5"))

        # Check all read_ids are present in their respective files
        for outname in outnames:
            with p5.Reader(tmp_path / outname) as reader:
                assert sorted(reader.read_ids) == sorted(list(mapping[outname]))


class TestSubsetFilenameTemplating:
    """Test the output filename templating"""

    @pytest.mark.parametrize(
        "columns,expected",
        [
            (["mux"], "mux-{mux}.pod5"),
            (["mux", "channel"], "mux-{mux}_channel-{channel}.pod5"),
            (["channel", "mux"], "channel-{channel}_mux-{mux}.pod5"),
        ],
    )
    def test_default_template(self, columns: List[str], expected: str):
        template = create_default_filename_template(columns)
        assert template == expected


class TestTableMappingParser:
    """Test the table parsing functionality"""

    @staticmethod
    def _create_inspect_reads_mapping(
        tmp_path: Path, capsys: pytest.CaptureFixture, columns: List[str]
    ) -> Tuple[Dict, Dict]:
        # Run pod5 inspect reads
        with p5.Reader(POD5_PATH) as reader:
            do_reads_command(reader)

        # Capture stdout from pod5 inspect reads
        captured_stdout = str(capsys.readouterr().out)

        # CSV
        csv_path = tmp_path / "table.csv"
        with csv_path.open("w") as csv:
            csv.writelines(captured_stdout.splitlines(keepends=True))

        csv_table_mapping = parse_table_mapping(
            csv_path,
            filename_template=create_default_filename_template(columns),
            subset_columns=columns,
        )

        # TSV
        tsv_path = tmp_path / "table.tsv"
        with tsv_path.open("w") as csv:
            tsv = captured_stdout.replace(",", "\t")
            csv.writelines(tsv.splitlines(keepends=True))

        tsv_table_mapping = parse_table_mapping(
            tsv_path,
            filename_template=create_default_filename_template(columns),
            subset_columns=columns,
        )

        return csv_table_mapping, tsv_table_mapping

    def test_table_parsing_well(self, tmp_path: Path, capsys: pytest.CaptureFixture):
        """Use pod5 inspect reads to produce a well subsampling"""

        columns = ["well"]
        csv, tsv = self._create_inspect_reads_mapping(tmp_path, capsys, columns)

        # Manually checked
        expected_mapping = {
            "well-2.pod5": {
                "002fde30-9e23-4125-9eae-d112c18a81a7",
                "009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e",
                "008468c3-e477-46c4-a6e2-7d021a4ebf0b",
                "00728efb-2120-4224-87d8-580fbb0bd4b2",
                "007cc97e-6de2-4ff6-a0fd-1c1eca816425",
            },
            "well-4.pod5": {
                "00919556-e519-4960-8aa5-c2dfa020980c",
                "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
                "008ed3dc-86c2-452f-b107-6877a473d177",
                "006d1319-2877-4b34-85df-34de7250a47b",
                "00925f34-6baf-47fc-b40c-22591e27fb5c",
            },
        }

        assert csv == expected_mapping
        assert tsv == expected_mapping

    def test_csv_table_parsing_channel(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ):
        """Use pod5 inspect reads test channel subsampling"""

        columns = ["channel"]
        csv, tsv = self._create_inspect_reads_mapping(tmp_path, capsys, columns)

        # Manually checked
        expected_mapping = {
            "channel-109.pod5": {"0000173c-bf67-44e7-9a9c-1ad0bc728e74"},
            "channel-126.pod5": {"007cc97e-6de2-4ff6-a0fd-1c1eca816425"},
            "channel-147.pod5": {"00728efb-2120-4224-87d8-580fbb0bd4b2"},
            "channel-199.pod5": {"00919556-e519-4960-8aa5-c2dfa020980c"},
            "channel-2.pod5": {"008468c3-e477-46c4-a6e2-7d021a4ebf0b"},
            "channel-452.pod5": {"009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e"},
            "channel-463.pod5": {"002fde30-9e23-4125-9eae-d112c18a81a7"},
            "channel-474.pod5": {"008ed3dc-86c2-452f-b107-6877a473d177"},
            "channel-489.pod5": {"006d1319-2877-4b34-85df-34de7250a47b"},
            "channel-53.pod5": {"00925f34-6baf-47fc-b40c-22591e27fb5c"},
        }

        assert csv == expected_mapping
        assert tsv == expected_mapping

    def test_csv_table_parsing_mixed(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ):
        """Use pod5 inspect reads test channel / end_reason (text) subsampling"""

        columns = ["well", "end_reason"]
        csv, tsv = self._create_inspect_reads_mapping(tmp_path, capsys, columns)

        # Manually checked
        expected_mapping = {
            "well-2_end_reason-unknown.pod5": {
                "002fde30-9e23-4125-9eae-d112c18a81a7",
                "009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e",
                "008468c3-e477-46c4-a6e2-7d021a4ebf0b",
                "00728efb-2120-4224-87d8-580fbb0bd4b2",
                "007cc97e-6de2-4ff6-a0fd-1c1eca816425",
            },
            "well-4_end_reason-unknown.pod5": {
                "00919556-e519-4960-8aa5-c2dfa020980c",
                "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
                "008ed3dc-86c2-452f-b107-6877a473d177",
                "006d1319-2877-4b34-85df-34de7250a47b",
                "00925f34-6baf-47fc-b40c-22591e27fb5c",
            },
        }

        assert csv == expected_mapping
        assert tsv == expected_mapping


class TestCSVMappingParser:
    """Test the CSV Mapping functionality"""

    @pytest.mark.parametrize("csv_content,result", [(CSV_CONTENT_1, CSV_RESULT_1)])
    def test_good_parse_csv_mapping(
        self,
        tmp_path: Path,
        csv_content: str,
        result: Dict[str, Set[str]],
    ):
        """
        Given an example csv input mapping, parse it and assert it's content
        matches the expected result
        """
        example_csv = tmp_path / "example.csv"
        example_csv.write_text(csv_content)
        mapping = parse_csv_mapping(example_csv)

        assert mapping == result


class TestJSONMappingParser:
    """Test the JSON Mapping functionality"""

    @classmethod
    def _write_example_json_mapping(cls, output: Path, json_mapping: Dict[Any, Any]):
        """
        Create an example json subset mapping file in output using the supplied json_mapping
        """
        with output.open("w") as _fh:
            json.dump(json_mapping, _fh)

    @pytest.mark.parametrize(
        "json_content",
        [
            {
                "target_1.pod5": ["r1"],
            },
            {
                "target_1.pod5": ["r1", "r2"],
                "target_2.pod5": ["r3", "r4"],
                "target_3.pod5": ["r5"],
            },
            {
                "t1": ["r1"],
                "t2.pod5": ["r1"],
            },
        ],
    )
    def test_good_parse_json_mapping(
        self, tmp_path: Path, json_content: Dict[Any, Any]
    ):
        """
        Test that given a json mapping file that the parser correctly identifies if it
        matches the required schema
        """
        example_json = tmp_path / "example.json"
        self._write_example_json_mapping(example_json, json_content)
        parsed_json = parse_json_mapping(example_json)

        assert parsed_json == json_content

    @pytest.mark.parametrize("bad_mapping", ["read_id", [], {}, [""], ["ok", []]])
    def test_bad_parse_json_mapping(self, tmp_path: Path, bad_mapping: Any):
        """
        Test that given a json mapping that is badly formatted that the parser
        correctly identifies if it as such
        """
        example_json = tmp_path / "example.json"
        with pytest.raises(jsonschema.exceptions.ValidationError):
            self._write_example_json_mapping(example_json, {"target": bad_mapping})
            parse_json_mapping(example_json)
