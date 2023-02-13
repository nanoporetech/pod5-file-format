import json
import typing
from pathlib import Path

import jsonschema
import pytest

from pod5.tools.pod5_subset import parse_csv_mapping, parse_json_mapping

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


class TestCSVMappingParser:
    """Test the CSV Mapping functionality"""

    @pytest.mark.parametrize("csv_content,result", [(CSV_CONTENT_1, CSV_RESULT_1)])
    def test_good_parse_csv_mapping(
        self,
        tmp_path: Path,
        csv_content: str,
        result: typing.Dict[str, typing.Set[str]],
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
    def _write_example_json_mapping(
        cls, output: Path, json_mapping: typing.Dict[typing.Any, typing.Any]
    ):
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
        self, tmp_path: Path, json_content: typing.Dict[typing.Any, typing.Any]
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
    def test_bad_parse_json_mapping(self, tmp_path: Path, bad_mapping: typing.Any):
        """
        Test that given a json mapping that is badly formatted that the parser
        correctly identifies if it as such
        """
        example_json = tmp_path / "example.json"
        with pytest.raises(jsonschema.exceptions.ValidationError):
            self._write_example_json_mapping(example_json, {"target": bad_mapping})
            parse_json_mapping(example_json)
