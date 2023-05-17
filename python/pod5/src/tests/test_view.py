from pathlib import Path
import random
from typing import Any, Dict
import polars as pl

import pytest

import pod5 as p5
from pod5.tools.pod5_view import (
    Field,
    assert_unique_acquisition_id,
    get_reads_tables,
    join_reads_to_run_info,
    parse_read_table_chunks,
    parse_reads_table_all,
    parse_run_info_table,
    view_pod5,
    select_fields,
    get_field_or_raise,
    resolve_output,
    write,
    write_header,
    FIELDS,
)


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"

ALL_FIELDS = [
    "read_id",
    "filename",
    "read_number",
    "channel",
    "mux",
    "end_reason",
    "start_time",
    "start_sample",
    "duration",
    "num_samples",
    "minknow_events",
    "sample_rate",
    "median_before",
    "predicted_scaling_scale",
    "predicted_scaling_shift",
    "tracked_scaling_scale",
    "tracked_scaling_shift",
    "num_reads_since_mux_change",
    "time_since_mux_change",
    "run_id",
    "sample_id",
    "experiment_id",
    "flow_cell_id",
    "pore_type",
]


class TestView:
    """Test view application"""

    def is_equal_or_not_set(self, field: str, expected: str) -> None:
        assert field == expected or (field == "" and expected == "not_set")

    def _compare(self, record: p5.ReadRecord, row: Dict[str, Any]) -> None:
        assert str(record.read_id) == row["read_id"]
        assert record.read_number == int(row["read_number"])
        assert record.pore.well == int(row["mux"])
        assert record.pore.channel == int(row["channel"])
        assert record.end_reason.name == row["end_reason"]
        assert record.start_sample / record.run_info.sample_rate == float(
            row["start_time"]
        )
        assert record.start_sample == int(row["start_sample"])
        assert record.num_samples / record.run_info.sample_rate == float(
            row["duration"]
        )
        assert record.num_samples == int(row["num_samples"])
        assert record.num_minknow_events == float(row["minknow_events"])
        assert record.run_info.sample_rate == int(row["sample_rate"])
        pytest.approx(record.median_before, float(row["median_before"]))
        pytest.approx(
            record.predicted_scaling.scale, float(row["predicted_scaling_scale"])
        )
        pytest.approx(
            record.predicted_scaling.shift, float(row["predicted_scaling_shift"])
        )
        pytest.approx(record.tracked_scaling.scale, float(row["tracked_scaling_scale"]))
        pytest.approx(record.tracked_scaling.shift, float(row["tracked_scaling_shift"]))
        assert record.num_reads_since_mux_change == int(
            row["num_reads_since_mux_change"]
        )
        pytest.approx(record.time_since_mux_change, float(row["time_since_mux_change"]))
        assert record.run_info.protocol_run_id == row["run_id"]
        self.is_equal_or_not_set(record.run_info.sample_id, row["sample_id"])
        self.is_equal_or_not_set(record.run_info.experiment_name, row["experiment_id"])
        self.is_equal_or_not_set(record.run_info.flow_cell_id, row["flow_cell_id"])
        assert record.pore.pore_type == row["pore_type"]

    def test_view(self, tmp_path: Path):
        """Test that the merge tool runs a trivial example"""

        # Test all pod5 inputs in test data, which will likely contain duplicates
        output = tmp_path / "test.tsv"
        view_pod5([POD5_PATH], output)

        assert output.exists()

        with output.open("r") as _fh:
            content = _fh.readlines()

        # 10 lines + 1 header
        assert len(content) == 11

        header = content[0]
        assert list(map(str.strip, header.split("\t"))) == ALL_FIELDS

        with p5.Reader(POD5_PATH) as reader:
            for idx, record in enumerate(reader):
                items = list(map(str.strip, content[idx + 1].split("\t")))
                row = {name: items[ALL_FIELDS.index(name)] for name in ALL_FIELDS}
                POD5_PATH.name == row["filename"]

                self._compare(record, row)

            assert idx == 9

    def test_view_no_input(self, tmp_path: Path):
        """Test that the merge tool raises AssertionError if found no files"""
        with pytest.raises(AssertionError, match="Found no pod5 files"):
            view_pod5([tmp_path], tmp_path)

    def test_write_stdout(self, capsys: pytest.CaptureFixture) -> None:
        """Test that polars writes to stdout when path is None"""

        ldf = next(get_reads_tables(POD5_PATH, select_fields()))
        write_header(None, select_fields())
        write(ldf, None)
        content: str = capsys.readouterr().out
        err: str = capsys.readouterr().err
        assert not err
        lines = content.splitlines()
        header = lines[0]
        assert list(map(str.strip, header.split("\t"))) == ALL_FIELDS
        # Empty trailing line
        assert len(lines) == 11
        assert len(set(lines)) == len(lines)

    def test_is_loadable(self, tmp_path: Path) -> None:
        output = tmp_path / "test.tsv"
        view_pod5([POD5_PATH], output)

        df = pl.read_csv(output, separator="\t")
        with p5.Reader(POD5_PATH) as reader:
            for idx, record in enumerate(reader):
                row = df.row(idx, named=True)
                POD5_PATH.name == row["filename"]

                self._compare(record, row)

    def test_parse_run_info(self, pod5_factory) -> None:
        """Test the run_info table parser"""
        a_pod5 = pod5_factory(10)
        with p5.Reader(a_pod5) as reader:
            run_info = parse_run_info_table(reader)

        assert isinstance(run_info, pl.LazyFrame)
        run_info = run_info.collect()
        assert run_info.is_unique().all()
        assert "context_tags" not in run_info.columns
        assert "tracking_id" not in run_info.columns

    def test_parse_reads_all(self, pod5_factory) -> None:
        """Test the reads table parser where the file is small enough to do in one go"""
        a_pod5 = pod5_factory(10)
        with p5.Reader(a_pod5) as reader:
            reads = parse_reads_table_all(reader)

        assert isinstance(reads, pl.LazyFrame)
        assert "read_id" in reads.columns
        assert "run_info" in reads.columns
        assert len(reads.collect()) == 10

    def test_parse_reads_multi_chunk(self, pod5_factory) -> None:
        """Test the reads table parser"""
        a_pod5 = pod5_factory(1100)
        with p5.Reader(a_pod5) as reader:
            tables = [t for t in parse_read_table_chunks(reader, approx_size=999)]

        assert len(tables) == 2
        for table in tables:
            assert isinstance(table, pl.LazyFrame)
            assert "read_id" in table.columns
            assert "run_info" in table.columns

        all_reads = pl.concat(tables)
        assert len(all_reads.collect()) == 1100

    def test_unique_on_duplicated_run_info(self) -> None:
        """Legacy bug where run_info data was duplicated"""
        reads_data = {"read_id": ["a", "b", "c"], "run_info": ["r1", "r1", "r1"]}
        reads = pl.DataFrame(reads_data).lazy()

        run_info_data_dupl = {"acquisition_id": ["r1", "r1"], "data": ["d1", "d1"]}
        run_info = pl.DataFrame(run_info_data_dupl).lazy()

        assert_unique_acquisition_id(run_info, Path.cwd())
        joined = join_reads_to_run_info(reads, run_info)

        assert len(reads.collect()) == 3
        assert len(run_info.collect()) == 2
        # If len(joined) is 6, the uniqueness of run_info has failed and the
        # join operation has doubled-up every row
        assert len(joined.collect()) == 3


class TestSelection:
    """Test selection options"""

    def test_select(self) -> None:
        """Test select options"""
        assert set(ALL_FIELDS) == select_fields()

        assert {"read_id"} == select_fields(group_read_id=True)

        assert {"read_id"} == select_fields(include="read_id")
        assert {"read_id", "filename"} == select_fields(include="read_id,filename")
        assert {"read_id", "filename"} == select_fields(include=",read_id,filename,,,")
        assert {"read_id", "filename"} == select_fields(include=" read_id ,  filename ")
        assert {"mux", "channel"} == select_fields(include=" mux,  channel")
        assert set(ALL_FIELDS) == select_fields(include=",".join(ALL_FIELDS))
        assert set(ALL_FIELDS) == select_fields(include="")

        assert "read_id" not in select_fields(exclude="read_id")
        assert {"pore_type", "mux"} not in select_fields(exclude="pore_type,mux")
        assert set(ALL_FIELDS) == select_fields(exclude="")
        assert set(ALL_FIELDS) == select_fields(exclude=", ,")

        drop_rid = set(ALL_FIELDS) - {"read_id"}
        assert drop_rid == select_fields(exclude="read_id")
        assert drop_rid == select_fields(exclude=",read_id,,   ,")

    @pytest.mark.parametrize("field", ["read_i", "mix", "ed_reason", "_", "channell"])
    def test_misspelling(self, field: str) -> None:
        """Test select raises errors on unknown / misspelled fields"""
        with pytest.raises(KeyError, match=f"Field: '{field}' did not match"):
            select_fields(include=field)

    def test_randomly(self) -> None:
        """Randomly include / exclude"""
        for idx in range(1_000):
            random.seed(idx)
            incl = set(random.choices(ALL_FIELDS, k=random.randint(1, len(ALL_FIELDS))))
            excl = set(random.choices(ALL_FIELDS, k=random.randint(0, len(ALL_FIELDS))))

            expected = incl - excl
            include = ",".join(incl)
            exclude = ",".join(excl)
            try:
                assert expected == select_fields(include=include, exclude=exclude)
            except RuntimeError:
                assert len(expected) == 0

    def test_get_field(self) -> None:
        """Test get_field_or_raise"""
        with pytest.raises(KeyError, match="any known fields"):
            get_field_or_raise("blah")

        with pytest.raises(KeyError, match="any known fields"):
            get_field_or_raise("")

        for field in ALL_FIELDS:
            ret = get_field_or_raise(field)
            assert isinstance(ret, Field)


class TestMisc:
    def test_resolve_output(self, tmp_path: Path) -> None:
        assert resolve_output(None, True) is None
        assert resolve_output(None, False) is None

        no_exist = tmp_path / "no_exist"
        assert resolve_output(no_exist, False) == no_exist
        assert resolve_output(no_exist, True) == no_exist

        exist = tmp_path / "exist"
        exist.touch()
        assert resolve_output(exist, True) == exist

        exist.touch()
        with pytest.raises(FileExistsError):
            resolve_output(exist, False)

        # Test the default output path if a directory is given
        assert tmp_path / "view.txt" == resolve_output(tmp_path, False)

    def test_fields(self) -> None:
        assert all(key == field for key, field in zip(ALL_FIELDS, FIELDS.keys()))
        assert len(FIELDS) > 0

    def test_unique_acquisition_id(self) -> None:
        pass_example = pl.DataFrame(
            {"acquisition_id": [1, 1, 3], "b": [1, 1, 3], "c": [2, 2, 3]}
        ).lazy()
        assert_unique_acquisition_id(pass_example, Path("none"))

        fail_example = pl.DataFrame(
            {"acquisition_id": [1, 1, 3], "b": [1, 2, 3], "c": [2, 1, 3]}
        ).lazy()
        with pytest.raises(AssertionError, match="acquisition_id"):
            assert_unique_acquisition_id(fail_example, Path("none"))
