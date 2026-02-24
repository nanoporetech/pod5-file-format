import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Set

import lib_pod5 as p5b
import polars as pl
import pytest
from polars.testing import assert_frame_equal, assert_series_equal

import pod5
from pod5.tools.pod5_inspect import inspect_pod5
from pod5.tools.pod5_subset import (
    PL_DEST_FNAME,
    PL_READ_ID,
    assert_filename_template,
    build_targets_dict,
    column_keys_from_template,
    create_default_filename_template,
    fstring_to_polars,
    get_separator,
    parse_csv_mapping,
    parse_table_mapping,
    subset_pod5,
)


def get_resource_module() -> ModuleType | None:
    # resource module only available in posix platforms
    # This module is used to set the resource limits when testing subset's
    # file handle / resource management
    try:
        import resource

        return resource
    except ModuleNotFoundError:
        return None


HAS_RLIMIT = hasattr(get_resource_module(), "RLIMIT_NOFILE")

CSV_RESULT_1 = {
    "repeated_name": {"r1", "r2"},
    "multi_read": {"r1", "r2", "r3"},
    "handle_spaces": {"r2", "r3", "r5"},
}

MAPPING = {
    "well-2.pod5": {
        "002fde30-9e23-4125-9eae-d112c18a81a7",
    },
    "well-4.pod5": {
        "00919556-e519-4960-8aa5-c2dfa020980c",
        "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
        "00925f34-6baf-47fc-b40c-22591e27fb5c",
    },
}

MAPPING_REPEATED = {
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

MAPPING_DUPLICATED = {
    "well-2.pod5": {
        "002fde30-9e23-4125-9eae-d112c18a81a7",
        "002fde30-9e23-4125-9eae-d112c18a81a7",
        "002fde30-9e23-4125-9eae-d112c18a81a7",
        "00925f34-6baf-47fc-b40c-22591e27fb5c",
    },
    "well-4.pod5": {
        "00919556-e519-4960-8aa5-c2dfa020980c",
        "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
        "00925f34-6baf-47fc-b40c-22591e27fb5c",
        "00925f34-6baf-47fc-b40c-22591e27fb5c",
    },
}

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v4.pod5"


class TestSubset:
    """Test that pod5 subset subsets files"""

    @staticmethod
    def csv_mapping_single(path: Path, mapping: Dict[str, Set[str]]) -> Path:
        """read ids are exploded over many lines"""
        output = path / "csv.csv"
        with output.open("w") as _fh:
            for target, read_ids in mapping.items():
                for read_id in read_ids:
                    _fh.write(f"{path / target},{read_id}\n")
        return output

    def _test_subset(self, tmp: Path, csv: Path, mapping: Dict[str, Set[str]]) -> None:
        # Known good mapping

        targets = parse_csv_mapping(csv)
        targets_dict = build_targets_dict(targets)

        p5b.subset_pod5s_with_mapping(
            [POD5_PATH],
            tmp,
            targets_dict,
            # threads=threads,
            False,
            False,
            False,
        )

        # Assert only the expected files are output
        expected_outnames = list(mapping.keys())
        actual_outnames = list(path.name for path in tmp.glob("*.pod5"))
        assert sorted(expected_outnames) == sorted(actual_outnames)

        # Check all read_ids are present in their respective files
        for outname in expected_outnames:
            with pod5.Reader(tmp / outname) as reader:
                assert reader.read_ids
                # Set here asserts that there are no duplicates in putput
                assert sorted(reader.read_ids) == sorted(set(mapping[outname]))

    def test_subset_base(self, tmp_path: Path):
        """Test a known-good basic use case"""
        csv = self.csv_mapping_single(tmp_path, MAPPING)
        self._test_subset(tmp_path, csv, MAPPING)

    def test_subset_shared_read_id(self, tmp_path: Path):
        """Test subsample with a mapping with shared reads_ids in outputs"""
        csv = self.csv_mapping_single(tmp_path, MAPPING_REPEATED)
        self._test_subset(tmp_path, csv, MAPPING_REPEATED)

    def test_subset_duplicate_read_id(self, tmp_path: Path):
        """Test subsample with a mapping with shared reads_ids in outputs"""
        csv = self.csv_mapping_single(tmp_path, MAPPING_REPEATED)
        self._test_subset(tmp_path, csv, MAPPING_REPEATED)

    def test_subset_dir_and_recurse(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        csv = tmp_path / "csv.csv"
        data = tmp_path / "subdir/test.pod5"
        data.parent.mkdir(exist_ok=False, parents=True)
        data.write_bytes(POD5_PATH.read_bytes())

        inspect_pod5("reads", [POD5_PATH])
        captured_stdout = str(capsys.readouterr().out)
        with csv.open("w") as csv_write:
            csv_write.writelines(captured_stdout)

        output = tmp_path / "output_dir"
        subset_pod5(
            [tmp_path],
            output=output,
            csv=None,
            table=csv,
            columns=["well"],
            threads=2,
            template="",
            read_id_column="read_id",
            missing_ok=False,
            ignore_incomplete_template=False,
            force_overwrite=False,
            recursive=True,
        )
        assert output.exists()
        for output_pod5 in output.glob("*.pod5"):
            with pod5.Reader(output_pod5) as reader:
                assert reader.read_ids

    def test_assert_overwrite(self, tmp_path: Path) -> None:
        """Test overwriting existing files if requested"""

        # Create a file we know will be written to in `test_subset_base`:
        (tmp_path / "well-2.pod5").touch()

        # Run the test, expecting an error
        with pytest.raises(RuntimeError):
            self.test_subset_base(tmp_path)

    def _pod5s_with_view_table(
        self, num_input_pod5s: int, tmp_path: Path, pod5_factory
    ) -> tuple[Path, Path]:
        """Create cached pod5s and the view table"""
        pod5_path = Path.cwd()
        num_reads = 0
        print(f"Creating {num_input_pod5s} pod5s for tests")
        for n in range(1, num_input_pod5s):
            num_reads += n
            pod5_path = Path(pod5_factory(n))
        print(f"Finished creating {num_input_pod5s} pod5s for tests")
        # Run pod5 view to create the view table
        from pod5.tools.pod5_view import view_pod5

        table = tmp_path / "view.table"
        view_pod5(
            inputs=[pod5_path.parent],
            output=table,
            recursive=True,
        )

        return pod5_path.parent, table

    @pytest.mark.skipif(not HAS_RLIMIT, reason="POSIX only")
    def test_subset_ulimit_below_num_outputs(
        self, tmp_path: Path, capsys: pytest.CaptureFixture, pod5_factory
    ):
        resource = get_resource_module()
        assert resource is not None

        num_input_pod5s = 50
        pod5_dir, table = self._pod5s_with_view_table(
            num_input_pod5s, tmp_path, pod5_factory
        )
        num_reads = sum(range(num_input_pod5s + 1))  # T(50)=1275

        output = tmp_path / "output_dir"
        cmd = [
            sys.executable,
            "-m",
            "pod5.tools.main",
            "subset",
            "--table",
            str(table),
            "--columns",
            "channel",  # assuming channel * mux is mostly unique
            "mux",
            "--output",
            str(output),
            "--recursive",
            "--force-overwrite",
            str(pod5_dir),
        ]

        def set_nofile_le_num_outputs():
            # Set ulimit below max number of output handles.
            # Using (num_reads / 2) as channel * mux should be unique per read
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            rlimit_soft, rlimit_hard = min(num_reads // 3, soft), hard
            resource.setrlimit(resource.RLIMIT_NOFILE, (rlimit_soft, rlimit_hard))

        r = subprocess.run(
            cmd,
            preexec_fn=set_nofile_le_num_outputs,
            capture_output=True,
            text=True,
            check=False,
        )

        assert r.returncode == 0, r.stderr

    @pytest.mark.skipif(not HAS_RLIMIT, reason="POSIX only")
    def test_subset_ulimit_below_num_inputs(
        self, tmp_path: Path, capsys: pytest.CaptureFixture, pod5_factory
    ):
        resource = get_resource_module()
        assert resource is not None

        num_input_pod5s = 50
        pod5_dir, table = self._pod5s_with_view_table(
            num_input_pod5s, tmp_path, pod5_factory
        )

        output = tmp_path / "output_dir"
        cmd = [
            sys.executable,
            "-m",
            "pod5.tools.main",
            "subset",
            "--table",
            str(table),
            "--columns",
            "mux",
            "--output",
            str(output),
            "--recursive",
            "--force-overwrite",
            str(pod5_dir),
        ]

        def set_nofile_le_num_inputs():
            # Set ulimit below max number of input handles.
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            rlimit_soft, rlimit_hard = min(num_input_pod5s - 4, soft), hard
            resource.setrlimit(resource.RLIMIT_NOFILE, (rlimit_soft, rlimit_hard))

        r = subprocess.run(
            cmd,
            preexec_fn=set_nofile_le_num_inputs,
            capture_output=True,
            text=True,
            check=False,
        )

        assert r.returncode == 0, r.stderr


class TestFilenameTemplating:
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

    @pytest.mark.parametrize(
        "keys,template",
        [
            (["mux"], "mux-{mux}.pod5"),
            (["mux", "channel"], "mux-{mux}_channel-{channel}.pod5"),
            (["channel", "mux"], "channel-{channel}_mux-{mux}.pod5"),
            (["aa", "bb", "aa"], "{aa}_{bb}_{aa}.pod5"),
            (["cc", "cc", "cc"], "!{cc}.{cc}.{cc}"),
            (["cc"], "{{{cc}}}"),
            ([], "{{{}}}"),
            ([], ""),
            ([], "foo.pod5"),
        ],
    )
    def test_column_keys_from_template(self, keys: List[str], template: str) -> None:
        assert keys == column_keys_from_template(template)

    @pytest.mark.parametrize(
        "keys,pl_template,template",
        [
            (["mux"], "mux-{}.pod5", "mux-{mux}.pod5"),
            (["mux", "ch"], "mux-{}_ch-{}.pod5", "mux-{mux}_ch-{ch}.pod5"),
            (["ch", "mux"], "ch-{}_mux-{}.pod5", "ch-{ch}_mux-{mux}.pod5"),
            (["aa", "bb", "aa"], "{}_{}_{}.pod5", "{aa}_{bb}_{aa}.pod5"),
            (["cc", "cc", "cc"], "!{}.{}.{}", "!{cc}.{cc}.{cc}"),
            (["cc"], "{{{}}}", "{{{cc}}}"),
            ([], "{{{}}}", "{{{}}}"),
            ([], "", ""),
            ([], "foo.pod5", "foo.pod5"),
        ],
    )
    def test_fstring_to_polars(
        self, keys: List[str], pl_template: str, template: str
    ) -> None:
        expected_pl, expected_keys = fstring_to_polars(template)
        assert expected_pl == pl_template
        assert expected_keys == keys

    def test_template_assertions(self) -> None:
        with pytest.raises(KeyError):
            assert_filename_template("some_{unknown}.pod5", ["known"], True)

        with pytest.raises(KeyError):
            assert_filename_template("some_{unknown}_{known}.pod5", ["known"], True)

        # Ignore incomplete
        assert_filename_template("some.pod5", ["known"], True)
        with pytest.raises(KeyError):
            assert_filename_template("some.pod5", ["known"], False)


class TestParse:
    def test_csv_separator(self, tmp_path: Path) -> None:
        csv = tmp_path / "csv.csv"
        with csv.open("w") as writer:
            writer.writelines(["this,is,a,csv,line", "some,other,line"])
        assert "," == get_separator(csv)

    def test_tsv_separator(self, tmp_path: Path) -> None:
        tsv = tmp_path / "tsv.tsv"
        with tsv.open("w") as writer:
            writer.writelines(["this\tis\ta\ttab\tline", "some\tother\tline"])
        assert "\t" == get_separator(tsv)

    def _inspect_reads_content(
        self, paths: List[Path], capsys: pytest.CaptureFixture
    ) -> str:
        inspect_pod5("reads", paths)
        return str(capsys.readouterr().out)

    def _write_csv(self, tmp_path: Path, content: str) -> Path:
        csv_path = tmp_path / "table.csv"
        with csv_path.open("w") as csv:
            csv.writelines(content.splitlines(keepends=True))
        return csv_path

    def _write_tsv(self, tmp_path: Path, content: str) -> Path:
        tsv_path = tmp_path / "table.tsv"
        with tsv_path.open("w") as tsv:
            tsv_content = content.replace(",", "\t")
            tsv.writelines(tsv_content.splitlines(keepends=True))
        return tsv_path

    def test_csv_tsv_parse_equal_1(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        content = self._inspect_reads_content([POD5_PATH], capsys)
        csv = self._write_csv(tmp_path=tmp_path, content=content)
        tsv = self._write_tsv(tmp_path=tmp_path, content=content)

        csv_ldf = parse_table_mapping(csv, None, ["channel"])
        tsv_ldf = parse_table_mapping(tsv, None, ["channel"])

        assert isinstance(csv_ldf, pl.LazyFrame)
        assert isinstance(tsv_ldf, pl.LazyFrame)

        csv_channel = csv_ldf.collect()
        tsv_channel = tsv_ldf.collect()

        assert len(csv_channel) > 0
        assert len(csv_channel) == len(tsv_channel)
        assert all(c == t for c, t in zip(csv_channel.rows(), tsv_channel.rows()))
        assert_frame_equal(csv_channel, tsv_channel)

        assert "channel" in csv_channel.columns
        assert PL_READ_ID in csv_channel.columns
        assert PL_DEST_FNAME in csv_channel.columns

        expected_mapping = {
            "channel-109.pod5": ["0000173c-bf67-44e7-9a9c-1ad0bc728e74"],
            "channel-126.pod5": ["007cc97e-6de2-4ff6-a0fd-1c1eca816425"],
            "channel-147.pod5": ["00728efb-2120-4224-87d8-580fbb0bd4b2"],
            "channel-199.pod5": ["00919556-e519-4960-8aa5-c2dfa020980c"],
            "channel-2.pod5": ["008468c3-e477-46c4-a6e2-7d021a4ebf0b"],
            "channel-452.pod5": ["009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e"],
            "channel-463.pod5": ["002fde30-9e23-4125-9eae-d112c18a81a7"],
            "channel-474.pod5": ["008ed3dc-86c2-452f-b107-6877a473d177"],
            "channel-489.pod5": ["006d1319-2877-4b34-85df-34de7250a47b"],
            "channel-53.pod5": ["00925f34-6baf-47fc-b40c-22591e27fb5c"],
        }

        records = []
        for fname, rids in expected_mapping.items():
            records.append([fname, rids])

        expected = (
            pl.from_records(records, schema=[PL_DEST_FNAME, PL_READ_ID], orient="row")
            .explode(PL_READ_ID)
            .with_columns(pl.col(PL_DEST_FNAME).cast(pl.Categorical))
        )

        assert_series_equal(
            expected.get_column(PL_DEST_FNAME).sort(),
            csv_channel.get_column(PL_DEST_FNAME).sort(),
        )
        assert_series_equal(
            expected.get_column(PL_READ_ID).sort(),
            csv_channel.get_column(PL_READ_ID).sort(),
        )

    def test_csv_tsv_parse_equal_2(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        content = self._inspect_reads_content([POD5_PATH], capsys)
        csv = self._write_csv(tmp_path=tmp_path, content=content)
        tsv = self._write_tsv(tmp_path=tmp_path, content=content)

        csv_df = parse_table_mapping(csv, None, ["well", "end_reason"]).collect()
        tsv_df = parse_table_mapping(tsv, None, ["well", "end_reason"]).collect()

        assert len(csv_df) > 0
        assert len(csv_df) == len(tsv_df)
        assert all(c == t for c, t in zip(csv_df.rows(), tsv_df.rows()))
        assert_frame_equal(csv_df, tsv_df)

        assert "well" in csv_df.columns
        assert "end_reason" in csv_df.columns

        expected_mapping = {
            "well-2_end_reason-unknown.pod5": [
                "002fde30-9e23-4125-9eae-d112c18a81a7",
                "009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e",
                "008468c3-e477-46c4-a6e2-7d021a4ebf0b",
                "00728efb-2120-4224-87d8-580fbb0bd4b2",
                "007cc97e-6de2-4ff6-a0fd-1c1eca816425",
            ],
            "well-4_end_reason-unknown.pod5": [
                "00919556-e519-4960-8aa5-c2dfa020980c",
                "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
                "008ed3dc-86c2-452f-b107-6877a473d177",
                "006d1319-2877-4b34-85df-34de7250a47b",
                "00925f34-6baf-47fc-b40c-22591e27fb5c",
            ],
        }

        records = []
        for fname, rids in expected_mapping.items():
            records.append([fname, rids])

        expected = (
            pl.from_records(records, schema=[PL_DEST_FNAME, PL_READ_ID], orient="row")
            .explode(PL_READ_ID)
            .with_columns(pl.col(PL_DEST_FNAME).cast(pl.Categorical))
        )

        assert_series_equal(
            expected.get_column(PL_DEST_FNAME).sort(),
            csv_df.get_column(PL_DEST_FNAME).sort(),
        )
        assert_series_equal(
            expected.get_column(PL_READ_ID).sort(),
            csv_df.get_column(PL_READ_ID).sort(),
        )

    def test_parse_csv_filters_invalid_read_ids(self, tmp_path: Path) -> None:
        csv = tmp_path / "invalid.csv"
        valid_id = "00000000-0000-0000-0000-000000000000"
        invalid_id = "not-a-uuid"
        with csv.open("w") as writer:
            writer.write(f"{tmp_path / 'valid.pod5'},{valid_id}\n")
            writer.write(f"{tmp_path / 'invalid.pod5'},{invalid_id}\n")

        parsed = parse_csv_mapping(csv).collect()

        assert parsed.height == 1
        assert parsed.get_column(PL_READ_ID).to_list() == [valid_id]
        assert parsed.get_column(PL_DEST_FNAME).to_list() == [
            str(tmp_path / "valid.pod5")
        ]

    def test_parse_table_filters_invalid_read_ids(self, tmp_path: Path) -> None:
        table = tmp_path / "table.csv"
        valid_id = "11111111-1111-1111-1111-111111111111"
        invalid_id = "1234"
        with table.open("w") as writer:
            writer.writelines(
                [
                    "sample,read_id\n",
                    f"A,{valid_id}\n",
                    f"B,{invalid_id}\n",
                ]
            )

        parsed = parse_table_mapping(table, None, ["sample"]).collect()

        assert parsed.height == 1
        assert parsed.get_column(PL_READ_ID).to_list() == [valid_id]
        assert parsed.get_column(PL_DEST_FNAME).to_list() == ["sample-A.pod5"]
