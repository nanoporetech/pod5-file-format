"""
Testing Pod5 Tools
"""
import argparse
import os
from pathlib import Path
import subprocess
import sys
from typing import Callable, Dict
from unittest.mock import Mock, patch
from uuid import UUID

import h5py
import numpy as np
from pod5.tools.utils import collect_inputs, limit_threads
import pytest
import vbz_h5py_plugin  # noqa: F401

import pod5
from pod5.tools import main, parsers

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
FAST5_PATH = TEST_DATA_PATH / "multi_fast5_zip.fast5"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"
SUBSET_CSV_PATH = TEST_DATA_PATH / "subset_mapping_examples/subset.csv"
READ_IDS_PATH = TEST_DATA_PATH / "subset_mapping_examples/read_ids.txt"


def assert_exit_code(func: Callable, func_kwargs: Dict, exit_code: int = 0) -> None:
    """Assert that a function returns the given SystemExit exit code"""
    try:
        func(**func_kwargs)
    except SystemExit as exc:
        assert exc.code == exit_code


class TestPod5Tools:
    """Test the Pod5 tools interface"""

    @patch("pod5.tools.main.run_tool")
    def test_main_calls_run(self, m_run_tool: Mock) -> None:
        """Assert that main calls run_tool and that it returns to main"""
        m_run_tool.return_value = "_return_value"
        return_value = main.main()
        m_run_tool.assert_called()
        assert return_value == "_return_value"

    def test_run_tool_debug_env(
        self, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Assert that exceptions are printed nicely without POD5_DEBUG"""

        dummy_error_string = "Dummy Error String"

        def _func() -> None:
            raise Exception(dummy_error_string)

        parser = argparse.ArgumentParser()
        parser.set_defaults(func=_func)

        # Intentionally raise an error
        with monkeypatch.context() as mkp:
            mkp.setenv("POD5_DEBUG", "0")
            mkp.setattr("argparse._sys.argv", ["_raises_an_exception"])
            assert_exit_code(parsers.run_tool, {"parser": parser}, 1)

        error_str: str = capsys.readouterr().err
        assert "POD5_DEBUG=1" in error_str
        assert dummy_error_string in error_str

    def test_run_tool_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Assert that exceptions are raised if POD5_DEBUG is set"""
        dummy_error_string = "Dummy Error String"

        def _func() -> None:
            raise Exception(dummy_error_string)

        parser = argparse.ArgumentParser()
        parser.set_defaults(func=_func)

        # Intentionally raise an error
        with monkeypatch.context() as mkp:
            mkp.setenv("POD5_DEBUG", "1")
            mkp.setattr("argparse._sys.argv", ["_raises_an_exception"])

            with pytest.raises(Exception, match=dummy_error_string):
                parsers.run_tool(parser)

    def test_pod5_version_argument(self, capsys: pytest.CaptureFixture) -> None:
        """Assert that pod5 has a --version argument"""
        with patch("argparse._sys.argv", ["pod5", "--version"]):
            assert_exit_code(main.main, {}, 0)

        assert f"pod5 version: {pod5.__version__}" in capsys.readouterr().out.lower()

    @pytest.mark.parametrize("subcommand", ["fast5", "to_fast5", "from_fast5"])
    def test_convert_exists(self, subcommand: str) -> None:
        """Assert that pod5 convert exists"""

        with patch("argparse._sys.argv", ["pod5", "convert", subcommand, "--help"]):
            assert_exit_code(main.main, {}, 0)

    @pytest.mark.parametrize("subcommand", ["summary", "read", "reads", "debug"])
    def test_inspect_exists(self, subcommand: str) -> None:
        """Assert that pod5 inspect exists"""

        with patch("argparse._sys.argv", ["pod5", "inspect", subcommand, "--help"]):
            assert_exit_code(main.main, {}, 0)

    @pytest.mark.parametrize(
        "command",
        ["convert", "inspect", "filter", "merge", "subset", "repack", "update"],
    )
    def test_tool_exists(self, command: str) -> None:
        """Assert that a pod5 tool exists"""

        with patch("argparse._sys.argv", ["pod5", command, "--help"]):
            assert_exit_code(main.main, {}, 0)

    def test_convert_from_fast5_runs(self, tmp_path: Path) -> None:
        """Assert that typical commands are valid"""

        args = [
            "pod5",
            "convert",
            "from_fast5",
            str(FAST5_PATH),
            "--output",
            str(tmp_path / "new.pod5"),
            "--strict",
        ]
        with patch("argparse._sys.argv", args):
            main.main()

    def test_convert_to_fast5_runs(self, tmp_path: Path) -> None:
        """Assert that typical commands are valid"""

        outdir = tmp_path / "outdir"
        args = [
            "pod5",
            "convert",
            "to_fast5",
            str(POD5_PATH),
            "--output",
            str(outdir),
        ]
        with patch("argparse._sys.argv", args):
            main.main()

        assert outdir.exists()
        fast5s = list(outdir.glob("*.fast5"))
        assert len(fast5s) > 0

        with h5py.File(fast5s[0]) as f5:
            read_id = str(list(f5.keys())[0])
            assert read_id.startswith("read_")
            assert UUID(read_id[len("read_") :])
            signal = np.array(f5[read_id]["Raw/Signal"])
            assert len(signal) > 0

    @pytest.mark.parametrize("subcommand", ["summary", "reads"])
    def test_inspect_command_runs(self, tmp_path: Path, subcommand: str) -> None:
        """Assert that typical commands are valid"""

        args = [
            "pod5",
            "inspect",
            subcommand,
            str(POD5_PATH),
        ]
        with patch("argparse._sys.argv", args):
            main.main()

    def test_inspect_read_finds_read(self, capsys: pytest.CaptureFixture) -> None:
        """Assert that inspect read finds a known read"""

        args = [
            "pod5",
            "inspect",
            "read",
            str(POD5_PATH),
            "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
        ]
        with patch("argparse._sys.argv", args):
            main.main()

        stdout_lines = str(capsys.readouterr().out).splitlines()

        # A few expected lines from the tool
        assert "read_id: 0000173c-bf67-44e7-9a9c-1ad0bc728e74" in stdout_lines
        assert "read_number:\t1093" in stdout_lines
        assert "start_sample:\t4534321" in stdout_lines
        assert "median_before:\t183.1077423095703" in stdout_lines

    def test_merge_command_runs(self, tmp_path: Path) -> None:
        """Assert that typical commands are valid"""

        args = [
            "pod5",
            "merge",
            str(POD5_PATH),
            "--output",
            str(tmp_path / "new.pod5"),
        ]
        with patch("argparse._sys.argv", args):
            main.main()

    def test_repack_command_runs(self, tmp_path: Path) -> None:
        """Assert that typical commands are valid"""

        args = [
            "pod5",
            "repack",
            str(POD5_PATH),
            "--output",
            str(tmp_path / "new.pod5"),
        ]
        with patch("argparse._sys.argv", args):
            main.main()

    def test_recover_command_runs(self) -> None:
        """Assert that typical commands are valid"""

        args = [
            "pod5",
            "recover",
            str(POD5_PATH),
        ]
        with patch("argparse._sys.argv", args):
            main.main()

    def test_subset_command_runs(self, tmp_path: Path) -> None:
        """Assert that typical commands are valid"""

        output = Path(tmp_path / "test_dir")
        output.mkdir()
        args = [
            "pod5",
            "subset",
            str(POD5_PATH),
            "--output",
            str(output),
            "--csv",
            str(SUBSET_CSV_PATH),
        ]
        with patch("argparse._sys.argv", args):
            main.main()

        # assert len(list(output.rglob("*pod5"))) == 2

    def test_filter_command_runs(self, tmp_path: Path) -> None:
        """Assert that typical commands are valid"""

        args = [
            "pod5",
            "filter",
            str(POD5_PATH),
            "--output",
            str(tmp_path / "take.pod5"),
            "--ids",
            str(READ_IDS_PATH),
        ]
        with patch("argparse._sys.argv", args):
            main.main()

    def test_update_command_runs(self, tmp_path: Path) -> None:
        """Assert that typical commands are valid"""

        args = [
            "pod5",
            "update",
            str(POD5_PATH),
            "--output",
            str(tmp_path),
        ]
        with patch("argparse._sys.argv", args):
            main.main()

    def test_view_command_runs(self) -> None:
        """Assert that typical commands are valid"""

        args = [
            "pod5",
            "view",
            str(POD5_PATH),
        ]
        with patch("argparse._sys.argv", args):
            main.main()

    def test_view_command_runs_list_fields(self) -> None:
        """Assert that typical commands are valid"""

        args = [
            "pod5",
            "view",
            "--list-fields",
        ]
        with patch("argparse._sys.argv", args):
            main.main()

    @pytest.mark.skipif(
        sys.platform.startswith("win") and sys.version_info < (3, 8),
        reason="windows py3.7 pathlib concatenation issue",
    )
    @pytest.mark.parametrize(
        "script", list((Path(__file__).parent.parent / "pod5/tools").glob("pod5*.py"))
    )
    def test_scripts_run_directly(self, script: Path) -> None:
        """pod5 tools should run if executed directly as scripts"""
        python_exe = Path(sys.executable)
        subprocess.check_call([python_exe, script.absolute(), "--help"])


class TestUtils:
    def test_collect_inputs(self, tmp_path: Path) -> None:
        expected = [
            tmp_path / "a.pod5",
            tmp_path / "longer-name.pod5",
            tmp_path / "sub/a.pod5",
            tmp_path / "sub/sub2/xx.pod5",
        ]
        not_expected = [
            tmp_path / ".pod5",  # Exclude hidden files
            tmp_path / "other.txt",
            tmp_path / "pod5.p5",
            tmp_path / "a.pod5.p5",
            tmp_path / "sub/other.png",
            tmp_path / "sub/sub3/bad.pods",
        ]

        for path in expected + not_expected:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()

        assert all([path.exists() for path in expected + not_expected])

        # Expect all pod5s recursively
        recurse = collect_inputs([tmp_path], recursive=True, pattern="*.pod5")
        assert set(expected) == set(tmp_path.rglob("*[a-z0-9].pod5"))
        assert recurse == set(expected)
        assert set(not_expected).isdisjoint(recurse)

        # Expect all pod5s in top level
        top = collect_inputs([tmp_path], recursive=False, pattern="*.pod5")
        assert set(tmp_path.glob("*[a-z0-9].pod5")) == top
        assert set(top).isdisjoint(not_expected)

        # Files aren't duplicated in similar patterns
        dupl = collect_inputs([tmp_path], recursive=False, pattern=["*.pod5", "*d5"])
        assert dupl == top

        # Expect no matches
        assert not collect_inputs([tmp_path], recursive=True, pattern="*.none")

        # Expect file_pattern to find other than *.pod5
        p5_suffix = collect_inputs([tmp_path], recursive=True, pattern="*.p5")
        assert p5_suffix
        assert p5_suffix == set(path for path in not_expected if path.suffix == ".p5")

        expect_mixed = set([tmp_path / "other.txt", tmp_path / "sub/other.png"])
        assert expect_mixed == collect_inputs(
            [tmp_path], recursive=True, pattern=["*.txt", "*.png"]
        )

    def test_collect_inputs_non_existent(self, tmp_path: Path) -> None:
        """Test FileExistsError raised if input doesn't exist"""
        with pytest.raises(FileExistsError, match="inputs do not exist"):
            collect_inputs([tmp_path / "non_existent.txt"], False, "*.txt")

    @pytest.mark.skipif(os.cpu_count() is None, reason="os.cpu_count is None")
    def test_limit_threads(self) -> None:
        """Test thread limiting"""
        cpus = os.cpu_count()
        if cpus is None:
            assert False
        limit_threads(-1) == cpus
        limit_threads(0) == cpus
        for i in range(1, cpus + 1):
            assert limit_threads(i) == i
        assert limit_threads(cpus + 1) == cpus
        limit_threads(1_000_000) == cpus
