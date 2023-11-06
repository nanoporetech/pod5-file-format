from pathlib import Path
import random
from uuid import uuid4
import numpy as np

import pod5 as p5
from pod5.repack import Repacker
from pod5.tools.pod5_repack import repack_pod5
from tests.conftest import skip_if_windows
import pytest


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"


class TestRepack:
    """Test that pod5 repack runs"""

    def test_works(self, tmp_path: Path) -> None:
        data = tmp_path / "subdir/test.pod5"
        data.parent.mkdir(exist_ok=False, parents=True)
        data.write_bytes(POD5_PATH.read_bytes())

        output = tmp_path / "output"
        assert not (output / "test.pod5").exists()
        repack_pod5(
            [tmp_path], output, threads=2, force_overwrite=False, recursive=True
        )
        assert output.is_dir()
        assert (output / "test.pod5").is_file()

        with p5.Reader(output / "test.pod5") as dest:
            with p5.Reader(data) as source:
                for d_read, s_read in zip(dest, source):
                    assert d_read.read_id == s_read.read_id
                    assert d_read.pore == s_read.pore
                    assert d_read.calibration == s_read.calibration
                    assert np.array_equal(d_read.signal, s_read.signal)

    def test_detect_name_collision(self, tmp_path: Path) -> None:
        data = tmp_path / "subdir/test.pod5"
        data.parent.mkdir(exist_ok=False, parents=True)
        data.write_bytes(POD5_PATH.read_bytes())

        similar_name = tmp_path / "test.pod5"
        similar_name.touch()

        output = tmp_path / "output"
        with pytest.raises(ValueError, match="same filename"):
            repack_pod5(
                [tmp_path], output, threads=2, force_overwrite=False, recursive=True
            )

    @skip_if_windows
    def test_overwrite_symlink(self, tmp_path: Path) -> None:
        symlink = tmp_path / "subdir/test.pod5"
        symlink.parent.mkdir(exist_ok=False, parents=True)
        symlink.symlink_to(POD5_PATH)

        dest = tmp_path / "test.pod5"
        dest.touch()

        with pytest.raises(FileExistsError, match="Refusing to overwrite"):
            repack_pod5(
                [tmp_path / "subdir"],
                tmp_path,
                threads=2,
                force_overwrite=False,
                recursive=True,
            )

        repack_pod5(
            [tmp_path / "subdir"],
            tmp_path,
            threads=2,
            force_overwrite=True,
            recursive=True,
        )

        assert dest.is_file()
        with p5.Reader(dest) as reader:
            assert reader.read_ids

    def test_overwrite_data(self, tmp_path: Path) -> None:
        data = tmp_path / "subdir/test.pod5"
        data.parent.mkdir(exist_ok=False, parents=True)
        data.write_bytes(POD5_PATH.read_bytes())

        dest = tmp_path / "test.pod5"
        dest.touch()

        repack_pod5(
            [tmp_path / "subdir"],
            tmp_path,
            threads=2,
            force_overwrite=True,
            recursive=True,
        )

        assert dest.is_file()
        with p5.Reader(dest) as reader:
            assert reader.read_ids


class TestRepacker:
    def test_add_all(self, tmp_path: Path, pod5_factory) -> None:
        path = pod5_factory(10)

        dest = tmp_path / "dest.pod5"
        repacker = Repacker()
        with p5.Writer(dest) as writer:
            output = repacker.add_output(writer, check_duplicate_read_ids=True)

            assert repacker.reads_requested == 0
            assert repacker.reads_completed == 0
            assert (
                not repacker.is_complete
            )  # Output not marked finished, so not complete

            with p5.Reader(path) as reader:
                repacker.add_all_reads_to_output(output, reader)
            repacker.set_output_finished(output)
            repacker.finish()

            assert repacker.reads_requested == 10
            assert repacker.reads_completed == 10
            assert repacker.is_complete

    def test_add_selection(self, tmp_path: Path, pod5_factory) -> None:
        path = pod5_factory(1100)

        dest = tmp_path / "dest.pod5"
        repacker = Repacker()
        with p5.Writer(dest) as writer:
            output = repacker.add_output(writer)

            with p5.Reader(path) as reader:
                selection = set(random.sample(reader.read_ids, 50))
                repacker.add_selected_reads_to_output(output, reader, selection)

            repacker.set_output_finished(output)
            repacker.finish()

            assert repacker.reads_requested == len(selection)
            assert repacker.reads_completed == len(selection)
            assert repacker.is_complete

        with p5.Reader(dest) as confirm:
            assert set(confirm.read_ids) == set(selection)

        repacker.finish()

    def test_missing_selection(self, tmp_path: Path, pod5_factory) -> None:
        path = pod5_factory(10)

        dest = tmp_path / "dest.pod5"
        repacker = Repacker()
        with p5.Writer(dest) as writer:
            output = repacker.add_output(writer)

            with p5.Reader(path) as reader:
                selection = set([str(uuid4())])
                with pytest.raises(RuntimeError, match="Failed to find"):
                    repacker.add_selected_reads_to_output(output, reader, selection)

            repacker.set_output_finished(output)
            repacker.finish()
