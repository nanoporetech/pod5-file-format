"""
Pod5 test fixtures
"""
from contextlib import contextmanager
import os
from datetime import datetime, timezone
from pathlib import Path
import psutil
import shutil
import sys
from typing import Generator, Optional, Set
from uuid import UUID, uuid4, uuid5

import numpy
import numpy.typing
from pod5.pod5_types import ShiftScalePair
import pytest
import pod5 as p5

TEST_UUID = uuid4()

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"
POD5_PATH = TEST_DATA_PATH / "multi_fast5_zip_v3.pod5"

POD5_TEST_SEED = int(os.getenv("POD5_TEST_SEED", numpy.random.randint(1, 9999)))


skip_if_windows = pytest.mark.skipif(
    sys.platform.startswith("win"), reason="no symlink privilege on windows CI"
)


# Run pytest from the tests directory (containing conftest.py) to use this argument
def pytest_addoption(parser):
    """Add configurable random seed for testing"""
    parser.addoption(
        "--pod5-test-seed",
        type=int,
        default=numpy.random.randint(1, 9999),
        help="pod5_factory test seed",
    )


@contextmanager
def assert_no_leaked_handles() -> Generator[None, None, None]:
    proc = psutil.Process()
    before = set(proc.open_files())
    yield
    after = set(proc.open_files())
    leaked_handles = after - before
    leaked_handles = set(h for h in leaked_handles if ".log" not in str(h.path).lower())
    if leaked_handles:
        raise AssertionError(f"Leaked handles: {leaked_handles}")


def assert_no_leaked_handles_win(path: Path) -> None:
    """Attempt to rename the file at `path` this shows up leaked handles on windows"""
    if sys.platform.lower().startswith("win"):
        try:
            os.rename(POD5_PATH, POD5_PATH.with_suffix(".TEMP"))
            os.rename(POD5_PATH.with_suffix(".TEMP"), POD5_PATH)
        except OSError:
            raise AssertionError(f"File handle to {path} still open")


@pytest.fixture(scope="function")
def reader() -> Generator[p5.Reader, None, None]:
    """Create a Reader from a pod5 file"""
    with assert_no_leaked_handles():
        with p5.Reader(path=POD5_PATH) as reader:
            yield reader
    assert_no_leaked_handles_win(POD5_PATH)


@pytest.fixture(scope="function")
def writer(tmp_path: Path) -> Generator[p5.Writer, None, None]:
    """Create a Pod5Writer to a file in a temporary directory"""
    test_pod5 = tmp_path / "test.pod5"
    with p5.Writer(test_pod5) as writer:
        yield writer

    try:
        os.rename(test_pod5, test_pod5.with_suffix(".TEMP"))
        os.rename(test_pod5.with_suffix(".TEMP"), test_pod5)
    except OSError:
        assert False, "File handle still open"


def rand_float(seed: int) -> float:
    """Return a random float in the half-open interval [0, 1)"""
    numpy.random.seed(seed)
    return float(numpy.random.rand(1)[0])


def rand_int(low: int, high: int, seed: int) -> int:
    """Returns a random integer in the half-open interval [low, high)"""
    numpy.random.seed(seed)
    return int(numpy.random.randint(low, high))


def rand_str(prefix: str, seed: int) -> str:
    """Create a random string by appending random integer to prefix"""
    numpy.random.seed(seed)
    return f"{prefix}_{numpy.random.randint(1, 9999999)}"


def _random_read_id(seed: int = 1) -> UUID:
    """Create a random read_id UUID"""
    return uuid5(TEST_UUID, str(seed))


@pytest.fixture(scope="function")
def random_read_id(request) -> UUID:
    """Create a random read_id UUID"""
    return _random_read_id(request.param)


def _random_pore(seed: int) -> p5.Pore:
    """Create a random Pore object"""
    return p5.Pore(
        rand_int(0, 3000, seed), rand_int(0, 4, seed), rand_str("pore_type", seed)
    )


@pytest.fixture(scope="function")
def random_pore(request) -> p5.Pore:
    """Create a random Pore object"""
    return _random_pore(request.param)


def _random_calibration(seed: int = 1) -> p5.Calibration:
    """Create a random Calibration object"""
    return p5.Calibration(rand_float(seed), rand_float(seed + 1))


@pytest.fixture(scope="function")
def random_calibration(request) -> p5.Calibration:
    """Create a random Calibration object"""
    return _random_calibration(request.param)


def _random_end_reason(seed: int = 1) -> p5.EndReason:
    """Create a random EndReason object"""
    return p5.EndReason(
        p5.EndReasonEnum(rand_int(0, 5, seed)), bool(rand_int(0, 1, seed))
    )


@pytest.fixture(scope="function")
def random_end_reason(request) -> p5.EndReason:
    """Create a random EndReason object"""
    return _random_end_reason(request.param)


def _random_run_info(seed: int = 1) -> p5.RunInfo:
    """Create a random RunInfo object"""
    return p5.RunInfo(
        acquisition_id=rand_str("acq_id", seed),
        acquisition_start_time=datetime.fromtimestamp(
            rand_int(0, 1, seed), timezone.utc
        ),
        adc_max=rand_int(0, 1000, seed),
        adc_min=rand_int(-1000, 0, seed),
        context_tags={rand_str("context", seed): rand_str("tag", seed)},
        experiment_name=rand_str("exp_name", seed),
        flow_cell_id=rand_str("flow_cell", seed),
        flow_cell_product_code=rand_str("product_code", seed),
        protocol_name=rand_str("protocol", seed),
        protocol_run_id=rand_str("protocol_run_id", seed),
        protocol_start_time=datetime.fromtimestamp(rand_int(0, 1, seed), timezone.utc),
        sample_id=rand_str("sample_id", seed),
        sample_rate=rand_int(0, 10000, seed),
        sequencing_kit=rand_str("seq_kit", seed),
        sequencer_position=rand_str("position", seed),
        sequencer_position_type=rand_str("position_type", seed),
        software=rand_str("software", seed),
        system_name=rand_str("system_name", seed),
        system_type=rand_str("system_type", seed),
        tracking_id={rand_str("tracking", seed): rand_str("id", seed)},
    )


@pytest.fixture(scope="function")
def random_run_info(request) -> p5.RunInfo:
    """Create a random RunInfo object"""
    return _random_run_info(request.param)


def _random_signal(seed: int = 1) -> numpy.typing.NDArray[numpy.int16]:
    """Generate a random signal"""
    numpy.random.seed(seed)
    size = rand_int(0, 200_000, seed)
    return numpy.random.randint(-32768, 32767, size, dtype=numpy.int16)


@pytest.fixture(scope="function")
def random_signal(request) -> numpy.typing.NDArray[numpy.int16]:
    """Generate a random signal"""
    return _random_signal(request.param)


def _random_read(seed: int = 1) -> p5.Read:
    """Generate a Read with random data"""
    signal = _random_signal(seed)
    return p5.Read(
        read_id=_random_read_id(seed),
        pore=_random_pore(seed),
        calibration=_random_calibration(seed),
        read_number=rand_int(0, 100000, seed),
        start_sample=rand_int(0, 10000000, seed),
        median_before=rand_float(seed),
        end_reason=_random_end_reason(seed),
        run_info=_random_run_info(seed % 4),
        predicted_scaling=ShiftScalePair(rand_float(seed), rand_float(seed + 1)),
        tracked_scaling=ShiftScalePair(rand_float(seed + 2), rand_float(seed + 3)),
        signal=signal,
    )


@pytest.fixture(scope="function")
def random_read(request) -> p5.Read:
    """Generate a Read with random data"""
    return _random_read(request.param)


def _random_read_pre_compressed(seed: int = 1) -> p5.CompressedRead:
    """Generate a Read with random data"""
    signal = _random_signal(seed)
    return p5.CompressedRead(
        read_id=_random_read_id(seed),
        pore=_random_pore(seed),
        calibration=_random_calibration(seed),
        read_number=rand_int(0, 100000, seed),
        start_sample=rand_int(0, 10000000, seed),
        median_before=rand_float(seed),
        end_reason=_random_end_reason(seed),
        run_info=_random_run_info(seed % 4),
        predicted_scaling=ShiftScalePair(rand_float(seed), rand_float(seed + 1)),
        tracked_scaling=ShiftScalePair(rand_float(seed + 2), rand_float(seed + 3)),
        signal_chunks=[p5.vbz_compress_signal(signal)],
        signal_chunk_lengths=[len(signal)],
    )


@pytest.fixture(scope="function")
def random_read_pre_compressed(request) -> p5.CompressedRead:
    """Generate a Read with random data"""
    return _random_read_pre_compressed(request.param)


def _seeder(seed: int) -> Generator[int, None, None]:
    """Generates seed values for numpy.rand.seed"""
    idx = 0
    while True:
        value = (seed + idx) % 2**23
        idx += 13
        yield value


@pytest.fixture(scope="session")
def pod5_factory(request, tmp_path_factory: pytest.TempPathFactory, pytestconfig):
    """
    Create and cache a temporary pod5 file of `n_records` random reads with a
    default name unless given `name_parts` like `subdir/my.pod5`. Files
    are cached under their path and are cleaned.
    """

    POD5_TEST_SEED = pytestconfig.getoption("pod5_test_seed")

    tmp_path = tmp_path_factory.mktemp("pod5_factory")
    existing_pod5s: Set[Path] = set([])

    seeder = _seeder(POD5_TEST_SEED)

    def _pod5_factory(
        n_records: int = 100,
        name: Optional[str] = None,
    ) -> Path:
        """Generate pod5 files with `n_records` with an optionally specified `name`"""
        assert n_records > 0

        if name:
            path = tmp_path / name
        else:
            path = tmp_path / f"pod5_fixture_{n_records}.pod5"

        if path in existing_pod5s:
            if path.is_file():
                return path
            existing_pod5s.remove(path)

        reads = [_random_read(seed=next(seeder)) for _ in range(n_records)]
        with p5.Writer(path=path, software_name="pod5_pytest_fixture") as writer:
            writer.add_reads(reads)

        existing_pod5s.add(path)
        assert path.is_file()
        return path

    yield _pod5_factory

    for path in existing_pod5s:
        path.unlink()

    # Write the test seed to stdout, need to disable capturemanager first
    capmanager = request.config.pluginmanager.getplugin("capturemanager")
    with capmanager.global_and_fixture_disabled():
        print(f"\n\nPOD5_TEST_SEED: {POD5_TEST_SEED}")


@pytest.fixture(scope="session")
def nested_dataset(tmp_path_factory: pytest.TempPathFactory, pod5_factory) -> Path:
    """
    Creates a nested directory structure with temporary pod5 files.

    Symbolic links are only created when not running on windows systems.

    ./root/root_10.pod5
    ./root/subdir/subdir_11.pod5
    ./root/subdir/symbolic_9.pod5 --> ../../outer/symbolic_9.pod5
    ./root/subdir/subsubdir/subsubdir_12.pod5
    ./root/subdir/subsubdir/empty.txt
    ./root/linked/ --> ../linked/

    ./outer/symbolic_9.pod5
    ./linked/linked_8.pod5

    Returns path to root/
    """
    tmp_path = tmp_path_factory.mktemp("pod5_nested_directory")
    root = tmp_path / "root"
    sub_dir = root / "subdir"
    subsub_dir = sub_dir / "subsubdir"
    Path.mkdir(subsub_dir, parents=True)

    root_pod5: Path = pod5_factory(10)
    subdir_pod5: Path = pod5_factory(11)
    subsubdir_pod5: Path = pod5_factory(12)

    shutil.copyfile(str(root_pod5), str(root / "root_10.pod5"))
    shutil.copyfile(str(subdir_pod5), str(sub_dir / "subdir_11.pod5"))
    shutil.copyfile(str(subsubdir_pod5), str(subsub_dir / "subsubdir_12.pod5"))

    (subsub_dir / "empty.txt").touch()

    # Linked file
    outer_dir = tmp_path / "outer"
    Path.mkdir(outer_dir, parents=True)
    symbolic_pod5: Path = pod5_factory(9)
    symb_path = outer_dir / "symbolic_9.pod5"

    if not sys.platform.startswith("win"):
        shutil.copyfile(str(symbolic_pod5), str(symb_path))
        (sub_dir / "symbolic_9.pod5").symlink_to(symb_path)
    else:
        shutil.copyfile(str(symbolic_pod5), str((sub_dir / "symbolic_9.pod5")))

    # Linked directory
    linked_src_dir = tmp_path / "linked"
    Path.mkdir(linked_src_dir, parents=True)
    linked_pod5: Path = pod5_factory(8)
    shutil.copyfile(str(linked_pod5), str(linked_src_dir / "linked_8.pod5"))

    if not sys.platform.startswith("win"):
        (root / "linked").symlink_to(linked_src_dir)
    else:
        (root / "linked").mkdir(parents=True)
        shutil.copyfile(str(linked_pod5), str(root / "linked" / "linked_8.pod5"))

    return root
