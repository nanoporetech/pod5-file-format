"""
Pod5_format test fixtures
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator
from uuid import UUID, uuid4, uuid5

import os
import pytest

import numpy
import numpy.typing
import pod5_format as p5

TEST_UUID = uuid4()

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent / "test_data"
POD5_COMBINED_PATH = TEST_DATA_PATH / "multi_fast5_zip.pod5"


@pytest.fixture(scope="function")
def combined_reader() -> Generator[p5.CombinedReader, None, None]:
    """Create a CombinedReader from a combined file"""
    with p5.CombinedReader(combined_path=POD5_COMBINED_PATH) as reader:
        yield reader

    assert reader._handles._file_reader is None
    assert reader._handles._signal_reader is None
    assert reader._handles._read_reader is None

    try:
        os.rename(POD5_COMBINED_PATH, POD5_COMBINED_PATH.with_suffix(".TEMP"))
        os.rename(POD5_COMBINED_PATH.with_suffix(".TEMP"), POD5_COMBINED_PATH)
    except OSError:
        assert False, "File handle still open"


@pytest.fixture(scope="function")
def combined_writer(tmp_path: Path) -> Generator[p5.Writer, None, None]:
    """Create a Pod5Writer to a combined file in a temporary directory"""
    test_pod5 = tmp_path / "test_combined.pod5"
    with p5.Writer.open_combined(test_pod5) as writer:
        yield writer

    assert writer._writer is None

    try:
        os.rename(test_pod5, test_pod5.with_suffix(".TEMP"))
        os.rename(test_pod5.with_suffix(".TEMP"), test_pod5)
    except OSError:
        assert False, "File handle still open"


def rand_float() -> float:
    """Return a random float in the half-open interval [0, 1)"""
    return float(numpy.random.rand(1)[0])


def rand_int(low: int, high: int) -> int:
    """Returns a random integer in the half-open interval [low, high)"""
    return int(numpy.random.randint(low, high, 1)[0])


def rand_str(prefix: str) -> str:
    """Create a random string by appending random integer to prefix"""
    return f"{prefix}_{numpy.random.randint(1)}"


def _random_read_id(seed: int = 1) -> UUID:
    """Create a random read_id UUID"""
    numpy.random.seed(seed)
    return uuid5(TEST_UUID, str(seed))


@pytest.fixture(scope="function")
def random_read_id(request) -> UUID:
    """Create a random read_id UUID"""
    return _random_read_id(request.param)


def _random_pore(seed: int = 1) -> p5.Pore:
    """Create a random Pore object"""
    numpy.random.seed(seed)
    return p5.Pore(rand_int(0, 3000), rand_int(0, 4), rand_str("pore_type"))


@pytest.fixture(scope="function")
def random_pore(request) -> p5.Pore:
    """Create a random Pore object"""
    return _random_pore(request.param)


def _random_calibration(seed: int = 1) -> p5.Calibration:
    """Create a random Calibration object"""
    numpy.random.seed(seed)
    return p5.Calibration(rand_float(), rand_float())


@pytest.fixture(scope="function")
def random_calibration(request) -> p5.Calibration:
    """Create a random Calibration object"""
    return _random_calibration(request.param)


def _random_end_reason(seed: int = 1) -> p5.EndReason:
    """Create a random EndReason object"""
    numpy.random.seed(seed)
    return p5.EndReason(p5.EndReasonEnum(rand_int(0, 5)), rand_int(0, 1))


@pytest.fixture(scope="function")
def random_end_reason(request) -> p5.EndReason:
    """Create a random EndReason object"""
    return _random_end_reason(request.param)


def _random_run_info(seed: int = 1) -> p5.RunInfo:
    """Create a random RunInfo object"""
    numpy.random.seed(seed)
    return p5.RunInfo(
        acquisition_id=rand_str("acq_id"),
        acquisition_start_time=datetime.fromtimestamp(rand_int(0, 1), timezone.utc),
        adc_max=rand_int(0, 1000),
        adc_min=rand_int(-1000, 0),
        context_tags=[(rand_str("context"), rand_str("tag"))],
        experiment_name=rand_str("exp_name"),
        flow_cell_id=rand_str("flow_cell"),
        flow_cell_product_code=rand_str("product_code"),
        protocol_name=rand_str("protocol"),
        protocol_run_id=rand_str("protocol_run_id"),
        protocol_start_time=datetime.fromtimestamp(rand_int(0, 1), timezone.utc),
        sample_id=rand_str("sample_id"),
        sample_rate=rand_int(0, 10000),
        sequencing_kit=rand_str("seq_kit"),
        sequencer_position=rand_str("position"),
        sequencer_position_type=rand_str("position_type"),
        software=rand_str("software"),
        system_name=rand_str("system_name"),
        system_type=rand_str("system_type"),
        tracking_id=[(rand_str("tracking"), rand_str("id"))],
    )


@pytest.fixture(scope="function")
def random_run_info(request) -> p5.RunInfo:
    """Create a random RunInfo object"""
    return _random_run_info(request.param)


def _random_signal(seed: int = 1) -> numpy.typing.NDArray[numpy.int16]:
    """Generate a random signal"""
    numpy.random.seed(seed)
    size = rand_int(0, 1000)
    return numpy.random.randint(0, 1024, size, dtype=numpy.int16)


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
        read_number=rand_int(0, 100000),
        start_time=rand_int(0, 10000000),
        median_before=rand_float(),
        end_reason=_random_end_reason(seed),
        run_info=_random_run_info(seed),
        signal=signal,
        samples_count=signal.shape[0],
    )


@pytest.fixture(scope="function")
def random_read(request) -> p5.Read:
    """Generate a Read with random data"""
    return _random_read(request.param)


def _random_read_pre_compressed(seed: int = 1) -> p5.Read:
    """Generate a Read with random data"""
    signal = _random_signal(seed)
    return p5.Read(
        read_id=_random_read_id(seed),
        pore=_random_pore(seed),
        calibration=_random_calibration(seed),
        read_number=rand_int(0, 100000),
        start_time=rand_int(0, 10000000),
        median_before=rand_float(),
        end_reason=_random_end_reason(seed),
        run_info=_random_run_info(seed),
        signal=p5.vbz_compress_signal(signal),
        samples_count=signal.shape[0],
    )


@pytest.fixture(scope="function")
def random_read_pre_compressed(request) -> p5.Read:
    """Generate a Read with random data"""
    return _random_read_pre_compressed(request.param)
