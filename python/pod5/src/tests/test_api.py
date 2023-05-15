import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Union
from uuid import UUID, uuid4, uuid5

import numpy as np
import pytest

import pod5 as p5
from pod5.api_utils import format_read_ids, pack_read_ids
from pod5.writer import Writer

TEST_UUID = uuid4()


def gen_test_read(seed, compressed=False) -> Union[p5.Read, p5.CompressedRead]:
    np.random.seed(seed)

    def get_random_float() -> float:
        return float(np.random.rand(100000)[0])

    def get_random_int(low: int, high: int) -> int:
        return int(np.random.randint(low, high, 1)[0])

    def get_random_str(prefix: str) -> str:
        return f"{prefix}_{np.random.randint(100000)}"

    size = get_random_int(0, 1000)
    signal = np.random.randint(0, 1024, size, dtype=np.int16)

    cls = p5.Read  # type: ignore
    signal_args = {"signal": signal}  # type: ignore

    if compressed:
        cls = p5.CompressedRead  # type: ignore
        signal_args = {
            "signal_chunks": [p5.signal_tools.vbz_compress_signal(signal)],  # type: ignore
            "signal_chunk_lengths": [len(signal)],  # type: ignore
        }

    return cls(
        uuid5(TEST_UUID, str(seed)),
        p5.Pore(
            get_random_int(0, 3000),
            get_random_int(0, 4),
            get_random_str("pore_type"),
        ),
        p5.Calibration(get_random_float(), get_random_float()),
        get_random_int(0, 100000),
        get_random_int(0, 10000000),
        get_random_float(),
        p5.EndReason(
            p5.EndReasonEnum(get_random_int(0, 5)), bool(get_random_int(0, 1))
        ),
        p5.RunInfo(
            get_random_str("acq_id"),
            datetime.fromtimestamp(get_random_int(0, 1), timezone.utc),
            get_random_int(0, 1000),
            get_random_int(-1000, 0),
            {get_random_str("context"): get_random_str("tag")},
            get_random_str("exp_name"),
            get_random_str("flow_cell"),
            get_random_str("product_code"),
            get_random_str("protocol"),
            get_random_str("protocol_run_id"),
            datetime.fromtimestamp(get_random_int(0, 1), timezone.utc),
            get_random_str("sample_id"),
            get_random_int(0, 10000),
            get_random_str("seq_kit"),
            get_random_str("position"),
            get_random_str("position_type"),
            get_random_str("software"),
            get_random_str("system_name"),
            get_random_str("system_type"),
            {get_random_str("tracking"): get_random_str("id")},
        ),
        num_minknow_events=5,
        tracked_scaling=p5.pod5_types.ShiftScalePair(10.0, 50),
        predicted_scaling=p5.pod5_types.ShiftScalePair(5.0, 100.0),
        num_reads_since_mux_change=123,
        time_since_mux_change=456.0,
        **signal_args,
    )


def run_writer_test(f: Writer):
    test_read = gen_test_read(0, compressed=False)
    print("read", test_read.read_id, test_read.run_info.adc_max)
    f.add_read(test_read)

    test_read = gen_test_read(1, compressed=True)
    print("read", test_read.read_id, test_read.run_info.adc_max)
    f.add_read(test_read)

    test_reads = [
        gen_test_read(2),
        gen_test_read(3),
        gen_test_read(4),
        gen_test_read(5),
    ]
    print("read", test_reads[0].read_id, test_reads[0].run_info.adc_max)
    f.add_reads(test_reads)

    test_reads = [
        gen_test_read(6, compressed=True),
        gen_test_read(7, compressed=True),
        gen_test_read(8, compressed=True),
        gen_test_read(9, compressed=True),
    ]
    f.add_reads(test_reads)
    assert test_reads[0].sample_count > 0


def run_reader_test(reader: p5.Reader):
    # Check top level file metadata

    assert reader.writing_software == "Python API"
    assert reader.file_identifier != UUID(int=0)

    read_count = 0
    read_id_strs = set()
    for idx, read in enumerate(reader.reads()):
        read_count += 1
        data = gen_test_read(idx)

        read_id_strs.add(str(read.read_id))

        assert isinstance(data, p5.Read)

        assert data.read_id == read.read_id
        assert data.read_number == read.read_number
        assert data.start_sample == read.start_sample
        assert pytest.approx(data.median_before) == read.median_before

        assert data.pore == read.pore
        assert pytest.approx(data.calibration.offset) == read.calibration.offset
        assert pytest.approx(data.calibration.scale) == read.calibration.scale
        assert data.run_info == read.run_info
        assert (
            data.run_info.adc_max - data.run_info.adc_min + 1
            == read.calibration_digitisation
        )
        assert (
            pytest.approx(
                data.calibration.scale
                * (data.run_info.adc_max - data.run_info.adc_min + 1)
            )
            == read.calibration_range
        )
        assert data.end_reason.name == read.end_reason.name
        assert data.end_reason.forced == read.end_reason.forced

        assert data.num_minknow_events == read.num_minknow_events
        assert data.tracked_scaling == read.tracked_scaling
        assert data.predicted_scaling == read.predicted_scaling
        assert data.num_reads_since_mux_change == read.num_reads_since_mux_change
        assert data.time_since_mux_change == read.time_since_mux_change

        assert data.sample_count == read.sample_count
        # Expecting poor compression given the random input
        assert 0 < read.byte_count < (len(data.signal) * data.signal.itemsize + 24)
        assert len(read.signal_rows) >= 1

        assert not read.has_cached_signal
        assert (read.signal == data.signal).all()
        assert (
            pytest.approx(read.signal_pa)
            == (data.signal + data.calibration.offset) * data.calibration.scale
        )
        chunk_signals = [read.signal_for_chunk(i) for i in range(len(read.signal_rows))]
        assert (np.concatenate(chunk_signals) == data.signal).all()
        assert isinstance(read.end_reason_index, int)
        assert read.end_reason_index == read.end_reason.reason.value
        assert isinstance(read.run_info_index, int)

    assert reader.num_reads == read_count
    assert set(reader.read_ids) == read_id_strs

    # Try to walk through the file in read batches:
    for idx, batch in enumerate(reader.read_batches(preload={"samples"})):
        assert len(batch.cached_samples_column) == batch.num_reads

    # Try to walk through specific batches in the file:
    for batch in reader.read_batches(batch_selection=[0], preload={"samples"}):
        assert len(batch.cached_samples_column) == batch.num_reads
        assert len(batch.cached_sample_count_column) == batch.num_reads
        for idx, read in enumerate(batch.reads()):
            data = gen_test_read(idx)
            assert isinstance(data, p5.Read)
            assert read.has_cached_signal
            assert (read.signal == data.signal).all()

    # Try to walk through all reads in the file:
    for idx, read in enumerate(reader.reads(preload={"samples"})):
        data = gen_test_read(idx)

        assert isinstance(data, p5.Read)
        assert read.has_cached_signal
        assert (read.signal == data.signal).all()

    # Try to walk through some reads in the file with a bad read id, not ignoring bad ids
    with pytest.raises(RuntimeError):
        for idx, read in enumerate(reader.reads(["bad-id"], missing_ok=False)):
            # Shouldn't hit this!
            assert False

    # Try to walk through some reads in the file with a bad read id, ignoring bad ids
    for idx, read in enumerate(reader.reads(["bad-id"], missing_ok=True)):
        # Shouldn't hit this!
        assert False

    reads = list(reader.reads())
    search_reads = [
        reads[6],
        reads[3],
        reads[1],
    ]

    search = reader.reads([str(r.read_id) for r in search_reads])

    found_ids = set()
    for i, searched_read in enumerate(search):
        found_ids.add(searched_read.read_id)
    assert found_ids == set(r.read_id for r in search_reads)


@pytest.mark.filterwarnings("ignore: pod5.")
def test_pyarrow_from_pathlib():
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / "example.pod5"
        with p5.Writer(path) as _fh:
            run_writer_test(_fh)

        with p5.Reader(path) as _fh:
            run_reader_test(_fh)


@pytest.mark.filterwarnings("ignore: pod5.")
def test_pyarrow_from_str():
    with tempfile.TemporaryDirectory() as temp:
        path = str(Path(temp) / "example.pod5")
        with p5.Writer(path) as _fh:
            run_writer_test(_fh)

        with p5.Reader(path) as _fh:
            run_reader_test(_fh)


def test_read_id_packing():
    """
    Assert pack_read_ids repacks and format_read_ids correctly unpacks collections
    of read ids
    """
    rids = [str(uuid4()) for _ in range(10)]
    packed_rids = pack_read_ids(rids)

    assert len(rids) == 10
    assert isinstance(packed_rids, np.ndarray)
    assert packed_rids.dtype == np.uint8

    unpacked_rids = format_read_ids(packed_rids)
    assert isinstance(unpacked_rids, list)

    for rid, unpacked in zip(rids, unpacked_rids):
        assert type(rid) == type(unpacked)
        assert rid == unpacked
