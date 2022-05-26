from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from uuid import uuid4, uuid5, UUID

import numpy
import pytest

import pod5_format
import pod5_format.signal_tools

TEST_UUID = uuid4()

ReadData = namedtuple(
    "ReadData",
    [
        "read_id",
        "pore",
        "calibration",
        "read_number",
        "start_sample",
        "median_before",
        "end_reason",
        "run_info",
        "signal",
    ],
)


def gen_test_read(seed) -> ReadData:
    numpy.random.seed(seed)

    def get_random_float():
        return float(numpy.random.rand(1)[0])

    def get_random_int(low, high):
        return int(numpy.random.randint(low, high, 1)[0])

    def get_random_str(prefix):
        return f"{prefix}_{numpy.random.randint(1)}"

    size = get_random_int(0, 1000)
    signal = numpy.random.randint(0, 1024, size, dtype=numpy.int16)

    return ReadData(
        uuid5(TEST_UUID, str(seed)).bytes,
        pod5_format.PoreData(
            get_random_int(0, 3000), get_random_int(0, 4), get_random_str("pore_type_")
        ),
        pod5_format.CalibrationData(get_random_float(), get_random_float()),
        get_random_int(0, 100000),
        get_random_int(0, 10000000),
        get_random_float(),
        pod5_format.EndReasonData(
            pod5_format.EndReason(get_random_int(0, 5)), get_random_int(0, 1)
        ),
        pod5_format.RunInfoData(
            get_random_str("acq_id"),
            datetime.fromtimestamp(get_random_int(0, 1), timezone.utc),
            get_random_int(0, 1000),
            get_random_int(-1000, 0),
            [
                (get_random_str("context"), get_random_str("tag")),
            ],
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
            [
                (get_random_str("tracking"), get_random_str("id")),
            ],
        ),
        signal,
    )


def run_writer_test(f):
    test_read = gen_test_read(0)
    f.add_read(
        test_read.read_id,
        f.find_pore(**test_read.pore._asdict())[0],
        f.find_calibration(**test_read.calibration._asdict())[0],
        test_read.read_number,
        test_read.start_sample,
        test_read.median_before,
        f.find_end_reason(**test_read.end_reason._asdict())[0],
        f.find_run_info(**test_read.run_info._asdict())[0],
        test_read.signal,
        test_read.signal.shape[0],
        pre_compressed_signal=False,
    )

    test_read = gen_test_read(1)
    f.add_read(
        test_read.read_id,
        f.find_pore(**test_read.pore._asdict())[0],
        f.find_calibration(**test_read.calibration._asdict())[0],
        test_read.read_number,
        test_read.start_sample,
        test_read.median_before,
        f.find_end_reason(**test_read.end_reason._asdict())[0],
        f.find_run_info(**test_read.run_info._asdict())[0],
        [pod5_format.signal_tools.vbz_compress_signal(test_read.signal)],
        [test_read.signal.shape[0]],
        pre_compressed_signal=True,
    )

    test_reads = [
        gen_test_read(2),
        gen_test_read(3),
        gen_test_read(4),
        gen_test_read(5),
    ]
    f.add_reads(
        numpy.array(
            [numpy.frombuffer(r.read_id, dtype=numpy.uint8) for r in test_reads]
        ),
        numpy.array(
            [f.find_pore(**r.pore._asdict())[0] for r in test_reads], dtype=numpy.int16
        ),
        numpy.array(
            [f.find_calibration(**r.calibration._asdict())[0] for r in test_reads],
            dtype=numpy.int16,
        ),
        numpy.array([r.read_number for r in test_reads], dtype=numpy.uint32),
        numpy.array([r.start_sample for r in test_reads], dtype=numpy.uint64),
        numpy.array([r.median_before for r in test_reads], dtype=numpy.float32),
        numpy.array(
            [f.find_end_reason(**r.end_reason._asdict())[0] for r in test_reads],
            dtype=numpy.int16,
        ),
        numpy.array(
            [f.find_run_info(**r.run_info._asdict())[0] for r in test_reads],
            dtype=numpy.int16,
        ),
        [r.signal for r in test_reads],
        numpy.array([len(r.signal) for r in test_reads], dtype=numpy.uint64),
        pre_compressed_signal=False,
    )

    test_reads = [
        gen_test_read(6),
        gen_test_read(7),
        gen_test_read(8),
        gen_test_read(9),
    ]
    f.add_reads(
        numpy.array(
            [numpy.frombuffer(r.read_id, dtype=numpy.uint8) for r in test_reads]
        ),
        numpy.array(
            [f.find_pore(**r.pore._asdict())[0] for r in test_reads], dtype=numpy.int16
        ),
        numpy.array(
            [f.find_calibration(**r.calibration._asdict())[0] for r in test_reads],
            dtype=numpy.int16,
        ),
        numpy.array([r.read_number for r in test_reads], dtype=numpy.uint32),
        numpy.array([r.start_sample for r in test_reads], dtype=numpy.uint64),
        numpy.array([r.median_before for r in test_reads], dtype=numpy.float32),
        numpy.array(
            [f.find_end_reason(**r.end_reason._asdict())[0] for r in test_reads],
            dtype=numpy.int16,
        ),
        numpy.array(
            [f.find_run_info(**r.run_info._asdict())[0] for r in test_reads],
            dtype=numpy.int16,
        ),
        # Pass an array of arrays here, as we have pre compressed data
        # top level array is per read, then the sub arrays are chunks within the reads.
        # the two arrays here should have the same dimensions, first contains compressed
        # sample array, the second contains the sample counts
        [[pod5_format.signal_tools.vbz_compress_signal(r.signal)] for r in test_reads],
        numpy.array([[len(r.signal)] for r in test_reads], dtype=numpy.uint64),
        pre_compressed_signal=True,
    )


def run_reader_test(r):
    for idx, read in enumerate(r.reads()):
        print(idx)
        data = gen_test_read(idx)

        assert UUID(bytes=data.read_id) == read.read_id
        assert data.read_number == read.read_number
        assert data.start_sample == read.start_sample
        assert pytest.approx(data.median_before) == read.median_before

        assert data.pore == read.pore
        assert pytest.approx(data.calibration.offset) == read.calibration.offset
        assert pytest.approx(data.calibration.scale) == read.calibration.scale
        assert str(data.end_reason.name).split(".")[1].lower() == read.end_reason.name
        assert data.end_reason.forced == read.end_reason.forced

        assert data.run_info == read.run_info

        assert data.signal.shape[0] == read.sample_count
        # Expecting poor compression given the random input
        assert 0 < read.byte_count < (len(data.signal) * data.signal.itemsize + 24)
        assert len(read.signal_rows) >= 1

        assert not read.has_cached_signal
        assert (read.signal == data.signal).all()
        chunk_signals = [read.signal_for_chunk(i) for i in range(len(read.signal_rows))]
        assert (numpy.concatenate(chunk_signals) == data.signal).all()

    # Try to walk through the file in read batches:
    for idx, batch in enumerate(r.read_batches(preload={"samples"})):
        assert len(batch.cached_samples_column) == batch.num_reads

    # Try to walk through specific batches in the file:
    for batch in r.read_batches(batch_selection=[0], preload={"samples"}):
        print(idx)
        assert len(batch.cached_samples_column) == batch.num_reads
        for idx, read in enumerate(batch.reads()):
            data = gen_test_read(idx)
            assert read.has_cached_signal
            assert (read.signal == data.signal).all()

    # Try to walk through all reads in the file:
    for idx, read in enumerate(r.reads(preload={"samples"})):
        print(idx)
        data = gen_test_read(idx)

        assert read.has_cached_signal
        assert (read.signal == data.signal).all()

    reads = list(r.reads())
    search_reads = [
        reads[6],
        reads[3],
        reads[1],
    ]

    search = r.reads(
        [r.read_id for r in search_reads],
    )
    found_ids = set()
    for i, searched_read in enumerate(search):
        found_ids.add(searched_read.read_id)
    assert found_ids == set(r.read_id for r in search_reads)


def test_pyarrow_combined():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "combined.pod5"
        with pod5_format.create_combined_file(path) as f:
            run_writer_test(f)

        with pod5_format.open_combined_file(path, use_c_api=False) as r:
            run_reader_test(r)


def test_pyarrow_split():
    with tempfile.TemporaryDirectory() as td:
        signal = Path(td) / "split_signal.pod5"
        reads = Path(td) / "split_reads.pod5"
        with pod5_format.create_split_file(signal, reads) as f:
            run_writer_test(f)

        with pod5_format.open_split_file(signal, reads, use_c_api=False) as r:
            run_reader_test(r)


def test_pyarrow_split_one_name():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "split.pod5"
        with pod5_format.create_split_file(p) as f:
            run_writer_test(f)

        with pod5_format.open_split_file(p, use_c_api=False) as r:
            run_reader_test(r)
