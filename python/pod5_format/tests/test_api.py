from datetime import datetime, timezone
from pathlib import Path
import tempfile
from uuid import uuid4, uuid5, UUID

import numpy
from pod5_format.writer import Writer
import pytest

import pod5_format as p5

TEST_UUID = uuid4()


def gen_test_read(seed) -> p5.Read:
    numpy.random.seed(seed)

    def get_random_float() -> float:
        return float(numpy.random.rand(1)[0])

    def get_random_int(low: int, high: int) -> int:
        return int(numpy.random.randint(low, high, 1)[0])

    def get_random_str(prefix: str) -> str:
        return f"{prefix}_{numpy.random.randint(1)}"

    size = get_random_int(0, 1000)
    signal = numpy.random.randint(0, 1024, size, dtype=numpy.int16)

    return p5.Read(
        uuid5(TEST_UUID, str(seed)),
        p5.Pore(
            get_random_int(0, 3000), get_random_int(0, 4), get_random_str("pore_type_")
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
        signal=signal,
        num_minknow_events=5,
        tracked_scaling=p5.pod5_types.ShiftScalePair(10.0, 50),
        predicted_scaling=p5.pod5_types.ShiftScalePair(5.0, 100.0),
        trust_predicted_scaling=p5.pod5_types.ShiftScaleBoolPair(True, False),
    )


def single_read_attributes_as_dict(writer, read_object):
    return {
        "read_id": read_object.read_id,
        "pore": writer.add(read_object.pore),
        "calibration": writer.add(read_object.calibration),
        "read_number": read_object.read_number,
        "start_sample": read_object.start_sample,
        "median_before": read_object.median_before,
        "end_reason": writer.add(read_object.end_reason),
        "run_info": writer.add(read_object.run_info),
        "num_minknow_events": read_object.num_minknow_events,
        "tracked_scaling": read_object.tracked_scaling,
        "predicted_scaling": read_object.predicted_scaling,
        "trust_predicted_scaling": read_object.trust_predicted_scaling,
    }


def read_list_attributes_as_dict(writer, read_objects):
    return {
        "read_ids": numpy.array(
            [numpy.frombuffer(r.read_id.bytes, dtype=numpy.uint8) for r in read_objects]
        ),
        "pores": numpy.array(
            [writer.add(r.pore) for r in read_objects], dtype=numpy.int16
        ),
        "calibrations": numpy.array(
            [writer.add(r.calibration) for r in read_objects],
            dtype=numpy.int16,
        ),
        "read_numbers": numpy.array(
            [r.read_number for r in read_objects], dtype=numpy.uint32
        ),
        "start_samples": numpy.array(
            [r.start_sample for r in read_objects], dtype=numpy.uint64
        ),
        "median_befores": numpy.array(
            [r.median_before for r in read_objects], dtype=numpy.float32
        ),
        "end_reasons": numpy.array(
            [writer.add(r.end_reason) for r in read_objects],
            dtype=numpy.int16,
        ),
        "run_infos": numpy.array(
            [writer.add(r.run_info) for r in read_objects],
            dtype=numpy.int16,
        ),
        "num_minknow_events": numpy.array(
            [r.num_minknow_events for r in read_objects], dtype=numpy.uint64
        ),
        "tracked_scaling_scale": numpy.array(
            [r.tracked_scaling.scale for r in read_objects], dtype=numpy.float32
        ),
        "tracked_scaling_shift": numpy.array(
            [r.tracked_scaling.shift for r in read_objects], dtype=numpy.float32
        ),
        "predicted_scaling_scale": numpy.array(
            [r.predicted_scaling.scale for r in read_objects], dtype=numpy.float32
        ),
        "predicted_scaling_shift": numpy.array(
            [r.predicted_scaling.shift for r in read_objects], dtype=numpy.float32
        ),
        "trust_predicted_scale": numpy.array(
            [r.trust_predicted_scaling.scale for r in read_objects], dtype=numpy.float32
        ),
        "trust_predicted_shift": numpy.array(
            [r.trust_predicted_scaling.shift for r in read_objects], dtype=numpy.float32
        ),
    }


def run_writer_test(f: Writer):
    test_read = gen_test_read(0)
    print("read", test_read.read_id, test_read.run_info.adc_max)
    f.add_read(
        signal=test_read.signal,
        **single_read_attributes_as_dict(f, test_read),
    )

    test_read = gen_test_read(1)
    print("read", test_read.read_id, test_read.run_info.adc_max)
    f.add_read_pre_compressed(
        signal_chunks=[p5.signal_tools.vbz_compress_signal(test_read.signal)],
        signal_chunk_lengths=[test_read.sample_count],
        **single_read_attributes_as_dict(f, test_read),
    )

    test_reads = [
        gen_test_read(2),
        gen_test_read(3),
        gen_test_read(4),
        gen_test_read(5),
    ]
    print("read", test_reads[0].read_id, test_reads[0].run_info.adc_max)
    f.add_reads(
        **read_list_attributes_as_dict(f, test_reads),
        signals=[r.signal for r in test_reads],
    )

    test_reads = [
        gen_test_read(6),
        gen_test_read(7),
        gen_test_read(8),
        gen_test_read(9),
    ]
    f.add_reads_pre_compressed(
        **read_list_attributes_as_dict(f, test_reads),
        # Pass an array of arrays here, as we have pre compressed data
        # top level array is per read, then the sub arrays are chunks within the reads.
        # the two arrays here should have the same dimensions, first contains compressed
        # sample array, the second contains the sample counts
        signal_chunks=[
            [p5.signal_tools.vbz_compress_signal(r.signal)] for r in test_reads
        ],
        signal_chunk_lengths=[[len(r.signal)] for r in test_reads],
    )


def run_reader_test(reader: p5.Reader):
    for idx, read in enumerate(reader.reads()):
        data = gen_test_read(idx)

        assert data.read_id == read.read_id
        assert data.read_number == read.read_number
        assert data.start_sample == read.start_sample
        assert pytest.approx(data.median_before) == read.median_before

        assert data.pore == read.pore
        assert pytest.approx(data.calibration.offset) == read.calibration.offset
        assert pytest.approx(data.calibration.scale) == read.calibration.scale
        print(data.read_id)
        print("In", data.run_info)
        print("Out", read.run_info)
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
        assert str(data.end_reason.name).split(".")[1].lower() == read.end_reason.name
        assert data.end_reason.forced == read.end_reason.forced

        assert data.num_minknow_events == read.num_minknow_events
        assert data.tracked_scaling == read.tracked_scaling
        assert data.predicted_scaling == read.predicted_scaling
        assert data.trust_predicted_scaling == read.trust_predicted_scaling

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
        assert (numpy.concatenate(chunk_signals) == data.signal).all()

    # Try to walk through the file in read batches:
    for idx, batch in enumerate(reader.read_batches(preload={"samples"})):
        assert len(batch.cached_samples_column) == batch.num_reads

    # Try to walk through specific batches in the file:
    for batch in reader.read_batches(batch_selection=[0], preload={"samples"}):
        assert len(batch.cached_samples_column) == batch.num_reads
        assert len(batch.cached_sample_count_column) == batch.num_reads
        for idx, read in enumerate(batch.reads()):
            data = gen_test_read(idx)
            assert read.has_cached_signal
            assert (read.signal == data.signal).all()

    # Try to walk through all reads in the file:
    for idx, read in enumerate(reader.reads(preload={"samples"})):
        data = gen_test_read(idx)

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

    search = reader.reads(
        [r.read_id for r in search_reads],
    )
    found_ids = set()
    for i, searched_read in enumerate(search):
        found_ids.add(searched_read.read_id)
    assert found_ids == set(r.read_id for r in search_reads)


@pytest.mark.filterwarnings("ignore: pod5_format.")
def test_pyarrow_combined():
    with tempfile.TemporaryDirectory() as temp:
        path = Path(temp) / "combined.pod5"
        with p5.Writer.open_combined(path) as _fh:
            run_writer_test(_fh)

        with p5.Reader.from_combined(path) as _fh:
            run_reader_test(_fh)


@pytest.mark.filterwarnings("ignore: pod5_format.")
def test_pyarrow_combined_str():
    with tempfile.TemporaryDirectory() as temp:
        path = str(Path(temp) / "combined.pod5")
        with p5.create_combined_file(path) as _fh:
            run_writer_test(_fh)

        with p5.open_combined_file(path) as _fh:
            run_reader_test(_fh)


@pytest.mark.filterwarnings("ignore: pod5_format.")
def test_pyarrow_split():
    with tempfile.TemporaryDirectory() as temp:
        signal = Path(temp) / "split_signal.pod5"
        reads = Path(temp) / "split_reads.pod5"
        with p5.Writer.open_split(signal, reads) as _fh:
            run_writer_test(_fh)

        with p5.Reader.from_split(signal, reads) as _fh:
            run_reader_test(_fh)


@pytest.mark.filterwarnings("ignore: pod5_format.")
def test_pyarrow_split_str():
    with tempfile.TemporaryDirectory() as temp:
        signal = str(Path(temp) / "split_signal.pod5")
        reads = str(Path(temp) / "split_reads.pod5")
        with p5.create_split_file(signal, reads) as _fh:
            run_writer_test(_fh)

        with p5.open_split_file(signal, reads) as _fh:
            run_reader_test(_fh)


def test_pyarrow_split_one_name():
    with tempfile.TemporaryDirectory() as temp:
        split_path = Path(temp) / "split.pod5"

        with p5.Writer.open_split(split_path) as _fh:
            run_writer_test(_fh)

        with p5.Reader.from_inferred_split(split_path) as _fh:
            run_reader_test(_fh)
