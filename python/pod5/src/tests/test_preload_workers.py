"""Signal-preloader worker control using small generated signals, never sample files."""

from dataclasses import replace
from pathlib import Path
from unittest import mock
from uuid import UUID

import numpy as np
import pod5 as p5
import pod5.writer as writer_module
import pytest

from tests.conftest import _random_read


@pytest.fixture
def synthetic_preload_file(tmp_path: Path):
    reads = [
        replace(
            _random_read(100 + i),
            read_id=UUID(int=i + 1),
            signal=np.arange(11 + i, dtype=np.int16) * (i + 1),
        )
        for i in range(6)
    ]
    path = tmp_path / "synthetic-workers.pod5"
    create_file = writer_module.p5b.create_file

    def create_small_batches(filename, software_name, options):
        options.read_table_batch_size = 2
        return create_file(filename, software_name, options)

    with mock.patch("pod5.writer.p5b.create_file", side_effect=create_small_batches):
        with p5.Writer(path) as writer:
            writer.add_reads(reads)
    return path, {str(read.read_id): read.signal.copy() for read in reads}


ROUTES = ["reads", "selected_reads", "batches", "some_batches", "selected_batches"]


def read_route(reader, route, ids, **kwargs):
    if route == "reads":
        return list(reader.reads(**kwargs)), ids
    if route == "selected_reads":
        chosen = [ids[0], ids[-1]]
        return list(reader.reads(selection=chosen, **kwargs)), chosen
    if route == "some_batches":
        batches = reader.read_batches(batch_selection=[0, 2], **kwargs)
        expected = ids[0:2] + ids[4:6]
    elif route == "selected_batches":
        expected = [ids[0], ids[-1]]
        batches = reader.read_batches(selection=expected, **kwargs)
    else:
        batches = reader.read_batches(**kwargs)
        expected = ids
    return [read for batch in batches for read in batch.reads()], expected


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("preload", [{"samples"}, {"sample_count"}, set()])
def test_preload_workers_default_compatible(synthetic_preload_file, route, preload):
    path, signals = synthetic_preload_file
    with p5.Reader(path) as reader:
        assert reader.batch_count == 3
        reads, expected = read_route(reader, route, list(signals), preload=preload)
        assert [str(read.read_id) for read in reads] == expected
        for read in reads:
            np.testing.assert_array_equal(read.signal, signals[str(read.read_id)])
            assert read.num_samples == len(signals[str(read.read_id)])


@pytest.mark.parametrize("method", ["reads", "read_batches"])
@pytest.mark.parametrize(
    "workers,error",
    [
        (0, ValueError),
        (-1, ValueError),
        (True, TypeError),
        (False, TypeError),
        (1.5, TypeError),
        ("2", TypeError),
        (object(), TypeError),
        (1 << 256, OverflowError),
    ],
)
@pytest.mark.parametrize("preload", [None, {"samples"}])
def test_preload_workers_validation(
    synthetic_preload_file, method, workers, error, preload
):
    path, _ = synthetic_preload_file
    with p5.Reader(path) as reader:
        with pytest.raises(error, match="preload_workers"):
            list(getattr(reader, method)(preload=preload, preload_workers=workers))


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("workers", [None, 1, 2, np.int64(2)])
@pytest.mark.parametrize("preload", [{"samples"}, {"sample_count"}])
def test_preload_workers_native_selection(
    synthetic_preload_file, route, workers, preload
):
    path, signals = synthetic_preload_file
    with p5.Reader(path) as reader:
        native = reader.inner_file_reader
        tracked = mock.Mock(wraps=native)
        actual_workers = []

        def trace(method):
            def call(*args, **kwargs):
                loader = method(*args, **kwargs)
                actual_workers.append(loader.worker_count)
                if workers is not None:
                    assert loader.worker_count == workers
                else:
                    assert loader.worker_count > 0
                return loader

            return call

        for name in (
            "batch_get_signal",
            "batch_get_signal_batches",
            "batch_get_signal_selection",
        ):
            getattr(tracked, name).side_effect = trace(getattr(native, name))
        with mock.patch.object(reader, "_file_reader", tracked):
            reads, expected = read_route(
                reader, route, list(signals), preload=preload, preload_workers=workers
            )
        assert len(actual_workers) == 1
        assert [str(read.read_id) for read in reads] == expected
        for read in reads:
            np.testing.assert_array_equal(read.signal, signals[str(read.read_id)])
            assert read.num_samples == len(signals[str(read.read_id)])


@pytest.mark.parametrize(
    "method",
    ["batch_get_signal", "batch_get_signal_batches", "batch_get_signal_selection"],
)
@pytest.mark.parametrize(
    "workers,error",
    [
        (0, ValueError),
        (-1, ValueError),
        (True, TypeError),
        (1.5, TypeError),
        (False, TypeError),
        ("2", TypeError),
        (1 << 256, OverflowError),
    ],
)
def test_native_preload_workers_validation(
    synthetic_preload_file, method, workers, error
):
    path, _ = synthetic_preload_file
    with p5.Reader(path) as reader:
        args = [True, True]
        if method == "batch_get_signal_batches":
            args.append(np.array([0], dtype=np.uint32))
        elif method == "batch_get_signal_selection":
            args.extend(
                [np.array([1, 0, 0], dtype=np.uint32), np.array([0], dtype=np.uint32)]
            )
        with pytest.raises(error):
            getattr(reader.inner_file_reader, method)(*args, worker_count=workers)


@pytest.mark.parametrize("workers", [None, 1, 2])
@pytest.mark.parametrize("method", ["reads", "read_batches"])
def test_preload_workers_missing_selection(synthetic_preload_file, method, workers):
    path, signals = synthetic_preload_file
    selection = [next(iter(signals)), str(UUID(int=1000))]
    with p5.Reader(path) as reader:
        options = dict(
            selection=selection, preload={"samples"}, preload_workers=workers
        )
        with pytest.raises(RuntimeError, match="Failed to find"):
            list(getattr(reader, method)(**options))
        values = list(getattr(reader, method)(missing_ok=True, **options))
        if method == "read_batches":
            values = [read for batch in values for read in batch.reads()]
        assert [str(read.read_id) for read in values] == selection[:1]
        np.testing.assert_array_equal(values[0].signal, signals[selection[0]])


@pytest.mark.parametrize("preload", [None, {"samples"}])
@pytest.mark.parametrize("explicit_none", [False, True])
def test_preload_workers_legacy_subclass(
    synthetic_preload_file, preload, explicit_none
):
    path, signals = synthetic_preload_file
    calls = []

    class LegacyReader(p5.Reader):
        # Deliberately retain a pre-existing override without the new keyword.
        def read_batches(  # type: ignore[override]
            self, selection=None, batch_selection=None, missing_ok=False, preload=None
        ):
            calls.append(preload)
            return super().read_batches(
                selection=selection,
                batch_selection=batch_selection,
                missing_ok=missing_ok,
                preload=preload,
            )

    with LegacyReader(path) as reader:
        if explicit_none:
            reads = list(reader.reads(preload=preload, preload_workers=None))
        else:
            reads = list(reader.reads(preload=preload))
        assert calls == [preload]
        assert [str(read.read_id) for read in reads] == list(signals)
        for read in reads:
            np.testing.assert_array_equal(read.signal, signals[str(read.read_id)])


@pytest.mark.parametrize("workers", [1, 2])
def test_preload_workers_subclass_forwarding(synthetic_preload_file, workers):
    path, signals = synthetic_preload_file
    calls = []

    class TrackingReader(p5.Reader):
        def read_batches(
            self,
            selection=None,
            batch_selection=None,
            missing_ok=False,
            preload=None,
            preload_workers=None,
        ):
            calls.append(
                (selection, batch_selection, missing_ok, preload, preload_workers)
            )
            return super().read_batches(
                selection=selection,
                batch_selection=batch_selection,
                missing_ok=missing_ok,
                preload=preload,
                preload_workers=preload_workers,
            )

    with TrackingReader(path) as reader:
        # Without preloading this also exercises the public call on old native modules.
        reads = list(reader.reads(preload_workers=workers))
        assert calls == [(None, None, False, None, workers)]
        assert [str(read.read_id) for read in reads] == list(signals)
        for read in reads:
            np.testing.assert_array_equal(read.signal, signals[str(read.read_id)])
