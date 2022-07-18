"""
Tool for converting fast5 files to the pod5 format
"""

import argparse
from collections import namedtuple
import datetime
from pathlib import Path
import sys
import time
import typing
import uuid
import multiprocessing as mp
from queue import Empty

import h5py
import iso8601
import numpy
import more_itertools
from ont_fast5_api.compression_settings import register_plugin

import pod5_format as p5
from pod5_format.reader_utils import make_split_filename

from .utils import iterate_inputs


register_plugin()


def h5py_get_str(value):
    if isinstance(value, str):
        return value
    return value.decode("utf-8")


def format_sample_count(count):
    units = [
        (1000000000000, "T"),
        (1000000000, "G"),
        (1000000, "M"),
        (1000, "K"),
    ]

    for div, unit in units:
        if count > div:
            return f"{count/div:.1f} {unit}Samples"

    return f"{count} Samples"


def find_end_reason(end_reason: int) -> p5.EndReason:
    """Return a Pod5EndReason instance from the given end_reason integer"""
    if end_reason is not None:
        if end_reason == 2:
            return p5.EndReason(name=p5.EndReasonEnum.MUX_CHANGE, forced=True)
        if end_reason == 3:
            return p5.EndReason(name=p5.EndReasonEnum.UNBLOCK_MUX_CHANGE, forced=True)
        if end_reason == 4:
            return p5.EndReason(
                name=p5.EndReasonEnum.DATA_SERVICE_UNBLOCK_MUX_CHANGE, forced=True
            )
        if end_reason == 5:
            return p5.EndReason(name=p5.EndReasonEnum.SIGNAL_POSITIVE, forced=False)
        if end_reason == 6:
            return p5.EndReason(name=p5.EndReasonEnum.SIGNAL_NEGATIVE, forced=False)
    return p5.EndReason(name=p5.EndReasonEnum.UNKNOWN, forced=False)


def get_datetime_as_epoch_ms(time_str):
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    if time_str == None:
        return 0
    try:
        return iso8601.parse_date(h5py_get_str(time_str))
    except iso8601.iso8601.ParseError:
        return 0


ReadRequest = namedtuple("ReadRequest", [])
StartFile = namedtuple("StartFile", ["read_count"])
EndFile = namedtuple("EndFile", ["file", "read_count"])

ReadList = namedtuple("ReadList", ["file", "reads"])

READ_CHUNK_SIZE = 100


class RunCache:
    """Store the aquisition_id for caching the run_info for each fast5 file"""

    def __init__(self, acq_id, run_info):
        self.acquisition_id = acq_id
        self.run_info = run_info


def create_run_info(
    acq_id, adc_max, adc_min, channel_id, context_tags, device_type, tracking_id
) -> p5.RunInfo:
    """Create a Pod5RunInfo instance from parsed fast5 data"""
    return p5.RunInfo(
        acquisition_id=acq_id,
        acquisition_start_time=get_datetime_as_epoch_ms(tracking_id["exp_start_time"]),
        adc_max=adc_max,
        adc_min=adc_min,
        context_tags=context_tags,
        experiment_name="",
        flow_cell_id=h5py_get_str(tracking_id.get("flow_cell_id", b"")),
        flow_cell_product_code=h5py_get_str(
            tracking_id.get("flow_cell_product_code", b"")
        ),
        protocol_name=h5py_get_str(tracking_id["exp_script_name"]),
        protocol_run_id=h5py_get_str(tracking_id["protocol_run_id"]),
        protocol_start_time=get_datetime_as_epoch_ms(
            tracking_id.get("protocol_start_time", None)
        ),
        sample_id=h5py_get_str(tracking_id["sample_id"]),
        sample_rate=int(channel_id.attrs["sampling_rate"]),
        sequencing_kit=h5py_get_str(context_tags.get("sequencing_kit", b"")),
        sequencer_position=h5py_get_str(tracking_id.get("device_id", b"")),
        sequencer_position_type=h5py_get_str(
            tracking_id.get("device_type", device_type)
        ),
        software="python-pod5-converter",
        system_name=h5py_get_str(tracking_id.get("host_product_serial_number", b"")),
        system_type=h5py_get_str(tracking_id.get("host_product_code", b"")),
        tracking_id=tracking_id,
    )


def get_reads_from_files(
    in_q, out_q, fast5_files, pre_compress_signal, signal_chunk_size
):
    # Persist this flag in case we encounter an error in a file.
    has_request_for_reads = False

    for fast5_file in fast5_files:
        file_read_sent_count = 0
        try:
            with h5py.File(str(fast5_file), "r") as inp:
                run_cache = None
                out_q.put(StartFile(len(inp.keys())))

                for keys in more_itertools.chunked(inp.keys(), READ_CHUNK_SIZE):
                    # Allow the out queue to throttle us back if we are too far ahead.
                    while not has_request_for_reads:
                        try:
                            _ = in_q.get(timeout=1)
                            has_request_for_reads = True
                            break
                        except Empty:
                            continue

                    reads = []
                    for key in keys:
                        attrs = inp[key].attrs
                        channel_id = inp[key]["channel_id"]
                        raw = inp[key]["Raw"]

                        pore_type = p5.Pore(
                            channel=int(channel_id.attrs["channel_number"]),
                            well=raw.attrs["start_mux"],
                            pore_type=h5py_get_str(attrs.get("pore_type", b"not_set")),
                        )
                        calibration_type = p5.Calibration.from_range(
                            offset=channel_id.attrs["offset"],
                            adc_range=channel_id.attrs["range"],
                            digitisation=channel_id.attrs["digitisation"],
                        )
                        end_reason_type = find_end_reason(
                            raw.attrs["end_reason"]
                            if "end_reason" in raw.attrs
                            else None
                        )

                        if "run_id" in attrs:
                            acq_id = h5py_get_str(attrs["run_id"])
                        else:
                            acq_id = h5py_get_str(
                                inp[key]["tracking_id"].attrs["run_id"]
                            )

                        if not run_cache or run_cache.acquisition_id != acq_id:
                            adc_min = 0
                            adc_max = 2047
                            device_type_guess = "promethion"
                            if channel_id.attrs["digitisation"] == 8192:
                                adc_min = -4096
                                adc_max = 4095
                                device_type_guess = "minion"

                            run_info_type = create_run_info(
                                acq_id=acq_id,
                                adc_max=adc_max,
                                adc_min=adc_min,
                                channel_id=channel_id,
                                context_tags=dict(inp[key]["context_tags"].attrs),
                                device_type=device_type_guess,
                                tracking_id=dict(inp[key]["tracking_id"].attrs),
                            )
                            run_cache = RunCache(acq_id, run_info_type)
                        else:
                            run_info_type = run_cache.run_info

                        signal = raw["Signal"][()]
                        sample_count = signal.shape[0]
                        if pre_compress_signal:
                            sample_count = []
                            signal_arr = []
                            for start in range(0, len(signal), signal_chunk_size):
                                signal_slice = signal[start : start + signal_chunk_size]
                                signal_arr.append(
                                    p5.signal_tools.vbz_compress_signal(signal_slice)
                                )
                                sample_count.append(len(signal_slice))

                        reads.append(
                            p5.Read(
                                uuid.UUID(h5py_get_str(raw.attrs["read_id"])).bytes,
                                pore_type,
                                calibration_type,
                                raw.attrs["read_number"],
                                raw.attrs["start_time"],
                                raw.attrs["median_before"],
                                end_reason_type,
                                run_info_type,
                                signal_arr,
                                sample_count,
                            )
                        )

                    file_read_sent_count += len(reads)
                    out_q.put(ReadList(fast5_file, reads))
                    has_request_for_reads = False
        except Exception as exc:
            import traceback

            traceback.print_exc()
            print(f"Error in file {fast5_file}: {exc}", file=sys.stderr)

        out_q.put(EndFile(fast5_file, file_read_sent_count))


def add_reads(
    writer: p5.Writer,
    reads: typing.Iterable[p5.Read],
    pre_compressed_signal: bool,
):

    pore_types = numpy.array(
        [writer.find_pore(r.pore)[0] for r in reads], dtype=numpy.int16
    )
    calib_types = numpy.array(
        [writer.find_calibration(r.calibration)[0] for r in reads],
        dtype=numpy.int16,
    )
    end_reason_types = numpy.array(
        [writer.find_end_reason(r.end_reason)[0] for r in reads],
        dtype=numpy.int16,
    )
    run_info_types = numpy.array(
        [writer.find_run_info(r.run_info)[0] for r in reads], dtype=numpy.int16
    )

    writer.add_reads(
        numpy.array([numpy.frombuffer(r.read_id, dtype=numpy.uint8) for r in reads]),
        pore_types,
        calib_types,
        numpy.array([r.read_number for r in reads], dtype=numpy.uint32),
        numpy.array([r.start_time for r in reads], dtype=numpy.uint64),
        numpy.array([r.median_before for r in reads], dtype=numpy.float32),
        end_reason_types,
        run_info_types,
        # Pass an array of arrays here, as we have pre compressed data
        # top level array is per read, then the sub arrays are chunks within the reads.
        # the two arrays here should have the same dimensions, first contains compressed
        # sample array, the second contains the sample counts
        [r.signal for r in reads],
        [numpy.array(r.samples_count, dtype=numpy.uint64) for r in reads],
        pre_compressed_signal=pre_compressed_signal,
    )


class OutputHandler:
    def __init__(
        self,
        output_root: Path,
        one_to_one: bool,
        output_split: bool,
        force_overwrite: bool,
    ):
        self.output_root = output_root
        self._one_to_one = one_to_one
        self._output_split = output_split
        self._force_overwrite = force_overwrite
        self._input_to_output_path: typing.Dict[Path, Path] = {}
        self._output_files: typing.Dict[Path, p5.Writer] = {}

    def _open_writer(self, output_path: Path) -> p5.Writer:
        """Get the writer from existing handles or create a new one if unseen"""
        if output_path in self._output_files:
            return self._output_files[output_path]

        if self._output_split:
            signal_path, reads_path = make_split_filename(output_path)
            for path in [signal_path, reads_path]:
                if self._force_overwrite:
                    path.unlink(missing_ok=True)
            writer = p5.Writer.open_split(signal_path, reads_path)
        else:
            if self._force_overwrite:
                output_path.unlink(missing_ok=True)
            writer = p5.Writer.open_combined(output_path)

        self._output_files[output_path] = writer
        return writer

    def get_writer(self, input_path: Path) -> p5.Writer:
        """Get a Pod5Writer to write data from the input_path"""
        if input_path not in self._input_to_output_path:
            if self._one_to_one:
                out_path = self.output_root / input_path.with_suffix(".pod5").name
            else:
                out_path = self.output_root / "output.pod5"
            self._input_to_output_path[input_path] = out_path

        output_path = self._input_to_output_path[input_path]
        return self._open_writer(output_path=output_path)

    def set_input_complete(self, input_path: Path) -> None:
        """Close the Pod5Writer for associated input_path"""
        if not self._one_to_one:
            return

        if input_path not in self._input_to_output_path:
            return

        output_path = self._input_to_output_path[input_path]
        self._output_files[output_path].close()
        del self._output_files[output_path]
        print("Deleted input_complete")

    def close_all(self):
        """Close all open writers"""
        for writer in self._output_files.values():
            writer.close()
            print(f"Close all deleted: {writer}")
            del writer
        self._output_files = {}


def main():
    parser = argparse.ArgumentParser("Convert a fast5 file into an pod5 file")

    parser.add_argument("input", type=Path, nargs="+", help="Input path for fast5 file")
    parser.add_argument("output", type=Path, help="Output path for the pod5 file(s)")
    parser.add_argument(
        "-r",
        "--recursive",
        default=False,
        action="store_true",
        help="Search for input files recursively",
    )
    parser.add_argument(
        "--active-readers",
        default=10,
        type=int,
        help="How many file readers to keep active",
    )
    parser.add_argument(
        "--output-one-to-one",
        action="store_true",
        help="Output files should be 1:1 with input files, written as children of [output] argument.",
    )
    parser.add_argument(
        "--output-split",
        action="store_true",
        help="Output files should use the pod5 split format.",
    )
    parser.add_argument(
        "--force-overwrite", action="store_true", help="Overwrite destination files"
    )
    parser.add_argument(
        "--signal-chunk-size",
        default=102400,
        help="Chunk size to use for signal data set",
    )

    args = parser.parse_args()

    ctx = mp.get_context("spawn")
    read_request_queue = ctx.Queue()
    read_data_queue = ctx.Queue()

    # Always writing compressed files right now.
    pre_compress_signal = True

    # Divide up files between readers:
    pending_files = list(iterate_inputs(args.input, args.recursive, "*.fast5"))
    file_count = len(pending_files)
    items_per_reads = max(1, len(pending_files) // args.active_readers)
    active_processes: typing.List[mp.Process] = []
    while pending_files:
        files = pending_files[:items_per_reads]
        pending_files = pending_files[items_per_reads:]
        p = ctx.Process(
            target=get_reads_from_files,
            args=(
                read_request_queue,
                read_data_queue,
                files,
                pre_compress_signal,
                args.signal_chunk_size,
            ),
        )
        p.start()
        active_processes.append(p)

    # start requests for reads, we probably dont need more reads in memory at a time
    for _ in range(args.active_readers * 3):
        read_request_queue.put(ReadRequest())

    if args.output.exists() and args.output.is_file():
        raise FileExistsError("Invalid output location - already exists as file")
    args.output.mkdir(parents=True, exist_ok=True)
    output_handler = OutputHandler(
        args.output, args.output_one_to_one, args.output_split, args.force_overwrite
    )

    print("Converting reads...")
    t_start = t_last_update = time.time()
    update_interval = 15  # seconds

    files_started = 0
    files_ended = 0
    read_count = 0
    reads_processed = 0
    sample_count = 0

    while files_ended < file_count:
        now = time.time()
        if t_last_update + update_interval < now:
            t_last_update = now
            mb_total = (sample_count * 2) / (1000 * 1000)
            time_total = t_last_update - t_start
            print(
                f"{files_ended}/{files_started}/{file_count} files\t"
                f"{reads_processed}/{read_count} reads, {format_sample_count(sample_count)}, {mb_total/time_total:.1f} MB/s"
            )

        try:
            item = read_data_queue.get(timeout=0.5)
        except Empty:
            continue

        if isinstance(item, ReadList):
            writer = output_handler.get_writer(item.file)

            add_reads(writer, item.reads, pre_compress_signal)

            reads_in_this_chunk = len(item.reads)
            reads_processed += reads_in_this_chunk

            sample_count += sum(sum(r.samples_count) for r in item.reads)

            # Inform the input queues we can handle another read now:
            read_request_queue.put(ReadRequest())
        elif isinstance(item, StartFile):
            files_started += 1
            read_count += item.read_count
            continue
        elif isinstance(item, EndFile):
            output_handler.set_input_complete(item.file)
            files_ended += 1

    if reads_processed != read_count:
        print(
            "!!! Some reads count not be converted due to errors !!!", file=sys.stderr
        )
    print(
        f"{files_started}/{files_ended}/{file_count} files\t"
        f"{reads_processed}/{read_count} reads, {format_sample_count(sample_count)}"
    )

    print(f"Conversion complete: {sample_count} samples")

    for p in active_processes:
        p.join()
        p.close()

    output_handler.close_all()


if __name__ == "__main__":
    main()
