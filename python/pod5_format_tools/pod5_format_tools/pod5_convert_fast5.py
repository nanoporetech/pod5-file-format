import argparse
from collections import namedtuple
import datetime
from pathlib import Path
import sys
import time
import uuid

import h5py
import iso8601
import numpy
import more_itertools
from ont_fast5_api.compression_settings import register_plugin
import pod5_format
import multiprocessing as mp
from queue import Empty

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


def find_end_reason(end_reason):
    if end_reason is not None:
        if end_reason == 2:
            return {"name": pod5_format.EndReason.MUX_CHANGE, "forced": True}
        elif end_reason == 3:
            return {"name": pod5_format.EndReason.UNBLOCK_MUX_CHANGE, "forced": True}
        elif end_reason == 4:
            return {
                "name": pod5_format.EndReason.DATA_SERVICE_UNBLOCK_MUX_CHANGE,
                "forced": True,
            }
        elif end_reason == 5:
            return {"name": pod5_format.EndReason.SIGNAL_POSITIVE, "forced": False}
        elif end_reason == 6:
            return {"name": pod5_format.EndReason.SIGNAL_NEGATIVE, "forced": False}

    return {"name": pod5_format.EndReason.UNKNOWN, "forced": False}


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
Read = namedtuple(
    "Read",
    [
        "read_id",
        "pore",
        "calibration",
        "read_number",
        "start_time",
        "median_before",
        "end_reason",
        "run_info",
        "signal",
        "sample_count",
    ],
)
ReadList = namedtuple("ReadList", ["file", "reads"])

READ_CHUNK_SIZE = 100


class RunCache:
    def __init__(self, acq_id, run_info):
        self.acquisition_id = acq_id
        self.run_info = run_info


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

                read_count = len(inp.keys())

                for keys in more_itertools.chunked(inp.keys(), READ_CHUNK_SIZE):
                    # Allow the out queue to throttle us back if we are too far ahead.
                    while not has_request_for_reads:
                        try:
                            item = in_q.get(timeout=1)
                            has_request_for_reads = True
                            break
                        except Empty:
                            continue

                    reads = []
                    for key in keys:
                        attrs = inp[key].attrs
                        channel_id = inp[key]["channel_id"]
                        raw = inp[key]["Raw"]

                        pore_type = {
                            "channel": int(channel_id.attrs["channel_number"]),
                            "well": raw.attrs["start_mux"],
                            "pore_type": h5py_get_str(
                                attrs.get("pore_type", b"not_set")
                            ),
                        }
                        calib_type = {
                            "offset": channel_id.attrs["offset"],
                            "adc_range": channel_id.attrs["range"],
                            "digitisation": channel_id.attrs["digitisation"],
                        }
                        end_reason_type = find_end_reason(
                            raw.attrs["end_reason"]
                            if "end_reason" in raw.attrs
                            else None
                        )

                        acq_id = None
                        if "run_id" in attrs:
                            acq_id = h5py_get_str(attrs["run_id"])
                        else:
                            acq_id = h5py_get_str(
                                inp[key]["tracking_id"].attrs["run_id"]
                            )

                        if not run_cache or run_cache.acquisition_id != acq_id:
                            adc_min = 0
                            adc_max = 2047
                            device_type_guess = b"promethion"
                            if channel_id.attrs["digitisation"] == 8192:
                                adc_min = -4096
                                adc_max = 4095
                                device_type_guess = b"minion"

                            tracking_id = dict(inp[key]["tracking_id"].attrs)
                            context_tags = dict(inp[key]["context_tags"].attrs)
                            run_info_type = {
                                "acquisition_id": acq_id,
                                "acquisition_start_time": get_datetime_as_epoch_ms(
                                    tracking_id["exp_start_time"]
                                ),
                                "adc_max": adc_max,
                                "adc_min": adc_min,
                                "context_tags": tuple(context_tags.items()),
                                "experiment_name": "",
                                "flow_cell_id": h5py_get_str(
                                    tracking_id.get("flow_cell_id", b"")
                                ),
                                "flow_cell_product_code": h5py_get_str(
                                    tracking_id.get("flow_cell_product_code", b"")
                                ),
                                "protocol_name": h5py_get_str(
                                    tracking_id["exp_script_name"]
                                ),
                                "protocol_run_id": h5py_get_str(
                                    tracking_id["protocol_run_id"]
                                ),
                                "protocol_start_time": get_datetime_as_epoch_ms(
                                    tracking_id.get("protocol_start_time", None)
                                ),
                                "sample_id": h5py_get_str(tracking_id["sample_id"]),
                                "sample_rate": int(channel_id.attrs["sampling_rate"]),
                                "sequencing_kit": h5py_get_str(
                                    context_tags.get("sequencing_kit", b"")
                                ),
                                "sequencer_position": h5py_get_str(
                                    tracking_id.get("device_id", b"")
                                ),
                                "sequencer_position_type": h5py_get_str(
                                    tracking_id.get("device_type", device_type_guess)
                                ),
                                "software": "python-pod5-converter",
                                "system_name": h5py_get_str(
                                    tracking_id.get("host_product_serial_number", b"")
                                ),
                                "system_type": h5py_get_str(
                                    tracking_id.get("host_product_code", b"")
                                ),
                                "tracking_id": tuple(tracking_id.items()),
                            }
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
                                    pod5_format.signal_tools.vbz_compress_signal(
                                        signal_slice
                                    )
                                )
                                sample_count.append(len(signal_slice))

                        reads.append(
                            Read(
                                uuid.UUID(h5py_get_str(raw.attrs["read_id"])).bytes,
                                pore_type,
                                calib_type,
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


def add_reads(file, reads, pre_compressed_signal):
    pore_types = numpy.array(
        [file.find_pore(**r.pore)[0] for r in reads], dtype=numpy.int16
    )
    calib_types = numpy.array(
        [file.find_calibration(**r.calibration)[0] for r in reads],
        dtype=numpy.int16,
    )
    end_reason_types = numpy.array(
        [file.find_end_reason(**r.end_reason)[0] for r in reads],
        dtype=numpy.int16,
    )
    run_info_types = numpy.array(
        [file.find_run_info(**r.run_info)[0] for r in reads], dtype=numpy.int16
    )

    file.add_reads(
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
        [numpy.array(r.sample_count, dtype=numpy.uint64) for r in reads],
        pre_compressed_signal=pre_compressed_signal,
    )


def iterate_inputs(input_items):
    for input_item in input_items:
        if input_item.is_dir():
            for file in input_item.glob("*.fast5"):
                yield file
        else:
            yield input_item


class FileWrapper:
    def __init__(self, file):
        self.pod5_file = file


class OutputHandler:
    def __init__(self, output_path_root, one_to_one, output_split, force_overwrite):
        self.output_path_root = output_path_root
        self._one_to_one = one_to_one
        self._output_split = output_split
        self._force_overwrite = force_overwrite
        self._input_to_output_path = {}
        self._output_files = {}

    def get_file(self, input_path):
        if input_path not in self._input_to_output_path:
            output_path = None
            if self._one_to_one:
                output_path = (
                    self.output_path_root / input_path.with_suffix(".pod5").name
                )
            else:
                output_path = self.output_path_root / "output.pod5"
            self._input_to_output_path[input_path] = output_path

        output_path = self._input_to_output_path[input_path]
        if output_path not in self._output_files:
            if self._output_split:
                signal_file = Path(str(output_path.with_suffix("")) + "_signal.pod5")
                reads_file = Path(str(output_path.with_suffix("")) + "_reads.pod5")
                for file in [signal_file, reads_file]:
                    if self._force_overwrite:
                        file.unlink(missing_ok=True)
                pod5_file = pod5_format.create_split_file(signal_file, reads_file)
            else:
                if self._force_overwrite:
                    output_path.unlink(missing_ok=True)
                pod5_file = pod5_format.create_combined_file(output_path)
            self._output_files[output_path] = FileWrapper(pod5_file)
        return self._output_files[output_path]

    def input_complete(self, input_path):
        if not self._one_to_one:
            return

        if input_path not in self._input_to_output_path:
            return
        output_path = self._input_to_output_path[input_path]
        self._output_files[output_path].pod5_file.close()
        del self._output_files[output_path]

    def close_all(self):
        for v in self._output_files.values():
            v.pod5_file.close()
        self._output_files = {}


def main():
    parser = argparse.ArgumentParser("Convert a fast5 file into an pod5 file")

    parser.add_argument("input", type=Path, nargs="+", help="Input path for fast5 file")
    parser.add_argument("output", type=Path, help="Output path for the pod5 file(s)")
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
    pending_files = list(iterate_inputs(args.input))
    file_count = len(pending_files)
    items_per_reads = max(1, len(pending_files) // args.active_readers)
    active_processes = []
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
        raise Exception("Invalid output location - already exists as file")
    args.output.mkdir(parents=True, exist_ok=True)
    output_handler = OutputHandler(
        args.output, args.output_one_to_one, args.output_split, args.force_overwrite
    )

    print(f"Converting reads...")
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
            out_file = output_handler.get_file(item.file)

            add_reads(out_file.pod5_file, item.reads, pre_compress_signal)

            reads_in_this_chunk = len(item.reads)
            reads_processed += reads_in_this_chunk

            sample_count += sum(sum(r.sample_count) for r in item.reads)

            # Inform the input queues we can handle another read now:
            read_request_queue.put(ReadRequest())
        elif isinstance(item, StartFile):
            files_started += 1
            read_count += item.read_count
            continue
        elif isinstance(item, EndFile):
            out_file = output_handler.input_complete(item.file)
            files_ended += 1

    if reads_processed != read_count:
        print(
            "!!! Some reads count not be converted due to errors !!!", file=sys.stderr
        )
    print(
        f"{files_started}/{files_ended}/{file_count} files\t"
        f"{reads_processed}/{read_count} reads, {format_sample_count(sample_count)}"
    )

    output_handler.close_all()
    print(f"Conversion complete: {sample_count} samples")

    for p in active_processes:
        p.join()


if __name__ == "__main__":
    main()
