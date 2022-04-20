import argparse
from collections import namedtuple
import datetime
from pathlib import Path
import time
import uuid

import h5py
import iso8601
import mkr_format
import multiprocessing as mp
from queue import Empty


def find_end_reason(end_reason):
    if end_reason == 2:
        return {"name": mkr_format.EndReason.MUX_CHANGE, "forced": True}
    elif end_reason == 3:
        return {"name": mkr_format.EndReason.UNBLOCK_MUX_CHANGE, "forced": True}
    elif end_reason == 4:
        return {
            "name": mkr_format.EndReason.DATA_SERVICE_UNBLOCK_MUX_CHANGE,
            "forced": True,
        }
    elif end_reason == 5:
        return {"name": mkr_format.EndReason.SIGNAL_POSITIVE, "forced": False}
    elif end_reason == 6:
        return {"name": mkr_format.EndReason.SIGNAL_NEGATIVE, "forced": False}

    return {"name": mkr_format.EndReason.UNKNOWN, "forced": False}


def get_datetime_as_epoch_ms(time_str):
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    if time_str == None:
        return 0
    try:
        date = iso8601.parse_date(time_str.decode("utf-8"))
    except iso8601.iso8601.ParseError:
        return 0
    return int((date - epoch).total_seconds() * 1000)


StartFile = namedtuple("StartFile", ["read_count"])
EndFile = namedtuple("EndFile", [])
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
    ],
)


def get_reads_from_file(q, fast5_file):
    with h5py.File(str(fast5_file), "r") as inp:
        q.put(StartFile(len(inp.keys())))

        read_count = len(inp.keys())

        for key in inp.keys():
            attrs = inp[key].attrs
            channel_id = inp[key]["channel_id"]
            raw = inp[key]["Raw"]

            pore_type = {
                "channel": int(channel_id.attrs["channel_number"]),
                "well": raw.attrs["start_mux"],
                "pore_type": attrs["pore_type"].decode("utf-8"),
            }
            calib_type = {
                "offset": channel_id.attrs["offset"],
                "adc_range": channel_id.attrs["range"],
                "digitisation": channel_id.attrs["digitisation"],
            }
            end_reason_type = find_end_reason(raw.attrs["end_reason"])

            adc_min = 0
            adc_max = 0
            if channel_id.attrs["digitisation"] == 8192:
                adc_min = -4096
                adc_max = 4095

            tracking_id = dict(inp[key]["tracking_id"].attrs)
            context_tags = dict(inp[key]["context_tags"].attrs)
            run_info_type = {
                "acquisition_id": attrs["run_id"].decode("utf-8"),
                "acquisition_start_time_ms": get_datetime_as_epoch_ms(
                    tracking_id["exp_start_time"]
                ),
                "adc_max": adc_max,
                "adc_min": adc_min,
                "context_tags": tuple(context_tags.items()),
                "experiment_name": "",
                "flow_cell_id": tracking_id.get("flow_cell_id", b"").decode("utf-8"),
                "flow_cell_product_code": tracking_id.get(
                    "flow_cell_product_code", b""
                ).decode("utf-8"),
                "protocol_name": tracking_id["exp_script_name"].decode("utf-8"),
                "protocol_run_id": tracking_id["protocol_run_id"].decode("utf-8"),
                "protocol_start_time_ms": get_datetime_as_epoch_ms(
                    tracking_id.get("protocol_start_time", None)
                ),
                "sample_id": tracking_id["sample_id"].decode("utf-8"),
                "sample_rate": int(channel_id.attrs["sampling_rate"]),
                "sequencing_kit": context_tags.get("sequencing_kit", b"").decode(
                    "utf-8"
                ),
                "sequencer_position": tracking_id.get("device_id", b"").decode("utf-8"),
                "sequencer_position_type": tracking_id["device_type"].decode("utf-8"),
                "software": "python-mkr-converter",
                "system_name": tracking_id.get(
                    "host_product_serial_number", b""
                ).decode("utf-8"),
                "system_type": tracking_id.get("host_product_code", b"").decode(
                    "utf-8"
                ),
                "tracking_id": tuple(tracking_id.items()),
            }

            q.put(
                Read(
                    uuid.UUID(raw.attrs["read_id"].decode("utf-8")).bytes,
                    pore_type,
                    calib_type,
                    raw.attrs["read_number"],
                    raw.attrs["start_time"],
                    raw.attrs["median_before"],
                    end_reason_type,
                    run_info_type,
                    raw["Signal"][()],
                )
            )

    q.put(EndFile())


def add_read(file, read):
    pore_type, _ = file.find_pore(**read.pore)
    calib_type, _ = file.find_calibration(**read.calibration)
    end_reason_type, _ = file.find_end_reason(**read.end_reason)
    run_info_type, _ = file.find_run_info(**read.run_info)

    file.add_read(
        read.read_id,
        pore_type,
        calib_type,
        read.read_number,
        read.start_time,
        read.median_before,
        end_reason_type,
        run_info_type,
        read.signal,
    )


def main():
    parser = argparse.ArgumentParser("Convert a fast5 file into an mkr file")

    parser.add_argument("input_file", type=Path, nargs="+")
    parser.add_argument("output_file", type=Path)
    parser.add_argument(
        "--record-batch-size",
        type=int,
        default=2000,
        help="Number of items to put in one record batch.",
    )

    args = parser.parse_args()

    ctx = mp.get_context("spawn")
    read_data_queue = ctx.Queue()

    reader_processes = []
    for file in args.input_file:
        p = ctx.Process(target=get_reads_from_file, args=(read_data_queue, file))
        p.start()
        reader_processes.append(p)

    with mkr_format.create_combined_file(args.output_file) as out:
        print(f"Converting reads...")
        t_start = t_last_update = time.time()
        update_interval = 5  # seconds

        read_count = 0
        reads_processed = 0
        sample_count = 0

        while True:
            try:
                item = read_data_queue.get(timeout=0.5)
            except Empty:
                continue

            if isinstance(item, Read):
                add_read(out, item)
            elif isinstance(item, StartFile):
                read_count += item.read_count
                continue
            elif isinstance(item, EndFile):
                break

            if reads_processed % args.record_batch_size == 0:
                out.flush()

            sample_count += item.signal.shape[0]
            reads_processed += 1

            now = time.time()
            if t_last_update + update_interval < now:
                t_last_update = now
                mb_total = (sample_count * 2) / (1000 * 1000)
                time_total = t_last_update - t_start
                print(
                    f"{100.0*reads_processed/read_count:.1f} %\t{reads_processed}/{read_count} reads, {sample_count} samples, {mb_total/time_total:.1f} MB/s"
                )

    for p in reader_processes:
        p.join()

    print(f"Conversion complete: {sample_count} samples")


if __name__ == "__main__":
    main()
