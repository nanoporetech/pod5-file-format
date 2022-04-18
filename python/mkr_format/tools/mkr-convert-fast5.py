import argparse
import datetime
from pathlib import Path
import time
import uuid

import h5py
import iso8601
import mkr_format


def find_end_reason(end_reason):
    if end_reason == 2:
        return mkr_format.EndReason.MUX_CHANGE, True
    elif end_reason == 3:
        return mkr_format.EndReason.UNBLOCK_MUX_CHANGE, True
    elif end_reason == 4:
        return mkr_format.EndReason.DATA_SERVICE_UNBLOCK_MUX_CHANGE, True
    elif end_reason == 5:
        return mkr_format.EndReason.SIGNAL_POSITIVE, False
    elif end_reason == 6:
        return mkr_format.EndReason.SIGNAL_NEGATIVE, False

    return mkr_format.EndReason.UNKNOWN, False


def get_datetime_as_epoch_ms(time_str):
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    if time_str == None:
        return 0
    date = iso8601.parse_date(time_str.decode("utf-8"))
    return int((date - epoch).total_seconds() * 1000)


def main():
    parser = argparse.ArgumentParser("Convert a fast5 file into an mkr file")

    parser.add_argument("input_file", type=Path)
    parser.add_argument("output_file", type=Path)

    args = parser.parse_args()

    with mkr_format.create_combined_file(args.output_file) as out:
        with h5py.File(str(args.input_file), "r") as inp:
            read_count = len(inp.keys())
            print(f"Converting {read_count} reads...")
            t_start = t_last_update = time.time()
            update_interval = 5  # seconds

            reads_processed = 0
            sample_count = 0

            for key in inp.keys():
                attrs = inp[key].attrs
                channel_id = inp[key]["channel_id"]
                raw = inp[key]["Raw"]

                pore_type, _ = out.find_pore(
                    int(channel_id.attrs["channel_number"]),
                    raw.attrs["start_mux"],
                    attrs["pore_type"].decode("utf-8"),
                )
                calib_type, _ = out.find_calibration(
                    offset=channel_id.attrs["offset"],
                    adc_range=channel_id.attrs["range"],
                    digitisation=channel_id.attrs["digitisation"],
                )
                end_reason_type, _ = out.find_end_reason(
                    *find_end_reason(raw.attrs["end_reason"])
                )

                adc_min = 0
                adc_max = 0
                if channel_id.attrs["digitisation"] == 8192:
                    adc_min = -4096
                    adc_max = 4095

                tracking_id = dict(inp[key]["tracking_id"].attrs)
                context_tags = dict(inp[key]["context_tags"].attrs)
                run_info_type, _ = out.find_run_info(
                    attrs["run_id"].decode("utf-8"),
                    get_datetime_as_epoch_ms(tracking_id["exp_start_time"]),
                    adc_max,
                    adc_min,
                    tuple(context_tags.items()),
                    "",
                    tracking_id.get("flow_cell_id", b"").decode("utf-8"),
                    tracking_id.get("flow_cell_product_code", b"").decode("utf-8"),
                    tracking_id["exp_script_name"].decode("utf-8"),
                    tracking_id["protocol_run_id"].decode("utf-8"),
                    get_datetime_as_epoch_ms(
                        tracking_id.get("protocol_start_time", None)
                    ),
                    tracking_id["sample_id"].decode("utf-8"),
                    int(channel_id.attrs["sampling_rate"]),
                    context_tags.get("sequencing_kit", b"").decode("utf-8"),
                    tracking_id.get("device_id", b"").decode("utf-8"),
                    tracking_id["device_type"].decode("utf-8"),
                    "python-mkr-converter",
                    tracking_id.get("host_product_serial_number", b"").decode("utf-8"),
                    tracking_id.get("host_product_code", b"").decode("utf-8"),
                    tuple(tracking_id.items()),
                )

                out.add_read(
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

                sample_count += raw["Signal"].shape[0]
                reads_processed += 1

                now = time.time()
                if t_last_update + update_interval < now:
                    t_last_update = now
                    mb_total = (sample_count * 2) / (1000 * 1000)
                    time_total = t_last_update - t_start
                    print(
                        f"{(reads_processed/read_count)*100:.1f} %\t{reads_processed} reads, {sample_count} samples, {mb_total/time_total:.1f} MB/s"
                    )

            print("Conversion complete: {sample_count} samples")


if __name__ == "__main__":
    main()
