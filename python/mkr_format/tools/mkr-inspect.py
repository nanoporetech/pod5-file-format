import argparse
from pathlib import Path

import mkr_format


def try_open_file(filename: Path, files_handled: set):
    return mkr_format.open_combined_file(filename)


def do_reads_command(file):
    for read in file.reads():
        sample_count = read.sample_count
        byte_count = read.byte_count

        pore_data = read.pore
        calibration_data = read.calibration
        end_reason_data = read.end_reason
        run_info_data = read.run_info

        fields = [
            read.read_id,
            pore_data.channel,
            pore_data.well,
            pore_data.pore_type,
            read.read_number,
            read.start_sample,
            end_reason_data.name,
            f"{read.median_before:.1f}",
            f"{calibration_data.offset:.1f}",
            f"{calibration_data.scale:.1f}",
            read.sample_count,
            read.byte_count,
            f"{read.byte_count / float(read.sample_count*2):.3f}",
        ]
        print("\t".join(str(f) for f in fields))


def do_summary_command(file):
    batch_count = 0
    read_count = 0
    sample_count = 0
    byte_count = 0
    min_sample = float("inf")
    max_sample = 0
    for batch in file.read_batches():
        batch_count += 1
        for read in batch.reads():
            read_count += 1
            read_sample_count = read.sample_count
            sample_count += read_sample_count
            byte_count += read.byte_count
            min_sample = min(min_sample, read.start_sample)
            max_sample = max(max_sample, read.start_sample + read_sample_count)

    print(f"Contains {read_count} reads, in {batch_count} batches")
    print(f"Reads span from sample {min_sample} to {max_sample}")
    print(
        f"{sample_count} samples, {byte_count} bytes: {100*byte_count/float(sample_count*2):.1f} % signal compression ratio"
    )


def main():
    parser = argparse.ArgumentParser("Convert a fast5 file into an mkr file")

    subparser = parser.add_subparsers(title="command", dest="command")
    summary_parser = subparser.add_parser("summary")
    summary_parser.add_argument("input_files", type=Path, nargs="+")

    reads_parser = subparser.add_parser("reads")
    reads_parser.add_argument("input_files", type=Path, nargs="+")

    args = parser.parse_args()

    files_handled = set()

    for filename in args.input_files:
        print(f"File: {filename}")
        file = try_open_file(filename, files_handled)

        if args.command == "reads":
            do_reads_command(file)
        elif args.command == "summary":
            do_summary_command(file)


if __name__ == "__main__":
    main()
