import argparse
from pathlib import Path

import mkr_format


def try_open_file(filename: Path, files_handled: set):
    return mkr_format.open_combined_file(filename)


def main():
    parser = argparse.ArgumentParser("Convert a fast5 file into an mkr file")

    parser.add_argument("input_files", type=Path, nargs="+")

    args = parser.parse_args()

    files_handled = set()

    for filename in args.input_files:
        print(f"File: {filename}")
        reader = try_open_file(filename, files_handled)

        for read in reader.iter_reads():
            fields = [
                read.read_id,
                read.read_number,
                read.start_sample,
                read.median_before,
            ]
            print("\t".join(str(f) for f in fields))


if __name__ == "__main__":
    main()
