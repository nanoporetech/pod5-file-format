#!/usr/bin/python3

import argparse
from pathlib import Path
from uuid import UUID

import numpy
import pandas as pd

import mkr_format


def select_reads(file, selection):
    if selection is not None:
        return file.select_reads(UUID(s) for s in selection)
    else:
        return file.reads()


def run(input_dir, output, select_read_ids=None, get_columns=[], c_api=False):
    output.mkdir(parents=True, exist_ok=True)

    if select_read_ids is not None:
        print(f"Selecting {len(select_read_ids)} specific read ids")
    if get_columns is not None:
        print(f"Selecting columns: {get_columns}")

    read_ids = []
    extracted_columns = {"read_id": read_ids}
    print(f"Search for input files in {input_dir}")
    for file in input_dir.glob("*.mkr"):
        print(f"Searching for read ids in {file}")

        file = mkr_format.open_combined_file(file, use_c_api=c_api)

        for read in select_reads(file, select_read_ids):
            read_ids.append(read.read_id)

            for c in get_columns:
                if not c in extracted_columns:
                    extracted_columns[c] = []
                col = extracted_columns[c]
                if c == "samples":
                    col.append(numpy.sum(read.signal))
                else:
                    col.append(getattr(read, c))

    df = pd.DataFrame(extracted_columns)
    print(f"Selected {len(read_ids)} items")
    df.to_csv(output / "read_ids.csv", index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--select-ids",
        type=str,
        help="CSV file with a read_id column, listing ids to find in input files",
    )
    parser.add_argument(
        "--get-column",
        default=[],
        nargs="+",
        type=str,
        help="Add columns that should be extacted",
    )
    parser.add_argument(
        "--c-api", action="store_true", help="Use C API rather than PyArrow"
    )

    args = parser.parse_args()

    select_read_ids = None
    if args.select_ids:
        select_read_ids = pd.read_csv(args.select_ids)["read_id"]

    run(
        args.input,
        args.output,
        select_read_ids=select_read_ids,
        get_columns=args.get_column,
        c_api=args.c_api,
    )


if __name__ == "__main__":
    main()
