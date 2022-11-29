#!/usr/bin/env python3

import argparse
from pathlib import Path

import h5py
import numpy
import pandas as pd


def select_reads(file, selection):
    if selection is not None:
        for read in selection:
            path = f"/read_{read}"
            if path not in file:
                continue
            yield read, path
    else:
        for key in file.keys():
            if key.startswith("read_"):
                yield key[5:], key


def run(input_dir, output, select_read_ids=None, get_columns=[]):
    output.mkdir(parents=True, exist_ok=True)

    if select_read_ids is not None:
        print(f"Selecting {len(select_read_ids)} specific read ids")
    if get_columns is not None:
        print(f"Selecting columns: {get_columns}")

    read_ids = []
    extracted_columns = {"read_id": read_ids}
    print(f"Search for input files in {input_dir}")
    for file in input_dir.glob("*.fast5"):
        print(f"Searching for reads in {file}")

        file = h5py.File(file, "r")

        for read_id, read_path in select_reads(file, select_read_ids):
            read_ids.append(read_id)

            for c in get_columns:
                if c not in extracted_columns:
                    extracted_columns[c] = []
                col = extracted_columns[c]

                if c == "read_number":
                    col.append(file[f"{read_path}/Raw"].attrs["read_number"])
                elif c == "sample_count":
                    col.append(len(file[f"{read_path}/Raw"]["Signal"]))
                elif c == "samples":
                    col.append(numpy.sum(file[f"{read_path}/Raw"]["Signal"]))

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

    args = parser.parse_args()

    select_read_ids = None
    if args.select_ids:
        select_read_ids = pd.read_csv(args.select_ids)["read_id"]

    run(
        args.input,
        args.output,
        select_read_ids=select_read_ids,
        get_columns=args.get_column,
    )


if __name__ == "__main__":
    main()
