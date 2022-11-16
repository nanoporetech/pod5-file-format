#!/usr/bin/python3

import argparse
from pathlib import Path
from uuid import UUID

import pandas as pd

import pod5 as p5


def main():
    parser = argparse.ArgumentParser(
        "Iterate through specific read ids in an pod5 file"
    )
    parser.add_argument("input", type=Path)
    parser.add_argument("read_ids_csv", type=Path)
    args = parser.parse_args()

    read_ids_to_find = [UUID(r) for r in pd.read_csv(args.read_ids_csv)["read_id"]]

    with p5.Reader(args.input) as reader:
        for read in reader.reads(read_ids_to_find):
            print(f"Found read {read.read_id}")
            print(f"  Read has  {read.sample_count} samples")


if __name__ == "__main__":
    main()
