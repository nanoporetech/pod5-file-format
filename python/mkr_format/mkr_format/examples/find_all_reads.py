#!/usr/bin/python3

import argparse
from pathlib import Path

import mkr_format


def main():
    parser = argparse.ArgumentParser("Iterate through all read ids in an mkr file")
    parser.add_argument("input", type=Path)
    args = parser.parse_args()

    file = mkr_format.open_combined_file(args.input)
    for read in file.reads():
        print(f"Found read {read.read_id}")
        print(f"  Read has  {read.sample_count} samples")


if __name__ == "__main__":
    main()
