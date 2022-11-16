#!/usr/bin/python3

import argparse
from pathlib import Path

import pod5 as p5


def main():
    parser = argparse.ArgumentParser("Iterate through all read ids in an pod5 file")
    parser.add_argument("input", type=Path)
    args = parser.parse_args()

    with p5.Reader(args.input) as reader:
        for read in reader.reads():
            print(f"Found read {read.read_id}")
            print(f"  Read has  {read.sample_count} samples")


if __name__ == "__main__":
    main()
