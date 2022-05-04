#!/usr/bin/python3

import argparse
import sys

import pandas as pd
from pandas.testing import assert_frame_equal


def check_consistency(df1, df2):
    df1 = df1.sort_values("read_id", ignore_index=True)
    df2 = df2.sort_values("read_id", ignore_index=True)

    assert_frame_equal(df1, df2)

    print("Data frames are consistent")
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_a")
    parser.add_argument("input_b")

    args = parser.parse_args()

    a = pd.read_csv(args.input_a)
    b = pd.read_csv(args.input_b)

    print(f"Check consistency of files {args.input_a} and {args.input_b}")
    check_consistency(a, b)


if __name__ == "__main__":
    main()
