#!/usr/bin/python3

import argparse
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("output_csv", type=Path)
    parser.add_argument("--select-ratio", type=float)

    args = parser.parse_args()

    df = pd.read_csv(args.input_csv)

    selected_rows_df = df.sample(frac=args.select_ratio)

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    selected_rows_df.to_csv(args.output_csv)


if __name__ == "__main__":
    main()
