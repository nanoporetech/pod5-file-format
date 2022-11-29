import argparse
import itertools
import sys
from pathlib import Path

import pod5 as p5


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_a", type=Path)
    parser.add_argument("input_b", type=Path)

    args = parser.parse_args()

    file_a = p5.Reader(args.input_a)
    file_b = p5.Reader(args.input_b)

    fields = [
        "read_number",
        "start_sample",
        "median_before",
        "pore",
        "calibration",
        "end_reason",
        "run_info",
    ]

    errors = 0
    read_count = 0
    for a, b in itertools.zip_longest(file_a.reads(), file_b.reads()):
        read_count += 1

        if a.read_id != b.read_id:
            print(
                f"Different reads found in file at row {read_count}: {a.read_id} vs {b.read_id}"
            )
            errors += 1

        read_id = a.read_id

        for field in fields:
            a_val = getattr(a, field)
            b_val = getattr(b, field)
            # Handle NAN specially:
            if a_val != a_val:
                if b_val == b_val:
                    print(
                        f"Read {read_id}: Field {field} not equal: {a_val} vs {b_val}"
                    )
                    errors += 1
            else:
                if a_val != b_val:
                    print(
                        f"Read {read_id}: Field {field} not equal: {a_val} vs {b_val}"
                    )
                    errors += 1

        if (a.signal != b.signal).any():
            print(
                f"Read {read_count} {read_id} signal not equal: {len(a.signal)} elements:"
                f" {a.signal} vs {len(b.signal)} elements: {b.signal}"
            )
            errors += 1

    if errors == 0:
        print("Files consistent")
        sys.exit(0)

    print("Errors detected")
    sys.exit(1)


if __name__ == "__main__":
    main()
