import argparse
import itertools
import sys

import pod5_format


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_a")
    parser.add_argument("input_b")

    args = parser.parse_args()

    file_a = pod5_format.open_combined_file(args.input_a)
    file_b = pod5_format.open_combined_file(args.input_b)

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
            print(f"{read_id} signal not equal: {a_val} vs {b_val}")
            errors += 1

    if errors == 0:
        print("Files consistent")
        sys.exit(0)

    print("Errors detected")
    sys.exit(1)


if __name__ == "__main__":
    main()
