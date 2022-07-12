""" 
Tool for repacking pod5 files into a single output
"""
import argparse
from pathlib import Path

import random
import sys
import time

import pod5_format.repack


def open_file(input_filename):
    return pod5_format.open_combined_file(input_filename)


def repack(inputs, output: Path, force_overwrite: bool):
    print(f"Repacking inputs {' '.join(str(i) for i in inputs)} into {output}")

    repacker = pod5_format.repack.Repacker()

    outputs = []
    for input_filename in inputs:
        input_file = open_file(input_filename)

        output_filename = output / input_filename.name
        output_filename.parent.mkdir(parents=True, exist_ok=True)

        if output_filename.exists():
            if force_overwrite:
                if output_filename == input_filename:
                    print(
                        f"Refusing to overwrite {input_filename} - output directory is the same as input directory"
                    )
                    sys.exit(1)
                # Otherwise remove the output path
                output_filename.unlink()

            else:
                print("Refusing to overwrite output  without --force-overwrite")
                sys.exit(1)

        output = pod5_format.create_combined_file(output_filename)
        outputs.append(output)
        output_ref = repacker.add_output(output)

        # Add all reads to the repacker
        repacker.add_all_reads_to_output(output_ref, input_file)

    # Wait for repacking to complete:
    start_time = time.time()
    last_time = time.time()
    last_bytes_complete = 0
    while not repacker.is_complete:
        time.sleep(15)

        new_bytes_complete = repacker.reads_sample_bytes_completed
        bytes_delta = new_bytes_complete - last_bytes_complete
        last_bytes_complete = new_bytes_complete

        time_now = time.time()
        time_delta = time_now - last_time
        last_time = time_now

        mb_per_sec = (bytes_delta / (1000 * 1000)) / time_delta

        pct_complete = 100 * (repacker.batches_completed / repacker.batches_requested)

        print(
            f"{pct_complete:.1f} % complete",
            repacker.batches_completed,
            repacker.batches_requested,
            repacker.pending_batch_writes,
            f"{mb_per_sec:.1f} MB/s",
        )

    bytes_complete = repacker.reads_sample_bytes_completed
    time_delta = time.time() - start_time

    mb_per_sec = (bytes_complete / (1000 * 1000)) / time_delta
    print(
        f"100.0 % complete",
        repacker.batches_completed,
        repacker.batches_requested,
        f"{mb_per_sec:.1f} MB/s",
    )

    repacker.finish()
    for output in outputs:
        output.close()


def main():
    parser = argparse.ArgumentParser("Repack a pod5 files into a single output")

    parser.add_argument(
        "input", type=Path, nargs="+", help="Input pod5 file(s) to repack"
    )
    parser.add_argument("output", type=Path, help="Output path for pod5 files")

    parser.add_argument(
        "--force-overwrite", action="store_true", help="Overwrite destination files"
    )

    args = parser.parse_args()

    repack(args.input, args.output, args.force_overwrite)


if __name__ == "__main__":
    main()
