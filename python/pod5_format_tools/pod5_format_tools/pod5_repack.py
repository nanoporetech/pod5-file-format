""" 
Tool for repacking pod5 files into a single output
"""
import argparse
from pathlib import Path

import random
import time

import pod5_format.repack


def open_file(input_filename):
    return pod5_format.open_combined_file(input_filename)


def repack(inputs: list[Path], output: Path):
    print(f"Repacking inputs {' '.join(str(i) for i in inputs)} into {output}")

    output.parent.mkdir(parents=True, exist_ok=True)

    # Create 10 output files we can write some reads to:
    inputs = [open_file(i) for i in inputs]
    outputs = []
    for i in range(10):
        filename = Path(str(output) + str(i))
        if filename.exists():
            filename.unlink()
        outputs.append(pod5_format.create_combined_file(filename))

    repacker = pod5_format.repack.Repacker()

    # Add all output files to the repacker
    output_refs = [repacker.add_output(output) for output in outputs]

    # Grab random 10% of input file and throw it in each output file
    for i in inputs:
        print("get read ids")
        read_ids = []
        for batch in i.read_batches():
            read_ids.extend(pod5_format.format_read_ids(batch.read_id_column))
        sampled_read_ids = random.sample(read_ids, int(len(read_ids) / 10))
        print("Find read locations")

        for output_ref in output_refs:
            repacker.add_selected_reads_to_output(output_ref, i, sampled_read_ids)

    # Wait for repacking to complete:
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

    repacker.finish()
    for _output in outputs:
        _output.close()


def main():
    parser = argparse.ArgumentParser("Repack a pod5 files into a single output")

    parser.add_argument("input", type=Path, nargs="+")
    parser.add_argument("output", type=Path)

    parser.add_argument(
        "--force-overwrite", action="store_true", help="Overwrite destination files"
    )

    args = parser.parse_args()

    if args.output.exists():
        if args.force_overwrite:
            args.output.unlink()
        else:
            print("Refusing to overwrite output without --force-overwrite")
            return

    repack(args.input, args.output)


if __name__ == "__main__":
    main()
