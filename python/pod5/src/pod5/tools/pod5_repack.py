"""
Tool for repacking pod5 files to potentially improve performance
"""
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import sys
import typing
from pathlib import Path
from tqdm import tqdm

import pod5 as p5
import pod5.repack
from pod5.tools.parsers import prepare_pod5_repack_argparser, run_tool


def repack_pod5_file(src: Path, dest: Path):
    """Repack the source pod5 file into dest"""
    repacker = pod5.repack.Repacker()
    with p5.Reader(src) as reader:
        with p5.Writer(dest) as writer:
            # Add all reads to the repacker
            repacker_output = repacker.add_output(writer)
            repacker.add_all_reads_to_output(repacker_output, reader)
            repacker.wait(show_pbar=False, finish=True)


def repack_pod5(
    inputs: typing.List[Path], output: Path, threads: int, force_overwrite: bool
):
    """Given a list of pod5 files, repack their contents and write files 1-1"""

    # Create output directory if required
    if not output.is_dir():
        output.mkdir(parents=True, exist_ok=True)

    # Remove existing files if required
    for input_filename in inputs:
        output_filename = output / input_filename.name

        if output_filename.exists():
            if output_filename == input_filename:
                print(f"Refusing to overwrite {input_filename} inplace")
                sys.exit(1)
            if force_overwrite:
                output_filename.unlink()
            else:
                print("Refusing to overwrite output  without --force-overwrite")
                sys.exit(1)

    disable_pbar = not bool(int(os.environ.get("POD5_PBAR", 1)))
    futures = {}
    with ProcessPoolExecutor(max_workers=threads) as executor:

        pbar = tqdm(total=len(inputs), ascii=True, disable=disable_pbar, unit="Files")

        for src in inputs:
            dest = output / src.name
            futures[executor.submit(repack_pod5_file, src=src, dest=dest)] = dest

        for future in as_completed(futures):
            tqdm.write(f"Finished {futures[future]}")
            pbar.update(1)

    pbar.close()
    print("Done")


def main():
    run_tool(prepare_pod5_repack_argparser())


if __name__ == "__main__":
    main()
