"""
Tool for repacking pod5 files to potentially improve performance
"""
from concurrent.futures import ProcessPoolExecutor, as_completed
import typing
from pathlib import Path
from tqdm.auto import tqdm

import pod5 as p5
import pod5.repack
from pod5.tools.utils import (
    DEFAULT_THREADS,
    PBAR_DEFAULTS,
    assert_no_duplicate_filenames,
    collect_inputs,
    limit_threads,
)
from pod5.tools.parsers import prepare_pod5_repack_argparser, run_tool


def resolve_overwrite(src: Path, dest: Path, force: bool) -> None:
    if dest.exists():
        if dest == src:
            raise FileExistsError(f"Refusing to overwrite {src} inplace")
        if force:
            dest.unlink()
        else:
            raise FileExistsError(
                "Refusing to overwrite output without --force-overwrite"
            )


def repack_pod5_file(src: Path, dest: Path):
    """Repack the source pod5 file into dest"""
    repacker = pod5.repack.Repacker()
    with p5.Writer(dest) as writer:
        repacker_output = repacker.add_output(writer, False)
        with p5.Reader(src) as reader:
            # Add all reads to the repacker
            repacker.add_all_reads_to_output(repacker_output, reader)
        repacker.set_output_finished(repacker_output)
        repacker.finish()


def repack_pod5(
    inputs: typing.List[Path],
    output: Path,
    threads: int = DEFAULT_THREADS,
    force_overwrite: bool = False,
    recursive: bool = False,
):
    """Given a list of pod5 files, repack their contents and write files 1-1"""

    if output.exists() and not output.is_dir():
        raise ValueError(f"Output cannot be an existing file: {output}")

    # Create output directory if required
    if not output.is_dir():
        output.mkdir(parents=True, exist_ok=True)

    threads = limit_threads(threads)

    _inputs = collect_inputs(
        inputs, recursive=recursive, pattern="*.pod5", threads=threads
    )
    assert_no_duplicate_filenames(_inputs)

    # Remove existing files if required
    for input_filename in _inputs:
        output_filename = output / input_filename.name
        resolve_overwrite(input_filename, output_filename, force_overwrite)

    futures = {}
    with ProcessPoolExecutor(max_workers=threads) as executor:
        pbar = tqdm(total=len(_inputs), unit="Files", **PBAR_DEFAULTS)

        for src in _inputs:
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
