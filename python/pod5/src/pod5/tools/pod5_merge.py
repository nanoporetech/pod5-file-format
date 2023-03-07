"""
Tool for merging pod5 files
"""

import os
import typing
from pathlib import Path
from more_itertools import chunked

import pod5 as p5
import pod5.repack as p5_repack
from pod5.tools.parsers import prepare_pod5_merge_argparser, run_tool
from tqdm import tqdm

# Default number of files to merge at a time
DEFAULT_CHUNK_SIZE = 100


def assert_no_duplicate_reads(paths: typing.Iterable[Path]) -> None:
    """
    Raise AssertionError if we detect any duplicate read_ids in the pod5 files given.
    """
    read_ids = set()
    for path in paths:
        with p5.Reader(path) as reader:
            for read in reader.reads():
                if read.read_id in read_ids:
                    raise AssertionError(
                        "Duplicate read_id detected but --duplicate_ok not set"
                    )
                read_ids.add(read.read_id)


def merge_pod5(
    inputs: typing.Iterable[Path],
    output: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    duplicate_ok: bool = False,
    force_overwrite: bool = False,
) -> None:
    """
    Merge the an iterable of input pod5 paths into the specified output path
    """

    if output.exists():
        if force_overwrite:
            output.unlink()
        else:
            raise FileExistsError(
                f"Output files already exists and --force_overwrite not set. "
                f"Refusing to overwrite {output}."
            )

    if not output.parent.exists():
        output.parent.mkdir(parents=True, exist_ok=True)

    # Assert inputs exist
    non_existent_inputs = [path for path in inputs if not path.is_file()]
    if non_existent_inputs:
        raise FileExistsError(f"Some input(s) do not exist: {non_existent_inputs}")

    if not duplicate_ok:
        assert_no_duplicate_reads(inputs)

    # Open the output file writer
    with p5.Writer(output.absolute()) as writer:

        # Attach the writer to the repacker
        repacker = p5_repack.Repacker()
        repacker_output = repacker.add_output(writer)

        inputs = list(inputs)
        chunks = list(chunked(inputs, chunk_size))

        disable_pbar = not bool(int(os.environ.get("POD5_PBAR", 1)))
        pbar = tqdm(
            total=len(inputs),
            ascii=True,
            disable=disable_pbar or len(chunks) == 1,
            unit="Files",
        )

        for chunk in chunks:
            # Submit each reader handle to the repacker
            readers = [p5.Reader(path) for path in chunk]
            for reader in readers:
                repacker.add_all_reads_to_output(repacker_output, reader)

            # blocking wait for the repacker to complete merging inputs
            repacker.wait(finish=False, show_pbar=len(chunks) == 1, leave_pbar=True)

            # Close all the input handles
            for reader in readers:
                reader.close()

            pbar.update(len(chunk))

        repacker.finish()
    return


def main():
    """pod5_merge main program"""
    run_tool(prepare_pod5_merge_argparser())


if __name__ == "__main__":
    main()
