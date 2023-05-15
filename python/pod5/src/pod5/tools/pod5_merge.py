"""
Tool for merging pod5 files
"""

from typing import Iterable, Set
from pathlib import Path
from tqdm.auto import tqdm

import pod5 as p5
import pod5.repack as p5_repack
from pod5.tools.parsers import prepare_pod5_merge_argparser, run_tool
from pod5.tools.utils import (
    PBAR_DEFAULTS,
    collect_inputs,
    init_logging,
    logged_all,
)

logger = init_logging()


@logged_all
def assert_no_duplicate_reads(paths: Iterable[Path]) -> int:
    """
    Raise AssertionError if we detect any duplicate read_ids in the pod5 files given.
    """

    read_ids: Set[str] = set()
    for path in paths:
        msg = f"Duplicate read_ids detected in {path.name} but --duplicate-ok not set"

        with p5.Reader(path) as reader:
            ids = reader.read_ids
            set_ids = set(ids)

            if len(ids) != len(set_ids):
                raise AssertionError(msg)

            if not read_ids.isdisjoint(set_ids):
                raise AssertionError(msg)

            read_ids.update(set_ids)

    return len(read_ids)


@logged_all
def merge_pod5(
    inputs: Iterable[Path],
    output: Path,
    duplicate_ok: bool = False,
    force_overwrite: bool = False,
    recursive: bool = False,
) -> None:
    """
    Merge the an iterable of input pod5 paths into the specified output path
    """

    if output.exists():
        if force_overwrite:
            output.unlink()
        else:
            raise FileExistsError(
                f"Output files already exists and --force-overwrite not set. "
                f"Refusing to overwrite {output}."
            )

    if not output.parent.exists():
        output.parent.mkdir(parents=True, exist_ok=True)

    inputs = collect_inputs(inputs, recursive=recursive, pattern="*.pod5")

    if not duplicate_ok:
        total_reads = assert_no_duplicate_reads(inputs)
    else:
        total_reads = 0
        for path in inputs:
            with p5.Reader(path) as reader:
                total_reads += reader.num_reads

    print(f"Merging {total_reads} reads from {len(inputs)} files")

    # Open the output file writer
    with p5.Writer(output.absolute()) as writer:
        # Attach the writer to the repacker
        repacker = p5_repack.Repacker()
        repacker_output = repacker.add_output(writer)
        prev = 0

        pbar = tqdm(
            total=len(inputs),
            desc="Merging",
            unit="Files",
            leave=True,
            position=0,
            **PBAR_DEFAULTS,
        )

        # Copy all reads from each input
        for path in inputs:
            with p5.Reader(path) as reader:
                pbar2 = tqdm(
                    total=reader.num_reads,
                    desc=reader.path.name,
                    unit="Reads",
                    leave=False,
                    position=1,
                    **PBAR_DEFAULTS,
                )

                repacker.add_all_reads_to_output(repacker_output, reader)
                for n_written in repacker.waiter():
                    pbar2.update(n_written - prev)
                    prev = n_written

                pbar2.close()
                pbar.update()

        repacker.finish()
        del repacker
        pbar.close()

    return


def main():
    """pod5_merge main program"""
    run_tool(prepare_pod5_merge_argparser())


if __name__ == "__main__":
    main()
