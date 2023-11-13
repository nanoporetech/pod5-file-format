"""
Tool for merging pod5 files
"""

from time import sleep
from typing import Iterable
from pathlib import Path
from tqdm.auto import tqdm

import pod5 as p5
import pod5.repack as p5_repack
from pod5.tools.parsers import prepare_pod5_merge_argparser, run_tool
from pod5.tools.utils import (
    DEFAULT_THREADS,
    PBAR_DEFAULTS,
    collect_inputs,
    init_logging,
    logged_all,
)

logger = init_logging()


@logged_all
def merge_pod5(
    inputs: Iterable[Path],
    output: Path,
    duplicate_ok: bool = False,
    force_overwrite: bool = False,
    recursive: bool = False,
    threads: int = DEFAULT_THREADS,
    readers: int = 5,
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

    _inputs = collect_inputs(
        inputs, recursive=recursive, pattern="*.pod5", threads=threads
    )

    print(f"Merging reads from {len(_inputs)} files")
    logger.debug(f"Merging reads from {len(_inputs)} files into {output.absolute()}")

    # Open the output file writer
    with p5.Writer(output.absolute()) as writer:
        # Attach the writer to the repacker
        repacker = p5_repack.Repacker()
        repacker_output = repacker.add_output(writer, not duplicate_ok)

        pbar = tqdm(
            total=len(_inputs),
            desc="Merging",
            unit="File",
            leave=True,
            position=0,
            **PBAR_DEFAULTS,
        )

        active_limit = max(readers, 1)
        logger.debug(f"{active_limit=}")

        opened_readers = 0
        active = 0
        while _inputs or active > 0:
            pbar.update(opened_readers - active - pbar.n)

            active = repacker.currently_open_file_reader_count
            if _inputs and (active < active_limit):
                next_input = _inputs.pop()
                logger.debug(f"submitting: {next_input=}")
                with p5.Reader(next_input) as reader:
                    opened_readers += 1
                    repacker.add_all_reads_to_output(repacker_output, reader)
                    continue

            if not _inputs:
                logger.debug("no inputs remaining - finishing")
                repacker.set_output_finished(repacker_output)
                break

            sleep(0.2)
            logger.debug(f"{len(_inputs)=}, {active=}, {active>0=}")

        repacker.finish()
        del repacker
        pbar.update(opened_readers - active - pbar.n)
        pbar.close()

    return


def main():
    """pod5_merge main program"""
    run_tool(prepare_pod5_merge_argparser())


if __name__ == "__main__":
    main()
