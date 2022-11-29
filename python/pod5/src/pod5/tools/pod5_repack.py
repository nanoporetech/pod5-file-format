"""
Tool for repacking pod5 files into a single output
"""
import sys
import typing
from pathlib import Path

import pod5 as p5
import pod5.repack
from pod5.tools.parsers import prepare_pod5_repack_argparser, run_tool


def repack_pod5(inputs: typing.List[Path], output: Path, force_overwrite: bool):
    """Given a list of pod5 files, repack their contents"""
    print(f"Repacking inputs {' '.join(str(i) for i in inputs)} into {output}")

    if not output.exists():
        output.mkdir(parents=True, exist_ok=True)

    repacker = pod5.repack.Repacker()

    writers: typing.List[p5.Writer] = []
    for input_filename in inputs:
        reader = p5.Reader(input_filename)

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

        writer = p5.Writer(output_filename)
        writers.append(writer)
        output_ref = repacker.add_output(writer)

        # Add all reads to the repacker
        repacker.add_all_reads_to_output(output_ref, reader)

    repacker.wait()

    for writer in writers:
        writer.close()


def main():
    run_tool(prepare_pod5_repack_argparser())


if __name__ == "__main__":
    main()
