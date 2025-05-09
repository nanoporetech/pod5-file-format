"""
Tool for recovering truncated pod5 files
"""

import dataclasses
import typing
from contextlib import suppress
from pathlib import Path

import lib_pod5 as p5b
import pod5 as p5
from pod5.tools.parsers import prepare_pod5_recover_argparser, run_tool
from pod5.tools.utils import collect_inputs


@dataclasses.dataclass
class RecoveredData:
    """
    Holds info about recovered data.
    """

    signal_rows: int = 0
    reads: int = 0
    run_infos: int = 0
    files_with_errors: int = 0


def is_file_ok(path: Path) -> bool:
    try:
        with p5.Reader(path):
            pass
        return True
    except RuntimeError:
        return False


def recover_pod5(
    inputs: typing.List[Path], force_overwrite: bool, recursive: bool, cleanup: bool
):
    """
    Given a list of truncated pod5 files, recover their data.
    """

    paths = collect_inputs(inputs, recursive=recursive, pattern=["*.tmp", "*.pod5"])

    paths_to_recover = [p for p in paths if not is_file_ok(p)]

    if len(paths_to_recover) == 0:
        print(f"None of the {len(paths)} files given need recovery")
        return

    for path in paths_to_recover:
        dest = path.parent / (path.stem + "_recovered.pod5")
        if dest.exists():
            if force_overwrite:
                dest.unlink()
            else:
                raise FileExistsError(
                    f"Output files already exists and --force-overwrite not set. "
                    f"Refusing to overwrite {dest}."
                )

    recovered_data = RecoveredData()
    options = p5b.RecoverFileOptions()
    options.cleanup = cleanup
    for input_file in paths_to_recover:
        dest = input_file.parent / (input_file.stem + "_recovered.pod5")
        try:
            details = p5b.recover_file(
                str(input_file.resolve()), str(dest.resolve()), options
            )
            recovered_data.signal_rows += details.row_counts.signal
            recovered_data.run_infos += details.row_counts.run_info
            recovered_data.reads += details.row_counts.reads
            print(f"{dest} - Recovered")
            for cleanup_error in details.cleanup_errors:
                print(
                    "Warning cleanup failed to cleanup file "
                    + f"'{cleanup_error.file_path}' due to : {cleanup_error.description}"
                )
        except RuntimeError as error:
            recovered_data.files_with_errors += 1
            print(f"{dest} - Recovery failed - {str(error)}")
            with suppress(FileNotFoundError):
                dest.unlink()

    print(
        f"Recovered {recovered_data.signal_rows} signal rows, "
        f"{recovered_data.reads} reads, "
        f"{recovered_data.run_infos} run infos, "
        f"{recovered_data.files_with_errors} files with errors"
    )


def main():
    run_tool(prepare_pod5_recover_argparser())


if __name__ == "__main__":
    main()
