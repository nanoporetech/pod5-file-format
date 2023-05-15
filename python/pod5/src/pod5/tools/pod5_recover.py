"""
Tool for recovering truncated pod5 files
"""
import dataclasses
import typing
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


def do_consistency_check(path: Path, recovered: RecoveredData) -> bool:
    """
    Count the number of recoverd records in `path` updating `recovered`.
    Returns True is this file was successfully recovered
    """
    try:
        with p5.Reader(path) as file:
            for i in range(file.signal_table.num_record_batches):
                batch = file.signal_table.get_batch(i)
                recovered.signal_rows += batch.num_rows

            for i in range(file.run_info_table.num_record_batches):
                batch = file.run_info_table.get_batch(i)
                recovered.run_infos += batch.num_rows

            for i in range(file.read_table.num_record_batches):
                batch = file.read_table.get_batch(i)
                recovered.reads += batch.num_rows
    except RuntimeError:
        recovered.files_with_errors += 1
        return False
    return True


def is_file_ok(path: Path) -> bool:
    try:
        with p5.Reader(path):
            pass
        return True
    except RuntimeError:
        return False


def recover_pod5(inputs: typing.List[Path], force_overwrite: bool, recursive: bool):
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
    for input_file in paths_to_recover:
        dest = path.parent / (path.stem + "_recovered.pod5")
        # recover_file returns us an open writer:
        f = p5b.recover_file(str(input_file.resolve()), str(dest.resolve()))

        # We don't want to write any additional data:
        f.close()

        # Check how consistent the recovered file is:
        success = do_consistency_check(dest.resolve(), recovered_data)
        if not success:
            print(f"{dest} - Recovery failed")
            dest.unlink()
        else:
            print(f"{dest} - Recovered")

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
