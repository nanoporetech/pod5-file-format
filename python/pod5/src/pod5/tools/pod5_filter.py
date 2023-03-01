"""
Tool for subsetting pod5 files into one or more outputs using a list of read ids
"""


from pathlib import Path
from typing import List, Set
from uuid import UUID

from pod5.tools.parsers import prepare_pod5_filter_argparser, run_tool
from pod5.tools.pod5_subset import calculate_transfers, launch_subsetting


def parse_ids(ids_path: Path) -> Set[str]:
    """Parse the list of read_ids checking all are valid uuids"""
    read_ids = set([])
    with ids_path.open("r") as _fh:
        for line_no, raw in enumerate(_fh.readlines()):
            line = raw.strip()
            if not line or line.startswith("#") or line == "read_id":
                continue

            try:
                # Check that all lines are valid uuids
                UUID(line)
                read_ids.add(line)
            except ValueError as exc:
                raise RuntimeError(
                    f'Invalid UUID read_id on line {line_no} - "{raw}"'
                ) from exc
    return read_ids


def filter_pod5(
    inputs: List[Path],
    output: Path,
    ids: Path,
    missing_ok: bool,
    duplicate_ok: bool,
    force_overwrite: bool,
) -> None:
    """Prepare the pod5 filter mapping and run the repacker"""

    # Remove output file
    if output.exists():
        if not force_overwrite:
            raise FileExistsError(
                f"Output file already exists and --force_overwrite not set - {output}"
            )
        else:
            output.unlink()

    # Create parent directories if they do not exist
    if not output.parent.exists():
        output.parent.mkdir(parents=True, exist_ok=True)

    read_ids = parse_ids(ids)
    if len(read_ids) == 0:
        raise AssertionError("Selected 0 read_ids. Nothing to do")

    resolved_targets = {_id: set([output]) for _id in read_ids}

    # Map the target outputs to which source read ids they're comprised of
    transfers = calculate_transfers(
        inputs=list(inputs),
        read_targets=resolved_targets,
        missing_ok=missing_ok,
        duplicate_ok=duplicate_ok,
    )

    print("Subsetting please wait...")
    launch_subsetting(transfers=transfers)

    print("Done")

    return


def main():
    """pod5 filter main"""
    run_tool(prepare_pod5_filter_argparser())


if __name__ == "__main__":
    main()
