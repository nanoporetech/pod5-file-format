import sys
from pathlib import Path

import lib_pod5 as p5b


TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent.parent / "test_data"


def _collect_migration_tmp_matches(root: Path, version_count: int) -> set[Path]:
    matches: set[Path] = set()
    for count in range(version_count):
        matches.update(root.glob(f".tmp_pod5_v{count}_v{count+1}_migration_*"))
    return matches


def main():
    """Test migrate pod5 files to the latest version"""
    expected_path = Path(".")
    VERSION_COUNT = 5
    original_matches = _collect_migration_tmp_matches(expected_path, VERSION_COUNT)

    file = p5b.open_file(str(TEST_DATA_PATH / "multi_fast5_zip_v0.pod5"))
    new_matches = (
        _collect_migration_tmp_matches(expected_path, VERSION_COUNT) - original_matches
    )

    # Only 3 matches expected as only v0, v1, v2 undergo physical migration
    assert (
        len(new_matches) == 3
    ), f"Expected 3 new tmp migration files, found {len(new_matches)}"
    file.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
