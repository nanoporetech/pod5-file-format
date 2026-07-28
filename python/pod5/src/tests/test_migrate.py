import os
import subprocess
import sys
from pathlib import Path

TEST_DATA_PATH = Path(__file__).parent.parent.parent.parent.parent / "test_data"


class TestMigrate:
    """Test migrate pod5 files to the latest version"""

    def test_migration_env_var(self, tmp_path: Path) -> None:
        """Test that POD5_MIGRATION_TMP_DIR env var is respected during migration"""

        # Setup env variables
        env = os.environ.copy()
        env["POD5_MIGRATION_TMP_DIR"] = str(tmp_path)

        # Run the subprocess with POD5_MIGRATION_TMP_DIR set to tmp_path
        args = [
            sys.executable,
            "./python/pod5/src/tests/test_script/check_migration_tmp_dir.py",
        ]
        rc = subprocess.run(
            args,
            env=env,
        ).returncode
        assert rc == 0

    def test_migration_tmp_dir_bad_input(self) -> None:
        """Test that POD5_MIGRATION_TMP_DIR env var fails gracefully when set to a file instead of a directory"""

        # Setup env variables
        env = os.environ.copy()
        env["POD5_MIGRATION_TMP_DIR"] = str(Path(__file__))

        # Run the subprocess with POD5_MIGRATION_TMP_DIR set to a file (invalid directory)
        args = [
            sys.executable,
            "./python/pod5/src/tests/test_script/check_migration_tmp_dir_bad_input.py",
        ]
        rc = subprocess.run(
            args,
            env=env,
        ).returncode
        assert rc == 0
