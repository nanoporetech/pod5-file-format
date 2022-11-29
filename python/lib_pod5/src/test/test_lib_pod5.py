"""
Basic lib pod5 tets
"""

from pathlib import Path

from lib_pod5 import Pod5FileReader, create_file, open_file


def test_create_file(tmp_path: Path) -> None:
    """Test that a lib-pod5 can create a pod5 file"""

    target = tmp_path / "test.pod5"
    assert tmp_path.exists()
    assert not target.exists()

    create_file(str(target), "test")
    assert target.exists()


def test_open_file(tmp_path: Path) -> None:
    """Test that a lib-pod5 can create and re-open a pod5 file"""
    target = tmp_path / "test.pod5"
    create_file(str(target), "test")
    assert target.exists()

    reader = open_file(str(target))
    assert isinstance(reader, Pod5FileReader)

    reader.close()
