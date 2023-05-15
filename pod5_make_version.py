"""
Write the POD5Version.cmake file by inspecting the _version.py file created by
setuptools_scm
"""

from pathlib import Path
from _version import __version__, __version_tuple__


def create_pod5_version_cmake():
    """Use the _version.py output from setuptools_scm to define the pod5 version"""
    with (Path(__file__).parent / "cmake/POD5Version.cmake").open("w") as _fh:
        vtup = __version_tuple__
        _fh.writelines(
            [
                "# Created by pod5_make_version.py \n",
                f"set(POD5_VERSION_MAJOR {vtup[0]} )\n",
                f"set(POD5_VERSION_MINOR {vtup[1]} )\n",
                f"set(POD5_VERSION_REV {vtup[2]} )\n",
                f"set(POD5_NUMERIC_VERSION {vtup[0]}.{vtup[1]}.{vtup[2]} )\n",
                f"set(POD5_FULL_VERSION {__version__} ) \n",
            ]
        )


def copy_version_py():
    """Copy the _version.py file into the lib_pod5 and pod5 project directories"""
    vpy = Path("_version.py")
    assert vpy.exists()

    lib_vpy = Path(__file__).parent / "python/lib_pod5/src/lib_pod5/_version.py"
    api_vpy = Path(__file__).parent / "python/pod5/src/pod5/_version.py"

    if lib_vpy.exists():
        lib_vpy.unlink()

    if api_vpy.exists():
        api_vpy.unlink()

    lib_vpy.write_bytes(vpy.read_bytes())
    api_vpy.write_bytes(vpy.read_bytes())


if __name__ == "__main__":
    print(f"Writing POD5Version.cmake with version: {__version__}")
    create_pod5_version_cmake()

    print("Copying _version.py into lib_pod5 and pod5 source directories")
    copy_version_py()
