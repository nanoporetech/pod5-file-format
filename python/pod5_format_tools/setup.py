"""
pod5_format_tools setup.py
Proprietary and confidential information of Oxford Nanopore Technologies plc
All rights reserved; (c)2022: Oxford Nanopore Technologies plc

This script can either install a development version of pod5_format_tools to the current
Python environment, or create a Python wheel.

Note that this is *not* intended to be run from within the "pod5_format_tools" folder of
the pod5_file_format repository, because the libraries are
not actually installed there. See INSTALL.md for further details.

"""

from pathlib import Path
import sys

from setuptools import find_packages, setup

PYTHON_ROOT = Path(__file__).resolve().parent

sys.path.insert(0, str(PYTHON_ROOT / "pod5_format_tools"))
from _version import __version__

del sys.path[0]

setup(
    name="pod5_format_tools",
    version=__version__,
    description="Tools for use with the POD5 file format",
    author="Oxford Nanopore Technologies plc",
    author_email="support@nanoporetech.com",
    url="https://github.com/nanoporetech/pod5-file-format",
    packages=find_packages(),
    install_requires=[
        "h5py",
        "jsonschema",
        "ont-fast5-api",
        "pandas",
        "pod5_format",
    ],
)
