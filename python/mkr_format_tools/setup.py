"""
mkr_format_tools setup.py
Proprietary and confidential information of Oxford Nanopore Technologies plc
All rights reserved; (c)2022: Oxford Nanopore Technologies plc

This script can either install a development version of mkr_format_tools to the current
Python environment, or create a Python wheel.

Note that this is *not* intended to be run from within the "mkr_format_tools" folder of
the mkr_file_format repository, because the libraries are
not actually installed there. See INSTALL.md for further details.

"""

import os
from pathlib import Path
import platform
from setuptools import find_packages, setup
from setuptools.dist import Distribution
import sys

PYTHON_ROOT = Path(__file__).resolve().parent

sys.path.insert(0, str(PYTHON_ROOT / "mkr_format_tools"))
from _version import __version__

del sys.path[0]

setup(
    name="mkr_format_tools",
    version=__version__,
    description="Tools for use with the MKR file format",
    author="Oxford Nanopore Technologies plc",
    author_email="support@nanoporetech.com",
    url="http://www.nanoporetech.com",
    packages=find_packages(),
    install_requires=[
        "mkr_format",
        "h5py",
    ],
)
