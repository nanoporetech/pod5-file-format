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

setup(
    name="mkr_format_tools",
    version="0.0.1",
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
