"""
pod5_format setup.py
Proprietary and confidential information of Oxford Nanopore Technologies plc
All rights reserved; (c)2022: Oxford Nanopore Technologies plc

This script can either install a development version of pod5_format to the current
Python environment, or create a Python wheel.

Note that this is *not* intended to be run from within the "pod5_format" folder of
the pod5_file_format repository, because the libraries are
not actually installed there. See INSTALL.md for further details.

"""

import os
from pathlib import Path
import platform
from setuptools import find_packages, setup
from setuptools.dist import Distribution
import sys

PYTHON_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PYTHON_ROOT.parent

sys.path.insert(0, str(PYTHON_ROOT / "pod5_format"))
from _version import __version__

del sys.path[0]


data_files = ["pod5_format_pybind.*"]

# We need to force setuptools to think we have platform-specific
# extensions (which we do), because it won't notice on its own.
# See http://stackoverflow.com/a/36886459/6103219
class BinaryDistribution(Distribution):
    def has_ext_modules(_):
        return True


#####################################
# Add distribution-specific options #
#####################################

extra_setup_args = {}

if "bdist_wheel" in sys.argv:
    # We need to convince distutils to put pyguppy in a platform-dependent
    # location (as opposed to a "universal" one) or auditwheel will complain
    # later. This is a hack to get it to do that.
    # See https://github.com/pypa/auditwheel/pull/28#issuecomment-212082647
    from distutils.command.install import install

    class BinaryInstall(install):
        def __init__(self, dist):
            super().__init__(dist)
            # We should be able to set install_lib = self.install_platlib
            # but that doesn't appear to work on OSX or Linux, so we have to do this.
            if platform.system() != "Windows":
                self.install_lib = ""
            else:
                self.install_lib = self.install_platlib

    extra_setup_args["cmdclass"] = {"install": BinaryInstall}


setup(
    name="pod5_format",
    version=__version__,
    description="Python bindings for the POD5 file format",
    author="Oxford Nanopore Technologies plc",
    author_email="support@nanoporetech.com",
    url="https://github.com/nanoporetech/pod5-file-format",
    packages=find_packages(),
    package_data={"pod5_format": data_files},
    install_requires=[
        "iso8601",
        "more_itertools",
        "numpy >= 1.20.0",
        "packaging",
        "pyarrow ~= 7.0.0",
        "pytz",
    ],
    distclass=BinaryDistribution,
    **extra_setup_args,
)
