"""
lib_pod5 setup.py
Proprietary and confidential information of Oxford Nanopore Technologies plc
All rights reserved; (c)2022: Oxford Nanopore Technologies plc

This script can either install a development version of pod5 to the current
Python environment, or create a Python wheel.

Note that this is *not* intended to be run from within the "pod5" folder of
the pod5_file_format repository, because the libraries are
not actually installed there. See INSTALL.md for further details.

"""
import os
import sys

import setuptools

extra_setup_args = {}

if "bdist_wheel" in sys.argv:
    # We need to convince distutils to put lib-pod5 in a platform-dependent
    # location (as opposed to a "universal" one) or auditwheel will complain
    # later. This is a hack to get it to do that.
    # See https://github.com/pypa/auditwheel/pull/28#issuecomment-212082647
    import platform
    from distutils.command.install import install

    from wheel.bdist_wheel import bdist_wheel

    class BinaryInstall(install):
        def __init__(self, dist):
            super().__init__(dist)
            # We should be able to set install_lib = self.install_platlib
            # but that doesn't appear to work on OSX or Linux, so we have to do this.
            if platform.system() != "Windows":
                self.install_lib = ""
            else:
                self.install_lib = self.install_platlib

    class BdistWheel(bdist_wheel):
        def finalize_options(self):
            bdist_wheel.finalize_options(self)
            self.root_is_pure = False

        def get_tag(self):
            python, abi, plat = bdist_wheel.get_tag(self)
            if "FORCE_PYTHON_PLATFORM" in os.environ:
                plat = os.environ["FORCE_PYTHON_PLATFORM"]
            return python, abi, plat

    extra_setup_args["cmdclass"] = {"install": BinaryInstall, "bdist_wheel": BdistWheel}


if __name__ == "__main__":
    setuptools.setup(
        has_ext_modules=lambda: True,
        **extra_setup_args,
    )
