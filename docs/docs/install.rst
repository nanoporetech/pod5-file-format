=======
Install
=======

Install pod5
============

The most recent release of the  ``pod5`` package can be installed using pip_

.. code-block:: console

   $ pip install pod5

This package depends upon ``lib_pod5`` which provides the OS-dependent C++ pod5 library.
The ``lib_pod5`` package will be installed automatically py pip_ and as such,
common OS platforms and should work out of the box.


Installation from Source
========================

If you want to contribute to pod5 or if our pre-built binaries
do not meet your platform requirements, you can build pod5 from source using the
instructions below.

Developing with Conan
---------------------

For this development process you will need `conan <https://conan.io/>`_ installed.
You can install ``conan<2`` using ``pip`` or your platforms' package manager (e.g. ``brew``)
You will also need the python ``setuptools_scm==7.1.0`` package to manage pod5 versioning:

.. code-block:: console

   # Conan and python build dependencies
   $ pip install "conan<2" build
   $ conan --version
   Conan version 1.48.0

.. code-block:: console

   # Get the repo and submodules
   $ git clone https://github.com/nanoporetech/pod5-file-format.git
   $ cd pod5-file-format
   $ git submodule update --init --recursive

.. code-block:: console

   # Creates _version.py and cmake/POD5Version.cmake which are ignored by git
   $ pip install setuptools_scm==7.1.0
   $ python -m setuptools_scm
   $ python -m pod5_make_version

.. code-block:: console

   # Install libs for a Release build using the system default compiler + settings:
   # Note the build=missing, will build any libs not available on your current platform as binaries:
   $ mkdir build
   $ cd build
   $ conan install --build=missing -s build_type=Release .. && cmake -DENABLE_CONAN=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake .. && make -j

Arm 64 MacOS Builds
+++++++++++++++++++

.. note::

   On OSX arm builds, an extra argument may be needed to make cmake build an arm64
   build on all OSX platforms:

.. code-block:: console

   $ cmake -DENABLE_CONAN=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_OSX_ARCHITECTURES=arm64 ..


Developing
----------

Building the project requires that several tools and libraries are available

* `CMake <https://cmake.org/>`_
* Arrow_
* `Zstd <https://github.com/facebook/zstd#build-instructions>`_
* `Boost <https://www.boost.org/>`_
* `Flatbuffers <https://google.github.io/flatbuffers/>`_


.. code-block:: console

   $ sudo apt install -y -V ca-certificates lsb-release wget
   $ wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   $ sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   $ sudo apt update

   # Now install the rest of the dependencies:
   $ sudo apt install cmake libzstd-dev libzstd-dev libboost-dev libboost-filesystem-dev libflatbuffers-dev

   # Finally start build of POD5:
   $ git clone https://github.com/nanoporetech/pod5-file-format.git
   $ cd pod5-file-format
   $ git submodule update --init --recursive
   $ mkdir build
   $ cd build
   $ cmake ..
   $ make -j


Python Development Setup
========================

After completing the required :ref:`build <docs/install:Installation from Source>` stages above,
to create a Python virtual environment for development use the ``Makefile`` in
the ``python`` directory to install the two pod5 python packages including all ``dev``
dependencies such as ``pre-commit`` and ``black``.

.. code-block:: console

   $ cd python
   $ make install

Note this will completely ``clean`` the existing python virtual environment. If one only
requires an update to the python environment entry-points (e.g. ``pod5 inspect``) simply
run:

.. code-block:: console

   $ make update


Installing Pre-commit Hooks
---------------------------

The project uses ``pre-commit`` to ensure code is consistently formatted, you can set this
up using ``pip`` but if you chose to use the supplied ``Makefile`` this will be done
automatically as :ref:`detailed above <docs/install:Python Development Setup>`

.. code-block:: console

   $ cd pod5-file-format

   # Install pre-commit hooks in your pod5-file-format repo:
   $ pip install pre-commit==v2.21.0
   $ pre-commit install

   # Run hooks on all files:
   $ pre-commit run --all-files
