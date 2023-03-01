Development
===========

If you want to contribute to pod5_file_format, or our pre-built binaries do not meet your platform requirements, you can build pod5 from source using the instructions in `docs/install.rst`.

### Developing

Building the project requires several tools and libraries are available:

- CMake
- Arrow
- Zstd
- Boost
- Flatbuffers

```bash
# Docs on installing arrow from here: https://arrow.apache.org/install/
> sudo apt install -y -V ca-certificates lsb-release wget
> wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
> sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
> sudo apt update
# Now install the rest of the dependencies:
> sudo apt install cmake libzstd-dev libzstd-dev libboost-dev libboost-filesystem-dev libflatbuffers-dev libarrow-dev=8.0.0-1
# Finally start build of POD5:
> git clone https://github.com/nanoporetech/pod5-file-format.git
> cd pod5-file-format
> git submodule update --init --recursive
> mkdir build
> cd build
> cmake ..
> make -j
```

### Pre commit

The project uses pre-commit to ensure code is consistently formatted, you can set this up using pip:

```bash
> pip install pre-commit==v2.21.0
# Install pre-commit hooks in your pod5-file-format repo:
> cd pod5-file-format
> pre-commit install
# Run hooks on all files:
> pre-commit run --all-files
```

Python Development
==================

After completing the required build stages above, to create a Python virtual environment for development follow the instructions below .

```bash

> cd python
> make install

```
