MKR File Format
===============

MKR is a file format for storing nanopore dna data in an easily accessible way.
The format is able to be written in a streaming manner which allows a sequencing
instrument to directly write the format.

Data in MKR is stored using [Apache Arrow](https://github.com/apache/arrow), allowing
users to consume data in many languages using standard tools.

What does this project contain
------------------------------

This project contains a core library for reading and writing MKR data, and a toolkit for
accessing this data in other languages.


Usage
-----

The MKR is bundled as a python module for easy use in scripts, a user can install using:

```bash
> pip install mkr_format
```

The python module comes with several tools to assist users with mkr files, and a python library to write custom scripts against.

Please see [examples](./python/mkr_format/examples) for documentation on using the library.

Tools
-----

### mkr-convert-fast5

Generate an mkr file from a set of input fast5 files:

```bash
> mkr-convert-fast5 input_fast5_1.fast5 input_fast5_2.fast5 output_mkr_file.mkr
```

### mkr-inspect

Inspect an mkr file to extract details about the contents:

```bash
> mkr-inspect mkr_file.mkr
```

Development
-----------

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
> sudo apt install cmake libzstd-dev libzstd-dev libboost-dev libboost-filesystem-dev libflatbuffers-dev
# Finally start build of MKR:
> mkdir build
> cd build
> cmake ..
> make -j
```
