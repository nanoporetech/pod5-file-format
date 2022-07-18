POD5 File Format
===============

POD5 is a file format for storing nanopore dna data in an easily accessible way.
The format is able to be written in a streaming manner which allows a sequencing
instrument to directly write the format.

Data in POD5 is stored using [Apache Arrow](https://github.com/apache/arrow), allowing
users to consume data in many languages using standard tools.

What does this project contain
------------------------------

This project contains a core library for reading and writing POD5 data, and a toolkit for
accessing this data in other languages.


Usage
-----

POD5 is also bundled as a python module for easy use in scripts, a user can install using:

```bash
> pip install pod5_format
```

This python module provides the python library to write custom scripts against.

Please see [examples](./python/pod5_format/pod5_format/examples) for documentation on using the library.

Tools
-----

POD5 also provides [a selection of tools](./python/README.md) which can be installed with:

```bash
> pip install pod5_format_tools
```

Design
------

For information about the design of POD5, see the [docs](./docs/README.md).

Development
-----------

If you want to contribute to pod5_file_format, or our pre-built binaries do not meet your platform requirements, you can build pod5 from source using the instructions in [DEV.md](DEV.md)
