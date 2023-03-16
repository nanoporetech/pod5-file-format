[![Documentation Status](https://readthedocs.org/projects/pod5-file-format/badge/?version=latest)](https://pod5-file-format.readthedocs.io/)

POD5 File Format
================

POD5 File Format
================

POD5 is a file format for storing nanopore dna data in an easily accessible way.
The format is able to be written in a streaming manner which allows a sequencing
instrument to directly write the format.

Data in POD5 is stored using [Apache Arrow](https://github.com/apache/arrow), allowing
users to consume data in many languages using standard tools.

What does this project contain
------------------------------

This project contains a core library for reading and writing POD5 data, and a toolkit for
accessing this data in other languages.

Documentation
-------------

Full documentation is found at https://pod5-file-format.readthedocs.io/


Usage
-----

POD5 is also bundled as a python module for easy use in scripts, a user can install using:

```bash
> pip install pod5
```

This python module provides the python library to write custom scripts against.

Please see [examples](./python/pod5/examples) for documentation on using the library.

The `pod5` package also provides [a selection of tools](./python/pod5/README.md).


Design
------

For information about the design of POD5, see the [docs](./docs/README.md).

Development
-----------

If you want to contribute to pod5_file_format, or our pre-built binaries do not meet your platform requirements, you can build pod5 from source using the instructions in [DEV.md](DEV.md)
