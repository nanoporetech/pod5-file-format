Design documentation for POD5
============================

The POD5 file format has been specifically designed to be suitable for Nanopore read data, we had some specific design goals:

Design Goals
------------

The primary purpose of this file format is store reads produced by Oxford Nanopore sequencing, and in particular the signal data from those reads (which can then be basecalled or processed in other ways).

This file format has the following design goals:

- Good write performance for MinKNOW
- Recoverable if the writing process crashes
- Good read performance for downstream tools, including basecall model generation
- Efficient use of space
- Straightforward to implement and maintain
- Extensibility

Note that trade-offs have been made between these goals, but we have mostly aimed to make those run-time decisions.

We have also chosen not to optimise for editing existing files.

More detailed information around general format goals can be found in [DESIGN](./DESIGN.md), more detailed format specification is available in [SPECIFICATION](./SPECIFICATION.md).
