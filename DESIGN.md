MKR File Format Design Details
==============================

The MKR file format has been specifically designed to be suitable for Nanopore read data, we had some specific design goals:

Design Goals
------------

The primary purpose of this file format is store reads produced by Oxford Nanopore sequencing, and in particular the signal data from those reads (which can then be basecalled or processed in other ways).

This file format has the following design goals (roughly in priority order):

- Good write performance for MinKNOW
- Recoverable if the writing process crashes
- Good read performance for downstream tools, including basecall model generation
- Efficient use of space
- Straightforward to implement and maintain
- Extensibilty

Note that trade-offs have been made between these goals, but we have mostly aimed to make those run-time decisions.

We have also chosen not to optimise for editing existing files.


### Write performance

The aspects of this format that are designed to maximise write performance are:

    Data can be written sequentially
        The sequential access pattern makes it easy to use efficient operating system APIs (such as io_uring on Linux)
        The sequential access pattern helps the operating system's I/O scheduler maximise throughput
    Signal data from different reads can be interleaved, and data streams can be safely abandoned (at the cost of using more space than necessary)
        This allows MinKNOW to write out data as it arrives, potentially avoiding the need have an intermediate caching format (this file format can be used for the cache and the final output)
    Support for space- and CPU-efficient compression routines (VBZ)
        This reduces the amount of data that needs to be written, which reduces I/O load

### Recovery

The aspects of this format that are designed to allow for recovery if the writing process crashes are:

- A way to indicate that a file is actually complete as intended (complete files end with a recognisable footer)
- The Apache Feather format can be assembled by reading it sequentially, without using the footer
- The data file format is append-only, which means that once data is recorded it cannot be corrupted by later updates

### Read performance

The aspects of this format that are designed to maximise read performance are:

- The Apache Feather format can be memory mapped and used directly
- Apache Arrow has significant existing engineering work geared around efficient access to data, from the layout of the data itself to the library tooling
- Storing direct information about signal data locations with the row table
  - This allows quick access to a read's data without scanning the data file
- It is possible to only decode part of a long read, due to read data being stored in chunks
  - This is useful for model training
- Read access does not require locking or otherwise modifying the file
  - This allows multi-threaded and multi-process access to a file for reading

### Efficient use of space

The aspects of this format that are designed to maximise use of space are:

- Support for efficient compression routines (VBZ)
- Apache Arrow's support for dictionary encoding
- Apache Arrow's support for compressing buffers with standard compression routines

### Ease of implementation

The aspects of this format that are designed to make the format easy to implement are:

- Relying on an existing, widely-used format (Apache Arrow)

### Extensibility

The aspects of this format that are designed to make the format extensible are:

- Apache Arrow uses a self-describing schema with named columns, so it is straightfoward to write code that is resilient in the face of things like additional columns being added.


Format Specification
--------------------

### Overview

The file format is, at its core, a collection of Apache Arrow tables, stored in the Apache Feather 2 (also know as Apache Arrow IPC File) format. These can be stored separately, linked by having a common filename component, or bundled into a single file for ease of file management.

In its unbundled format, there are two required files:

```
    <prefix>_signal.arrow
    <prefix>_reads.arrow
```

Optionally, index files can also be provided:

```
    <preifx>_index_read_id.arrow (index by read_id)
    <preifx>_index_*.arrow (optional, extension point for other indexes)
```

Each of these is an Apache Feather 2 file, and can be opened directly using the Apache Arrow library's IPC routines. The schema of the tables is described below. The naming scheme above is recommended (and should be the default when creating these files), but tooling should provide a way for users to explicitly every filename when opening files (in case the user has renamed them to a different scheme).

These can be stored in a bundled file, named <prefix>.mkr and described below.