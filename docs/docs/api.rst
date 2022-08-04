============
Introduction
============

The `pod5-format` python module can be used to read and write nanopore reads. It wraps
the compiled C-API bindings of the same name.

This page provides a quick introduction to the `pod5-format API`` with some examples and 
comprehensive usage instructions.

Install
-------

The ``pod5-format`` package can be installed as shown below::
   
   pip install pod5_format

This python package will contain the underlying C-API already compiled for the most 
common OS platforms and should thererfore work out of the box.

Open a POD5 File
----------------

To use the module to open a combined POD5 file, create a 
:class:`~pod5_format.reader.CombinedReader` instance::

    from pod5_format import CombinedReader

    combined_reader = CombinedReader("combined.pod5")

Similarly, for split POD5 files where the signal data and read information are stored
in separate files, create a :class:`~pod5_format.reader.SplitReader` instance::

    from pod5_format import SplitReader

    split_reader = SplitReader("signal.pod5", "reads.pod5")

It is strongly recommended that users use python's ``with`` statement to ensure that
the opened resources are safely closed when they are no longer needed::

    from pod5_format import CombinedReader

    with CombinedReader("combined.pod5") as combined_reader:
        # Use combined_reader within the context manager

Note that both :class:`~pod5_format.reader.CombinedReader` and 
:class:`~pod5_format.reader.SplitReader` are sub-classes of :class:`~pod5_format.reader.Reader`
which contains the working logic to read POD5 files. The sub-classes are included
to explicitly provide support for both combined and split POD5 file use-cases.

Iterate Over Reads
------------------

With an open :class:`~pod5_format.reader.Reader`, to iterate over all reads in a POD5 file, 
call :func:`~pod5_format.reader.Reader.reads` to generate a
:class:`~pod5_format.reader.ReadRecord` instance for each read in the file::

    import pod5_format as p5

    with p5.CombinedReader("combined.pod5") as combined_reader:
        for read_record in combined_reader.reads():
            print(read_record.read_id)

To iterate over a filtered selection of read_ids, provide :func:`~pod5_format.reader.Reader.reads`
with an iterable of read_ids as strings::

    import pod5_format as p5

    # Create a collection of curated read_ids
    read_ids = {
        "00445e58-3c58-4050-bacf-3411bb716cc3",
        "00520473-4d3d-486b-86b5-f031c59f6591",
    }

    with p5.CombinedReader("combined.pod5") as combined_reader:
        for read_record in combined_reader.reads(read_ids):
            assert read_record.read_id in read_ids

