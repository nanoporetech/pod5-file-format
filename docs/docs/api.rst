=====================
Getting Started
=====================

The ``pod5`` python module can be used to read and write nanopore reads stored
in POD5 files.

This page provides a quick introduction to the pod5 API with introductory examples.

Please refer to the :ref:`installation documentation <docs/install:Install>` for details
on how to install the pod5 packages.

Reading POD5 Files
===================

To open a collection of POD5 files as a dataset use the :class:`~pod5.dataset.DatasetReader` class.
The :class:`~pod5.dataset.DatasetReader` takes one or more paths to files and/or directories.
Directories can be searched for POD5 files recursively with the `recursive` parameter.

It is strongly recommended that users use python's
`with statement <https://docs.python.org/3/reference/compound_stmts.html#the-with-statement>`_
to ensure that any opened resources (e.g. file handles) are safely closed when they are
no longer needed.

For more information on the DatasetReader refer to :ref:`Reading POD5 Datasets <docs/dataset:Reading POD5 Datasets>`

.. code-block:: python

    import pod5

    paths = ["/foo_directory/", "./bar/", "/baz/file.pod5"]
    with pod5.DatasetReader(paths, recursive=True) as dataset:
        # Use DatasetReader within this context manager
        ...

To open a single POD5 file use the :class:`~pod5.reader.Reader` class. This is a
lower-level interface and has many more functions and properties to
access the inner working of POD5 files such as the underlying arrow tables.

.. code-block:: python

    import pod5

    with pod5.Reader("example.pod5") as reader:
        # Use reader within this context manager
        ...
    # Resources are safely closed

Iterate Over Reads
------------------

With an open :class:`~pod5.reader.Reader` or :class:`~pod5.dataset.DatasetReader`
call :func:`~pod5.reader.Reader.reads` to generate a :class:`~pod5.reader.ReadRecord`
instance for each read in the file or dataset:

.. code-block:: python

    import pod5

    # Iterate over every record in the file using reads()
    with pod5.Reader("example.pod5") as reader:
        for read_record in reader.reads():
            print(read_record.read_id)

    # Iterate over every record in the dataset using __iter__
    with pod5.DatasetReader("./dataset/", recursive=True) as dataset:
        for read_record in dataset:
            print(read_record.read_id, read_record.path)

To iterate over a selection of read_ids, provide the ``reads`` method on either
the :class:`~pod5.dataset.DatasetReader` (:func:`~pod5.dataset.DatasetReader.reads`)
or  :class:`~pod5.reader.Reader` (:func:`~pod5.reader.Reader.reads`) a
collection of read_ids which must be string ``UUID``'s' :

.. note::

    The order of records returned by Reader iterators is always the order on-disk
    even when specifying a `selection` of read_ids.


.. code-block:: python

    import pod5

    # Create a collection of read_id UUIDs as string
    read_ids = {
        "00445e58-3c58-4050-bacf-3411bb716cc3",
        "00520473-4d3d-486b-86b5-f031c59f6591",
    }

    with pod5.Reader("example.pod5") as reader:
        for read_record in reader.reads(read_ids):
            assert str(read_record.read_id) in read_ids

    # An example using DatasetReader
    with pod5.DatasetReader("/path/to/dataset/") as dataset:
        other_ids = set(read_id.startswith("00") for read_id in dataset.read_ids)
        for read_record in dataset.reads(other_ids):
            assert str(read_record.read_id) in other_ids


Reads and ReadRecords
---------------------

Nanopore sequencing data comprises Reads which are formed from signal data and other
metadata about how and when the sample was sequenced. This data is accessible via the
:class:`~pod5.pod5_types.Read` or :class:`~pod5.reader.ReadRecord` classes.

Although these two classes have very similar interfaces, know that the
:class:`~pod5.reader.ReadRecord` is a `Read`
formed from a POD5 file record which uses caching to improve read performance.

Here are some of the most important members of a :class:`~pod5.reader.ReadRecord`.
Please read the :class:`~pod5.reader.ReadRecord` API reference for the complete set.

.. currentmodule:: pod5.reader

.. autosummary::

    ReadRecord
    ReadRecord.read_id
    ReadRecord.calibration
    ReadRecord.end_reason
    ReadRecord.pore
    ReadRecord.read_number
    ReadRecord.run_info
    ReadRecord.signal

Plotting Example
----------------

Here is an example of how a user may plot a read's signal data against time.

.. code-block:: python

    """
    Example use of pod5 to plot the signal data from a selected read.
    """

    import matplotlib.pyplot as plt
    import numpy as np

    import pod5

    # Using the example pod5 file provided
    example_pod5 = "test_data/multi_fast5_zip.pod5"
    selected_read_id = '0000173c-bf67-44e7-9a9c-1ad0bc728e74'

    with pod5.Reader(example_pod5) as reader:

        # Read the selected read from the pod5 file
        # next() is required here as Reader.reads() returns a Generator
        read = next(reader.reads([selected_read_id]))

        # Get the signal data and sample rate
        sample_rate = read.run_info.sample_rate
        signal = read.signal

        # Compute the time steps over the sampling period
        time = np.arange(len(signal)) / sample_rate

        # Plot using matplotlib
        plt.plot(time, signal)


Writing POD5 Files
===================

The pod5 package provides the functionality to write POD5 files. Although most
users will only need to read files produced by `Oxford Nanopore <ont_>`_ sequencers
there are certainly use cases where writing one's own POD5 files would be desirable.

.. note::

    It is strongly recommended that users first look at the
    :ref:`tools <docs/tools:Tools>` package for tools to manipulate
    existing datasets.

    New tools may be added to support our users and if you have a suggestion for a
    new tool please submit a request on the `pod5-file-format GitHub issues page <p5_git_>`_.



Adding Reads Example
---------------------

Below is an example of how one may add reads to a new POD5 file using the :py:class:`~pod5.writer.Writer`
and its :py:meth:`~pod5.writer.Writer.add_read` method.

.. code-block:: python

    import pod5 as p5

    # Example container classes for read information
    pore = p5.Pore(channel=123, well=3, pore_type="pore_type")
    calibration = p5.Calibration(offset=0.1, scale=1.1)
    end_reason = p5.EndReason(name=p5.EndReasonEnum.SIGNAL_POSITIVE, forced=False)
    run_info = p5.RunInfo(
        acquisition_id = ...
        acquisition_start_time = ...
        adc_max = ...
        ...
    )
    signal = ... # some signal data

    read = p5.Read(
        read_id=UUID("0000173c-bf67-44e7-9a9c-1ad0bc728e74"),
        end_reason=end_reason,
        calibration=calibration,
        pore=pore,
        run_info=run_info,
        ...
        signal=signal,
        sample_count=len(signal),
        pre_compressed_signal=False,
    )

    with p5.Writer("example.pod5") as writer:
        # Write the read and all its metadata
        writer.add_read(read)
