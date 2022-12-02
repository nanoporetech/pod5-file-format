=====================
Getting Started
=====================

The ``pod5`` python module can be used to read and write nanopore reads stored
in POD5 files.

This page provides a quick introduction to the pod5-format API with introductory examples.

Please refer to the :ref:`installation documentation <docs/install:Install>` for details
on how to install the pod5-format packages.

Reading POD5 Files
========================


To use the module to open a POD5 file, create a :class:`~pod5.reader.Reader`.
It is strongly recommended that users use python's
`with statement <https://docs.python.org/3/reference/compound_stmts.html#the-with-statement>`_
to ensure that any opened resources (e.g. file handles) are safely closed when they are
no longer needed.

.. code-block:: python

    import pod5 as p5

    with p5.Reader("example.pod5") as reader:
        # Use reader within this context manager
        ...
    # Resources are safely closed

Iterate Over Reads
------------------

With an open :class:`~pod5.reader.Reader` call :func:`~pod5.reader.Reader.reads`
to generate a :class:`~pod5.reader.ReadRecord` instance for each read in the file:

.. code-block:: python

    import pod5 as p5

    with p5.Reader("example.pod5") as reader:
        for read_record in reader.reads():
            print(read_record.read_id)

To iterate over a selection of read_ids, provide :func:`~pod5.reader.Reader.reads`
with a collection of read_ids which must be string ``UUID``'s' :

.. code-block:: python

    import pod5 as p5

    # Create a collection of read_id UUIDs as string
    read_ids = {
        "00445e58-3c58-4050-bacf-3411bb716cc3",
        "00520473-4d3d-486b-86b5-f031c59f6591",
    }

    with p5.Reader("example.pod5") as reader:
        for read_record in reader.reads(read_ids):
            assert str(read_record.read_id) in read_ids


Reads and ReadRecords
---------------------

Nanopore sequencing data comprises Reads which are formed from signal data and other
metadata about how and when the sample was sequenced. This data is accessible via the
:class:`~pod5.pod5_types.Read` or :class:`~pod5.reader.ReadRecord` classes.

Although these two classes have very similar interfaces, know that the
:class:`~pod5.reader.ReadRecord` is a `Read`
formed from a POD5 file record which uses caching to improve read performance.

.. note::

    There will likely be revisions to this beta implementation to unify these similar
    classes into a common interface.

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

    import pod5 as p5

    # Using the example pod5 file provided
    example_pod5 = "test_data/multi_fast5_zip.pod5"
    selected_read_id = '0000173c-bf67-44e7-9a9c-1ad0bc728e74'

    with p5.Reader(example_pod5) as reader:

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

The pod5-format package provides the functionality to write POD5 files. Although most
users will only need to read files produced by `Oxford Nanopore <ont_>`_ sequencers
there are certainly use cases where writing ones own POD5 files would be desirable.

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
        # Write the read and all of its metadata
        writer.add_read(read)
