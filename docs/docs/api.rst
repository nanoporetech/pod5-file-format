=====================
Getting Started
=====================

The pod5-format python module can be used to read and write nanopore reads stored
in POD5 files.

This page provides a quick introduction to the pod5-format API with introductory examples.

Please refer to the :ref:`installation documentation <docs/install:Install>` for details
on how to install the pod5-format packages.

Reading POD5 Files
========================

POD5 data can come in two forms, combined and split, please head to 
:ref:`format overview <DESIGN:Summary>` for more information on the distinction 
between these forms.

To use the module to open a combined POD5 file, create a 
:class:`~pod5_format.reader.CombinedReader` instance:

.. code-block:: python

    from pod5_format import CombinedReader

    combined_reader = CombinedReader("combined.pod5")


Similarly, for split POD5 files where the signal data and read information are stored
in separate files, create a :class:`~pod5_format.reader.SplitReader` instance:

.. code-block:: python

    from pod5_format import SplitReader

    split_reader = SplitReader("signal.pod5", "reads.pod5")

It is strongly recommended that users use python's 
`with statement <https://docs.python.org/3/reference/compound_stmts.html#the-with-statement>`_ 
to ensure that any opened resources (e.g. file handles) are safely closed when they are 
no longer needed. 

.. code-block:: python

    from pod5_format import CombinedReader

    with CombinedReader("combined.pod5") as combined_reader:
        # Use combined_reader within this context manager
        ...
    # Resources are safely closed

Note that both :class:`~pod5_format.reader.CombinedReader` and 
:class:`~pod5_format.reader.SplitReader` are sub-classes of :class:`~pod5_format.reader.Reader`
which contains the working logic to read POD5 files. The sub-classes are included
to provide explicit support for both combined and split POD5 use cases.

Iterate Over Reads
------------------

With an open :class:`~pod5_format.reader.Reader` call :func:`~pod5_format.reader.Reader.reads` 
to generate a :class:`~pod5_format.reader.ReadRecord` instance for each read in the file:

.. code-block:: python

    import pod5_format as p5

    with p5.CombinedReader("combined.pod5") as combined_reader:
        for read_record in combined_reader.reads():
            print(read_record.read_id)

To iterate over a `filtered` selection of read_ids, provide :func:`~pod5_format.reader.Reader.reads`
with a collection of read_ids:

.. code-block:: python

    import pod5_format as p5

    # Create a collection of read_ids
    read_ids = {
        "00445e58-3c58-4050-bacf-3411bb716cc3",
        "00520473-4d3d-486b-86b5-f031c59f6591",
    }

    with p5.CombinedReader("combined.pod5") as combined_reader:
        for read_record in combined_reader.reads(read_ids):
            assert str(read_record.read_id) in read_ids


Reads and ReadRecords
---------------------

Nanopore sequencing data comprises Reads which are formed from signal data and other 
metadata about how and when the sample was sequenced. This data is accessible via the 
:class:`~pod5_format.pod5_types.Read` or :class:`~pod5_format.reader.ReadRecord` classes.

Although these two classes have very similar interfaces, know that the 
:class:`~pod5_format.reader.ReadRecord` is a `Read`
formed from a POD5 file record which uses caching to improve read performance.

.. note::
    
    There will likely be revisions to this beta implementation to unify these similar
    classes into a common interface.

Here are some of the most important members of a :class:`~pod5_format.reader.ReadRecord`.
Please read the :class:`~pod5_format.reader.ReadRecord` API reference for the complete set.

.. currentmodule:: pod5_format.reader

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
    Example use of pod5_format to plot the signal data from a selected read.
    """

    import matplotlib.pyplot as plt
    import numpy as np

    import pod5_format as p5

    # Using the example combined pod5 file provided
    combined_pod5 = "test_data/multi_fast5_zip.pod5"
    selected_read_id = '0000173c-bf67-44e7-9a9c-1ad0bc728e74'

    with p5.CombinedReader(combined_pod5) as reader:

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
    :ref:`pod5-format-tools <docs/tools:Tools>` package for tools to manipulate 
    existing datasets.
    
    New tools may be added to support our users and if you have a suggestion for a 
    new tool please submit a request on the `pod5-file-format GitHub issues page <p5_git_>`_.

Writing Reads to a POD5 File
----------------------------

To create a new combined POD5 file one must first generate **all** of the required read data
fields of which there are over 30. These fields are grouped into container classes
such as :class:`~pod5_format.pod5_types.RunInfo` and :class:`~pod5_format.pod5_types.Calibration`.

In many cases, instances of these container classes are shared among many reads. 
As such, each unique instance of these classes are stored in the POD5 file only once using a
`Dictionary Array <https://arrow.apache.org/docs/python/data.html#dictionary-arrays>`_.
It is the index of this object in the dictionary array which is stored alongside 
the read records. 

When writing reads to a POD5 one should use the :py:class:`~pod5_format.writer.Writer` 
method :py:meth:`~pod5_format.writer.Writer.add`, to add new instances of these 
container classes and to get their respective indices which are passed as parameters
to :py:meth:`~pod5_format.writer.Writer.add_read` or :py:meth:`~pod5_format.writer.Writer.add_reads`. 

However, if one first creates a :class:`~pod5_format.pod5_types.Read`
object and calls the :py:meth:`~pod5_format.writer.Writer.add_read_object` method 
the `Writer` will automatically manage the dictionary array indices.

Adding Reads Example
---------------------

Below is a conceptual example of how one may either add reads to a POD5 using
:py:meth:`~pod5_format.writer.Writer.add` and :py:meth:`~pod5_format.writer.Writer.add_read`
or by using the :py:meth:`~pod5_format.writer.Writer.add_read_object` method.

.. code-block:: python

    import pod5_format as p5

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

    # Create a new combined POD5 file and open a writer handle to it
    with p5.CombinedWriter("combined.pod5") as writer:
        writer.add_read(
            read_id=UUID("0000173c-bf67-44e7-9a9c-1ad0bc728e74"),
            end_reason=writer.add(end_reason), # Add new item and return the index
            calibration=writer.add(calibration),
            pore=writer.add(pore),
            run_info=writer.add(run_info),
            ...
            signal=signal,
            sample_count=len(signal),
            pre_compressed_signal=False
        )

    # Alternatively, one can create Read objects and have the writer manage indices
    # of its members
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

    with p5.CombinedWriter("combined.pod5") as writer:
        # Write the read object and all of its members
        writer.add_read_object(read)

