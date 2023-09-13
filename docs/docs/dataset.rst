=====================
Reading POD5 Datasets
=====================

Continuing from the :ref:`Reading POD5 Files introduction <docs/api:Reading POD5 Files>`

The :class:`~pod5.dataset.DatasetReader` aims to provide a reader interface for a
collection of POD5 files which is iterable (see :func:`~pod5.dataset.DatasetReader.reads`)
and indexable by read_id (see :func:`~pod5.dataset.DatasetReader.get_read`).

Loading a Dataset
-----------------

The :class:`~pod5.dataset.DatasetReader` parses all directory and file paths supplied to
the class constructor. This argument supports strings, paths or a collection of both.

.. code-block:: python

    from pathlib import Path
    import pod5

    # Load a single POD5 file
    with pod5.DatasetReader("string.pod5") as dataset:
        assert dataset.paths == [Path("string.pod5")]

    # Load a directory of POD5 files (without searching sub-directories)
    with pod5.DatasetReader(Path("/my/dataset")) as dataset:
        ...

    # Recursively load multiple directories and a specific file
    paths = [Path("/my/dataset"), Path("../other/dataset"), "extra.pod5"]
    with pod5.DatasetReader(paths, recursive=True) as dataset:
        ...


Iterate Over Reads
------------------

As shown in the introduction the :class:`~pod5.dataset.DatasetReader` class implements
the `__iter__` method which yields all :class:`~pod5.reader.ReadRecord`'s in the dataset.
This method is equivalent to calling :func:`~pod5.dataset.DatasetReader.reads` with no
parameters. A selection of read_ids can be passed to :func:`~pod5.dataset.DatasetReader.reads`
if only some records are required.

This is the most efficient way to inspect a large number of records.

.. note::

    The order of records returned by Reader iterators is always the order on-disk
    even when specifying a `selection` of read_ids.

.. code-block:: python

    import pod5

    with pod5.DatasetReader("./dataset/", recursive=True) as dataset:
        # Iterate over every `ReadRecord` in the dataset
        for read_record in dataset.reads():
            ...

        # This is equivalent to calling .reads()
        for read_record in dataset:
            ...

        # Example: Iterate over a random selection of records
        selection = random.sample(dataset.read_ids, 5)
        for read_record in dataset.reads(selection=selection)
            # Order of read_records is the on-disk record order
            ...


Random Access to Records
------------------------

The :func:`~pod5.dataset.DatasetReader.get_read` method allows users to select any record in
the dataset by read_id.

To efficiently access records from a POD5 dataset, the :class:`~pod5.dataset.DatasetReader`
will index the read_ids of all records and cache POD5 :class:`~pod5.reader.Reader` instances.

.. code-block:: python

    import pod5

    with pod5.DatasetReader("./dataset/") as dataset:
        read_id = "00445e58-3c58-4050-bacf-3411bb716cc3"

        # Dataset indexing will take place here
        read_record = dataset.get_read(read_id)

        # Returned object might be None if read_id is not found
        if read_record is None:
            print(f"dataset does not contain read_id: {read_id}")

        assert str(read_record.read_id) == read_id


Indexing Records
~~~~~~~~~~~~~~~~

If necessary, the :class:`~pod5.dataset.DatasetReader` will index every record
in the dataset. This may consume a significant amount of memory
depending on the size of the dataset. Indexing is only done when a call to a function
which requires the index is made. The functions which require the index are :func:`~pod5.dataset.DatasetReader.get_read`,
:func:`~pod5.dataset.DatasetReader.get_path`, and :func:`~pod5.dataset.DatasetReader.has_duplicate`.
To clear the index, freeing the memory, call :func:`~pod5.dataset.DatasetReader.clear_index`.


Cached Readers
~~~~~~~~~~~~~~

The :class:`~pod5.dataset.DatasetReader` class uses an LRU cache of :class:`~pod5.reader.Reader`
instances to reduce the overhead of repeatedly re-opening POD5 files. The size of this
cache can be controlled by setting `max_cached_readers` during initialisation.

Where possible, users should maximise the likelihood of a :class:`~pod5.reader.Reader`
cache hit by ensuring that successive calls to :func:`~pod5.dataset.DatasetReader.get_read`
access records in no more POD5s than the size of the LRU cache.
Randomly indexing read_ids into many files will result in repeatedly
opening the underlying files which will severely affect performance.

Duplicate Records
=================

For a typical dataset sourced from a sequencer, it is vanishingly unlikely that
there will be a single duplicate UUID read_id. It is much more likely that some
POD5 files in a dataset may be copied, merged, subset etc and that loading all files
in a dataset especially using the `recursive` mode will find duplicate read_ids.

The :class:`~pod5.dataset.DatasetReader` handles duplicate records in **iterators** by
returning all copies as they appear on disk. Similarly, having duplicate read_ids in
the `selection` will repeatedly return duplicates.

The :class:`~pod5.dataset.DatasetReader` handles duplicate records when **indexing** by
returning a **random** valid :class:`~pod5.reader.ReadRecord` if it exists or `None`.
The :class:`~pod5.reader.ReadRecord` instance returned is random because the indexing
process is multi-threaded.

If a duplicate read_id is detected, the API will issue a warning. This can be
disabled with `warn_duplicate_indexing=False` in the initialiser.


DatasetReader Reference
=======================

.. currentmodule:: pod5.dataset

.. autoclass:: DatasetReader
    :members:
    :undoc-members:
    :noindex:
