.. Pod5 File Format documentation master file

Pod5 File Format Documentation
==============================

:Date: |today|
:Version: |version|


POD5 is a file format for storing nanopore sequencing data in an easily accessible way.
The format is able to be written in a streaming manner which allows a sequencing
instrument to directly write the format.

Data in POD5 is stored using `Apache Arrow <https://github.com/apache/arrow>`_, allowing
users to consume data in many languages using standard tools.

This project contains a core library for reading and writing POD5 data, and a toolkit
for accessing this data in other languages.

Install
-------

POD5 is also bundled as a python module for easy use in scripts, a user can install using:

To install the latest python release, type:

.. code-block:: console

   $ pip install pod5

This python module provides the python library to write custom scripts against.
See the :ref:`Installation Documentation <docs/install:Install>` for further details.

.. toctree::
   :caption: Contents
   :maxdepth: 2

   docs/install.rst
   docs/api.rst
   docs/dataset.rst
   docs/tools.rst

.. toctree::
   :caption: Reference Documentation
   :maxdepth: 1
   :titlesonly:

   reference/api/pod5.rst
   reference/tools/pod5_tools.rst
   DESIGN.md
   SPECIFICATION.md


Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
