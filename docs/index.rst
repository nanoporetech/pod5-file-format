.. Pod5 File Format documentation master file

Welcome to Pod5 File Format documentation!
===========================================

POD5 is a file format for storing nanopore dna data in an easily accessible way.
The format is able to be written in a streaming manner which allows a sequencing
instrument to directly write the format.

Data in POD5 is stored using `Apache Arrow <https://github.com/apache/arrow>`_, allowing
users to consume data in many languages using standard tools.

This project contains a core library for reading and writing POD5 data, and a toolkit 
for accessing this data in other languages.

Install
-------

POD5 is also bundled as a python module for easy use in scripts, a user can install using:

To install the latest python release, type::
   
   pip install pod5_format

This python module provides the python library to write custom scripts against.


Contents
--------

.. toctree::
   :maxdepth: 2
   :titlesonly:

   docs/api.rst

API Reference
-------------

.. toctree::
   :maxdepth: 2
   :titlesonly:

   reference/api/modules.rst
   reference/tools/modules.rst
   

Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`