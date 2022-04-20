MKR File Format
===============

MKR is a file format for storing nanopore dna data in an easily accessible way.
The format is able to be written in a streaming manner which allows a sequencing
instrument to directly write the format.

Data in MKR is stored using [Apache Arrow](https://github.com/apache/arrow), allowing
users to consume data in many languages using standard tools.

What does this project contain
------------------------------

This project contains a core library for reading and writing MKR data, and a toolkit for
accessing this data in other languages.