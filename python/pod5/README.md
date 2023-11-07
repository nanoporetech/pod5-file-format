# POD5 Python Package

The `pod5` Python package contains the tools and python API wrapping the compiled bindings
for the POD5 file format from `lib_pod5`.

## Installation

The `pod5` package is available on [pypi](https://pypi.org/project/pod5/) and is
installed using `pip`:

``` console
  > pip install pod5
```

## Usage

### Reading a POD5 File

To read a `pod5` file provide the the `Reader` class with the input `pod5` file path
and call `Reader.reads()` to iterate over read records in the file. The example below
prints the read_id of every record in the input `pod5` file.

``` python
import pod5 as p5

with p5.Reader("example.pod5") as reader:
    for read_record in reader.reads():
        print(read_record.read_id)
```

To iterate over a selection of read_ids supply `Reader.reads()` with a collection
of read_ids which must be `UUID` compatible:

``` python
import pod5 as p5

# Create a collection of read_id UUIDs
read_ids: List[str] = [
  "00445e58-3c58-4050-bacf-3411bb716cc3",
  "00520473-4d3d-486b-86b5-f031c59f6591",
]

with p5.Reader("example.pod5") as reader:
    for read_record in reader.reads(selection=read_ids):
        assert str(read_record.read_id) in read_ids
```

### Plotting Signal Data Example

Here is an example of how a user may plot a read’s signal data against time.

``` python
import matplotlib.pyplot as plt
import numpy as np

import pod5 as p5

# Using the example pod5 file provided
example_pod5 = "test_data/multi_fast5_zip.pod5"
selected_read_id = '0000173c-bf67-44e7-9a9c-1ad0bc728e74'

with p5.Reader(example_pod5) as reader:

    # Read the selected read from the pod5 file
    # next() is required here as Reader.reads() returns a Generator
    read = next(reader.reads(selection=[selected_read_id]))

    # Get the signal data and sample rate
    sample_rate = read.run_info.sample_rate
    signal = read.signal

    # Compute the time steps over the sampling period
    time = np.arange(len(signal)) / sample_rate

    # Plot using matplotlib
    plt.plot(time, signal)
```

### Writing a POD5 File

The `pod5` package provides the functionality to write POD5 files.

It is strongly recommended that users first look at the available tools when
manipulating existing datasets, as there may already be a tool to meet your needs.
New tools may be added to support our users and if you have a suggestion for a
new tool or feature please submit a request on the
[pod5-file-format GitHub issues page](https://github.com/nanoporetech/pod5-file-format/issues).

Below is an example of how one may add reads to a new POD5 file using the `Writer`
and its `add_read()` method.

```python
import pod5 as p5

# Populate container classes for read metadata
pore = p5.Pore(channel=123, well=3, pore_type="pore_type")
calibration = p5.Calibration(offset=0.1, scale=1.1)
end_reason = p5.EndReason(name=p5.EndReasonEnum.SIGNAL_POSITIVE, forced=False)
run_info = p5.RunInfo(
    acquisition_id = ...
    acquisition_start_time = ...
    adc_max = ...
    ...
)
signal = ... # some signal data as numpy np.int16 array

read = p5.Read(
    read_id=UUID("0000173c-bf67-44e7-9a9c-1ad0bc728e74"),
    end_reason=end_reason,
    calibration=calibration,
    pore=pore,
    run_info=run_info,
    ...
    signal=signal,
)

with p5.Writer("example.pod5") as writer:
    # Write the read object
    writer.add_read(read)
```

## Tools

1. [pod5 view](#pod5-view)
2. [pod5 inspect](#pod5-inspect)
3. [pod5 merge](#pod5-merge)
4. [pod5 filter](#pod5-filter)
5. [pod5 subset](#pod5-subset)
6. [pod5 repack](#pod5-repack)
7. [pod5 recover](#pod5-recover)
8. [pod5 convert fast5](#pod5-convert-fast5)
9. [pod5 convert to_fast5](#pod5-convert-to_fast5)
10. [pod5 update](#pod5-update)

The ``pod5`` package provides the following tools for inspecting and manipulating
POD5 files as well as converting between ``.pod5`` and ``.fast5`` file formats.

To disable the `tqdm <https://github.com/tqdm/tqdm>`_  progress bar set the environment
variable ``POD5_PBAR=0``.

To enable debugging output which may also output detailed log files, set the environment
variable ``POD5_DEBUG=1``

### Pod5 View

The ``pod5 view`` tool is used to produce a table similarr to a sequencing summary
from the contents of ``.pod5`` files. The default output is a tab-separated table
written to stdout with all available fields.

This tools is indented to replace ``pod5 inspect reads`` and is over 200x faster.

``` bash
> pod5 view --help

# View the list of fields with a short description in-order (shortcut -L)
> pod5 view --list-fields

# Write the summary to stdout
> pod5 view input.pod5

# Write the summary of multiple pod5s to a file
> pod5 view *.pod5 --output summary.tsv

# Write the summary as a csv
> pod5 view *.pod5 --output summary.csv --separator ','

# Write only the read_ids with no header (shorthand -IH)
> pod5 view input.pod5 --ids --no-header

# Write only the listed fields
# Note: The field order is fixed the order shown in --list-fields
> pod5 view input.pod5 --include "read_id, channel, num_samples, end_reason"

# Exclude some unwanted fields
> pod5 view input.pod5 --exclude "filename, pore_type"
```

### Pod5 inspect

The ``pod5 inspect`` tool can be used to extract details and summaries of
the contents of ``.pod5`` files. There are two programs for users within ``pod5 inspect``
and these are read and reads

``` bash
> pod5 inspect --help
> pod5 inspect {reads, read, summary} --help
```

#### Pod5 inspect reads

> :warning: This tool is deprecated and has been replaced by ``pod5 view`` which is significantly faster.

Inspect all reads and print a csv table of the details of all reads in the given ``.pod5`` files.

``` bash
> pod5 inspect reads pod5_file.pod5

  read_id,channel,well,pore_type,read_number,start_sample,end_reason,median_before,calibration_offset,calibration_scale,sample_count,byte_count,signal_compression_ratio
  00445e58-3c58-4050-bacf-3411bb716cc3,908,1,not_set,100776,374223800,signal_positive,205.3,-240.0,0.1,65582,58623,0.447
  00520473-4d3d-486b-86b5-f031c59f6591,220,1,not_set,7936,16135986,signal_positive,192.0,-233.0,0.1,167769,146495,0.437
    ...
```

#### Pod5 inspect read

Inspect the pod5 file, find a specific read and print its details.

``` console
> pod5 inspect read pod5_file.pod5 00445e58-3c58-4050-bacf-3411bb716cc3

  File: out-tmp/output.pod5
  read_id: 0e5d6827-45f6-462c-9f6b-21540eef4426
  read_number:    129227
  start_sample:   367096601
  median_before:  171.889404296875
  channel data:
  channel: 2366
  well: 1
  pore_type: not_set
  end reason:
  name: signal_positive
  forced False
  calibration:
  offset: -243.0
  scale: 0.1462070643901825
  samples:
  sample_count: 81040
  byte_count: 71989
  compression ratio: 0.444
  run info
      acquisition_id: 2ca00715f2e6d8455e5174cd20daa4c38f95fae2
      acquisition_start_time: 2021-07-23 13:48:59.780000
      adc_max: 0
      adc_min: 0
      context_tags
      barcoding_enabled: 0
      basecall_config_filename: dna_r10.3_450bps_hac_prom.cfg
      experiment_duration_set: 2880
      ...
```

### Pod5 merge

``pod5 merge`` is a tool for merging multiple  ``.pod5`` files into one monolithic pod5 file.

The contents of the input files are checked for duplicate read_ids to avoid
accidentally merging identical reads. To override this check set the argument
``-D / --duplicate-ok``

``` bash
# View help
> pod5 merge --help

# Merge a pair of pod5 files
> pod5 merge example_1.pod5 example_2.pod5 --output merged.pod5

# Merge a glob of pod5 files
> pod5 merge *.pod5 -o merged.pod5

# Merge a glob of pod5 files ignoring duplicate read ids
> pod5 merge *.pod5 -o merged.pod5 --duplicate-ok
```

### Pod5 filter

``pod5 filter`` is a simpler alternative to ``pod5 subset`` where reads are subset from
one or more input ``.pod5`` files using a list of read ids provided using the ``--ids`` argument
and writing those reads to a *single* ``--output`` file.

See ``pod5 subset`` for more advanced subsetting.

``` bash
> pod5 filter example.pod5 --output filtered.pod5 --ids read_ids.txt
```

The ``--ids`` selection text file must be a simple list of valid UUID read_ids with
one read_id per line. Only records which match the UUID regex (lower-case) are used.
Lines beginning with a ``#`` (hash / pound symbol) are interpreted as comments.
Empty lines are not valid and may cause errors during parsing.

> The ``filter`` and ``subset`` tools will assert that any requested read_ids are
> present in the inputs. If a requested read_id is missing from the inputs
> then the tool will issue the following error:
>
> ``` bash
> POD5 has encountered an error: 'Missing read_ids from inputs but --missing-ok not set'
> ```
>
> To disable this warning then the '-M / --missing-ok' argument.

When supplying multiple input files to 'filter' or 'subset', the tools is
effectively performing a ``merge`` operation. The 'merge' tool is better suited
for handling very large numbers of input files.

#### Example filtering pipeline

This is a trivial example of how to select a random sample of 1000 read_ids from a
pod5 file using ``pod5 view`` and ``pod5 filter``.

``` bash
# Get a random selection of read_ids
> pod5 view all.pod5 --ids --no-header --output all_ids.txt
> all_ids.txt sort --random-sort | head --lines 1000 > 1k_ids.txt

# Filter to that selection
> pod5 filter all.pod5 --ids 1k_ids.txt --output 1k.pod5

# Check the output
> pod5 view 1k.pod5 -IH | wc -l
1000
```

### Pod5 subset

``pod5 subset`` is a tool for subsetting reads in ``.pod5`` files into one or more
output ``.pod5`` files. See also ``pod5 filter``

The ``pod5 subset`` tool requires a *mapping* which defines which read_ids should be
written to which output. There are multiple ways of specifying this mapping which are
defined in either a ``.csv`` file or by using a ``--table`` (csv or tsv)
and instructions on how to interpret it.

``pod5 subset`` aims to be a generic tool to subset from multiple inputs to multiple outputs.
If your use-case is to ``filter`` read_ids from one or more inputs into a single output
then ``pod5 filter`` might be a more appropriate tool as the only input is a list of read_ids.

``` bash
# View help
> pod5 subset --help

# Subset input(s) using a pre-defined mapping
> pod5 subset example_1.pod5 --csv mapping.csv

# Subset input(s) using a dynamic mapping created at runtime
> pod5 subset example_1.pod5 --table table.txt --columns barcode
```

> Care should be taken to ensure that when providing multiple input ``.pod5`` files to ``pod5 subset``
> that there are no read_id UUID clashes. If a duplicate read_id is detected an exception
> will be raised unless the ``--duplicate-ok`` argument is set. If ``--duplicate-ok`` is
> set then both reads will be written to the output, although this is not recommended.

#### Note on positional arguments

> The ``--columns`` argument will greedily consume values and as such, care should be taken
> with the placement of any positional arguments. The following line will result in an error
> as the input pod5 file is consumed by ``--columns`` resulting in no input file being set.

```bash
# Invalid placement of positional argument example.pod5
$ pod5 subset --table table.txt --columns barcode example.pod5
```

#### Creating a Subset Mapping

##### Target Mapping (.csv)

The example below shows a ``.csv`` subset target mapping. Any lines (e.g. header line)
which do not have a read_id which matches the UUID regex (lower-case) in the second
column is ignored.

``` text
target, read_id
output_1.pod5,132b582c-56e8-4d46-9e3d-48a275646d3a
output_1.pod5,12a4d6b1-da6e-4136-8bb3-1470ef27e311
output_2.pod5,0ff4dc01-5fa4-4260-b54e-1d8716c7f225
output_2.pod5,0e359c40-296d-4edc-8f4a-cca135310ab2
output_2.pod5,0e9aa0f8-99ad-40b3-828a-45adbb4fd30c
```

##### Target Mapping from Table

``pod5 subset`` can dynamically generate output targets and collect associated reads
based on a text file containing a table (csv or tsv) parsible by ``polars``.
This table file could be the output from ``pod5 view`` or from a sequencing summary.
The table must contain a header row and a series of columns on which to group unique
collections of values. Internally this process uses the
`polars.Dataframe.group_by <https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/api/polars.DataFrame.group_by.html>`_
function where the ``by`` parameter is the sequence of column names specified with
the ``--columns`` argument.

Given the following example ``--table`` file, observe the resultant outputs given various
arguments:

``` text
read_id    mux    barcode      length
read_a     1      barcode_a    4321
read_b     1      barcode_b    1000
read_c     2      barcode_b    1200
read_d     2      barcode_c    1234
```

``` bash
> pod5 subset example_1.pod5 --output barcode_subset --table table.txt --columns barcode
> ls barcode_subset
barcode-barcode_a.pod5     # Contains: read_a
barcode-barcode_b.pod5     # Contains: read_b, read_c
barcode-barcode_c.pod5     # Contains: read_d

> pod5 subset example_1.pod5 --output mux_subset --table table.txt --columns mux
> ls mux_subset
mux-1.pod5     # Contains: read_a, read_b
mus-2.pod5     # Contains: read_c, read_d

> pod5 subset example_1.pod5 --output barcode_mux_subset --table table.txt --columns barcode mux
> ls barcode_mux_subset
barcode-barcode_a_mux-1.pod5    # Contains: read_a
barcode-barcode_b_mux-1.pod5    # Contains: read_b
barcode-barcode_b_mux-2.pod5    # Contains: read_c
barcode-barcode_c_mux-2.pod5    # Contains: read_d
```

##### Output Filename Templating

When subsetting using a table the output filename is generated from a template
string. The automatically generated template is the sequential concatenation of
``column_name-column_value`` followed by the ``.pod5`` file extension.

The user can set their own filename template using the ``--template`` argument.
This argument accepts a string in the `Python f-string style <https://docs.python.org/3/tutorial/inputoutput.html#formatted-string-literals>`_
where the subsetting variables are used for keyword placeholder substitution.
Keywords should be placed within curly-braces. For example:

``` bash
# default template used = "barcode-{barcode}.pod5"
> pod5 subset example_1.pod5 --output barcode_subset --table table.txt --columns barcode

# default template used = "barcode-{barcode}_mux-{mux}.pod5"
> pod5 subset example_1.pod5 --output barcode_mux_subset --table table.txt --columns barcode mux

> pod5 subset example_1.pod5 --output barcode_subset --table table.txt --columns barcode --template "{barcode}.subset.pod5"
> ls barcode_subset
barcode_a.subset.pod5    # Contains: read_a
barcode_b.subset.pod5    # Contains: read_b, read_c
barcode_c.subset.pod5    # Contains: read_d
```

##### Example subsetting from ``pod5 inspect reads``

The ``pod5 inspect reads`` tool will output a csv table summarising the content of the
specified ``.pod5`` file which can be used for subsetting. The example below shows
how to split a ``.pod5`` file by the well field.

``` bash
# Create the csv table from inspect reads
> pod5 inspect reads example.pod5 > table.csv
> pod5 subset example.pod5 --table table.csv --columns well
```

### Pod5 repack

``pod5 repack`` will simply repack ``.pod5`` files into one-for-one output files of the same name.

``` bash
> pod5 repack pod5s/*.pod5 repacked_pods/
```

### Pod5 Recover

``pod5 recover`` will attempt to recover data from corrupted or truncated ``.pod5`` files
by copying all valid table batches and cleanly closing the new files. New files are written
as siblings to the inputs with the `_recovered.pod5` suffix.

``` bash
> pod5 recover --help
> pod5 recover broken.pod5
> ls
broken.pod5 broken_recovered.pod5
```

### pod5 convert fast5

The ``pod5 convert fast5`` tool takes one or more ``.fast5`` files and converts them
to one or more ``.pod5`` files.

If the tool detects single-read fast5 files, please convert them into multi-read
fast5 files using the tools available in the ``ont_fast5_api`` project.

The progress bar shown during conversion assumes the number of reads in an input
``.fast5`` is 4000. The progress bar will update the total value during runtime if
required.

> Some content previously stored in ``.fast5`` files is **not** compatible with the POD5
> format and will not be converted. This includes all analyses stored in the
> ``.fast5`` file.
>
> Please ensure that any other data is recovered from ``.fast5`` before deletion.

By default ``pod5 convert fast5`` will show exceptions raised during conversion as *warnings*
to the user. This is to gracefully handle potentially corrupt input files or other
runtime errors in long-running conversion tasks. The ``--strict`` argument allows
users to opt-in to strict runtime assertions where any exception raised will promptly
stop the conversion process with an error.

``` bash
# View help
> pod5 convert fast5 --help

# Convert fast5 files into a monolithic output file
> pod5 convert fast5 ./input/*.fast5 --output converted.pod5

# Convert fast5 files into a monolithic output in an existing directory
> pod5 convert fast5 ./input/*.fast5 --output outputs/
> ls outputs/
output.pod5 # default name

# Convert each fast5 to its relative converted output. The output files are written
# into the output directory at paths relatve to the path given to the
# --one-to-one argument. Note: This path must be a relative parent to all
# input paths.
> ls input/*.fast5
file_1.fast5 file_2.fast5 ... file_N.fast5
> pod5 convert fast5 ./input/*.fast5 --output output_pod5s/ --one-to-one ./input/
> ls output_pod5s/
file_1.pod5 file_2.pod5 ... file_N.pod5

# Note the different --one-to-one path which is now the current working directory.
# The new sub-directory output_pod5/input is created.
> pod5 convert fast5 ./input/*.fast5 output_pod5s --one-to-one ./
> ls output_pod5s/
input/file_1.pod5 input/file_2.pod5 ... input/file_N.pod5

# Convert all inputs so that they have neibouring pod5 in current directory
> pod5 convert fast5 *.fast5 --output . --one-to-one .
> ls
file_1.fast5 file_1.pod5 file_2.fast5 file_2.pod5  ... file_N.fast5 file_N.pod5

# Convert all inputs so that they have neibouring pod5 files from a parent directory
> pod5 convert fast5 ./input/*.fast5 --output ./input/ --one-to-one ./input/
> ls input/*
file_1.fast5 file_1.pod5 file_2.fast5 file_2.pod5  ... file_N.fast5 file_N.pod5
```

### Pod5 convert to_fast5

The ``pod5 convert to_fast5`` tool takes one or more ``.pod5`` files and converts them
to multiple ``.fast5`` files. The default behaviour is to write 4000 reads per output file
but this can be controlled with the ``--file-read-count`` argument.

``` bash
# View help
> pod5 convert to_fast5 --help

# Convert pod5 files to fast5 files with default 4000 reads per file
> pod5 convert to_fast5 example.pod5 --output pod5_to_fast5/
> ls pod5_to_fast5/
output_1.fast5 output_2.fast5 ... output_N.fast5
```

### Pod5 Update

The ``pod5 update`` tools is used to update old pod5 files to use the latest schema.
Currently the latest schema version is version 3.

Files are written into the ``--output`` directory with the same name.

``` bash
> pod5 update --help

# Update a named files
> pod5 update my.pod5 --output updated/
> ls updated
updated/my.pod5

# Update an entire directory
> pod5 update old/ -o updated/
```
