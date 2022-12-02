POD5 Python Package
===================

The `pod5` Python package contains the tools and python API wrapping the compiled bindings
for the POD5 file format from `lib_pod5`.

Installation
------------

The `pod5` package is available on [pypi](https://pypi.org/project/pod5/) and is
installed using `pip`:

``` console
pip install pod5
```

Reading a POD5 File
-------------------

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

Plotting Signal Data Example
----------------------------

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

Writing a POD5 File
-------------------

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

POD5 Format Tools
-----------------

The ``pod5`` package provides the following tools for inspecting and manipulating
`.pod5` files as well as converting between `.pod5` and `.fast5` file formats.

1. [pod5 update](#pod5%20update)
2. [pod5 inspect](#pod5%20inspect)
3. [pod5 subset](#pod5%20subset)
4. [pod5 repack](#pod5%20repack)
5. [pod5 convert fast5](#pod5%20convert%20fast5)
6. [pod5 convert to_fast5](#pod5%20convert%20to_fast5)

pod5 update
-----------

The `pod5 update` tool can be used to update a file in an older pod5 format to the latest available format.

```bash
# View help on pod5 update tools
> pod5 update --help
> pod5 update my-old-pod5-file.pod5 ./migrated_files/
```

pod5inspect
------------

The `pod5 inspect` tool can be used to extract details and summaries of the contents of `.pod5` files. There are three programs for users within `pod5 inspect` and these are [`reads`](#pod5%20inspect%20reads), [`read`](#pod5%20inspect%20read), and [`summary`](#pod5%20inspect%20summary),

```bash
# View help on pod5 inspect tools
> pod5 inspect --help
> pod5 inspect {reads, read, summary} --help
```

### pod5 inspect reads

Inspect all reads and print a csv table of the details of all reads in the given `.pod5` files.

```bash
> pod5 inspect reads pod5_file.pod5

# Sample Output:
read_id,channel,well,pore_type,read_number,start_sample,end_reason,median_before,calibration_offset,calibration_scale,sample_count,byte_count,signal_compression_ratio
00445e58-3c58-4050-bacf-3411bb716cc3,908,1,not_set,100776,374223800,signal_positive,205.3,-240.0,0.1,65582,58623,0.447
00520473-4d3d-486b-86b5-f031c59f6591,220,1,not_set,7936,16135986,signal_positive,192.0,-233.0,0.1,167769,146495,0.437
...
```

### pod5 inspect read

Inspect the pod5 file, find a specific read and print its details.

```bash
> pod5 inspect read pod5_file.pod5 00445e58-3c58-4050-bacf-3411bb716cc3

# Sample Output:
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

### pod5 inspect summary

Inspect the pod5 file, printing summary information on the reads in each batch

pod5 subset
----------

`pod5 subset` is a tool for separating the reads in `.pod5` files into one or more
output files. This tool can be used to create new `.pod5` files which contain a
user-defined subset of reads from the input.

The `pod5 subset` tool requires a mapping which defines which read_ids should be
written to which output. There are multiple ways of specifying this mapping which are
defined in either a `.csv` or `.json` file or by using a tab-separated table
(e.g. basecaller sequencing summary) and instructions on how to interpret it.

```bash
# View help
> pod5 subset --help

# Subset input(s) using a pre-defined mapping
> pod5 subset example_1.pod5 --csv mapping.csv
> pod5 subset examples_*.pod5 --json mapping.json

# Subset input(s) using a dynamic mapping created at runtime
> pod5 subset example_1.pod5 --summary summary.txt --columns barcode alignment_genome
```

### Important note on read_id clashes

Care should be taken to ensure that when providing multiple input `.pod5` files to `pod5 subset`
that there are no read_id UUID clashes. If this occurs both reads are written to the output.

### Creating a Subset mapping

The `.csv` or `.json` inputs should define a mapping of destination filename to an array
of read_ids which will be written to the destination.

In the example below of a `.csv` subset mapping, note that the output filename can be specified on multiple lines. This allows multi-line specifications to avoid excessively long lines.

```bash
# --csv mapping filename to array of read_id
output_1.pod5, 132b582c-56e8-4d46-9e3d-48a275646d3a, 12a4d6b1-da6e-4136-8bb3-1470ef27e311, ...
output_2.pod5, 0ff4dc01-5fa4-4260-b54e-1d8716c7f225
output_2.pod5, 0e359c40-296d-4edc-8f4a-cca135310ab2, 0e9aa0f8-99ad-40b3-828a-45adbb4fd30c
```

See below an example of a `.json` subset mapping. This file must of course be well-formatted
`json` in addition to the formatting standard required by the tool. The formatting requirements
for the `.json` subset mapping are that keys should be unique filenames mapped to an array
of read_id strings.

```json
{
    "output_1.pod5": [
        "0000173c-bf67-44e7-9a9c-1ad0bc728e74",
        "006d1319-2877-4b34-85df-34de7250a47b"
    ],
    "output_2.pod5": [
        "00925f34-6baf-47fc-b40c-22591e27fb5c",
        "009dc9bd-c5f4-487b-ba4c-b9ce7e3a711e"
    ]
}
```

### Subsetting from a summary

`pod5 subset` can dynamically generate output targets and collect associated reads
based on a tab-separated file (e.g. sequencing summary) which contains a header row
and a series of columns on which to group unique collections of values. Internally
this process uses the [`pandas.Dataframe.groupby`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html) function where the `by` parameter is the sequence of column names
specified using the ``--columns` argument.

The column names specified in `--columns` should be **categorical** in nature.
There is no restriction in-place however there may be an excessive number of output files
generated if a continuous variable was used for subsetting.

Given the following example summary file, observe the resultant outputs given various
arguments:

```text
read_id mux barcode     length
read_a  1   barcode_a   4321
read_b  1   barcode_b   1000
read_c  2   barcode_b   1200
read_d  2   barcode_c   1234
```

```bash
> pod5 subset example_1.pod5 --output barcode_subset --summary summary.txt --columns barcode
> ls barcode_subset
barcode-barcode_a.pod5 # Contains: read_a
barcode-barcode_b.pod5 # Contains: read_b, read_c
barcode-barcode_c.pod5 # Contains: read_d

> pod5 subset example_1.pod5 --output mux_subset --summary summary.txt --columns mux
> ls mux_subset
mux-1.pod5 # Contains: read_a, read_b
mus-2.pod5 # Contains: read_c, read_d

> pod5 subset example_1.pod5 --output barcode_mux_subset --summary summary.txt --columns barcode mux
> ls barcode_mux_subset
barcode-barcode_a_mux-1.pod5 # Contains: read_a
barcode-barcode_b_mux-1.pod5 # Contains: read_b
barcode-barcode_b_mux-2.pod5 # Contains: read_c
barcode-barcode_c_mux-2.pod5 # Contains: read_d
```

The output filename is generated from a template string. The automatically generated
template is the sequential concatenation of column_name-column_value followed by the
`.pod5` file extension. The user can set their own filename template using the ``--template``
argument. This argument accepts a string in the Python f-string style where the subsetting
variables are used for keyword placeholder substitution. Keywords should be placed
within curly-braces. For example:

From the examples above:

```bash
> pod5 subset example_1.pod5 --output barcode_subset --summary summary.txt --columns barcode
# default template used = "barcode-{barcode}.pod5"

> pod5 subset example_1.pod5 --output barcode_mux_subset --summary summary.txt --columns barcode mux
# default template used = "barcode-{barcode}_mux-{mux}.pod5"
```

Custom template example:
```bash
> pod5 subset example_1.pod5 --output barcode_subset --summary summary.txt --columns barcode --template "{barcode}.subset.pod5"
> ls barcode_subset
barcode_a.subset.pod5 # Contains: read_a
barcode_b.subset.pod5 # Contains: read_b, read_c
barcode_c.subset.pod5 # Contains: read_d
```

pod5 repack
-----------

`pod5 repack` will simply repack `.pod5` files into one-for-one output files of the same name.

``` bash
> pod5 repack pod5s/*.pod5 repacked_pods/
```

pod5 merge
-----------

`pod5 merge` will merge multiple `.pod5` files into one output file.

``` bash
> pod5 merge pod5s/*.pod5 merged.pod5
```


pod5 convert fast5
-----------------------

The `pod5 convert fast5` tool takes one or more `.fast5` files and converts them
to one or more `.pod5` files.

**Some content previously stored in fast5 files is not compatible with the pod5 format and will not be converted**

``` bash
# View help
> pod5 convert fast5 --help

# Convert fast5 files into a monolithic output file
> pod5 convert fast5 ./input/*.fast5 converted.pod5

# Convert fast5 files into a monolithic output in an existing directory
> pod5 convert fast5 ./input/*.fast5 outputs/
> ls outputs/
outputs/output.pod5 # default name

# Convert each fast5 to its relative converted output. The output files are written
# into the output directory at paths relatve to the path given to the
# --output-one-to-one argument. Note: This path must be a relative parent to all
# input paths.
> ls input/*.fast5
fast5_1.fast5 fast5_2.fast5 ... fast5_N.fast5
> pod5 convert fast5 ./input/*.fast5 output_pod5s --output-one-to-one input/
> ls output_pod5s/
fast5_1.pod5 fast5_2.pod5 ... fast5_N.pod5

# Note the different --output-one-to-one path which is now the current working directory.
# The new sub-directory output_pod5/input is created.
> pod5 convert fast5 ./input/*.fast5 output_pod5s --output-one-to-one ./
> ls output_pod5s/
input/fast5_1.pod5 input/fast5_2.pod5 ... input/fast5_N.pod5

```

pod5 convert to_fast5
---------------------

The `pod5 convert to_fast5` tool takes one or more `.pod5` files and converts them
to multiple `.fast5` files. The default behaviour is to write 4000 reads per output file
but this can be controlled with the `--file-read-count` argument.

``` bash
# View help
> pod5 convert to_fast5 --help

# Convert pod5 files to fast5 files with default 4000 reads per file
> pod5 convert to_fast5 example.pod5 pod5_to_fast5
> ls pod5_to_fast5/
output_1.fast5 output_2.fast5 ... output_N.fast5
```
