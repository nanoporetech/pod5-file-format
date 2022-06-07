POD5 File Format
===============

POD5 is a file format for storing nanopore dna data in an easily accessible way.
The format is able to be written in a streaming manner which allows a sequencing
instrument to directly write the format.

Data in POD5 is stored using [Apache Arrow](https://github.com/apache/arrow), allowing
users to consume data in many languages using standard tools.

What does this project contain
------------------------------

This project contains a core library for reading and writing POD5 data, and a toolkit for
accessing this data in other languages.


Usage
-----

POD5 is also bundled as a python module for easy use in scripts, a user can install using:

```bash
> pip install pod5_format
```

This python module provides the python library to write custom scripts against.

Please see [examples](./python/pod5_format/pod5_format/examples) for documentation on using the library.

Tools
-----

POD5 also provides a selection of tools.

```bash
> pip install pod5_format_tools
```

### pod5-convert-fast5

Generate an pod5 file from a set of input fast5 files:

```bash
> pod5-convert-fast5 input_fast5_1.fast5 input_fast5_2.fast5 ./output_pod5_files/
```

### pod5-inspect

Inspect an pod5 file to extract details about the contents:

```bash
> pod5-inspect reads pod5_file.pod5

# Sample Output:
read_id,channel,well,pore_type,read_number,start_sample,end_reason,median_before,calibration_offset,calibration_scale,sample_count,byte_count,signal_compression_ratio
00445e58-3c58-4050-bacf-3411bb716cc3,908,1,not_set,100776,374223800,signal_positive,205.3,-240.0,0.1,65582,58623,0.447
00520473-4d3d-486b-86b5-f031c59f6591,220,1,not_set,7936,16135986,signal_positive,192.0,-233.0,0.1,167769,146495,0.437
00563354-f067-43c9-9ad9-4895d4e2ff5c,2436,3,not_set,177417,374462953,signal_positive,236.8,-244.0,0.1,51498,51076,0.496
0056df1d-2767-491e-b56f-33e8699253e3,582,3,not_set,12365,16122713,signal_positive,194.4,-247.0,0.1,24783,22121,0.446
00589487-8847-4716-b224-bd2028027051,1050,2,not_set,5453,16079976,signal_positive,191.5,-213.0,0.1,139300,131981,0.474
...
```

Inspect an pod5 file for a specific read:

```bash
> pod5-inspect read pod5_file.pod5 00445e58-3c58-4050-bacf-3411bb716cc3

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

Design
------

For information about the design of POD5, see the [docs](./docs/README.md).

Development
-----------

If you want to contribute to pod5_file_format, or our pre-built binaries do not meet your platform requirements, you can build pod5 from source using the instructions in [DEV.md](DEV.md)
