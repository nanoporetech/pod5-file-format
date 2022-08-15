
POD5 Format Specification
=========================

## Overview

The file format is, at its core, a collection of Apache Arrow tables, stored in the Apache Feather 2 (also know as Apache Arrow IPC File) format. These can be stored separately, linked by having a common filename component, or bundled into a single file for ease of file management.

In its unbundled format, there are two required files:

```
    <prefix>_signal.arrow
    <prefix>_reads.arrow
```

Optionally, index files can also be provided:

```
    <prefix>_index_read_id.arrow (index by read_id)
    <prefix>_index_*.arrow (optional, extension point for other indexes)
```

Each of these is an Apache Feather 2 file, and can be opened directly using the Apache Arrow library's IPC routines. The schema of the tables is described below. The naming scheme above is recommended (and should be the default when creating these files), but tooling should provide a way for users to explicitly every filename when opening files (in case the user has renamed them to a different scheme).

These can be stored in a bundled file, named <prefix>.pod5 and described below.

### Table Schemas

POD5 files are a custom wrapper format around arrow that contain several [arrow tables](https://arrow.apache.org/docs/python/data.html#tables).

All the tables should have the following `custom_metadata` fields set on them:

| Name                    | Example Value                        | Notes                                                                                                                                       |
|-------------------------|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| MINKNOW:pod5_version    | 1.0.0                                | The version of this specification that the schema was based on.                                                                             |
| MINKNOW:software        | MinNOW Core 5.2.3                    | A free-form description of the software that wrote the file, intended to  help pin down the source of files that violate the specification. |
| MINKNOW:file_identifier | cbf91180-0684-4a39-bf56-41eaf437de9e | Must be identical across all tables. Allows checking that the files correspond to each other.                                               |

### Extension Types 

Several fields in the table schemas use [custom arrow types](https://arrow.apache.org/docs/python/data.html#custom-schema-and-field-metadata).

#### minknow.uuid

The schemas make extensive use of UUIDs to identify reads. This is stored using an extension type, with the following properties:

    Name: "minknow.uuid"
    Physical storage: FixedBinary(16)

#### minknow.vbz

Storage for VBZ-encoded data:

    Name: "minknow.vbz"
    Physical storage: LargeBinary

### Tables

#### Signal Table

The signal table contains the (optionally compressed) signal data where one row contains sequence of sample data, and some information about the sample data origin.

[tables/signal.toml] contains specific information about fields in the signal table.

#### Reads Table

The reads table contains a single row per read, and describes the metadata for each read. The signal column of the read links back to the signal table, and allows a reads signal to be retrieved.

Several fields of the Reads table are [dictionaries](https://arrow.apache.org/docs/python/data.html#dictionary-arrays), the contents of the table are stored in a lookup written prior to each batch of read rows, the read row itself then contains an integer index. This allows space savings on fields that would otherwise be repeated.

[tables/reads.toml] contains specific information about fields in the reads table.

### Combined file Layout

#### Layout

```
<signature "\213POD\r\n\032\n">
<section marker: 16 bytes>
<embedded file 1 (padded to 8-byte boundary)><section marker: 16 bytes>
...
<embedded file N (padded to 8-byte boundary)><section marker: 16 bytes>
<footer magic: "FOOTER\000\000">
<footer (padded to 8-byte boundary)>
<footer length: 8 bytes little-endian signed integer>
<section marker: 16 bytes>
<signature "\213POD\r\n\032\n">
```

    All padding bytes should be zero. They ensure memory mapped files have the alignment that Arrow expects.

#### Signature

The first and last eight bytes of the file are both a fixed set of values:

```
| Decimal          | 139  | 80   | 79   | 68   | 13   | 10   | 26   | 10   |
| Hexadecimal      | 0x8B | 0x50 | 0x4F | 0x44 | 0x0D | 0x0A | 0x1A | 0x0A |
| ASCII C Notation | \213 | P    | O    | D    | \r   | \n   | \032 | \n   |
```

The format of the signature is based on the PNG file signature, and inherits several useful features from it for detecting file corruption:

- The first byte is non-ASCII to reduce the probability it is interpreted as a text file.
- The first byte has the high bit set to catch file transfers that clear the top bit.
- The \r\n (CRLF) sequence and the final \n (LF) byte check that nothing has attempted to standardise line endings in the file.
- The second-last byte (\032) is the CTRL-Z sequence, which stops file display under MS-DOS.

##### Rationale

A unique, fixed signature for the file type allows quickly identifying that the file is in the expected format, and provides an easy way for tools like the UNIX `file` command to determine the file type.

Placing it at the end allows quickly checking whether the file is complete.


#### Section marker

The section marker is a 16-byte UUID, generated randomly for each file. All the section markers in a given file must be identical.

##### Rationale

This aids in recovery of partially-written files (that are missing a footer) - while most of the embedded Arrow IPC files can be scanned easily, it may not be obvious where the footer ends. A given randomly-generated 16-byte value is highly unlikely to occur in actual data, and can be scanned for to find the end of the embedded file for certain. The first section marker is just so that recovery tools know what to look for.

#### Footer magic

This is the ASCII string "FOOTER" padded to 8 bytes with zeroes. It helps find a partially-written footer when recovering files.

#### Footer

The footer is an encoded [FlatBuffer](https://google.github.io/flatbuffers/) table, using the schema below.

```fbs
namespace Minknow.ReadsFormat;
 
enum ContentType:short {
    // The Reads table (an Arrow table)
    ReadsTable,
    // The Signal table (an Arrow table)
    SignalTable,
    // An index for looking up data in the ReadsTable by read_id
    ReadIdIndex,
    // An index based on other columns and/or tables (it will need to be opened to find out what it indexes)
    OtherIndex,
}
 
enum Format:short {
    // The Apache Feather V2 format, also known as the Apache Arrow IPC File format.
    FeatherV2,
}
 
// Describes an embedded file.
table EmbeddedFile {
    // The start of the embedded file
    offset: int64;
    // The length of the embedded file (excluding any padding)
    length: int64;
    // The format of the file
    format: Format;
    // What contents should be expected in the file
    content_type: ContentType;
}
 
table Footer {
    // Must match the "MINKNOW:file_identifier" custom metadata entry in the schemas of the bundled tables.
    file_identifier: string;
    // A free-form description of the software that wrote the file, intended to help pin down the source of files that violate the specification.
    software: string;
    // The version of this specification that the table schemas are based on (1.0.0).
    pod5_version: string;
    // The Apache Arrow tables stored in the file.
    contents: [ EmbeddedFile ];
}
```

##### Rationale

FlatBuffers are used because the Arrow IPC file format already uses them for metadata, and they can be read from a memory mapped file or read buffer without further copying. They are also easily (and compatibly) extensible with more fields.

A footer is used instead of a header so the file can be written incrementally: the first table can be written directly to the file before it is known how long it will be or even how many tables there will be.

#### Footer length

This is a little-endian 8-byte signed integer giving the length of the footer buffer, including padding.

##### Rationale

This allows readers to find the start of the footer by starting at the end of the file and reading backwards.
