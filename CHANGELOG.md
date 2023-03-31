<!-- markdownlint-disable MD024 -->

# Changelog

All notable changes, updates, and fixes to pod5 will be documented here

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- Refactored `pod5 convert fast5` to use `concurrent.futures` only.
- Add further info to error message when signal cannot be decompressed by zstd
- Make merge operation not generate multiple identical run infos.

### Fixed

- Fixed closing uninitialised file handles.
- Fixed `pod5 inspect reads` repeating header

## [0.1.13] 2023-03-23

### Fixed

- Fix loading large pod5 files on virtual-memory limited systems.

## [0.1.12] 2023-03-20

### Added

- Added `--output` argument to `pod5 convert fast5` and `to_fast5` replacing positional argument of the same name
- Added `--strict` argument to `pod5 convert fast5` to promptly stop on exceptions
- Added readthedocs documentation links in README.md

### Changed

- Updated developer installation instructions to use `conan<2`
- Reworked `pod5 convert fast5` to tolerate runtime exceptions
- Use same type `run_info_index_t` for `pod5_get_file_run_info_count` and `pod5_get_file_run_info`.

### Fixed

- Fixed file handle leak in repacker

## [0.1.11] 2023-03-13

### Added

- Python API supports python 3.11
- Added missing python API wheels on windows

### Changed

- Changed python API dependency version `pyarrow~=11.0.0` from `8.0.0` to support python 3.11
- Changed python API dependency version `hdf5~=8.0.0` from `v7.0.0` to support python 3.11

## [0.1.10] 2023-03-09

### Added

- Added `pod5_get_read_count` to find the count of all reads in file
- Added `pod5_get_read_ids` to retrieve all read id's in file
- Added `pod5_get_file_run_info` to retrieve a run info at an absolute index in the file
- Added `pod5_free_run_info` to free run info's (replaces `pod5_release_run_info`)
- Added `pod5_get_file_run_info_count` to find the number of run info's in a file
- Added `pod5 filter` tool to subset pod5 files with simple list of read ids
- Added `tqdm` progress bar to `pod5 subset` (disable with `POD5_PBAR=0`)

### Changed

- Reworked `pod5 subset` to give better control over resources used
- `pod5 subset` can now parse csv and tsv tables / summaries
- `pod5 repack` now repacks all inputs one-to-one

### Deprecated

- Deprecated `pod5_release_run_info` (see `pod5_free_run_info`)

### Removed

- Removed filepath header line from `pod5 inspect reads`

## [0.1.9] 2023-03-07

### Added

- Added version attributes to `lib-pod5`

### Changed

- Versioning now controlled by VCS inspection using `setuptools_scm`

## [0.1.8] 2023-02-23

### Added

- Added more `read_id` getter methods to `Reader`
- Added support for python 3.8 + 3.10 on windows
- Added gcc7 linux build of pod5

### Changed

- Update to zlib 1.2.13
- Update to zstd 1.5.4
- Pinned `pre-commit=v2.21.0` while supporting `python3.7`
- Reworked `pod5 convert to_fast5` output filenames to allow for `1-1` mapping

### Fixed

- Fixed `pod5 inspect read`
- Fixed `pod5 convert to_fast5` creating an empty fast5 output
- Fixed `pod5 convert to_fast5` ignoring the `--force_overwrite` argument
- Fixed issue where thread_pool.h wasn't shipped.

## [0.1.5] - 2023-01-20

### Added

- Explicitly re-exported `lib-pod5` public symbols and added `py.typed` marker file to support type-checking.

### Fixed

- Fixed issue where closing many pod5 files in sequence is slow.
- Fixed incorrect python types and adopted python type-checking.

## [0.1.4] - 2022-12-22

### Added

- Linux python 3.11 wheels
- ReadTheDocs documentation support

### Fixed

- OSX arm64 wheel naming corrections - works with wider set of python executables

## [0.1.3] - 2022-12-16

### Added

- Added `Reader.__iter__` method.

### Changed

- Renamed `EndReason.name` to `EndReason.reason` to access the inner enum and added
    `EndReason.name` as a property to return the string representation of this enum value.
- `BaseRead`, `Read`, `CompressedRead`, `Calibration` and `Pore` dataclasses are now mutable.

### Removed

- Removed deprecated `Writer` functions.

### Fixed

- Fixed osx arm64 wheel compatibility for older python versions.
- Fixed EndReason type errors.
- Fixed EndReason in pod5 to fast5 conversion.

## [0.1.2] - 2022-12-06

### Changed

- Optimised the file writing utilities

## [0.1.1] - 2022-12-06

### Changed

- Restricted exported boost dependencies of conan package to just the boost::headers component.

## [0.1] - 2022-12-02

### Changed

- Documentation edits
- `Writer.add_reads` now handles both `Read` and `CompressedRead`.

### Deprecated

- Deprecated `Writer` methods `add_read_object` and `add_read_objects` for `add_read` and `add_reads` respectively.

### Removed

- Removed direct pod5 tool scripts.

### Fixed

- Fixed name of internal utils - "pad_file".
- Fixed spelling of various internal variables.
- Fixed `pod5 convert to_fast5`

## [0.0.43]

### Changed

- Reformat c++ code with more consistent format file.

## [0.0.42]

### Added

- Added `pod5` tools entry-point
- Added api to query file version information as written on disk.

### Changed

- Fixed signal_chunk_size type error in convert-from-fast5
- Replaced `ont_fast5_api` dependency with `vbz_h5py_plugin`
- Restructured Python packaging to include `lib_pod5_format` which contains the native bindings build from pybind11.
- `pod5_format` and `pod5_format_tools` are now pure python packages which depend on `lib_pod5_format`
- Python packages `pod5_format` and `pod5_format_tools` have been merged into single `pod5` pure-python package.
- `pod5-convert-from-fast5` `--output-one-to-one` reworked so that output files maintain the input structure making this argument more flexible and avoid filename clobbering.
- Added missing `lib_pod5.update_file` function to pyi.
- `pod5-convert-from-fast5` `output` now takes existing directories and
writes `output.pod5` (current behaviour) or creates a new file with the given name if it doesn't exist.
- Renamed arguments in tools relating to multi-processing / multi-threading from `-p/--processes` to the mode common `-t/--threads`.

## [0.0.41] - 2022-10-27

### Changed

- Fixed pod5-inspect erroring when loading data.
- Fixed issue where some files in between 0.34 - 0.38 wouldn't load correctly.

## [0.0.40] - 2022-10-27

### Changed

- Fixed migrating of large files from older versions.

## [0.0.39] - 2022-10-18

### Changed

- Fixed building against the c++ api - previously missing include files.

## [0.0.38] - 2022-10-18

### Changed

- All data in the read table that was previously contained in dictionaries of structs is now stored in the read table, or a new "run info" table.
    This change simplifies data access into the pod5 files, and helps users who want to convert the pod5 data to pandas or other arrow-compatible reader formats.
    Old data is migrated on load, and will continue to work, data can be permanently migrated using the tool `pod5-migrate`

### Removed

- Support for opening and writing "split" pod5 files. All API's now expect and return combined pod5 files.

## [0.0.37] - 2022-10-18

### Changed

- Updated Conan recipe to support building without specifying C++ standard version.

## [0.0.36] - 2022-10-07

### Changed

- Bump the Boost and Arrow versions to pick up latest changes.

## [0.0.35] - 2022-10-07

### Changed

- Support C++17 + C++20 with the conan package pod5 generates.

## [0.0.34] - 2022-10-05

### Changed

- Modified `pod5_format_tools/pod5_convert_to_fast5.py` to separate `pod5_convert_to_fast5_argparser()` and `convert_from_fast5()` out from `pod5_convert_from_fast5.main()`.

## [0.0.33] - 2022-10-05

### Added

- Added `num_samples` field to read table, containing the total number of samples a read contains. The field is filled in by API if it doesn't exist.

### Changed

- File version is now V2, due to the addition of `num_samples`.

## [0.0.32] - 2022-10-03

### Fixed

- Fixed an issue where multi-threaded access to a single batch could cause a crash discovered by dorado testing.
- Fixed help text in convert to fast5 script.
