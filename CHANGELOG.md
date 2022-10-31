# Changelog
All notable changes, updates, and fixes to pod5 will be documented here

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.41] - 2022-10-27
### Changed
- Fixed pod5-inspect erroring when loading data.
- Fixed issue where some files inbetween 0.34 - 0.38 wouldn't load correctly.

## [0.0.40] - 2022-10-27
### Changed
- Fixed migrating of large files from older versions.

## [0.0.39] - 2022-10-18
### Changed
- Fixed building against the c++ api - previously missing include files.

## [0.0.38] - 2022-10-18
### Removed
- Support for opening and writing "split" pod5 files. All API's now expect and return combined pod5 files.

### Changed
- All data in the read table that was previously contained in dictionaries of structs is now stored in the read table, or a new "run info" table.
    This change simplifies data access into the pod5 files, and helps users who want to convert the pod5 data to pandas or other arrow-compatible reader formats.
    Old data is migrated on load, and will continue to work, data can be permanently migrated using the tool `pod5-migrate`

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
### Changed
- File version is now V2, due to the addition of `num_samples`.

### Added
- Added `num_samples` field to read table, containing the total number of samples a read contains. The field is filled in by API if it doesnt exist.

## [0.0.32] - 2022-10-03
### Changed
- Fixed an issue where multi-threaded access to a single batch could cause a crash discovered by dorado testing.
- Fixed help text in convert to fast5 script.
