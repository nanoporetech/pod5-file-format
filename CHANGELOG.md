# Changelog
All notable changes, updates, and fixes to pod5 will be documented here

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
