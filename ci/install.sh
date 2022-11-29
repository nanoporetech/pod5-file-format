#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

# Tar up the archive build:
(
    cmake -DCMAKE_INSTALL_PREFIX="archive" -DBUILD_TYPE="Release" -DCOMPONENT="archive" -P "cmake_install.cmake"
    cmake -DCMAKE_INSTALL_PREFIX="archive" -DBUILD_TYPE="Release" -DCOMPONENT="third_party" -P "cmake_install.cmake"
)

# Find the wheel:
(
    cmake -DCMAKE_INSTALL_PREFIX="wheel" -DBUILD_TYPE="Release" -DCOMPONENT="wheel" -P "cmake_install.cmake"
)
