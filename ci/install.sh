#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

# Tar up the archive build:
(
    cmake -DCMAKE_INSTALL_PREFIX="archive" -DBUILD_TYPE="Release" -DCOMPONENT="archive" -P "cmake_install.cmake"
    if [ "$#" -ge 1 ] && [ "$1" == "STATIC_BUILD" ]; then
        if [[ "$OSTYPE" == "linux-gnu"* ]] && [[ -e "archive/lib64" ]]; then
            cp "../build/third_party/libs"/* "archive/lib64"
        else
            cp "../build/third_party/libs"/* "archive/lib"
        fi
    fi
)

# Find the wheel:
(
    cmake -DCMAKE_INSTALL_PREFIX="wheel" -DBUILD_TYPE="Release" -DCOMPONENT="wheel" -P "cmake_install.cmake"
)
