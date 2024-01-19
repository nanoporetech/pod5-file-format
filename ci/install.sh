#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

# Tar up the archive build:
(
    cmake -DCMAKE_INSTALL_PREFIX="archive" -DBUILD_TYPE="Release" -DCOMPONENT="archive" -P "cmake_install.cmake"
    if [[ "$OSTYPE" = "linux-gnu"* ]] && [ "$#" -ge 1 ] && [ "$1" = "STATIC_BUILD" ]; then
        gcc_version=$(gcc --version | awk 'NR==1 {print $3}' | sed 's/\([0-9.]*\).*/\1/')
        target_dir="archive/lib"

        [ "$gcc_version" != "7.5.0" ] && target_dir="archive/lib64"
        cp "../build/third_party/libs"/* "$target_dir"
    else
        if [ "$#" -ge 1 ] && [ "$1" = "STATIC_BUILD" ]; then
            cp "../build/third_party/libs"/* "archive/lib"
        fi
    fi

)

# Find the wheel:
(
    cmake -DCMAKE_INSTALL_PREFIX="wheel" -DBUILD_TYPE="Release" -DCOMPONENT="wheel" -P "cmake_install.cmake"
)
