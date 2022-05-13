#!/bin/bash

output_sku=$1
auditwheel_platform=$2

CURRENT_DIR=$(pwd)

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT="${SCRIPT_DIR}/../"

# Tar up the archive build:
(
    cmake -DCMAKE_INSTALL_PREFIX="archive" -DBUILD_TYPE="Release" -DCOMPONENT="archive" -P "cmake_install.cmake"
)

# Find the wheel:
(
    cmake -DCMAKE_INSTALL_PREFIX="wheel" -DBUILD_TYPE="Release" -DCOMPONENT="wheel" -P "cmake_install.cmake"
)