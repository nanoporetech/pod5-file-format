#!/bin/bash

output_sku=$1

CURRENT_DIR=$(pwd)

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT="${SCRIPT_DIR}/../"

cd ${REPO_ROOT}
mkr_version="$(cmake -P ci/get_tag_version.cmake 2>&1)"

cd ${CURRENT_DIR}

# Tar up the archive build:
(
    cmake -DCMAKE_INSTALL_PREFIX="archive" -DBUILD_TYPE="Release" -DCOMPONENT="archive" -P "cmake_install.cmake"
    cd ./archive
    tar -czf ${REPO_ROOT}/mkr-file-format-${mkr_version}-${output_sku}.tar.gz .
)

# Find the wheel:
(
    cmake -DCMAKE_INSTALL_PREFIX="wheel" -DBUILD_TYPE="Release" -DCOMPONENT="wheel" -P "cmake_install.cmake"
    # Wheels are optional:
    if [ -d "wheel/" ] ; then
        cd wheel/
        mv *.whl ${REPO_ROOT}/
    fi
)