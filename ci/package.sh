#!/bin/bash

output_sku=$1
auditwheel_platform=$2

CURRENT_DIR=$(pwd)

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT="${SCRIPT_DIR}/../"

cd ${REPO_ROOT}
pod5_version="$(cmake -P ci/get_tag_version.cmake 2>&1)"

cd ${CURRENT_DIR}

# Tar up the archive build:
(
    cd ./archive
    tar -cvzf ${REPO_ROOT}/pod5-file-format-${pod5_version}-${output_sku}.tar.gz .
)

# Find the wheel:
(
    # Wheels are optional:
    if [ -d "wheel/" ] ; then
        cd wheel/
        if [ -z ${auditwheel_platform} ]; then
            mv *.whl ${REPO_ROOT}/
        else
            echo "Running audit wheel"
            pwd
            ls
            auditwheel repair *.whl --plat ${auditwheel_platform} -w ${REPO_ROOT}/
        fi
    fi
)