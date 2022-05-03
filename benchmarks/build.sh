#!/bin/bash

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${script_dir}

cd image/
docker build -t mkr-benchmark-base -f Dockerfile.base .