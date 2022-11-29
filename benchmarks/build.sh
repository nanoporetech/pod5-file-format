#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "${script_dir}"

cd image/
docker build -t pod5-benchmark-base -f Dockerfile.base .
