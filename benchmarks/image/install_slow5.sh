#!/bin/bash

set -e

: "${SLOW_5_VERSION:=v1.0.0}"

apt update
apt install -y libzstd-dev libhdf5-dev

wget "https://github.com/hasindu2008/slow5tools/releases/download/$SLOW_5_VERSION/slow5tools-$SLOW_5_VERSION-release.tar.gz"
tar xvf slow5tools-$SLOW_5_VERSION-release.tar.gz
(
    cd slow5tools-$SLOW_5_VERSION/
    ./configure
    make zstd=1
)

# pyslow5 must be built with zstd support for fair comparison (otherwise default zlib is slower than zstd)
git clone -b ${SLOW_5_VERSION} https://github.com/hasindu2008/slow5lib

(
    cd slow5lib/

    echo "Installing numpy"
    pip install numpy

    make pyslow5
    echo "Installing pyslow5"
    PYSLOW5_ZSTD=1 pip install dist/*.tar.gz

    # adding slow5 C API benchmarks
    make zstd=1 && test/bench/build.sh
)
