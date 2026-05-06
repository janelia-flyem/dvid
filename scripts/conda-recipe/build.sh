#!/bin/bash

# Print some diagnostics about this build machine
echo "Checking file descriptor limit..."
ulimit -n
if [ "$(uname)" == "Darwin" ]; then
    top -l 1 -s 0 | grep PhysMem
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    free -m
fi

BUILD_WORK_DIR=$(pwd)

echo "******************"
echo "GOROOT: ${GOROOT}"
echo "******************"


if [ "$(uname)" == "Darwin" ]; then
    echo "CONDA_BUILD_SYSROOT: ${CONDA_BUILD_SYSROOT}"
    echo "MACOSX_DEPLOYMENT_TARGET: ${MACOSX_DEPLOYMENT_TARGET}"
fi

# The dvid repo is staged under this path by meta.yaml.
DVID_REPO=${BUILD_WORK_DIR}/src/github.com/janelia-flyem/dvid
cd ${DVID_REPO}

# Build
make dvid

# Test
if [[ -z ${DVID_CONDA_SKIP_TESTS} || ${DVID_CONDA_SKIP_TESTS} == 0 ]]; then
    make test
fi

# Install
make install
