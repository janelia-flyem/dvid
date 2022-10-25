#!/bin/bash

# Print some diagnostics about this build machine
echo "Checking file descriptor limit..."
ulimit -n
if [ "$(uname)" == "Darwin" ]; then
    top -l 1 -s 0 | grep PhysMem
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    free -m
fi

# GOPATH is just the build 'work' dir 
export GOPATH=$(pwd)

echo "******************"
echo "GOROOT: ${GOROOT}"
echo "******************"


if [ "$(uname)" == "Darwin" ]; then
    echo "CONDA_BUILD_SYSROOT: ${CONDA_BUILD_SYSROOT}"
    echo "MACOSX_DEPLOYMENT_TARGET: ${MACOSX_DEPLOYMENT_TARGET}"
fi

# The dvid repo was cloned to the appropriate internal directory
DVID_REPO=${GOPATH}/src/github.com/janelia-flyem/dvid
cd ${DVID_REPO}

# Build
make dvid

# Test
if [[ -z ${DVID_CONDA_SKIP_TESTS} || ${DVID_CONDA_SKIP_TESTS} == 0 ]]; then
    make test
fi

# Install
make install
