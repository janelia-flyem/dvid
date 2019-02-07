#!/bin/bash

# GOPATH is just the build 'work' dir 
export GOPATH=$(pwd)

echo "******************"
echo "GOROOT: ${GOROOT}"
echo "******************"

# The dvid repo was cloned to the appropriate internal directory
DVID_REPO=${GOPATH}/src/github.com/janelia-flyem/dvid
cd ${DVID_REPO}

# go-1.11 requires at least macOS 10.10, but conda-build targets 10.9 by default.
# Setting this variable explicitly overrides some linker warnings.
export MACOSX_DEPLOYMENT_TARGET=10.10

# In theory, most dependencies were already cloned thanks to the lists in meta.yaml.
# But the developer is free to add things to get-go-dependencies, too.
${DVID_REPO}/scripts/get-go-dependencies.sh

# Build
make dvid

# Test
make test

# Install
make install

# Remove the gcc symlink that was created in get-go-dependenices.sh
rm -rf ${PREFIX}/bin/gcc
