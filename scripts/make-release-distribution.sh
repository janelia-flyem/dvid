#!/bin/bash

set -e

ORIG_DIR=$(pwd)
THIS_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[  "$(uname)" == "Darwin" ]]; then
    OS=mac
elif [[ "$(uname)" == "Linux" ]]; then
    OS=linux
else
    1>&2 "Error: Unknown platform: $(uname)"
    exit 1
fi

echo "Building DVID distro from latest conda packages"

DVID_DISTRO_ENV=$(conda info --base)/envs/dvid-distro

# Cleanup from the previous distro env, if it exists.
if [ -d ${DVID_DISTRO_ENV} ]; then

    conda remove -y -n dvid-distro --all
fi

# Create the new distro environment and install dvid to it
conda create -y -n dvid-distro dvid

# Install both versions of the dvid-web-console (old and new)
conda install -y -n dvid-distro dvid-web-console=3
cp -R ${DVID_DISTRO_ENV}/http/dvid-web-console ${DVID_DISTRO_ENV}/http/dvid-web-console-3

# ...leave the old one active by default.
conda install -y -n dvid-distro dvid-web-console=2.1.6
cp -R ${DVID_DISTRO_ENV}/http/dvid-web-console ${DVID_DISTRO_ENV}/http/dvid-web-console-2

cp ${THIS_SCRIPT_DIR}/distro-files/* ${DVID_DISTRO_ENV}

# Make a bash array to capture the version of the dvid conda package
list_out=($(conda list -n dvid-distro dvid | grep -v '#' | head -n1))
version=${list_out[1]}

cd $(conda info --base)/envs

DIST_NAME=dvid-${version}-dist-${OS}
TARBALL_NAME=${DIST_NAME}.tar.bz2

rm -rf ${DIST_NAME}
rm -f ${TARBALL_NAME}

mv dvid-distro ${DIST_NAME}

echo "Creating ${TARBALL_NAME} ..."

tar -cjf ${TARBALL_NAME} ${DIST_NAME}

mv ${TARBALL_NAME} ${ORIG_DIR}

echo "Done."
