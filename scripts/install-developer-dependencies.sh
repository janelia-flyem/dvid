#!/bin/bash

if [ -z "${CONDA_PREFIX}" ]; then
     1>&2 echo "A conda environment must be active"
     exit 1
fi

if [[ "${CONDA_PREFIX}" == "$(conda info --base)" ]]; then
     1>&2 echo "The base conda environment is currently active.  Please use a non-base environment."
     exit 1
fi


if conda list nocgo | grep nocgo; then
    1>&2 echo "*** ERROR: ***"
    1>&2 echo "You have go-nocgo (or an associated package) installed in this environment."
    1>&2 echo "That is not compatible with go-cgo, which is necessary for DVID."
    1>&2 echo "Please remove all 'nocgo' packages, or activate a different environment."
    1>&2 echo "**************"
    exit
fi

set -e

THIS_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${THIS_SCRIPT_DIR}

GO_VERSION=1.11.9

if [[ $(uname) == "Darwin" ]]; then
    GO_PLATFORM_PKG=go-cgo_osx-64
    COMPILER_PACKAGE=clangxx_osx-64
else
    GO_PLATFORM_PKG=go-cgo_linux-64
    COMPILER_PACKAGE=gxx_linux-64
fi

CMD="conda install -y snappy basholeveldb lz4-c 'librdkafka>=0.11.4' go-cgo=${GO_VERSION} ${GO_PLATFORM_PKG} pkg-config ${COMPILER_PACKAGE}"
echo ${CMD}
${CMD}

# Some of those dependencies (namely, gcc) may have installed scripts to
# ${CONDA_PREFIX}/etc/conda/activate.d/
# so re-activate the current environment to ensure that those scripts have been run.
if which activate > /dev/null 2>&1; then
    source activate ${CONDA_DEFAULT_ENV}
else
    # Under conda 4.6, 'conda' isn't available from bash scripts unless
    # you explicitly define it by sourcing the activate script.
    # Super-annoying.
    # https://github.com/conda/conda/issues/7980#issuecomment-441358406
    CONDA_BASE=$(conda info --base)
    source ${CONDA_BASE}/etc/profile.d/conda.sh
    conda activate ${CONDA_DEFAULT_ENV}
fi

##
## Install go dependencies.
##
./get-go-dependencies.sh

echo "DONE."
