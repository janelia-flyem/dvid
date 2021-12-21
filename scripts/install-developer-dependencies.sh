#!/bin/bash

if [ -z "${CONDA_PREFIX}" ]; then
     1>&2 echo "A conda environment must be active"
     exit 1
fi

if [[ "${CONDA_PREFIX}" == "$(conda info --base)" ]]; then
     1>&2 echo "The base conda environment is currently active.  Please use a non-base environment."
     exit 1
fi


set -e

THIS_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${THIS_SCRIPT_DIR}

#GO_VERSION=1.16.10

if [[ $(uname) == "Darwin" ]]; then
    # Force MacOS builds to use current Xcode rather than download.
    echo "Note that XCode should be installed independently and command line tools like xcrun available."
else
    COMPILER_PACKAGE=gxx_linux-64
fi

CMD="conda install -y -c flyem-forge -c conda-forge snappy basholeveldb lz4-c 'librdkafka=1.3.0' go-cgo pkg-config ${COMPILER_PACKAGE}"
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
