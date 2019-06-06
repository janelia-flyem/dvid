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

GO_VERSION=1.12.5

if [[ $(uname) == "Darwin" ]]; then
    COMPILER_PACKAGE=clangxx_osx-64
else
    COMPILER_PACKAGE=gxx_linux-64
fi

CMD="conda install -y -c flyem-forge -c conda-forge snappy basholeveldb lz4-c 'librdkafka>=0.11.4' go=${GO_VERSION} pkg-config ${COMPILER_PACKAGE}"
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
