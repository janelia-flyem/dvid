#!/bin/bash

set -e

if [ -z "${CONDA_PREFIX}" ]; then
     1>&2 echo "A conda environment must be active"
     exit 1
fi

if [[ "${CONDA_PREFIX}" == "$(conda info --base)" ]]; then
     1>&2 echo "The base conda environment is currently active.  Please use a non-base environment."
     exit 1
fi

THIS_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${THIS_SCRIPT_DIR}

##
## Install compiled (conda) dependencies.
##
# The following python script requires the 'yaml' python module,
# which happens to be available in the conda base interpreter,
# so we use that interpreter to run it.
CONDA_PYTHON=$(conda info --base)/bin/python
${CONDA_PYTHON} _install_compiled_dependencies.py

# Some of those dependencies (namely, gcc) may have installed scripts to
# ${CONDA_PREFIX}/etc/conda/activate.d/
# so re-activate the current environment to ensure that those scripts have been run.

# Old versions of conda sourced an 'activate' script;
# new versions use the 'conda activate' command

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
