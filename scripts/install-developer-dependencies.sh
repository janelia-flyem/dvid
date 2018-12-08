#!/bin/bash

set -e

if [ -z "${CONDA_PREFIX}" ]; then
     1>&2 echo "A conda environment must be active"
     exit 1
fi

if [[ "${CONDA_PREFIX}" == "$(conda info --root)" ]]; then
     1>&2 echo "The root conda environment is currently active.  Please use a non-root environment."
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
CONDA_PYTHON=$(conda info --root)/bin/python
${CONDA_PYTHON} _install_compiled_dependencies.py

# Some of those dependencies (namely, gcc) may have installed scripts to
# ${CONDA_PREFIX}/etc/conda/activate.d/
# so re-activate the current environment to ensure that those scripts have been run.
source activate ${CONDA_DEFAULT_ENV}

##
## Install go dependencies.
##
./get-go-dependencies.sh

echo "DONE."
