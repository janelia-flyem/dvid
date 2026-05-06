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

CMD="conda install -y -c flyem-forge -c conda-forge basholeveldb snappy"
echo ${CMD}
${CMD}

echo "DONE."
