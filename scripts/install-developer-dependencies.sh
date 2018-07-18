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

# Install conda dependencies
# (This assumes that the most recent dvid package has up-to-date requirements)
echo "Installing conda dependencies..."
CMD="conda install --only-deps dvid"
echo $CMD
$CMD

# Install go library dependencies
echo "Installing go libraries..."
${THIS_SCRIPT_DIR}/get-go-dependencies.sh

echo "DONE."
