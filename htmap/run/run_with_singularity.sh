#!/usr/bin/env bash

set -e

img=$1
component=$2

# singularity cachedir would otherwise default to user home dir
d=${_CONDOR_SCRATCH_DIR}/.htmap_singularity
mkdir ${d}
export SINGULARITY_CACHEDIR=${d}

singularity exec --contain --bind ${_CONDOR_SCRATCH_DIR}:/tmp --workdir /tmp ${img} bash -c "python3 run.py ${component}"
