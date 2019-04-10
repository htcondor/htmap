#!/usr/bin/env bash

set -e

img=$1
component=$2

# would otherwise default to user home dir
export SINGULARITY_CACHEDIR=${_CONDOR_SCRATCH_DIR}

singularity exec --bind ${_CONDOR_SCRATCH_DIR}:/htmap/scratch ${img} bash -c "cd /htmap/scratch && python3 run.py ${component}"
