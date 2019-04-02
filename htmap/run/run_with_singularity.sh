#!/usr/bin/env bash

set -e

img=$1
component=$2

export SINGULARITY_CACHEDIR=$_CONDOR_SCRATCH_DIR

singularity exec --bind ${_CONDOR_SCRATCH_DIR}:${_CONDOR_SCRATCH_DIR} ${img} python3 ${_CONDOR_SCRATCH_DIR}/run.py ${component}
