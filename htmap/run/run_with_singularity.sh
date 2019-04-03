#!/usr/bin/env bash

set -e

img=$1
component=$2

export SINGULARITY_CACHEDIR=$_CONDOR_SCRATCH_DIR

singularity exec --bind ${_CONDOR_SCRATCH_DIR}:/_htmap_scratch ${img} python3 /_htmap_scratch/run.py ${component}
