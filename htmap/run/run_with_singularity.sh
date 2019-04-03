#!/usr/bin/env bash

set -e

img=$1
component=$2

export SINGULARITY_CACHEDIR=$_CONDOR_SCRATCH_DIR

singularity exec --writable --bind ${_CONDOR_SCRATCH_DIR}:/htmap_scratch ${img} bash -c "mkdir /htmap_scratch; ls /htmap_scratch; python3 /htmap_scratch/run.py ${component}"
