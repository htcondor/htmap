#!/usr/bin/env bash

set -e

img=$1
component=$2

export SINGULARITY_CACHEDIR=$_CONDOR_SCRATCH_DIR

singularity exec --bind ${_CONDOR_SCRATCH_DIR}:/htmap_scratch ${img} bash -c "/htmap_scratch/run.py ${component}"
