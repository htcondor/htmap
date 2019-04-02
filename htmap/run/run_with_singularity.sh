#!/usr/bin/env bash

set -e

img=$1
component=$2

singularity exec --bind ${_CONDOR_SCRATCH_DIR}:/htmap_scratch ${img} /htmap_scratch/run.py ${component}
