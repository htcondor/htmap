#!/usr/bin/env bash

set -e

img=$1
component=$2

export SINGULARITY_CACHEDIR=$_CONDOR_SCRATCH_DIR

singularity exec --bind ${_CONDOR_SCRATCH_DIR}:/scratch/htmap ${img} bash -c "ls; ls /scratch; ls /scratch/htmap; cd /scratch/htmap; ./run.py ${component}"
