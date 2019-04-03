#!/usr/bin/env bash

set -e

img=$1
component=$2

# would otherwise default to user home dir
export SINGULARITY_CACHEDIR=${_CONDOR_SCRATCH_DIR}

echo outside
pwd
ls -l
echo img ${img}
echo component ${component}

singularity exec --bind ${_CONDOR_SCRATCH_DIR}:/htmap/scratch ${img} bash -c "echo inside; pwd; cd /htmap/scratch; pwd; ls -l; ls -l /"
