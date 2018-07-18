#!/usr/bin/env bash

set -e

tar -xzf condormap.tar.gz

export PATH=$(pwd)/python/bin:$PATH

python run.py $1
