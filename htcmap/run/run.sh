#!/usr/bin/env bash

set -e

tar -xzf htcmap.tar.gz

export PATH=$(pwd)/python/bin:$PATH

python run.py $1
