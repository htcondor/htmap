#!/usr/bin/env bash

set -e

tar -xzf htmap.tar.gz

pip install --user --trusted-host pypi.org --trusted-host files.pythonhosted.org cloudpickle

export PATH=$(pwd)/python/bin:$PATH

python run.py $1
