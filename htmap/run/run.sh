#!/usr/bin/env bash

pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org cloudpickle

python run.py $1
