#!/usr/bin/env bash

pip install --user --trusted-host pypi.org --trusted-host files.pythonhosted.org cloudpickle

python run.py $1
