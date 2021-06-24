#!/usr/bin/env bash

set -e

export PYTHONPATH="$PWD:$PYTHONPATH"

echo "NOTE: CONNECT TO http://127.0.0.1:8000 NOT WHAT SPHINX-AUTOBUILD TELLS YOU"
sleep 3

sphinx-autobuild docs/source docs/_build --host 0.0.0.0 --watch htmap/
