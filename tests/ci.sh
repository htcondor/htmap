#!/usr/bin/env bash

set -e

echo "condor version"
condor_version
echo

echo "python bindings version"
python -c "import htcondor; print(htcondor.version())"
echo

echo "pytest version"
pytest --version
echo

pytest -n 4 --cov --durations=20

chmod -R +r .
