#!/usr/bin/env bash

set -e

printf "\n-----\n"

echo "HTCondor version:"
condor_version

echo

echo "HTCondor Python bindings version:"
python -c "import htcondor; print(htcondor.version())"

echo

echo "pytest version:"
pytest --version

printf "\n-----\n"

pytest -n 4 --cov --durations=20

coverage xml -o /tmp/coverage.xml
codecov -X gcov -t 492519e2-1bcf-4e8a-8a3e-e28be5d9de8d -f /tmp/coverage.xml
