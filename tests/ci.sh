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
codecov -X gcov -f /tmp/coverage.xml
