#!/usr/bin/env bash

set -e

python -c "import htcondor; print(htcondor.version())"
pytest --version

pytest -n 2 --cov --durations=20

coverage xml -o /tmp/coverage.xml

codecov -t 492519e2-1bcf-4e8a-8a3e-e28be5d9de8d -f /tmp/coverage.xml
