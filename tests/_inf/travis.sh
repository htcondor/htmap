#!/usr/bin/env bash

set -e

echo condor_version
pytest -n 2 --cov

codecov -t 492519e2-1bcf-4e8a-8a3e-e28be5d9de8d
