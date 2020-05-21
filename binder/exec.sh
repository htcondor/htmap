#!/usr/bin/env bash

set -e

CONTAINER_TAG=htmap-binder-exec

docker build -t ${CONTAINER_TAG} --file binder/Dockerfile .
docker run --rm --mount type=bind,source="$(pwd)"/docs/source/tutorials,target=/home/jovyan/tutorials ${CONTAINER_TAG} -- bash -l -c 'sleep 5 && condor_who -wait:60 "IsReady && STARTD_State =?= \"Ready\"" && rm -r /home/jovyan/tutorials/*.txt ; for x in $(find_notebooks); do nbstripout $x && jupyter nbconvert --to notebook --inplace --execute --allow-errors --ExecutePreprocessor.timeout=None $x && htmap remove --all ; done && rm -r /home/jovyan/tutorials/*.txt'
