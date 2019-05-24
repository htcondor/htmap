#!/usr/bin/env bash

set -e

CONTAINER_TAG=htmap-edit-tutorials

docker build -t ${CONTAINER_TAG} --file binder/Dockerfile .
docker run -it --rm -p 8888:8888 --mount type=bind,source="$(pwd)"/docs/source/tutorials,target=/home/jovyan/tutorials ${CONTAINER_TAG}
