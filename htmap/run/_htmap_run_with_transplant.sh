#!/usr/bin/env bash

set -e

mkdir htmap_python
tar -xzf $2 -C htmap_python/

htmap_python/bin/python _run.py $1
