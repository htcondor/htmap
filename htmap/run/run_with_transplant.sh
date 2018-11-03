#!/usr/bin/env bash

set -e

mkdir htmap_python
tar -xzf $2.tar.gz -C htmap_python/
echo "unpacked python install"

htmap_python/bin/python run.py $1
