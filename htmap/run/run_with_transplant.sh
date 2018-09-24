#!/usr/bin/env bash

mkdir htmap_python
tar -xzf htmap_python.tar.gz -C htmap_python/

htmap_python/python run.py $1
