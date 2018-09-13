#!/usr/bin/env python3

"""
Copyright 2018 HTCondor Team, Computer Sciences Department,
University of Wisconsin-Madison, WI.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys
import socket
import datetime
import os
from pathlib import Path

import cloudpickle


def print_node_info():
    print(f'Landed on execute node {socket.getfqdn()} ({socket.gethostbyname(socket.gethostname())}) at {datetime.datetime.utcnow()}')

    print('Local directory contents:')
    for path in Path.cwd().iterdir():
        print(f'    {path}')


def run_func(arg_hash):
    with Path('func').open(mode = 'rb') as file:
        fn = cloudpickle.load(file)

    with Path(f'{arg_hash}.in').open(mode = 'rb') as file:
        args, kwargs = cloudpickle.load(file)

    print(f'Running\n    {fn}\nwith args\n    {args}\nand kwargs\n    {kwargs}')

    output = fn(*args, **kwargs)

    with Path(f'{arg_hash}.out').open(mode = 'wb') as file:
        cloudpickle.dump(output, file)


def main(arg_hash):
    os.environ['HTMAP_ON_EXECUTE'] = "1"
    print_node_info()
    print()
    run_func(arg_hash = arg_hash)


if __name__ == '__main__':
    main(arg_hash = sys.argv[1])
