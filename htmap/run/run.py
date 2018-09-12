#!/usr/bin/env python3

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
