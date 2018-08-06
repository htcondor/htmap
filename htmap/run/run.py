import os
import sys
import socket
import datetime
from pathlib import Path

import cloudpickle


def print_node_info():
    print(f'Landed on execute node {socket.getfqdn()} ({socket.gethostbyname(socket.gethostname())}) at {datetime.datetime.utcnow()}')
    print(f'Execute node operating system: {os.uname()}')

    dir_contents = ", ".join(str(x) for x in Path.cwd().iterdir())
    print(f'Local directory contents: {dir_contents}')
    print()


def run_func(arg_hash):
    with Path('fn.pkl').open(mode = 'rb') as file:
        fn = cloudpickle.load(file)

    with Path(f'{arg_hash}.in').open(mode = 'rb') as file:
        args, kwargs = cloudpickle.load(file)

    print(f'Running\n    {fn}\nwith args\n    {args}\nand kwargs\n    {kwargs}')
    print()

    output = fn(*args, **kwargs)

    with Path(f'{arg_hash}.out').open(mode = 'wb') as file:
        cloudpickle.dump(output, file)


def main(arg_hash):
    print_node_info()
    run_func(arg_hash = arg_hash)


if __name__ == '__main__':
    main(arg_hash = sys.argv[1])
