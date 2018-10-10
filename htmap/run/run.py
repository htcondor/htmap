#!/usr/bin/env python3

# Copyright 2018 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import socket
import datetime
import os
import subprocess
from pathlib import Path

import cloudpickle


def print_node_info():
    print('Landed on execute node {} ({}) at {}'.format(
        socket.getfqdn(),
        socket.gethostbyname(socket.gethostname()),
        datetime.datetime.utcnow(),
    ))


def print_python_info():
    print('Python executable is\n    {}'.format(sys.executable))
    print('with installed packages')
    print('\n'.join('    {}'.format(line) for line in pip_freeze().splitlines()))


def pip_freeze() -> str:
    return subprocess.run(
        [sys.executable, '-m', 'pip', 'freeze', '--disable-pip-version-check'],
        stdout = subprocess.PIPE,
    ).stdout.decode('utf-8')


def print_working_dir_contents():
    print('Working directory contents:')
    for path in Path.cwd().iterdir():
        print('    {}'.format(path))


def load_func():
    with Path('func').open(mode = 'rb') as file:
        return cloudpickle.load(file)


def load_args_and_kwargs(arg_hash):
    with Path('{}.in'.format(arg_hash)).open(mode = 'rb') as file:
        return cloudpickle.load(file)


def save_output(arg_hash, output):
    with Path('{}.out'.format(arg_hash)).open(mode = 'wb') as file:
        cloudpickle.dump(output, file)


def print_run_info(arg_hash, func, args, kwargs):
    s = '\n'.join((
        'Running',
        '    {}'.format(func),
        'with args',
        '    {}'.format(args),
        'and kwargs',
        '    {}'.format(kwargs),
        'from input hash',
        '    {}'.format(arg_hash),
    ))

    print(s)


def main(arg_hash):
    print_node_info()
    print()
    print_working_dir_contents()
    print()
    print_python_info()
    print()

    os.environ['HTMAP_ON_EXECUTE'] = "1"

    func = load_func()
    args, kwargs = load_args_and_kwargs(arg_hash)

    print_run_info(arg_hash, func, args, kwargs)

    print('\n----- MAP COMPONENT OUTPUT START -----\n')
    output = func(*args, **kwargs)
    print('\n-----  MAP COMPONENT OUTPUT END  -----\n')

    save_output(arg_hash, output)

    print('Finished executing component at {}'.format(datetime.datetime.utcnow()))


if __name__ == '__main__':
    main(arg_hash = sys.argv[1])
