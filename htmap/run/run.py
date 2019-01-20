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

import shutil
import sys
import socket
import datetime
import os
import gzip
import textwrap
import traceback
import subprocess
import getpass
from pathlib import Path

# we need to fake the checkpoint function's location so that the references work out right
from types import ModuleType

checkpoint_module = ModuleType('htmap')
sys.modules[checkpoint_module.__name__] = checkpoint_module

TRANSFER_DIR = '_htmap_transfer'
CHECKPOINT_PREP = 'prep_checkpoint'
CHECKPOINT_CURRENT = 'current_checkpoint'
CHECKPOINT_OLD = 'old_checkpoint'


def checkpoint(*paths: os.PathLike):
    """
    Informs HTMap about the existence of checkpoint files.
    This function should be called every time the checkpoint files are changed, even if they have the same names as before.

    .. attention::

        This function is a no-op when executing locally, so you if you're testing your function it won't do anything.

    .. attention::

        The files will be copied, so try not to make the checkpoint files too large.

    Parameters
    ----------
    paths
        The paths to the checkpoint files.
    """
    # no-op if not on execute node
    if os.getenv('HTMAP_ON_EXECUTE') != "1":
        return

    transfer_dir = Path(os.getenv('_CONDOR_SCRATCH_DIR')) / TRANSFER_DIR

    # this is not the absolute safest method
    # but it's good enough for government work

    prep_dir = transfer_dir / CHECKPOINT_PREP
    curr_dir = transfer_dir / CHECKPOINT_CURRENT
    old_dir = transfer_dir / CHECKPOINT_OLD

    for d in (prep_dir, curr_dir, old_dir):
        d.mkdir(parents = True, exist_ok = True)

    for path in paths:
        path = Path(path)
        shutil.copy2(path, prep_dir / path.name)

    curr_dir.rename(old_dir)
    prep_dir.rename(curr_dir)
    shutil.rmtree(old_dir)


checkpoint_module.checkpoint = checkpoint


# import cloudpickle goes in the functions that need it directly
# so that errors are raised later

class ComponentResult:
    def __init__(
        self,
        *,
        component,
        status,
    ):
        self.component = component
        self.status = status


class ComponentOk(ComponentResult):
    status = 'OK'

    def __init__(
        self,
        *,
        output,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.output = output

    def __repr__(self):
        return '<OK for component {}>'.format(self.component)


class ComponentError(ComponentResult):
    status = 'ERR'

    def __init__(
        self,
        *,
        exception_msg,
        node_info,
        python_info,
        working_dir_contents,
        stack_summary,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.exception_msg = exception_msg

        self.node_info = node_info
        self.python_info = python_info
        self.working_dir_contents = [str(p.absolute()) for p in working_dir_contents]
        self.stack_summary = stack_summary

    def __repr__(self):
        return '<ERROR for component {}>'.format(self.component)


def get_node_info():
    try:
        user = getpass.getuser()
    except:
        user = None

    return (
        socket.getfqdn(),
        socket.gethostbyname(socket.gethostname()),
        datetime.datetime.utcnow(),
        user,
    )


def print_node_info(node_info):
    print('Landed on execute node {} ({}) at {} as {}'.format(*node_info))


def get_python_info():
    return (
        sys.executable,
        f"{'.'.join(str(x) for x in sys.version_info[:3])} {sys.version_info[3]}",
        pip_freeze(),
    )


def print_python_info(python_info):
    executable, version, packages = python_info
    print('Python executable is {} (version {})'.format(executable, version))
    print('with installed packages')
    print('\n'.join('  {}'.format(line) for line in packages.splitlines()))


def pip_freeze() -> str:
    return subprocess.run(
        [sys.executable, '-m', 'pip', 'freeze', '--disable-pip-version-check'],
        stdout = subprocess.PIPE,
    ).stdout.decode('utf-8').strip()


def get_working_dir_contents():
    return list(Path.cwd().iterdir())


def print_working_dir_contents(contents):
    print('Working directory contents:')
    for path in contents:
        print('  ' + str(path))


def print_run_info(component, func, args, kwargs):
    s = '\n'.join((
        'Running component {}'.format(component),
        '  {}'.format(func),
        'with args',
        '  {}'.format(args),
        'and kwargs',
        '  {}'.format(kwargs),
    ))

    print(s)


def load_object(path):
    import cloudpickle
    with gzip.open(path, mode = 'rb') as file:
        return cloudpickle.load(file)


def load_func():
    return load_object(Path('func'))


def load_args_and_kwargs(component):
    return load_object(Path('{}.in'.format(component)))


def save_object(obj, path):
    import cloudpickle
    with gzip.open(path, mode = 'wb') as file:
        cloudpickle.dump(obj, file)


def save_result(component, result, transfer_dir):
    save_object(result, transfer_dir / '{}.out'.format(component))


def build_frames(tb):
    iterator = traceback.walk_tb(tb)
    next(iterator)  # skip main's frame

    for frame, lineno in iterator:
        fname = frame.f_code.co_filename
        summ = traceback.FrameSummary(
            filename = fname,
            lineno = lineno,
            name = frame.f_code.co_name,
            lookup_line = os.path.exists(fname),
            locals = frame.f_locals,
        )

        yield summ


def load_checkpoint(scratch_dir, transfer_dir):
    curr_dir = transfer_dir / CHECKPOINT_CURRENT
    old_dir = transfer_dir / CHECKPOINT_OLD

    if curr_dir.exists():
        for path in curr_dir.iterdir():
            path.rename(scratch_dir / path.name)
    elif old_dir.exists():
        for path in old_dir.iterdir():
            path.rename(scratch_dir / path.name)


def clean_and_remake_dir(dir):
    shutil.rmtree(dir, ignore_errors = True)
    dir.mkdir()


def main(component):
    node_info = get_node_info()
    print_node_info(node_info)
    print()

    scratch_dir = Path(os.getenv('_CONDOR_SCRATCH_DIR'))
    transfer_dir = scratch_dir / TRANSFER_DIR

    load_checkpoint(scratch_dir, transfer_dir)
    clean_and_remake_dir(transfer_dir)

    contents = get_working_dir_contents()
    print_working_dir_contents(contents)
    print()

    try:
        python_info = get_python_info()
        print_python_info(python_info)
    except Exception as e:
        python_info = None
        print(e)
    print()

    os.environ['HTMAP_ON_EXECUTE'] = "1"

    try:
        func = load_func()
        args, kwargs = load_args_and_kwargs(component)

        print_run_info(component, func, args, kwargs)

        print('\n----- MAP COMPONENT OUTPUT START -----\n')
        output = func(*args, **kwargs)
        print('\n-----  MAP COMPONENT OUTPUT END  -----\n')

        result = ComponentOk(
            component = component,
            status = 'OK',
            output = output,
        )
    except Exception as e:
        print('\n-------  MAP COMPONENT ERROR  --------\n')

        (type, value, trace) = sys.exc_info()
        stack_summ = traceback.StackSummary.from_list(build_frames(trace))
        exc_msg = textwrap.dedent('\n'.join(traceback.format_exception_only(type, value))).rstrip()

        result = ComponentError(
            component = component,
            status = 'ERR',
            exception_msg = exc_msg,
            stack_summary = stack_summ,
            node_info = node_info,
            python_info = python_info,
            working_dir_contents = contents,
        )

    clean_and_remake_dir(transfer_dir)
    save_result(component, result, transfer_dir)

    print('Finished executing component at {}'.format(datetime.datetime.utcnow()))


if __name__ == '__main__':
    main(component = sys.argv[1])
