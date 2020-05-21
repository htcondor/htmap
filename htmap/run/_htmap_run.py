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

import os
import shutil
import sys
import socket
import datetime
import gzip
import textwrap
import traceback
import subprocess
import getpass
from pathlib import Path

TRANSFER_DIR = '_htmap_transfer'
USER_TRANSFER_DIR = '_htmap_user_transfer'
CHECKPOINT_PREP = '_htmap_prep_checkpoint'
CHECKPOINT_CURRENT = '_htmap_current_checkpoint'
CHECKPOINT_OLD = '_htmap_old_checkpoint'
TRANSFER_PLUGIN_CACHE = "_htmap_transfer_plugin_cache"
USER_URL_TRANSFER_DIR = '_htmap_user_url_transfer'
TRANSFER_PLUGIN_MARKER = "_htmap_do_output_transfer"

# import cloudpickle goes in the functions that need it directly
# so that errors are raised later


class ExecutionError:
    def __init__(
        self,
        *,
        component,
        exception_msg,
        node_info,
        python_info,
        scratch_dir_contents,
        stack_summary,
    ):
        self.component = component
        self.exception_msg = exception_msg
        self.node_info = node_info
        self.python_info = python_info
        self.scratch_dir_contents = [str(p.absolute()) for p in scratch_dir_contents]
        self.stack_summary = stack_summary

    def __repr__(self):
        return '<ExecutionError for component {}>'.format(self.component)


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
    if sys.executable == '':
        raise Exception("Was not able to determine Python executable.")

    v = sys.version_info
    return (
        sys.executable,
        "{}.{}.{}".format(v.major, v.minor, v.micro),
        pip_freeze(),
    )


def print_python_info(python_info):
    executable, version, packages = python_info
    print('Python executable is {} (version {})'.format(executable, version))
    print('with installed packages')
    print('\n'.join('  {}'.format(line) for line in packages.splitlines()))


def pip_freeze() -> str:
    freeze = subprocess.run(
        [sys.executable, '-m', 'pip', 'freeze', '--disable-pip-version-check'],
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
    )

    if freeze.returncode != 0:
        raise Exception("Failed to get pip freeze due to:\n{}".format(freeze.stderr.decode('utf-8')))

    return freeze.stdout.decode('utf-8').strip()


def print_dir_contents(root):
    msg = '\n'.join(_yield_dir_contents_tree(root))
    print(msg)


def _yield_dir_contents_tree(root, prefix = ""):
    contents = list(root.iterdir())
    for idx, path in enumerate(sorted(contents)):
        if idx < len(contents) - 1:
            tree = "|-"
            next_prefix = prefix + "|  "
        else:
            tree = "\\-"
            next_prefix = prefix + "   "

        yield f"{prefix}{tree} {'* ' if path.is_dir() else ''}{path.name}"

        if path.is_dir():
            yield from _yield_dir_contents_tree(
                path,
                prefix = next_prefix,
            )


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


def save_objects(objects, path):
    import cloudpickle
    with gzip.open(path, mode = 'wb') as file:
        for obj in objects:
            cloudpickle.dump(obj, file)


def save_output(component, status, result_or_error, transfer_dir):
    save_objects([status, result_or_error], transfer_dir / '{}.out'.format(component))


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
    """Move checkpoint files back into the scratch directory."""
    curr_dir = scratch_dir / CHECKPOINT_CURRENT
    old_dir = scratch_dir / CHECKPOINT_OLD

    if curr_dir.exists():
        for path in curr_dir.iterdir():
            path.rename(scratch_dir / path.name)
        curr_dir.rename(transfer_dir / curr_dir.name)
    elif old_dir.exists():
        for path in old_dir.iterdir():
            path.rename(scratch_dir / path.name)
        old_dir.rename(transfer_dir / curr_dir.name)


def clean_and_remake_dir(dir: Path) -> None:
    if dir.exists():
        shutil.rmtree(dir)
    dir.mkdir()


def main(component):
    os.environ['HTMAP_ON_EXECUTE'] = "1"
    os.environ['HTMAP_COMPONENT'] = f"{component}"

    node_info = get_node_info()
    print_node_info(node_info)
    print()

    scratch_dir = Path.cwd()
    transfer_dir = scratch_dir / TRANSFER_DIR
    transfer_dir.mkdir(exist_ok = True)
    user_transfer_dir = scratch_dir / USER_TRANSFER_DIR / os.getenv('HTMAP_COMPONENT')
    user_transfer_dir.mkdir(exist_ok = True, parents = True)
    Path(TRANSFER_PLUGIN_CACHE).mkdir(exist_ok = True, parents = True)
    Path(TRANSFER_PLUGIN_MARKER).touch(exist_ok = True)

    load_checkpoint(scratch_dir, transfer_dir)

    print("Scratch directory contents before run:")
    print_dir_contents(scratch_dir)
    print()

    try:
        python_info = get_python_info()
        print_python_info(python_info)
    except Exception:
        print("Failed to get information on Python due to:\n{}".format(traceback.format_exc()))
        python_info = None
    print()

    try:
        func = load_func()
        args, kwargs = load_args_and_kwargs(component)

        print_run_info(component, func, args, kwargs)

        print('\n----- MAP COMPONENT OUTPUT START -----\n')

        result_or_error = func(*args, **kwargs)
        status = 'OK'

        print('\n-----  MAP COMPONENT OUTPUT END  -----\n')
    except Exception:
        print('\n-------  MAP COMPONENT ERROR  --------\n')

        type, value, trace = sys.exc_info()
        stack_summ = traceback.StackSummary.from_list(build_frames(trace))
        exc_msg = textwrap.dedent('\n'.join(traceback.format_exception_only(type, value))).rstrip()

        result_or_error = ExecutionError(
            component = component,
            exception_msg = exc_msg,
            stack_summary = stack_summ,
            node_info = node_info,
            python_info = python_info,
            scratch_dir_contents = list(scratch_dir.iterdir()),
        )
        status = 'ERR'

        traceback.print_exc(file = sys.stdout)
        traceback.print_exc(file = sys.stderr)

        print('\n------ MAP COMPONENT ERROR END -------\n')

    clean_and_remake_dir(scratch_dir / CHECKPOINT_CURRENT)
    clean_and_remake_dir(transfer_dir)
    save_output(component, status, result_or_error, transfer_dir)

    print('Finished executing component at {}'.format(datetime.datetime.utcnow()))
    print()

    print("Scratch directory contents after run:")
    print_dir_contents(scratch_dir)


if __name__ == '__main__':
    main(component = sys.argv[1])
