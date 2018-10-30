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
import textwrap
import traceback
import subprocess
from pathlib import Path


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
        self.working_dir_contents = working_dir_contents
        self.stack_summary = stack_summary

    def __repr__(self):
        return '<ERROR for component {}>'.format(self.component)


def get_node_info():
    return (
        socket.getfqdn(),
        socket.gethostbyname(socket.gethostname()),
        datetime.datetime.utcnow(),
    )


def print_node_info(node_info):
    print('Landed on execute node {} ({}) at {}'.format(*node_info))


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
    return [str(p) for p in Path.cwd().iterdir()]


def print_working_dir_contents(contents):
    print('Working directory contents:')
    for path in contents:
        print('  ' + path)


def load_func():
    import cloudpickle
    with Path('func').open(mode = 'rb') as file:
        return cloudpickle.load(file)


def load_args_and_kwargs(component):
    import cloudpickle
    with Path('{}.in'.format(component)).open(mode = 'rb') as file:
        return cloudpickle.load(file)


def save_result(component, result):
    import cloudpickle
    with Path('{}.out'.format(component)).open(mode = 'wb') as file:
        cloudpickle.dump(result, file)


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


def main(component):
    node_info = get_node_info()
    print_node_info(node_info)
    print()
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

        result = ComponentError(
            component = component,
            status = 'ERR',
            exception_msg = textwrap.dedent('\n'.join(traceback.format_exception_only(type, value))).rstrip(),
            stack_summary = stack_summ,
            node_info = node_info,
            python_info = python_info,
            working_dir_contents = contents,
        )

    save_result(component, result)

    print('Finished executing component at {}'.format(datetime.datetime.utcnow()))


if __name__ == '__main__':
    main(component = sys.argv[1])
