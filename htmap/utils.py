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

from typing import Optional, Union, Iterable, Any, Mapping, MutableMapping, Callable, Dict
import logging

import os
import time
import datetime
import subprocess
import sys
import enum
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from . import exceptions

from classad import ClassAd

MutableMapping.register(ClassAd)

logger = logging.getLogger(__name__)


class StrEnum(enum.Enum):
    def __str__(self):
        return self.value


def wait_for_path_to_exist(
    path: Path,
    timeout: Optional[Union[int, float, datetime.timedelta]] = None,
    wait_time: Optional[Union[int, float, datetime.timedelta]] = 1,
) -> None:
    """
    Waits for the path `path` to exist.

    Parameters
    ----------
    path
        The target path to watch.
    timeout
        The maximum amount of time to wait for the path to exist before raising a :class:`htmap.exceptions.TimeoutError`.
    wait_time
        The time to wait between checks.
    """
    timeout = timeout_to_seconds(timeout)
    wait_time = timeout_to_seconds(wait_time) or .01  # minimum wait time

    start_time = time.time()
    while not path.exists():
        if timeout is not None and (timeout <= 0 or time.time() > start_time + timeout):
            raise exceptions.TimeoutError(f'timeout while waiting for {path} to exist')
        time.sleep(wait_time)


Timeout = Optional[Union[int, float, datetime.timedelta]]


def timeout_to_seconds(timeout: Optional[Union[int, float, datetime.timedelta]]) -> Optional[float]:
    """
    Coerce a timeout given as a :class:`datetime.timedelta` or an :class:`int` to a number of seconds as a :class:`float`.
    ``None`` is passed through.
    """
    if timeout is None:
        return timeout
    if isinstance(timeout, datetime.timedelta):
        return timeout.total_seconds()
    return float(timeout)


class rstr(str):
    """
    Identical to a normal Python string, except that it's ``__repr__``
    is its ``__str__``, to make it work nicer in notebooks.
    """

    def __repr__(self):
        return self.__str__()


def table(
    headers: Iterable[str],
    rows: Iterable[Iterable[Any]],
    fill: str = '',
    header_fmt: Callable[[str], str] = None,
    row_fmt: Callable[[str], str] = None,
    alignment: Dict[str, str] = None,
) -> str:
    """
    Return a string containing a simple table created from headers and rows of entries.

    Parameters
    ----------
    headers
        The column headers for the table.
    rows
        The entries for each row, for each column.
        Should be an iterable of iterables or mappings, with the outer level containing the rows,
        and each inner iterable containing the entries for each column.
        An iterable-type row is printed in order.
        A mapping-type row uses the headers as keys to align the stdout and can have missing values,
        which are filled using the ```fill`` value.
    fill
        The string to print in place of a missing value in a mapping-type row.
    header_fmt
        A function to be called on the header string.
        The return value is what will go in the output.
    row_fmt
        A function to be called on each row string.
        The return value is what will go in the output.
    alignment
        If ``True``, the first column will be left-aligned instead of centered.

    Returns
    -------
    table :
        A string containing the table.
    """
    if header_fmt is None:
        header_fmt = lambda _: _
    if row_fmt is None:
        row_fmt = lambda _: _
    if alignment is None:
        alignment = {}

    headers = tuple(headers)
    lengths = [len(h) for h in headers]

    align_methods = [alignment.get(h, "center") for h in headers]

    processed_rows = []
    for row in rows:
        if isinstance(row, Mapping):
            processed_rows.append([str(row.get(key, fill)) for key in headers])
        else:
            processed_rows.append([str(entry) for entry in row])

    for row in processed_rows:
        lengths = [max(curr, len(entry)) for curr, entry in zip(lengths, row)]

    header = header_fmt('  '.join(getattr(h, a)(l) for h, l, a in zip(headers, lengths, align_methods)).rstrip())

    lines = (
        row_fmt('  '.join(getattr(f, a)(l) for f, l, a in zip(row, lengths, align_methods)))
        for row in processed_rows
    )

    output = '\n'.join((
        header,
        *lines,
    ))

    return rstr(output)


def get_dir_size(path: Path, safe = True) -> int:
    """Return the size of a directory (including all contents recursively) in bytes."""
    size = 0
    for entry in os.scandir(path):
        try:
            if entry.is_file(follow_symlinks = False):
                size += entry.stat().st_size
            elif entry.is_dir():
                size += get_dir_size(Path(entry.path))
        except FileNotFoundError as e:
            if safe:
                raise e
            else:
                logger.error(f'path {entry} vanished while using it')
    return size


def num_bytes_to_str(num_bytes: Union[int, float]) -> str:
    """Return a number of bytes as a human-readable string."""
    for unit in ('B', 'KB', 'MB', 'GB'):
        if num_bytes < 1024:
            return f'{num_bytes:.1f} {unit}'
        num_bytes /= 1024
    return f'{num_bytes:.1f} TB'


def pip_freeze() -> str:
    """Return the text of a ``pip --freeze`` call."""
    return subprocess.run(
        [sys.executable, '-m', 'pip', 'freeze', '--disable-pip-version-check'],
        stdout = subprocess.PIPE,
    ).stdout.decode('utf-8').strip()


def read_events(maps):
    """Read the events logs of the given maps using a thread pool."""
    with ThreadPoolExecutor() as pool:
        pool.map(lambda map: map._state._read_events(), maps)


def is_interactive_session():
    import __main__ as main
    return any((
        bool(getattr(sys, 'ps1', sys.flags.interactive)),  # console sessions
        not hasattr(main, '__file__'),  # jupyter-like notebooks
    ))


def enable_debug_logging():
    logger = logging.getLogger('htmap')
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream = sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    logger.addHandler(handler)
