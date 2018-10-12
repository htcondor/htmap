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

from typing import Optional, Union, Iterable, Any, Mapping, MutableMapping
import logging

import time
import datetime
import subprocess
import sys
from pathlib import Path

from . import exceptions

from classad import ClassAd

MutableMapping.register(ClassAd)

logger = logging.getLogger(__name__)


def clean_dir(target_dir: Path):
    """
    Remove all files in the given directory `target_dir`.

    Parameters
    ----------
    target_dir
        The directory to clean up.
    """
    logger.debug(f'removing all files in {target_dir}...')
    for path in (p for p in target_dir.iterdir() if p.is_file()):
        path.unlink()
        logger.debug(f'removed file {path}')


def wait_for_path_to_exist(
    path: Path,
    timeout: Optional[Union[int, datetime.timedelta]] = None,
    wait_time: Union[int, datetime.timedelta] = 1,
):
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
    if isinstance(timeout, datetime.timedelta):
        timeout = timeout.total_seconds()
    if isinstance(wait_time, datetime.timedelta):
        wait_time = wait_time.total_seconds()

    start_time = time.time()
    while not path.exists():
        if timeout is not None and (timeout == 0 or time.time() > start_time + timeout):
            raise exceptions.TimeoutError(f'timeout while waiting for {path} to exist')
        time.sleep(wait_time)


class rstr(str):
    """Identical to a normal Python string, except that it's __repr__ is its __str__, to make it work nicer in notebooks."""

    def __repr__(self):
        return self.__str__()


def table(headers: Iterable[str], rows: Iterable[Iterable[Any]], fill: str = '', ) -> str:
    """
    Return a string containing a simple table created from headers and rows of entries.

    Parameters
    ----------
    headers
        The column headers for the table.
    rows
        The entries for each row, for each column.
        Should be an iterable of iterables or mappings, with the outer level containing the rows, and each inner iterable containing the entries for each column.
        A ``None`` in the outer iterable produces a horizontal bar at that position.
        An iterable-type row is printed in order.
        A mapping-type row uses the headers as keys to align the output and can have missing values, which are filled using the ```fill`` value.
    fill
        The string to print in place of a missing value in a mapping-type row.

    Returns
    -------
    table :
        A string containing the table.
    """
    lengths = [len(h) for h in headers]
    processed_rows = []

    for row in rows:
        if row is None:
            processed_rows.append(None)
        elif isinstance(row, Mapping):
            processed_rows.append([str(row.get(key, fill)) for key in headers])
        else:
            processed_rows.append([str(entry) for entry in row])

    for row in processed_rows:
        if row is None:
            continue
        lengths = [max(curr, len(entry)) for curr, entry in zip(lengths, row)]

    header = ' ' + ' │ '.join(h.center(l) for h, l in zip(headers, lengths)) + ' '
    bar = ''.join('─' if char != '│' else '┼' for char in header)
    bottom_bar = bar.replace('┼', '┴')

    lines = []
    for row in processed_rows:
        if row is None:
            lines.append(bar)
        else:
            lines.append(' ' + ' │ '.join(f.center(l) for f, l in zip(row, lengths)))

    output = '\n'.join((
        header,
        bar,
        *lines,
        bottom_bar,
    ))

    return rstr(output)


def get_file_size(path: Path) -> int:
    """Return the size of a file in bytes."""
    return path.stat().st_size


def get_dir_size(path: Path) -> int:
    """Return the size of a directory (including all contents recursively) in bytes."""
    size = 0
    for p in path.iterdir():
        if p.is_dir():
            size += get_dir_size(p)
        elif p.is_file():
            size += get_file_size(p)
    return size


def num_bytes_to_str(num_bytes: int) -> str:
    """Return a number of bytes as a human-readable string."""
    for unit in ('B', 'KB', 'MB', 'GB'):
        if num_bytes < 1024:
            return f'{num_bytes:.1f} {unit}'
        num_bytes /= 1024
    return f'{num_bytes:.1f} TB'


def get_file_size_as_str(path: Path) -> str:
    """Return the size of a file as a human-readable string."""
    return num_bytes_to_str(get_file_size(path))


def get_dir_size_as_str(path: Path) -> str:
    """Return the size of a directory (including all contents recursively) as a human-readable string."""
    return num_bytes_to_str(get_dir_size(path))


def pip_freeze() -> str:
    return subprocess.run(
        [sys.executable, '-m', 'pip', 'freeze', '--disable-pip-version-check'],
        stdout = subprocess.PIPE,
    ).stdout.decode('utf-8')
