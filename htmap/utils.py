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

from typing import Optional, Union, Iterable, Any, Mapping, MutableMapping, Callable
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


def clean_dir(
    target_dir: Path,
    on_file: Optional[Callable[[Path], None]] = None,
) -> None:
    """
    Remove all files in the given directory `target_dir`.

    Parameters
    ----------
    target_dir
        The directory to clean up.
    on_file
        A function to call on each file before deleting it.
    """
    if on_file is None:
        on_file = lambda p: None

    logger.debug(f'removing all files in {target_dir}...')
    for path in (p for p in target_dir.iterdir() if p.is_file()):
        on_file(path)
        path.unlink()
        logger.debug(f'removed file {path}')


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
    Identical to a normal Python string, except that it's ``__repr__`` is its ``__str__``,
    to make it work nicer in notebooks.
    """

    def __repr__(self):
        return self.__str__()


def table(
    headers: Iterable[str],
    rows: Iterable[Iterable[Any]],
    fill: str = '',
    header_fmt: Callable[[str], str] = None,
    row_fmt: Callable[[str], str] = None,
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
        A ``None`` in the outer iterable produces a horizontal bar at that position.
        An iterable-type row is printed in order.
        A mapping-type row uses the headers as keys to align the stdout and can have missing values,
        which are filled using the ```fill`` value.
    fill
        The string to print in place of a missing value in a mapping-type row.

    Returns
    -------
    table :
        A string containing the table.
    """
    if header_fmt is None:
        header_fmt = lambda _: _
    if row_fmt is None:
        row_fmt = lambda _: _

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

    header = header_fmt('  '.join(h.center(l) for h, l in zip(headers, lengths)))

    lines = []
    for row in processed_rows:
        if row is None:
            lines.append('')
        else:
            lines.append(row_fmt('  '.join(f.center(l) for f, l in zip(row, lengths))))

    output = '\n'.join((
        header,
        *lines,
    ))

    return rstr(output)


def get_dir_size(path: Path) -> int:
    """Return the size of a directory (including all contents recursively) in bytes."""
    size = 0
    for entry in os.scandir(path):
        if entry.is_file(follow_symlinks = False):
            size += entry.stat().st_size
        elif entry.is_dir():
            size += get_dir_size(Path(entry.path))
    return size


def num_bytes_to_str(num_bytes: Union[int, float]) -> str:
    """Return a number of bytes as a human-readable string."""
    for unit in ('B', 'KB', 'MB', 'GB'):
        if num_bytes < 1024:
            return f'{num_bytes:.1f} {unit}'
        num_bytes /= 1024
    return f'{num_bytes:.1f} TB'


def get_dir_size_as_str(path: Path) -> str:
    """Return the size of a directory (including all contents recursively) as a human-readable string."""
    return num_bytes_to_str(get_dir_size(path))


def pip_freeze() -> str:
    """Return the text of a ``pip --freeze`` call."""
    return subprocess.run(
        [sys.executable, '-m', 'pip', 'freeze', '--disable-pip-version-check'],
        stdout = subprocess.PIPE,
    ).stdout.decode('utf-8').strip()


def read_events(maps):
    """Read the events logs of the given maps in parallel."""
    with ThreadPoolExecutor() as pool:
        pool.map(lambda map: map._read_events(), maps)
