from typing import Optional, Union, Iterable, Any

import time
import datetime
import functools
from pathlib import Path

from . import settings, exceptions


def wait_for_path_to_exist(
    path: Path,
    timeout: Optional[Union[int, datetime.timedelta]] = 1,
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
        t = time.time()
        if timeout is not None and (timeout == 0 or t > start_time + timeout):
            raise exceptions.TimeoutError(f'timeout while waiting for {path} to exist')
        time.sleep(wait_time)


NEVER = object()


def temporary_cache(timeout: Optional[Union[int, datetime.timedelta]] = None):
    """
    Cache the result of a function for a certain amount of time.

    Parameters
    ----------
    timeout
        The length of time to cache the result of the function for.
    """
    if isinstance(timeout, datetime.timedelta):
        timeout = timeout.total_seconds()

    def decorator(func):
        last_call = NEVER
        cached_value = None

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal cached_value, last_call
            t = timeout if timeout is not None else settings.TEMPORARY_CACHE_TIMEOUT
            if last_call is NEVER or time.time() > last_call + t:
                cached_value = func(*args, **kwargs)
                last_call = time.time()
            return cached_value

        return wrapper

    return decorator


class rstr(str):
    """Identical to a normal Python string, except that it's __repr__ is its __str__, to make it work nicer in notebooks."""

    def __repr__(self):
        return self.__str__()


def table(headers: Iterable[str], rows: Iterable[Iterable[Any]]) -> str:
    """
    Return a string containing a simple table created from headers and rows of entries.

    Parameters
    ----------
    headers
        The column headers for the table.
    rows
        The entries for each row, for each column.
        Should be an iterable of iterables, with the outer level containing the rows, and each inner iterable containing the entries for each column.
        A ``None`` in the outer iterable produces a horizontal bar at that position.

    Returns
    -------
    table :
        A string containing the table.
    """
    lengths = [len(h) for h in headers]
    rows = [[str(entry) for entry in row] if row is not None else None for row in rows]
    for row in rows:
        if row is None:
            continue

        lengths = [max(curr, len(entry)) for curr, entry in zip(lengths, row)]

    header = ' ' + ' │ '.join(h.center(l) for h, l in zip(headers, lengths)) + ' '
    bar = ''.join('─' if char != '│' else '┼' for char in header)
    bottom_bar = bar.replace('┼', '┴')

    lines = []
    for row in rows:
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
