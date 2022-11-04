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

import datetime
import enum
import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Mapping, MutableMapping, Optional, Tuple, Union

import htcondor
from classad import ClassAd

from . import exceptions

MutableMapping.register(ClassAd)

logger = logging.getLogger(__name__)


class StrEnum(str, enum.Enum):
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
    wait_time = timeout_to_seconds(wait_time) or 0.01  # minimum wait time

    start_time = time.time()
    while not path.exists():
        if timeout is not None and (timeout <= 0 or time.time() > start_time + timeout):
            raise exceptions.TimeoutError(f"Timeout while waiting for {path} to exist")
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
    fill: str = "",
    header_fmt: Optional[Callable[[str], str]] = None,
    row_fmt: Optional[Callable[[str], str]] = None,
    alignment: Optional[Dict[str, str]] = None,
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
        A map of headers to string method names to use to align each column.

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

    header = header_fmt(
        "  ".join(getattr(h, a)(l) for h, l, a in zip(headers, lengths, align_methods)).rstrip()
    )

    lines = (
        row_fmt("  ".join(getattr(f, a)(l) for f, l, a in zip(row, lengths, align_methods)))
        for row in processed_rows
    )

    output = "\n".join((header, *lines,))

    return rstr(output)


class Timer:
    def __init__(self):
        self.start = None
        self.end = None

    @property
    def elapsed(self):
        """The elapsed time in seconds from the start of the timer to the end."""
        if self.start is None:
            raise ValueError("Timer hasn't started yet!")

        if self.end is None:
            raise ValueError("Timer hasn't stopped yet!")

        return self.end - self.start

    def __enter__(self):
        self.start = time.monotonic()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time.monotonic()


def get_dir_size(path: Path, safe: bool = True) -> int:
    """Return the size of a directory (including all contents recursively) in bytes."""
    size = 0
    for entry in os.scandir(path):
        try:
            if entry.is_file(follow_symlinks=False):
                size += entry.stat().st_size
            elif entry.is_dir():
                size += get_dir_size(Path(entry.path))
        except FileNotFoundError as e:
            if safe:
                raise e
            else:
                logger.error(f"Path {entry} vanished while using it")
    return size


def num_bytes_to_str(num_bytes: Union[int, float]) -> str:
    """Return a number of bytes as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


def pip_freeze() -> str:
    """Return the text of a ``pip --freeze`` call."""
    return (
        subprocess.run(
            [sys.executable, "-m", "pip", "freeze", "--disable-pip-version-check"],
            stdout=subprocess.PIPE,
        )
        .stdout.decode("utf-8")
        .strip()
    )


def is_interactive_session() -> bool:
    import __main__ as main

    return any(
        (
            bool(getattr(sys, "ps1", sys.flags.interactive)),  # console sessions
            not hasattr(main, "__file__"),  # jupyter-like notebooks
        )
    )


def is_jupyter() -> bool:
    # https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook/24937408
    # This seems quite fragile, but it also seems hard to determine otherwise...
    # I would not be shocked if this breaks in the future.
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True  # Jupyter notebook or qtconsole
        elif shell == "TerminalInteractiveShell":
            return False  # Terminal running IPython
        else:
            return False  # Something else...
    except NameError:
        return False  # Probably standard Python interpreter


def enable_debug_logging():
    logger = logging.getLogger("htmap")
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

    logger.addHandler(handler)


VERSION_RE = re.compile(r"^(\d+) \. (\d+) (\. (\d+))? ([ab](\d+))?$", re.VERBOSE | re.ASCII,)


def parse_version(v: str) -> Tuple[int, int, int, Optional[str], Optional[int]]:
    match = VERSION_RE.match(v)
    if match is None:
        raise Exception(f"Could not determine version info from {v}")

    (major, minor, micro, prerelease, prerelease_num) = match.group(1, 2, 4, 5, 6)

    out = (
        int(major),
        int(minor),
        int(micro or 0),
        prerelease[0] if prerelease is not None else None,
        int(prerelease_num) if prerelease_num is not None else None,
    )

    return out


EXTRACT_HTCONDOR_VERSION_RE = re.compile(r"(\d+\.\d+\.\d+)", flags=re.ASCII)

BINDINGS_VERSION_INFO = parse_version(
    EXTRACT_HTCONDOR_VERSION_RE.search(htcondor.version()).group(0)
)

try:
    condor_version = subprocess.run("condor_version", stdout=subprocess.PIPE).stdout.decode()
    HTCONDOR_VERSION_INFO = parse_version(
        EXTRACT_HTCONDOR_VERSION_RE.search(condor_version).group(0)
    )
except Exception:
    logger.warning(
        "Was not able to parse HTCondor version information. Is HTCondor itself installed, not just the bindings? Assuming bindings version for HTCondor version."
    )
    HTCONDOR_VERSION_INFO = BINDINGS_VERSION_INFO

# CAN_USE_URL_OUTPUT_TRANSFER = HTCONDOR_VERSION_INFO >= (8, 9, 2)
CAN_USE_URL_OUTPUT_TRANSFER = False
