from typing import Optional, Union

import time
import datetime

from . import exceptions


def wait_for_path_to_exist(
    path,
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
