from typing import Optional, Union

import time
import datetime
from pathlib import Path

from . import exceptions


def clean_dir(target_dir: Path) -> (int, int):
    num_files = 0
    num_bytes = 0
    for path in target_dir.iterdir():
        num_files += 1
        stat = path.stat()
        num_bytes += stat.st_size

        path.unlink()

    return num_files, num_bytes


def wait_for_path_to_exist(
    path,
    timeout: Optional[Union[int, datetime.timedelta]] = 1,
    wait_time: Union[int, datetime.timedelta] = 1,
):
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
