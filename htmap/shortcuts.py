from typing import Callable

import shutil

from . import mapper, result, exceptions


def recover(map_id: str) -> result.MapResult:
    return result.MapResult.recover(map_id)


def remove(map_id: str, not_exist_ok = True):
    try:
        r = recover(map_id)
        r.remove()
    except exceptions.MapIDNotFound as e:
        if not not_exist_ok:
            raise e


def force_remove(map_id: str):
    shutil.rmtree(mapper.map_dir_path(map_id))
