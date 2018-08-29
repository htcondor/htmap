from typing import Callable

from . import mapper, result, exceptions


def map(map_id: str, func: Callable, args, force_overwrite: bool = False, **kwargs) -> result.MapResult:
    return mapper.htmap(func).map(map_id, args, force_overwrite = force_overwrite, **kwargs)


def starmap(map_id: str, func: Callable, args, kwargs, force_overwrite: bool = False) -> result.MapResult:
    return mapper.htmap(func).starmap(map_id, args, kwargs, force_overwrite = force_overwrite)


def build_map(map_id: str, func: Callable, force_overwrite: bool = False) -> mapper.MapBuilder:
    return mapper.htmap(func).build_map(map_id, force_overwrite = force_overwrite)


def recover(map_id: str) -> result.MapResult:
    return result.MapResult.recover(map_id)


def remove(map_id: str):
    try:
        r = recover(map_id)
        r.remove()
    except exceptions.MapIDNotFound:
        pass
