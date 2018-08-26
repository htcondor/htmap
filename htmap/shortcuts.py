from typing import Callable

import htmap.result
from . import mapper


def map(map_id: str, func: Callable, args, **kwargs) -> htmap.result.MapResult:
    return mapper.htmap(func).map(map_id, args, **kwargs)


def productmap(map_id: str, func: Callable, *args, **kwargs) -> htmap.result.MapResult:
    return mapper.htmap(func).productmap(map_id, args, **kwargs)


def starmap(map_id: str, func: Callable, args, kwargs) -> htmap.result.MapResult:
    return mapper.htmap(func).starmap(map_id, args, kwargs)


def build_map(map_id: str, func: Callable) -> mapper.MapBuilder:
    return mapper.htmap(func).build_map(map_id)


def recover(map_id: str) -> htmap.result.MapResult:
    return htmap.result.MapResult.recover(map_id)
