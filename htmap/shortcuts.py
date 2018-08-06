from typing import Callable, Iterable

import shutil

from . import mapper, settings


def map(map_id: str, func: Callable, args, **kwargs) -> mapper.MapResult:
    return mapper.htmap(func).map(map_id, args, **kwargs)


def productmap(map_id: str, func: Callable, *args, **kwargs) -> mapper.MapResult:
    return mapper.htmap(func).productmap(map_id, args, **kwargs)


def starmap(map_id: str, func: Callable, args, kwargs) -> mapper.MapResult:
    return mapper.htmap(func).starmap(map_id, args, kwargs)


def build_map(map_id: str, func: Callable) -> mapper.MapBuilder:
    return mapper.htmap(func).build_map(map_id)


def recover(map_id: str) -> mapper.MapResult:
    return mapper.MapResult.recover(map_id)


def clean():
    for dir in (settings.HTMAP_DIR / settings.MAPS_DIR_NAME).iterdir():
        shutil.rmtree(dir)


def maps() -> Iterable[str]:
    yield from (path for path in (settings.HTMAP_DIR / settings.MAPS_DIR_NAME).iterdir())
