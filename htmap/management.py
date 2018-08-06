from typing import Tuple, Iterator

import shutil
from pathlib import Path

from htmap import settings


def _map_paths() -> Iterator[Path]:
    yield from (settings.HTMAP_DIR / settings.MAPS_DIR_NAME).iterdir()


def clean():
    for dir in _map_paths():
        shutil.rmtree(dir)


def maps() -> Tuple[str]:
    return tuple(path.stem for path in _map_paths())
