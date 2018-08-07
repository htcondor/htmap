from typing import Tuple, Iterator

import shutil
from pathlib import Path

from htmap import settings, mapper, shortcuts, utils


def _map_paths() -> Iterator[Path]:
    yield from (settings.HTMAP_DIR / settings.MAPS_DIR_NAME).iterdir()


def clean():
    for dir in _map_paths():
        shutil.rmtree(dir)


def map_ids() -> Tuple[str]:
    return tuple(path.stem for path in _map_paths())


def status() -> str:
    display = [
        mapper.JobStatus.HELD,
        mapper.JobStatus.IDLE,
        mapper.JobStatus.RUNNING,
        mapper.JobStatus.COMPLETED,
    ]

    maps = map_ids()
    results = [shortcuts.recover(map_id) for map_id in maps]
    counts = [result._status_counts() for result in results]

    return utils.table(
        headers = ['Map ID'] + [str(d) for d in display],
        rows = [
            [map_id] + [count[d] for d in display]
            for map_id, count in zip(maps, counts)
        ],
    )
