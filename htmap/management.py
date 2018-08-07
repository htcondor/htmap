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
    maps = map_ids()
    results = [shortcuts.recover(map_id) for map_id in maps]
    counts = [result._status_counts() for result in results]

    return utils.table(
        headers = ['Map ID'] + [str(d) for d in mapper.JobStatus.display_statuses],
        rows = [
            [map_id] + [count[d] for d in mapper.JobStatus.display_statuses]
            for map_id, count in sorted(
                zip(maps, counts),
                key = lambda mc: mc[1][mapper.JobStatus.RUNNING],
            )
        ],
    )
