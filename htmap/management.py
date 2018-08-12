from typing import Tuple, Iterator

import shutil
from pathlib import Path

from htmap import settings, mapper, shortcuts, utils


def _map_paths() -> Iterator[Path]:
    """Yield the paths to all existing map directories."""
    yield from (settings.HTMAP_DIR / settings.MAPS_DIR_NAME).iterdir()


def clean():
    """Remove all input and output files for all existing maps."""
    for dir in _map_paths():
        shutil.rmtree(dir)


def map_ids() -> Tuple[str]:
    """Return a tuple containing the ``map_id`` for existing maps."""
    return tuple(path.stem for path in _map_paths())


def status() -> str:
    """Return a string containing a table showing the status of all existing maps."""
    ids = map_ids()
    results = [shortcuts.recover(map_id) for map_id in ids]
    counts = [result._status_counts() for result in results]

    return utils.table(
        headers = ['Map ID'] + [str(d) for d in mapper.JobStatus.display_statuses()] + ['Data'],
        rows = [
            [map_id] + [count[d] for d in mapper.JobStatus.display_statuses()] + [utils.get_dir_size_as_str(mapper.map_dir_path(map_id))]
            for map_id, count in sorted(
                zip(ids, counts),
                key = lambda mc: mc[1][mapper.JobStatus.RUNNING],
            )
        ],
    )
