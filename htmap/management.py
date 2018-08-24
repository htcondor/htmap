from typing import Tuple, Iterator

from pathlib import Path

import htmap.result
import htmap.utils
from htmap import settings, shortcuts, utils


def _map_paths() -> Iterator[Path]:
    """Yield the paths to all existing map directories."""
    yield from (settings.HTMAP_DIR / settings.MAPS_DIR_NAME).iterdir()


def map_ids() -> Tuple[str]:
    """Return a tuple containing the ``map_id`` for existing maps."""
    return tuple(path.stem for path in _map_paths())


def map_results() -> Tuple[htmap.result.MapResult]:
    return tuple(shortcuts.recover(map_id) for map_id in map_ids())


def clean():
    """Remove all input and output files for all existing maps."""
    for map_result in map_results():
        map_result.remove()


def status() -> str:
    """Return a string containing a table showing the status of all existing maps."""
    ids = map_ids()
    results = map_results()
    counts = [result._status_counts() for result in results]

    return utils.table(
        headers = ['Map ID'] + [str(d) for d in result.results.JobStatus.display_statuses()] + ['Data'],
        rows = [
            [map_id] + [count[d] for d in result.results.JobStatus.display_statuses()] + [utils.get_dir_size_as_str(htmap.utils.map_dir_path(map_id))]
            for map_id, count in sorted(
                zip(ids, counts),
                key = lambda map_id_and_count: map_id_and_count[1][result.results.JobStatus.RUNNING],
            )
        ],
    )
