from typing import Tuple, Iterator

from pathlib import Path
import shutil

from . import mapping, result, settings, shortcuts, utils


def _map_paths() -> Iterator[Path]:
    """Yield the paths to all existing map directories."""
    try:
        yield from (settings['HTMAP_DIR'] / settings['MAPS_DIR_NAME']).iterdir()
    except FileNotFoundError:  # maps dir doesn't exist for some reason, which means we have no maps
        yield from ()


def map_ids() -> Tuple[str]:
    """Return a tuple containing the ``map_id`` for existing maps."""
    return tuple(path.stem for path in _map_paths())


def map_results() -> Tuple[result.MapResult]:
    """Return a tuple of all existing maps."""
    return tuple(shortcuts.recover(map_id) for map_id in map_ids())


def clean():
    """Remove all existing maps."""
    for map_result in map_results():
        map_result.remove()


def force_clean():
    """Remove all existing maps."""
    for map_dir in _map_paths():
        shutil.rmtree(map_dir)


def status() -> str:
    """Return a string containing a table showing the status of all existing maps, as well as their disk usage."""
    ids = map_ids()
    results = map_results()
    counts = [r._status_counts() for r in results]

    return utils.table(
        headers = ['Map ID'] + [str(d) for d in result.JobStatus.display_statuses()] + ['Data'],
        rows = [
            [map_id] + [count[d] for d in result.JobStatus.display_statuses()] + [utils.get_dir_size_as_str(mapping.map_dir_path(map_id))]
            for map_id, count in sorted(
                zip(ids, counts),
                key = lambda map_id_and_count: map_id_and_count[1][result.JobStatus.RUNNING],
            )
        ],
    )
