"""
Copyright 2018 HTCondor Team, Computer Sciences Department,
University of Wisconsin-Madison, WI.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from typing import Tuple, Iterator

from pathlib import Path

from . import mapper, result, settings, shortcuts, utils


def _map_paths() -> Iterator[Path]:
    """Yield the paths to all existing map directories."""
    yield from (settings['HTMAP_DIR'] / settings['MAPS_DIR_NAME']).iterdir()


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


def status() -> str:
    """Return a string containing a table showing the status of all existing maps, as well as their disk usage."""
    ids = map_ids()
    results = map_results()
    counts = [r._status_counts() for r in results]

    return utils.table(
        headers = ['Map ID'] + [str(d) for d in result.JobStatus.display_statuses()] + ['Data'],
        rows = [
            [map_id] + [count[d] for d in result.JobStatus.display_statuses()] + [utils.get_dir_size_as_str(mapper.map_dir_path(map_id))]
            for map_id, count in sorted(
                zip(ids, counts),
                key = lambda map_id_and_count: map_id_and_count[1][result.JobStatus.RUNNING],
            )
        ],
    )
