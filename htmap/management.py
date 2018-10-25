# Copyright 2018 HTCondor Team, Computer Sciences Department,
# University of Wisconsin-Madison, WI.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Tuple, Iterator
import logging

from pathlib import Path
import shutil

from . import mapping, maps, utils, exceptions

logger = logging.getLogger(__name__)


def recover(map_id: str) -> maps.Map:
    """
    Reconstruct a :class:`Map` from its ``map_id``.

    Parameters
    ----------
    map_id
        The ``map_id`` to search for.

    Returns
    -------
    map
        The result with the given ``map_id``.
    """
    return maps.Map.recover(map_id)


def _map_paths() -> Iterator[Path]:
    """Yield the paths to all existing map directories."""
    try:
        yield from mapping.maps_dir_path().iterdir()
    except FileNotFoundError:  # maps dir doesn't exist for some reason, which means we have no maps
        yield from ()


def map_ids() -> Tuple[str]:
    """Return a tuple containing the ``map_id`` for all existing maps."""
    return tuple(path.stem for path in _map_paths())


def map_results() -> Tuple[maps.Map, ...]:
    """Return a :class:`tuple` containing the :class:`Map` for all existing maps."""
    return tuple(recover(map_id) for map_id in map_ids())


def remove(map_id: str, not_exist_ok = True):
    """
    Remove the map with the given ``map_id``.

    Parameters
    ----------
    map_id
        The ``map_id`` to search for and remove.
    not_exist_ok
        If ``False``, raise :class:`htmap.exceptions.MapIdNotFound` if the ``map_id`` doesn't exist.
    """
    try:
        r = recover(map_id)
        r.remove()
    except (exceptions.MapIdNotFound, FileNotFoundError) as e:
        if not not_exist_ok:
            if not isinstance(e, exceptions.MapIdNotFound):
                raise exceptions.MapIdNotFound(f'map {map_id} not found') from e
            raise e


def force_remove(map_id: str):
    """
    Force-remove a map by trying to delete its map directory directly.

    .. warning::

        This operation is **not safe**, but might be necessary if your map directory has somehow become corrupted.
        See :ref:`cleanup-after-force-removal`.

    Parameters
    ----------
    map_id
        The ``map_id`` to force-remove.
    """
    shutil.rmtree(mapping.map_dir_path(map_id), ignore_errors = True)
    logger.debug(f'force-removed map {map_id}')


def clean():
    """Remove all existing maps."""
    logger.debug('cleaning maps directory...')
    for map_result in map_results():
        map_result.remove()
    logger.debug('cleaned maps directory')


def force_clean():
    """
    Force-remove all existing maps by trying to delete their map directories directly.

    .. warning::

        This operation is **not safe**, but might be necessary if your map directory has somehow become corrupted.
        See :ref:`cleanup-after-force-removal`.
    """
    for map_dir in _map_paths():
        shutil.rmtree(map_dir)

    logger.debug('force-cleaned maps directory')


def status() -> str:
    """Return a string containing a table showing the status of all existing maps, as well as their disk usage."""
    ids = map_ids()
    results = map_results()
    counts = [r.status_counts() for r in results]

    return utils.table(
        headers = ['Map ID'] + [str(d) for d in maps.Status.display_statuses()] + ['Data'],
        rows = [
            [map_id] + [count[d] for d in maps.Status.display_statuses()] + [utils.get_dir_size_as_str(mapping.map_dir_path(map_id))]
            for map_id, count in sorted(
                zip(ids, counts),
                key = lambda map_id_and_count: map_id_and_count[1][maps.Status.RUNNING],
            )
        ],
    )
