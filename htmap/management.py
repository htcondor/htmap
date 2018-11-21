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
import datetime

from . import mapping, utils, exceptions
from .maps import Map, ComponentStatus

logger = logging.getLogger(__name__)


def load(map_id: str) -> Map:
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
    return Map.load(map_id)


def _map_paths() -> Iterator[Path]:
    """Yield the paths to all existing map directories."""
    try:
        yield from mapping.maps_dir_path().iterdir()
    except FileNotFoundError:  # maps dir doesn't exist for some reason, which means we have no maps
        yield from ()


def map_ids() -> Tuple[str]:
    """Return a tuple containing the ``map_id`` for all existing maps."""
    return tuple(path.stem for path in _map_paths())


def load_maps() -> Tuple[Map, ...]:
    """Return a :class:`tuple` containing the :class:`Map` for all existing maps."""
    return tuple(load(map_id) for map_id in map_ids())


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
        r = load(map_id)
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
    shutil.rmtree(str(mapping.map_dir_path(map_id).absolute()), ignore_errors = True)
    logger.debug(f'force-removed map {map_id}')


def clean():
    """Remove all existing maps."""
    logger.debug('cleaning maps directory...')
    for map_result in load_maps():
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
        shutil.rmtree(str(map_dir.absolute()))

    logger.debug('force-cleaned maps directory')


def status(
    maps = None,
    state = True,
    meta = True,
) -> str:
    """
    Return a string containing a table showing the status of all existing maps,
    as well as metadata like local disk usage and remote memory usage.
    """
    if maps is None:
        maps = load_maps()

    maps = sorted(maps, key = lambda m: m.map_id)

    headers = ['Map ID']
    if state:
        headers += [str(d) for d in ComponentStatus.display_statuses()]
    if meta:
        headers += ['Local Data', 'Max Memory', 'Max Runtime', 'Total Runtime']

    rows = []
    for map in maps:
        row = [map.map_id]
        if state:
            row.extend(map.status_counts[d] for d in ComponentStatus.display_statuses())
        if meta:
            row.extend([
                utils.get_dir_size_as_str(mapping.map_dir_path(map.map_id)),
                utils.num_bytes_to_str(max(map.memory_usage) * 1024 * 1024),  # memory usage is measured in MB
                max(map.runtime),
                sum(map.runtime, datetime.timedelta()),
            ])

        rows.append(row)

    return utils.table(
        headers = headers,
        rows = rows,
    )
