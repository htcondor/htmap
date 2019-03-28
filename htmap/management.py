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

from typing import Tuple, Iterable, Dict, Union, NamedTuple, Callable, List
import logging

from pathlib import Path
import datetime
import collections
import json
import csv
import io
import textwrap

from . import maps, tags, mapping, utils, state, settings, exceptions

logger = logging.getLogger(__name__)


def load(tag: str) -> maps.Map:
    """
    Reconstruct a :class:`Map` from its ``tag``.

    Parameters
    ----------
    tag
        The ``tag`` to search for.

    Returns
    -------
    map
        The result with the given ``tag``.
    """
    return maps.Map.load(tag)


def load_maps() -> Tuple[maps.Map, ...]:
    """Return a :class:`tuple` containing the :class:`Map` for all existing maps."""
    return tuple(load(tag) for tag in tags.get_tags())


def remove(tag: str, not_exist_ok: bool = True) -> None:
    """
    Remove the map with the given ``tag``.

    Parameters
    ----------
    tag
        The ``tag`` to search for and remove.
    not_exist_ok
        If ``False``, raise :class:`htmap.exceptions.MapIdNotFound` if the ``tag`` doesn't exist.
    """
    try:
        load(tag).remove()
    except (exceptions.TagNotFound, FileNotFoundError) as e:
        if not not_exist_ok:
            if not isinstance(e, exceptions.TagNotFound):
                raise exceptions.TagNotFound(f'map {tag} not found') from e
            raise e


def clean(*, all: bool = False) -> List[str]:
    """
    Remove maps.
    By default, only removes transient maps.
    If ``all`` is ``True``, remove **all** maps, including non-transient ones.

    Parameters
    ----------
    all
        If ``True``, remove all maps, not just transient ones.
        Defaults to ``False``.

    Returns
    -------
    cleaned_tags
        A list of the tags of the maps that were removed.
    """
    logger.debug('cleaning maps...')
    cleaned_tags = []
    for map in load_maps():
        if map.is_transient or all:
            cleaned_tags.append(map.tag)
            map.remove()
    logger.debug(f'cleaned maps {cleaned_tags}')
    return cleaned_tags


def _extract_status_data(
    map,
    include_state = True,
    include_meta = True,
) -> dict:
    sd = {}

    sd['Tag'] = f'{"* " if map.is_transient else ""}{map.tag}'

    if include_state:
        sc = collections.Counter(map.component_statuses)

        sd.update({str(k): sc[k] for k in state.ComponentStatus.display_statuses()})

    if include_meta:
        sd['Local Data'] = utils.num_bytes_to_str(map.local_data)
        sd['Max Memory'] = utils.num_bytes_to_str(max(map.memory_usage) * 1024 * 1024)
        sd['Max Runtime'] = str(max(map.runtime))
        sd['Total Runtime'] = str(sum(map.runtime, datetime.timedelta()))

    return sd


def status(
    maps: Iterable[maps.Map] = None,
    include_state: bool = True,
    include_meta: bool = True,
) -> str:
    """
    Return a formatted table containing information on the given maps.

    Parameters
    ----------
    maps
        The maps to display information on.
        If ``None``, displays information on all existing maps.
    include_state
        If ``True``, include information on the state of the map's components.
    include_meta
        If ``True``, include information about the map's memory usage, disk usage, and runtime.

    Returns
    -------
    table :
        A text table containing information on the given maps.
    """
    return _status(
        maps = maps,
        include_state = include_state,
        include_meta = include_meta,
    )


def _status(
    maps: Iterable[maps.Map] = None,
    include_state: bool = True,
    include_meta: bool = True,
    header_fmt: Callable[[str], str] = None,
    row_fmt: Callable[[str], str] = None,
) -> str:
    if maps is None:
        maps = sorted(load_maps(), key = lambda m: (m.is_transient, m.tag))

    headers = ['Tag']
    if include_state:
        utils.read_events(maps)
        headers += [str(d) for d in state.ComponentStatus.display_statuses()]
    if include_meta:
        headers += ['Local Data', 'Max Memory', 'Max Runtime', 'Total Runtime']

    rows = [
        _extract_status_data(map, include_state = include_state, include_meta = include_meta)
        for map in maps
    ]

    return utils.table(
        headers = headers,
        rows = rows,
        header_fmt = header_fmt,
        row_fmt = row_fmt,
        alignment = {'Tag': 'ljust'},
    )


def status_json(
    maps: Iterable[maps.Map] = None,
    include_state: bool = True,
    include_meta: bool = True,
    compact: bool = False,
) -> str:
    """
    Return a JSON-formatted string containing information on the given maps.

    Disk and memory usage are reported in bytes.
    Runtimes are reported in seconds.

    Parameters
    ----------
    maps
        The maps to display information on.
        If ``None``, displays information on all existing maps.
    include_state
        If ``True``, include information on the state of the map's components.
    include_meta
        If ``True``, include information about the map's memory usage, disk usage, and runtime.
    compact
        If ``True``, the JSON will be formatted in the most compact possible representation.

    Returns
    -------
    json :
        A JSON-formatted dictionary containing information on the given maps.
    """
    if maps is None:
        maps = load_maps()

    maps = sorted(maps, key = lambda m: m.tag)

    if include_state:
        utils.read_events(maps)

    j = {}
    for map in maps:
        sc = collections.Counter(map.component_statuses)
        d: Dict[str, Union[dict, str, int, float]] = {'tag': map.tag}
        if include_state:
            status_to_count = {}
            for status in state.ComponentStatus.display_statuses():
                status_to_count[status.value.lower()] = sc[status]
            d['component_status_counts'] = status_to_count
        if include_meta:
            d['local_disk_usage'] = utils.get_dir_size(mapping.map_dir_path(map.tag))
            d['max_memory_usage'] = max(map.memory_usage) * 1024 * 1024
            d['max_runtime'] = max(map.runtime).total_seconds()
            d['total_runtime'] = sum(map.runtime, datetime.timedelta()).total_seconds()

        j[map.tag] = d

    if compact:
        separators = (',', ':')
        indent = None
    else:
        separators = (', ', ': ')
        indent = 4

    return json.dumps(j, separators = separators, indent = indent)


def status_csv(
    maps: Iterable[maps.Map] = None,
    include_state: bool = True,
    include_meta: bool = True,
) -> str:
    """
    Return a CSV-formatted string containing information on the given maps.

    Disk and memory usage are reported in bytes.
    Runtimes are reported in seconds.

    Parameters
    ----------
    maps
        The maps to display information on.
        If ``None``, displays information on all existing maps.
    include_state
        If ``True``, include information on the state of the map's components.
    include_meta
        If ``True``, include information about the map's memory usage, disk usage, and runtime.

    Returns
    -------
    csv :
        A CSV-formatted table containing information on the given maps.
    """
    if maps is None:
        maps = load_maps()

    maps = sorted(maps, key = lambda m: m.tag)

    if include_state:
        utils.read_events(maps)

    rows = []
    for map in maps:
        sc = collections.Counter(map.component_statuses)
        row: Dict[str, Union[str, int, float]] = {'tag': map.tag}
        if include_state:
            for status in state.ComponentStatus.display_statuses():
                row[status.value.lower()] = sc[status]
        if include_meta:
            row['local_disk_usage'] = utils.get_dir_size(mapping.map_dir_path(map.tag))
            row['max_memory_usage'] = max(map.memory_usage) * 1024 * 1024
            row['max_runtime'] = max(map.runtime).total_seconds()
            row['total_runtime'] = sum(map.runtime, datetime.timedelta()).total_seconds()

        rows.append(row)

    if len(maps) == 0:
        return ''

    output = io.StringIO()
    writer = csv.DictWriter(output, list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)

    return output.getvalue()


class Transplant(NamedTuple):
    hash: str
    path: Path
    created: datetime.datetime
    size: int
    packages: Tuple[str]

    @classmethod
    def load(cls, path: Path):
        """

        Parameters
        ----------
        path
            The path to the transplant install.

        Returns
        -------

        """

        return cls(
            hash = path.stem,
            path = path,
            created = datetime.datetime.fromtimestamp(path.stat().st_ctime),
            size = path.stat().st_size,
            packages = tuple(path.with_suffix('.pip').read_text().strip().split('\n')),
        )

    def remove(self):
        self.path.with_suffix('.pip').unlink()
        self.path.unlink()
        logger.info(f'removed transplant install {self.hash}, which was created at {self.created}')


def transplants() -> Tuple[Transplant, ...]:
    return tuple(sorted(
        (
            Transplant.load(p)
            for p in Path(settings['TRANSPLANT.DIR']).iterdir()
            if p.suffix != '.pip'
        ),
        key = lambda t: t.created,
    ))


def transplant_info() -> str:
    entries = []
    for q, t in enumerate(transplants()):
        packages = '\n'.join(
            textwrap.wrap(
                ', '.join(t.packages),
                subsequent_indent = ' ' * 4,
                break_long_words = False,
            )
        )
        entry = f'# {q}\nHash: {t.hash}\nCreated at: {t.created}\nPackages: {packages}'
        entries.append(entry)

    return utils.rstr('\n\n'.join(entries))
