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
from typing import Optional

import logging
import sys
import time
import random
import functools
import itertools
from pathlib import Path

import htmap
from htmap.management import _status
from htmap.utils import read_events

import click
from click_didyoumean import DYMGroup

from halo import Halo
from spinners import Spinners

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SPINNERS = list(name for name in Spinners.__members__ if name.startswith('dots'))


def make_spinner(*args, **kwargs):
    return Halo(
        *args,
        spinner = random.choice(SPINNERS),
        stream = sys.stderr,
        **kwargs,
    )


CONTEXT_SETTINGS = dict(help_option_names = ['-h', '--help'])


def _read_ids_from_stdin(ctx, param, value):
    if not value and not click.get_text_stream('stdin').isatty():
        return click.get_text_stream('stdin').read().split()
    else:
        return value


@click.group(context_settings = CONTEXT_SETTINGS, cls = DYMGroup)
@click.option(
    '--verbose', '-v',
    is_flag = True,
    default = False,
    help = 'Show log messages as the CLI runs.',
)
def cli(verbose):
    """HTMap command line tools."""
    htmap.settings['CLI'] = True
    if verbose:
        _start_htmap_logger()


def _start_htmap_logger():
    """Initialize a basic logger for HTMap for the CLI."""
    htmap_logger = logging.getLogger('htmap')
    htmap_logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream = sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    htmap_logger.addHandler(handler)

    return handler


@cli.command()
def ids():
    """Print map ids. Can be piped to other commands."""
    click.echo(_id_list())


_HEADER_FMT = functools.partial(click.style, bold = True)


class _RowFmt:
    """Stateful callback function for row formatting in status table."""

    def __init__(self, maps):
        self.maps = maps
        self.idx = -1

    def __call__(self, text):
        self.idx += 1
        return click.style(text, fg = _map_fg(self.maps[self.idx]))


def _map_fg(map) -> Optional[str]:
    sc = map.status_counts

    if sc[htmap.ComponentStatus.HELD] > 0:
        return 'red'
    elif sc[htmap.ComponentStatus.COMPLETED] == len(map):
        return 'green'
    elif sc[htmap.ComponentStatus.RUNNING] > 0:
        return 'cyan'
    elif sc[htmap.ComponentStatus.IDLE] > 0:
        return 'yellow'
    else:
        return None


@cli.command()
@click.option(
    '--no-state',
    is_flag = True,
    default = False,
    help = 'Do not show map component states.',
)
@click.option(
    '--no-meta',
    is_flag = True,
    default = False,
    help = 'Do not show map metadata (memory, runtime, etc.).',
)
@click.option(
    '--json',
    is_flag = True,
    default = False,
    help = 'Output as human-readable JSON.',
)
@click.option(
    '--jsonc',
    is_flag = True,
    default = False,
    help = 'Output as compact JSON.',
)
@click.option(
    '--csv',
    is_flag = True,
    default = False,
    help = 'Output as CSV.',
)
@click.option(
    '--live',
    is_flag = True,
    default = False,
    help = 'Live reloading.',
)
@click.option(
    '--no-color',
    is_flag = True,
    default = False,
    help = 'Disable color.'
)
def status(no_state, no_meta, json, jsonc, csv, live, no_color):
    """Print the status of all maps."""
    if (json, jsonc, csv, live).count(True) > 1:
        click.echo('Error: no more than one of --json, --jsonc, --csv, or --live can be set.')
        sys.exit(1)

    maps = sorted(htmap.load_maps())
    with make_spinner(text = 'Reading map component statuses...'):
        read_events(maps)

    shared_kwargs = dict(
        include_state = not no_state,
        include_meta = not no_meta,
    )

    if json:
        msg = htmap.status_json(maps, **shared_kwargs)
    elif jsonc:
        msg = htmap.status_json(maps, **shared_kwargs, compact = True)
    elif csv:
        msg = htmap.status_csv(maps, **shared_kwargs)
    else:
        msg = _status(
            maps,
            **shared_kwargs,
            header_fmt = _HEADER_FMT if not no_color else None,
            row_fmt = _RowFmt(maps) if not no_color else None,
        )

    click.echo(msg)

    try:
        while live:
            num_lines = len(msg.splitlines())
            msg = _status(
                maps,
                **shared_kwargs,
                header_fmt = _HEADER_FMT if not no_color else None,
                row_fmt = _RowFmt(maps) if not no_color else None,  # don't cache, must pass fresh each time
            )

            sys.stdout.write(f'\033[{num_lines}A\r')
            click.echo(msg)
            time.sleep(1)
    except KeyboardInterrupt:  # bypass click's interrupt handling and let it exit quietly
        pass


@cli.command()
@click.option(
    '--yes',
    is_flag = True,
    default = False,
    help = 'Do not ask for confirmation.',
)
@click.option(
    '--force',
    is_flag = True,
    default = False,
    help = 'Do a force-clean instead of a normal clean.',
)
def clean(yes, force):
    """Remove all maps, with more options than the remove command."""
    if not yes:
        click.secho(
            'Are you sure you want to delete all of your maps permanently?'
            '\nThis action cannot be undone!'
            '\nType YES to delete all of your maps: ',
            fg = 'red',
        )
        answer = input('> ')
    else:
        answer = 'YES'

    if answer == 'YES':
        with make_spinner('Cleaning maps...') as spinner:
            if not force:
                htmap.clean()
            else:
                htmap.force_clean()

            spinner.succeed('Cleaned maps')
    else:
        click.echo('Answer was not YES, maps have not been deleted.')


def _multi_id_args(func):
    apply = [
        click.argument(
            'mapids',
            nargs = -1,
            callback = _read_ids_from_stdin,
            required = False,
        ),
        click.option(
            '--all',
            is_flag = True,
            default = False,
            help = 'Act on all maps.'
        ),
    ]

    for a in reversed(apply):
        func = a(func)

    return func


@cli.command()
@_multi_id_args
def wait(mapids, all):
    """Wait for maps to complete."""
    if all:
        mapids = htmap.map_ids()

    _check_map_ids(mapids)

    for map_id in mapids:
        m = _cli_load(map_id)
        if not m.is_done:
            m.wait(show_progress_bar = True)


@cli.command()
@_multi_id_args
@click.option(
    '--force',
    is_flag = True,
    default = False,
    help = 'Do a force-remove instead of a normal remove.',
)
def remove(mapids, force, all):
    """Remove maps."""
    if all:
        mapids = htmap.map_ids()

    _check_map_ids(mapids)

    for map_id in mapids:
        with make_spinner(f'Removing map {map_id} ...') as spinner:
            if not force:
                _cli_load(map_id).remove()
            else:
                htmap.force_remove(map_id)

            spinner.succeed(f'Removed map {map_id}')


@cli.command()
@_multi_id_args
def hold(mapids, all):
    """Hold maps."""
    if all:
        mapids = htmap.map_ids()

    _check_map_ids(mapids)

    for map_id in mapids:
        with make_spinner(f'Holding map {map_id} ...') as spinner:
            _cli_load(map_id).hold()
            spinner.succeed(f'Held map {map_id}')


@cli.command()
@_multi_id_args
def release(mapids, all):
    """Release maps."""
    if all:
        mapids = htmap.map_ids()

    _check_map_ids(mapids)

    for map_id in mapids:
        with make_spinner(f'Releasing map {map_id} ...') as spinner:
            _cli_load(map_id).release()
            spinner.succeed(f'Released map {map_id}')


@cli.command()
@_multi_id_args
def pause(mapids, all):
    """Pause maps."""
    if all:
        mapids = htmap.map_ids()

    _check_map_ids(mapids)

    for map_id in mapids:
        with make_spinner(f'Pausing map {map_id} ...') as spinner:
            _cli_load(map_id).pause()
            spinner.succeed(f'Paused map {map_id}')


@cli.command()
@_multi_id_args
def resume(mapids, all):
    """Resume maps."""
    if all:
        mapids = htmap.map_ids()

    _check_map_ids(mapids)

    for map_id in mapids:
        with make_spinner(f'Resuming map {map_id} ...') as spinner:
            _cli_load(map_id).resume()
            spinner.succeed(f'Resumed map {map_id}')


@cli.command()
@_multi_id_args
def vacate(mapids, all):
    """Force maps to give up their claimed resources."""
    if all:
        mapids = htmap.map_ids()

    _check_map_ids(mapids)

    for map_id in mapids:
        with make_spinner(f'Vacating map {map_id} ...') as spinner:
            _cli_load(map_id).vacate()
            spinner.succeed(f'Vacated map {map_id}')


@cli.command()
@_multi_id_args
def reasons(mapids, all):
    """Print the hold reasons for maps."""
    if all:
        mapids = htmap.map_ids()

    _check_map_ids(mapids)

    reps = []
    for map_id in mapids:
        m = _cli_load(map_id)

        if len(m.holds) == 0:
            continue
        name = click.style(
            f'Map {m.map_id} ({len(m.holds)} hold{"s" if len(m.holds) > 1 else ""})',
            bold = True,
        )
        reps.append(f'{name}\n{m.hold_report()}')

    click.echo('\n'.join(reps))


@cli.command()
@click.argument('mapid')
@click.argument('component', type = int)
def stdout(mapid, component):
    """Look at the stdout for a map component."""
    click.echo(_cli_load(mapid).stdout(component))


@cli.command()
@click.argument('mapid')
@click.argument('component', type = int)
def stderr(mapid, component):
    """Look at the stderr for a map component."""
    click.echo(_cli_load(mapid).stderr(component))


@cli.command()
@click.argument('mapid')
@click.option(
    '--limit',
    type = int,
    default = 0,
    help = 'The maximum number of error reports to show (0 for no limit).',
)
def errors(mapid, limit):
    """Look at detailed error reports for a map."""
    m = _cli_load(mapid)
    reports = m.error_reports()
    if limit > 0:
        itertools.islice(reports, limit)

    for report in reports:
        click.echo(report)


@cli.command()
@click.argument('mapid')
@click.option(
    '--components',
    help = 'Rerun the given components',
)
@click.option(
    '--incomplete',
    is_flag = True,
    default = False,
    help = 'Rerun only the incomplete components of the map.'
)
@click.option(
    '--all',
    is_flag = True,
    default = False,
    help = 'Rerun the entire map',
)
def rerun(mapid, components, incomplete, all):
    """Rerun part or all of a map."""
    if tuple(map(bool, (components, incomplete, all))).count(True) != 1:
        click.echo('Error: exactly one of --components, --incomplete, and --all can be used.')
        sys.exit(1)

    m = _cli_load(mapid)

    if components:
        components = [int(c) for c in components.split()]
        with make_spinner(f'Rerunning components {components} of map {mapid} ...') as spinner:
            m.rerun_components(components)
            spinner.succeed(f'Reran components {components} of map {mapid}')
    elif incomplete:
        with make_spinner(f'Rerunning incomplete components of map {mapid} ...') as spinner:
            m.rerun_incomplete()
            spinner.succeed(f'Reran incomplete components {components} of map {mapid}')
    elif all:
        with make_spinner(f'Rerunning map {mapid} ...') as spinner:
            m.rerun()
            spinner.succeed(f'Reran map {mapid}')


@cli.command()
@click.argument('mapid')
@click.argument('newid')
def rename(mapid, newid):
    """Rename a map."""
    with make_spinner(f'Renaming map {mapid} to {newid} ...') as spinner:
        _cli_load(mapid).rename(newid)
        spinner.succeed(f'Renamed map {mapid} to {newid}')


@cli.command()
def version():
    """Print HTMap version information."""
    click.echo(htmap.version())


@cli.command()
@click.option(
    '--user',
    is_flag = True,
    default = False,
    help = 'Display only user settings (the contents of ~/.htmaprc).',
)
def settings(user):
    """Print HTMap's settings."""
    if not user:
        click.echo(str(htmap.settings))
    else:
        path = Path.home() / '.htmaprc'
        try:
            txt = path.read_text(encoding = 'utf-8')
        except FileNotFoundError:
            click.echo(f'Error: you do not have a ~/.htmaprc file ({path} was not found)')
            sys.exit(1)
        click.echo(txt)


@cli.command()
@click.argument('setting')
@click.argument('value')
def set(setting, value):
    """Change a setting in your ~/.htmaprc file."""
    htmap.USER_SETTINGS[setting] = value
    htmap.USER_SETTINGS.save(Path.cwd() / '.htmaprc')
    click.echo(f'changed setting {setting} to {value}')


@cli.group()
def transplants():
    """Manage transplant installs."""


@transplants.command()
def info():
    """Display information on available transplant installs."""
    click.echo(htmap.transplant_info())


@transplants.command()
@click.argument('index')
def remove(index):
    """Remove a transplant install by index."""
    try:
        index = int(index)
    except ValueError:
        click.echo(f'Error: index was not an integer (was {index}).')
        sys.exit(1)

    try:
        transplant = htmap.transplants()[index]
    except IndexError:
        click.echo(f'Error: could not find a transplant install with index {index}.')
        click.echo(f'Your transplant installs are:')
        click.echo(htmap.transplant_info())
        sys.exit(1)

    transplant.remove()


@cli.group()
def edit():
    """Edit a map's attributes."""


@edit.command()
@click.argument('mapid')
@click.argument(
    'memory',
    type = int,
)
def memory(mapid, memory):
    """Set a map's requested memory (in MB)."""
    _cli_load(mapid).set_memory(memory)


@edit.command()
@click.argument('mapid')
@click.argument(
    'disk',
    type = int,
)
def disk(mapid, disk):
    """Set a map's requested disk (in KB)."""
    _cli_load(mapid).set_disk(disk)


@cli.group()
def path():
    """Get paths to various things. Mostly for debugging."""


@path.command()
def logs():
    """Echo the path to the HTMap log file."""
    click.echo(str(htmap.LOG_FILE))


@path.command()
@click.argument('mapid')
def map(mapid):
    """Echo the path to the map's directory."""
    click.echo(str(_cli_load(mapid)._map_dir))


@path.command()
@click.argument('mapid')
def events(mapid):
    """Echo the path to the map's job event log."""
    click.echo(str(_cli_load(mapid)._event_log_path))


def _cli_load(map_id: str) -> htmap.Map:
    with make_spinner(text = f'Loading map {map_id}...') as spinner:
        try:
            return htmap.load(map_id)
        except Exception as e:
            spinner.fail(f'Error: could not find a map with map_id {map_id}')
            click.echo(f'Your map ids are:', err = True)
            click.echo(_id_list(), err = True)
            sys.exit(1)


def _id_list() -> str:
    return '\n'.join(htmap.map_ids())


def _check_map_ids(map_ids):
    if len(map_ids) == 0:
        click.echo('Warning: no map ids were passed', err = True)
