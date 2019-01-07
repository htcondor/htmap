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
def status(no_state, no_meta, json, jsonc, csv, live):
    """Print the status of all maps."""
    if (json, jsonc, csv, live).count(True) > 1:
        click.echo('Error: no more than one of --json, --jsonc, --csv, or --live can be set.')
        sys.exit(1)

    maps = htmap.load_maps()
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
            header_fmt = _HEADER_FMT,
            row_fmt = _RowFmt(maps),
        )

    click.echo(msg)

    try:
        while live:
            num_lines = len(msg.splitlines())
            msg = _status(
                maps,
                **shared_kwargs,
                header_fmt = _HEADER_FMT,
                row_fmt = _RowFmt(maps),
            )

            sys.stdout.write(f'\033[{num_lines}A\r')
            click.echo(msg)
            time.sleep(1)
    except KeyboardInterrupt:  # bypass click's interrupt handling and let it exit quietly
        pass


class _RowFmt:
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
    """Remove all maps."""
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


@cli.command()
@click.argument(
    'ids',
    nargs = -1,
    callback = _read_ids_from_stdin,
    required = False,
)
def wait(ids):
    """Wait for maps to complete."""
    _check_map_ids(ids)
    for map_id in ids:
        _cli_load(map_id).wait(show_progress_bar = True)


@cli.command()
@click.argument(
    'ids',
    nargs = -1,
    callback = _read_ids_from_stdin,
    required = False,
)
@click.option(
    '--force',
    is_flag = True,
    default = False,
    help = 'Do a force-remove instead of a normal remove.',
)
def remove(ids, force):
    """Remove maps."""
    _check_map_ids(ids)
    for map_id in ids:
        with make_spinner(f'Removing map {map_id} ...') as spinner:
            if not force:
                _cli_load(map_id).remove()
            else:
                htmap.force_remove(map_id)

            spinner.succeed(f'Removed map {map_id}')


@cli.command()
@click.argument(
    'ids',
    nargs = -1,
    callback = _read_ids_from_stdin,
    required = False,
)
def release(ids):
    """Release maps."""
    _check_map_ids(ids)
    for map_id in ids:
        with make_spinner(f'Releasing map {map_id} ...') as spinner:
            _cli_load(map_id).release()
            spinner.succeed(f'Released map {map_id}')


@cli.command()
@click.argument(
    'ids',
    nargs = -1,
    callback = _read_ids_from_stdin,
    required = False,
)
def hold(ids):
    """Hold maps."""
    _check_map_ids(ids)
    for map_id in ids:
        with make_spinner(f'Holding map {map_id} ...') as spinner:
            _cli_load(map_id).hold()
            spinner.succeed(f'Held map {map_id}')


@cli.command()
@click.argument(
    'ids',
    nargs = -1,
    callback = _read_ids_from_stdin,
    required = False,
)
def reasons(ids):
    """Print the hold reasons for maps."""
    _check_map_ids(ids)
    for map_id in ids:
        click.echo(_cli_load(map_id).hold_report())


@cli.command()
@click.argument('id')
@click.argument('newid')
def rename(id, newid):
    """Rename a map."""
    with make_spinner(f'Renaming map {id} to {newid} ...') as spinner:
        _cli_load(id).rename(newid)
        spinner.succeed(f'Renamed map {id} to {newid}')


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
