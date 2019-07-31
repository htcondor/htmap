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
import collections
import random
import functools
import itertools
import shutil
from pathlib import Path

import htmap
from htmap import names
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


@click.group(context_settings = CONTEXT_SETTINGS, cls = DYMGroup)
@click.option(
    '--verbose', '-v',
    is_flag = True,
    default = False,
    help = 'Show log messages as the CLI runs.',
)
def cli(verbose):
    """HTMap command line tools."""
    logger.debug(f'CLI called with arguments "{" ".join(sys.argv[1:])}"')
    htmap.settings['CLI'] = True
    if verbose:
        _start_htmap_logger()


def _start_htmap_logger():
    """Initialize a basic logger for HTMap for the CLI."""
    htmap_logger = logging.getLogger('htmap')
    htmap_logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream = sys.stderr)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s ~ %(levelname)s ~ %(name)s:%(funcName)s:%(lineno)d ~ %(message)s'))

    htmap_logger.addHandler(handler)

    return handler


@cli.command()
def tags():
    """Print tags. Can be piped to other commands."""
    click.echo(_tag_list())


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
    sc = collections.Counter(map.component_statuses)

    if sc[htmap.state.ComponentStatus.REMOVED] > 0:
        return 'magenta'
    elif (sc[htmap.state.ComponentStatus.HELD] + sc[htmap.state.ComponentStatus.ERRORED]) > 0:
        return 'red'
    elif sc[htmap.state.ComponentStatus.COMPLETED] == len(map):
        return 'green'
    elif sc[htmap.state.ComponentStatus.RUNNING] > 0:
        return 'cyan'
    elif sc[htmap.state.ComponentStatus.IDLE] > 0:
        return 'yellow'
    else:
        return 'white'


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
    '--format',
    type = click.Choice(['text', 'json', 'json_compact', 'csv']),
    default = 'text',
    help = 'Select output format: plain text, JSON, compact JSON, or CSV.'
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
def status(no_state, no_meta, format, live, no_color):
    """
    Print the status of all maps.
    Transient maps are prefixed with a *
    """
    if format != 'text' and live:
        click.echo('ERROR: cannot produce non-text live data.', err = True)
        sys.exit(1)

    maps = sorted((_cli_load(tag) for tag in htmap.get_tags()), key = lambda m: (m.is_transient, m.tag))
    if not no_state:
        with make_spinner(text = 'Reading map component statuses...'):
            read_events(maps)

    shared_kwargs = dict(
        include_state = not no_state,
        include_meta = not no_meta,
    )

    if format == 'json':
        msg = htmap.status_json(maps, **shared_kwargs)
    elif format == 'json_compact':
        msg = htmap.status_json(maps, **shared_kwargs, compact = True)
    elif format == 'csv':
        msg = htmap.status_csv(maps, **shared_kwargs)
    elif format == 'text':
        msg = _status(
            maps,
            **shared_kwargs,
            header_fmt = _HEADER_FMT if not no_color else None,
            row_fmt = _RowFmt(maps) if not no_color else None,
        )
    else:
        click.echo(f'ERROR: unknown format option "{format}"', err = True)
        sys.exit(1)

    click.echo(msg)

    try:
        while live:
            prev_len_lines = [len(line) for line in msg.splitlines()]

            maps = sorted(htmap.load_maps(), key = lambda m: (m.is_transient, m.tag))
            msg = _status(
                maps,
                **shared_kwargs,
                header_fmt = _HEADER_FMT if not no_color else None,
                row_fmt = _RowFmt(maps) if not no_color else None,  # don't cache, must pass fresh each time
            )

            move = f'\033[{len(prev_len_lines)}A\r'
            clear = '\n'.join(' ' * l for l in prev_len_lines) + '\n'

            sys.stdout.write(move + clear + move)
            click.echo(msg)

            time.sleep(1)
    except KeyboardInterrupt:  # bypass click's interrupt handling and let it exit quietly
        return


@cli.command()
@click.option(
    '--all',
    is_flag = True,
    default = False,
    help = 'Remove non-transient maps as well.',
)
def clean(all):
    """Clean up maps."""
    with make_spinner('Cleaning maps...') as spinner:
        cleaned_tags = htmap.clean(all = all)
        spinner.succeed(f'Cleaned maps {", ".join(cleaned_tags)}')


def _multi_tag_args(func):
    apply = [
        click.argument(
            'tags',
            nargs = -1,
            callback = _read_tags_from_stdin,
            autocompletion = _autocomplete_tag,
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


def _read_tags_from_stdin(ctx, param, value):
    if not value and not click.get_text_stream('stdin').isatty():
        return click.get_text_stream('stdin').read().split()
    else:
        return value


def _autocomplete_tag(ctx, args, incomplete):
    return [tag for tag in htmap.get_tags() if incomplete in tag]


TOTAL_WIDTH = 80

STATUS_AND_COLOR = [
    (htmap.ComponentStatus.COMPLETED, 'green'),
    (htmap.ComponentStatus.RUNNING, 'cyan'),
    (htmap.ComponentStatus.IDLE, 'yellow'),
    (htmap.ComponentStatus.SUSPENDED, 'red'),
    (htmap.ComponentStatus.HELD, 'red'),
    (htmap.ComponentStatus.ERRORED, 'red'),
    (htmap.ComponentStatus.REMOVED, 'magenta'),
]

STATUS_TO_COLOR = dict(STATUS_AND_COLOR)


def _calculate_bar_component_len(count, total, bar_width):
    if count == 0:
        return 0

    return max(int((count / total) * bar_width), 1)


@cli.command()
@_multi_tag_args
def wait(tags, all):
    """Wait for maps to complete."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    if len(tags) == 0:
        return

    maps = sorted((_cli_load(tag) for tag in tags), key = lambda m: (m.is_transient, m.tag))
    with make_spinner(text = 'Reading map component statuses...'):
        read_events(maps)

    try:
        longest_tag_len = max(len(tag) for tag in tags)
        bar_width = min(shutil.get_terminal_size().columns, TOTAL_WIDTH - (longest_tag_len + 1))

        click.echo('\n' * (len(maps) - 1))
        while any(not map.is_done for map in maps):
            bars = []
            for map in maps:
                sc = collections.Counter(map.component_statuses)

                bar_lens = {
                    status: _calculate_bar_component_len(sc[status], len(map), bar_width)
                    for status, _ in STATUS_AND_COLOR
                }
                bar_lens[htmap.ComponentStatus.IDLE] += bar_width - sum(bar_lens.values())

                bar = ''.join([
                    click.style('█' * bar_lens[status], fg = color)
                    for status, color in STATUS_AND_COLOR
                ])

                bars.append(f'{map.tag.ljust(longest_tag_len)} {bar}')

            msg = '\n'.join(bars)
            move = f'\033[{len(maps)}A\r'

            sys.stdout.write(move)
            click.echo(msg)

            time.sleep(1)
    except KeyboardInterrupt:  # bypass click's interrupt handling and let it exit quietly
        return


@cli.command()
@_multi_tag_args
@click.option(
    '--force',
    is_flag = True,
    default = False,
    help = 'Do not wait for HTCondor to remove the map components.',
)
def remove(tags, all, force):
    """Remove maps."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    for tag in tags:
        with make_spinner(f'Removing map {tag} ...') as spinner:
            _cli_load(tag).remove(force = force)

            spinner.succeed(f'Removed map {tag}')


@cli.command()
@_multi_tag_args
def hold(tags, all):
    """Hold maps."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    for tag in tags:
        with make_spinner(f'Holding map {tag} ...') as spinner:
            _cli_load(tag).hold()
            spinner.succeed(f'Held map {tag}')


@cli.command()
@_multi_tag_args
def release(tags, all):
    """Release maps."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    for tag in tags:
        with make_spinner(f'Releasing map {tag} ...') as spinner:
            _cli_load(tag).release()
            spinner.succeed(f'Released map {tag}')


@cli.command()
@_multi_tag_args
def pause(tags, all):
    """Pause maps."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    for tag in tags:
        with make_spinner(f'Pausing map {tag} ...') as spinner:
            _cli_load(tag).pause()
            spinner.succeed(f'Paused map {tag}')


@cli.command()
@_multi_tag_args
def resume(tags, all):
    """Resume maps."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    for tag in tags:
        with make_spinner(f'Resuming map {tag} ...') as spinner:
            _cli_load(tag).resume()
            spinner.succeed(f'Resumed map {tag}')


@cli.command()
@_multi_tag_args
def vacate(tags, all):
    """Force maps to give up their claimed resources."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    for tag in tags:
        with make_spinner(f'Vacating map {tag} ...') as spinner:
            _cli_load(tag).vacate()
            spinner.succeed(f'Vacated map {tag}')


@cli.command()
@_multi_tag_args
def reasons(tags, all):
    """Print the hold reasons for maps."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    reps = []
    for tag in tags:
        m = _cli_load(tag)

        if len(m.holds) == 0:
            continue
        name = click.style(
            f'Map {m.tag} ({len(m.holds)} hold{"s" if len(m.holds) > 1 else ""})',
            bold = True,
        )
        reps.append(f'{name}\n{m.hold_report()}')

    click.echo('\n'.join(reps))


@cli.command()
@click.argument('tag', autocompletion = _autocomplete_tag)
@click.argument('component', type = int)
def stdout(tag, component):
    """Look at the stdout for a map component."""
    click.echo(_cli_load(tag).stdout[component])


@cli.command()
@click.argument('tag', autocompletion = _autocomplete_tag)
@click.argument('component', type = int)
def stderr(tag, component):
    """Look at the stderr for a map component."""
    click.echo(_cli_load(tag).stderr[component])


@cli.command()
@_multi_tag_args
@click.option(
    '--limit',
    type = int,
    default = 0,
    help = 'The maximum number of error reports to show (0 for no limit).',
)
def errors(tags, all, limit):
    """Look at detailed error reports for a map."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    count = 0
    for tag in tags:
        m = _cli_load(tag)

        for report in m.error_reports():
            click.echo(report)
            count += 1
            if 0 < limit <= count:
                return


@cli.command()
@click.argument('tag', autocompletion = _autocomplete_tag)
@click.option(
    '--status',
    default = None,
    help = 'Print out only components that have this status. Case-insensitive.',
)
@click.option(
    '--no-color',
    is_flag = True,
    default = False,
    help = 'Disable color.'
)
def components(tag, status, no_color):
    m = _cli_load(tag)

    if status is None:
        longest_component = len(str(m.components[-1]))
        for component, s in enumerate(m.component_statuses):
            click.secho(
                f'{str(component).rjust(longest_component)} {s}',
                fg = STATUS_TO_COLOR[s] if not no_color else None,
            )
    else:
        try:
            status = htmap.ComponentStatus[status.upper()]
        except KeyError:
            click.echo(
                f"ERROR: {status} is not a recognized component status (valid options: {' | '.join(str(cs) for cs in htmap.ComponentStatus)})",
                err = True,
            )
            sys.exit(1)

        click.echo(' '.join(str(c) for c in m.components_by_status()[status]))


@cli.group()
def rerun():
    """Rerun (part of) a map."""


@rerun.command()
@click.argument('tag', autocompletion = _autocomplete_tag)
@click.argument(
    'components',
    nargs = -1,
    type = int,
)
def components(tag, components):
    """Rerun selected components from a single map."""
    m = _cli_load(tag)

    with make_spinner(f'Rerunning components {components} of map {tag} ...') as spinner:
        try:
            m.rerun(components)
        except htmap.exceptions.CannotRerunComponents as err:
            click.echo(f"ERROR: {err}", err = True)
        spinner.succeed(f'Reran components {components} of map {tag}')


@rerun.command()
@_multi_tag_args
def map(tags, all):
    """Rerun all of the components of any number of maps."""
    if all:
        tags = htmap.get_tags()

    _check_tags(tags)

    for tag in tags:
        m = _cli_load(tag)
        with make_spinner(f'Rerunning map {tag} ...') as spinner:
            m.rerun()
            spinner.succeed(f'Reran map {tag}')


@cli.command()
@click.argument('tag', autocompletion = _autocomplete_tag)
@click.argument('new')
def retag(tag, new):
    """Retag a map."""
    with make_spinner(f'Retagging map {tag} to {new} ...') as spinner:
        _cli_load(tag).retag(new)
        spinner.succeed(f'Retagged map {tag} to {new}')


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
            click.echo(
                f'ERROR: you do not have a ~/.htmaprc file ({path} was not found)',
                err = True,
            )
            sys.exit(1)
        click.echo(txt)


@cli.command()
@click.argument('setting')
@click.argument('value')
def set(setting, value):
    """Change a setting in your ~/.htmaprc file."""
    htmap.USER_SETTINGS[setting] = value
    htmap.USER_SETTINGS.save(Path.home() / '.htmaprc')
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
        click.echo(f'WARNING: index was not an integer (was {index})', err = True)
        sys.exit(1)

    try:
        transplant = htmap.transplants()[index]
    except IndexError:
        click.echo(f'ERROR: could not find a transplant install with index {index}', err = True)
        click.echo(f'Your transplant installs are:')
        click.echo(htmap.transplant_info())
        sys.exit(1)

    transplant.remove()


@cli.group()
def edit():
    """Edit a map's attributes."""


@edit.command()
@click.argument('tag', autocompletion = _autocomplete_tag)
@click.argument(
    'memory',
    type = int,
)
@click.option(
    '--unit',
    type = click.Choice(['MB', 'GB'], case_sensitive = False),
    default = 'MB',
)
def memory(tag, memory, unit):
    """Set a map's requested memory."""
    map = _cli_load(tag)
    msg = f'memory request for map {tag} to {memory} {unit}'

    multiplier = {
        'MB': 1,
        'GB': 1024,
    }[unit.upper()]
    memory_in_mb = memory * multiplier

    with make_spinner(text = f'Setting {msg}') as spinner:
        map.set_memory(memory_in_mb)
        spinner.succeed(f'Set {msg}')


@edit.command()
@click.argument('tag', autocompletion = _autocomplete_tag)
@click.argument(
    'disk',
    type = int,
)
@click.option(
    '--unit',
    type = click.Choice(['KB', 'MB', 'GB'], case_sensitive = False),
    default = 'GB',
)
def disk(tag, disk, unit):
    """Set a map's requested disk."""
    map = _cli_load(tag)
    msg = f'disk request for map {tag} to {disk} {unit}'

    multiplier = {
        'KB': 1,
        'MB': 1024,
        'GB': 1024 * 1024,
    }[unit.upper()]
    disk_in_kb = disk * multiplier

    with make_spinner(text = f'Setting {msg}') as spinner:
        map.set_disk(disk_in_kb)
        spinner.succeed(f'Set {msg}')


@cli.command()
@click.argument('tag', autocompletion = _autocomplete_tag)
def path(tag):
    """
    Get paths to various things.
    Mostly for debugging.
    The tag argument is a map tag, optionally followed by a colon (:) and a target.

    If you have a map tagged "foo",
    these commands would give the following paths (command -> path):

    \b
    htmap path foo -> the path to the map directory
    htmap path foo:map -> also the path to the map directory
    htmap path foo:tag -> the path to the map's tag file
    htmap path foo:events -> the map's event log
    htmap path foo:logs -> directory containing component stdout and stderr
    htmap path foo:inputs -> directory containing component inputs
    htmap path foo:outputs -> directory containing component outputs
    """
    if tag.count(':') == 0:
        tag, target = tag, None
    elif tag.count(':') == 1:
        tag, target = tag.split(':')
    else:
        click.echo('ERROR: can only have one ":" in tag', err = True)
        sys.exit(1)

    map = _cli_load(tag)
    map_dir = map._map_dir
    paths = {
        None: map_dir,
        'map': map_dir,
        'events': map_dir / names.EVENT_LOG,
        'logs': map_dir / names.JOB_LOGS_DIR,
        'inputs': map_dir / names.INPUTS_DIR,
        'outputs': map_dir / names.OUTPUTS_DIR,
        'tag': Path(htmap.settings['HTMAP_DIR']) / names.TAGS_DIR / map.tag,
    }

    click.echo(str(paths[target]))


@cli.command()
def logs():
    """
    Echo the path to HTMap's current log file.

    The log file rotates, so if you need to go further back in time,
    look at the rotated log files (stored next to the current log file).
    """
    click.echo(Path(htmap.settings['HTMAP_DIR']) / names.LOGS_DIR / 'htmap.log')


def _cli_load(tag: str) -> htmap.Map:
    with make_spinner(text = f'Loading map {tag}...') as spinner:
        try:
            return htmap.load(tag)
        except Exception as e:
            spinner.fail()
            click.echo(f'ERROR: could not find a map with tag {tag}', err = True)
            click.echo(f'Your map tags are:', err = True)
            click.echo(_tag_list(), err = True)
            sys.exit(1)


def _tag_list() -> str:
    return '\n'.join(htmap.get_tags())


def _check_tags(tags):
    if len(tags) == 0:
        click.echo('WARNING: no tags were passed', err = True)


if __name__ == '__main__':
    cli()
