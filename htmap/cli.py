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

from typing import Optional, List, Collection, Tuple

import logging
import sys
import time
import collections
import random
import functools
import shutil
from pathlib import Path

import htcondor
import htmap
from htmap import names, __version__
from htmap.management import _status, read_events

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
        enabled = htmap.settings['CLI.SPINNERS_ON'],
        **kwargs,
    )

color = click.option(
    '--color/--no-color',
    default = True,
    help = 'Toggle colorized output (defaults to colorized).'
)

CONTEXT_SETTINGS = dict(help_option_names = ['-h', '--help'])


@click.group(context_settings = CONTEXT_SETTINGS, cls = DYMGroup)
@click.option(
    '--verbose', '-v',
    is_flag = True,
    default = False,
    help = 'Show log messages as the CLI runs.',
)
@click.version_option(
    version = __version__,
    prog_name = "HTMap",
    message = '%(prog)s version %(version)s',
)
def cli(verbose):
    """
    HTMap command line tools.
    """
    if verbose:
        _start_htmap_logger()
    logger.debug(f'CLI called with arguments "{" ".join(sys.argv[1:])}"')
    htmap.settings['CLI.IS_CLI'] = True


def _start_htmap_logger():
    """Initialize a basic logger for HTMap for the CLI."""
    htmap.settings["CLI.SPINNERS_ON"] = False

    htmap_logger = logging.getLogger('htmap')
    htmap_logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream = sys.stderr)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s ~ %(levelname)s ~ %(name)s:%(lineno)d ~ %(message)s'))

    htmap_logger.addHandler(handler)

    return handler


@cli.command()
@click.option(
    '--pattern', '-p',
    multiple = True,
    help = 'Act on maps whose tags match glob-style patterns. Patterns must be enclosed in "". Pass -p multiple times for multiple patterns.'
)
def tags(pattern):
    """
    Print the tags of existing maps.
    """
    if len(pattern) == 0:
        click.echo(_fmt_tag_list())
    else:
        for p in set(pattern):
            click.echo(_fmt_tag_list(p))


_HEADER_FMT = functools.partial(click.style, bold = True)


class _RowFmt:
    """Stateful callback function for row formatting in status table."""

    def __init__(self, maps):
        self.maps = maps
        self.idx = -1

    def __call__(self, text):
        self.idx += 1
        return click.style(text, fg = _map_fg(self.maps[self.idx]))


def _map_fg(map: htmap.Map) -> Optional[str]:
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
    '--state/--no-state',
    default = True,
    help = 'Toggle display of component states (defaults to enabled).',
)
@click.option(
    '--meta/--no-meta',
    default = True,
    help = 'Toggle display of map metadata like memory, runtime, etc. (defaults to enabled).',
)
@click.option(
    '--format',
    type = click.Choice(['text', 'json', 'json_compact', 'csv']),
    default = 'text',
    help = 'Select output format: plain text, JSON, compact JSON, or CSV (defaults to plain text)'
)
@click.option(
    '--live/--no-live',
    default = False,
    help = 'Toggle live reloading of the status table (defaults to not live).',
)
@color
def status(state, meta, format, live, color):
    """
    Print a status table for all of your maps.

    Transient maps are prefixed with a leading "*".
    """
    if format != 'text' and live:
        click.echo('ERROR: cannot produce non-text live data.', err = True)
        sys.exit(1)

    maps = sorted((_cli_load(tag) for tag in htmap.get_tags()), key = lambda m: (m.is_transient, m.tag))
    for map in maps:
        if state:
            with make_spinner(text = f'Reading component statuses for map {map.tag}...'):
                map.component_statuses
        if meta:
            with make_spinner(text = f'Determining local data usage for map {map.tag}...'):
                map.local_data

    shared_kwargs = dict(
        include_state = state,
        include_meta = meta,
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
            header_fmt = _HEADER_FMT if color else None,
            row_fmt = _RowFmt(maps) if color else None,
        )
    else:  # pragma: unreachable
        # this is a safeguard; this code is actually unreachable, because
        # click detects the invalid choice before we hit this
        click.echo(f'ERROR: unknown format option "{format}"', err = True)
        sys.exit(2)

    click.echo(msg)

    try:
        while live:
            prev_lines = list(msg.splitlines())
            prev_len_lines = [len(line) for line in prev_lines]

            maps = sorted(htmap.load_maps(), key = lambda m: (m.is_transient, m.tag))
            msg = _status(
                maps,
                **shared_kwargs,
                header_fmt = _HEADER_FMT if color else None,
                row_fmt = _RowFmt(maps) if color else None,  # don't cache, must pass fresh each time
            )

            move = f'\033[{len(prev_len_lines)}A\r'
            clear = '\n'.join(' ' * len(click.unstyle(line)) for line in prev_lines) + '\n'

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
    """
    Clean up transient maps by removing them.

    Maps that have never had a tag explicitly set are assigned randomized tags
    and marked as "transient". This command removes maps marked transient
    (and can also remove all maps, not just transient ones, if the --all option
    is passed).
    """
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
            '--pattern', '-p',
            multiple = True,
            help = 'Act on maps whose tags match glob-style patterns. Pass -p multiple times for multiple patterns.'
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
    return sorted(tag for tag in htmap.get_tags() if tag.startswith(incomplete) and tag not in args)


TOTAL_WIDTH = 80

STATUS_AND_COLOR = [
    (htmap.ComponentStatus.COMPLETED, 'green'),
    (htmap.ComponentStatus.RUNNING, 'cyan'),
    (htmap.ComponentStatus.IDLE, 'yellow'),
    (htmap.ComponentStatus.UNMATERIALIZED, 'yellow'),
    (htmap.ComponentStatus.SUSPENDED, 'red'),
    (htmap.ComponentStatus.HELD, 'red'),
    (htmap.ComponentStatus.ERRORED, 'red'),
    (htmap.ComponentStatus.REMOVED, 'magenta'),
    (htmap.ComponentStatus.UNKNOWN, 'magenta'),
]

STATUS_TO_COLOR = dict(STATUS_AND_COLOR)


def _calculate_bar_component_len(count, total, bar_width):
    if count == 0:
        return 0

    return max(int((count / total) * bar_width), 1)


@cli.command()
@_multi_tag_args
def wait(tags, pattern, all):
    """Wait for maps to complete."""
    tags = _get_tags(all, pattern, tags)

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


@cli.command(short_help = "Remove maps; all components will be removed from the queue and all data associated with the maps will be permanently deleted.")
@_multi_tag_args
@click.option(
    '--force',
    is_flag = True,
    default = False,
    help = 'Do not wait for HTCondor to remove the map components before removing local data.',
)
def remove(tags, pattern, all, force):
    """
    This command removes a map from the Condor queue. Functionally, this
    command aborts a job.

    This function will completely remove a map from the Condor
    queue regardless of job state (running, executing, waiting, etc).
    All data associated with a removed map is permanently deleted.

    """
    tags = _get_tags(all, pattern, tags)

    for tag in tags:
        with make_spinner(f'Removing map {tag} ...') as spinner:
            _cli_load(tag).remove(force = force)

            spinner.succeed(f'Removed map {tag}')


@cli.command(short_help = "Hold maps; components will be prevented from running until released.")
@_multi_tag_args
def hold(tags, pattern, all):
    """
    This command holds a map.
    The components of the map will not be allowed to run until released
    (see the release command).


    HTCondor may itself hold your map components if it detects that
    something has gone wrong with them. Resolve the underlying problem,
    then use the release command to allow the components to run again.
    """
    tags = _get_tags(all, pattern, tags)

    for tag in tags:
        with make_spinner(f'Holding map {tag} ...') as spinner:
            _cli_load(tag).hold()
            spinner.succeed(f'Held map {tag}')


@cli.command(short_help = "Release maps; held components will become idle again.")
@_multi_tag_args
def release(tags, pattern, all):
    """
    This command releases a map, undoing holds.
    The held components of a released map will become idle again.

    HTCondor may itself hold your map components if it detects that
    something has gone wrong with them. Resolve the underlying problem,
    then use this command to allow the components to run again.
    """
    tags = _get_tags(all, pattern, tags)

    for tag in tags:
        with make_spinner(f'Releasing map {tag} ...') as spinner:
            _cli_load(tag).release()
            spinner.succeed(f'Released map {tag}')


@cli.command(short_help = "Pause maps; components will stop executing, but keep their resource claims.")
@_multi_tag_args
def pause(tags, pattern, all):
    """
    This command pauses a map.
    The running components of a paused map will keep their resource claims, but
    will stop actively executing.
    The map can be un-paused by resuming it (see the resume command).
    """
    tags = _get_tags(all, pattern, tags)

    for tag in tags:
        with make_spinner(f'Pausing map {tag} ...') as spinner:
            _cli_load(tag).pause()
            spinner.succeed(f'Paused map {tag}')


@cli.command(short_help = "Resume maps after pause; components will resume executing on their claimed resource.")
@_multi_tag_args
def resume(tags, pattern, all):
    """
    This command resumes a map (reverses the pause command).
    The running components of a resumed map will resume execution on their
    claimed resources.
    """
    tags = _get_tags(all, pattern, tags)

    for tag in tags:
        with make_spinner(f'Resuming map {tag} ...') as spinner:
            _cli_load(tag).resume()
            spinner.succeed(f'Resumed map {tag}')


@cli.command(short_help = "Vacate maps; components will give up claimed resources and become idle.")
@_multi_tag_args
def vacate(tags, pattern, all):
    """
    This command vacates a map.
    The running components of a vacated map will give up their claimed
    resources and become idle again.

    Checkpointing maps will still have access to their last checkpoint,
    and will resume from it as if execution was interrupted for any other
    reason.
    """
    tags = _get_tags(all, pattern, tags)

    for tag in tags:
        with make_spinner(f'Vacating map {tag} ...') as spinner:
            _cli_load(tag).vacate()
            spinner.succeed(f'Vacated map {tag}')


@cli.command()
@_multi_tag_args
def reasons(tags, pattern, all):
    """
    Print the hold reasons for map components.

    HTCondor may hold your map components if it detects that
    something has gone wrong with them. Resolve the underlying problem,
    then use the release command to allow the components to run again.
    """
    tags = _get_tags(all, pattern, tags)

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


tag = click.argument('tag', autocompletion = _autocomplete_tag)

component = click.argument(
    'component',
    type = int,
)

timeout = click.option(
    '--timeout',
    default = None,
    type = int,
    help = "How long to wait (in seconds) for the file to be available. If not set (the default), wait forever."
)


@cli.command()
@tag
@component
@timeout
def stdout(tag, component, timeout):
    """Look at the stdout for a map component."""
    click.echo(_cli_load(tag).stdout.get(component, timeout = timeout))


@cli.command()
@tag
@component
@timeout
def stderr(tag, component, timeout):
    """Look at the stderr for a map component."""
    click.echo(_cli_load(tag).stderr.get(component, timeout = timeout))


@cli.command()
@_multi_tag_args
@click.option(
    '--limit',
    type = int,
    default = 0,
    help = 'The maximum number of error reports to show (0, the default, for no limit).',
)
def errors(tags, pattern, all, limit):
    """Show execution error reports for map components."""
    tags = _get_tags(all, pattern, tags)

    count = 0
    for tag in tags:
        m = _cli_load(tag)

        for report in m.error_reports():
            click.echo(report)
            count += 1
            if 0 < limit <= count:
                return


@cli.command(short_help = "Print out the status of the individual components of a map.")
@tag
@click.option(
    '--status',
    type = click.Choice(list(htmap.ComponentStatus), case_sensitive = False),
    default = None,
    help = 'Print out only components that have this status. Case-insensitive. If not passed, print out the stats of all components (the default).',
)
@color
def components(tag, status, color):
    """Print out the status of the individual components of a map."""
    m = _cli_load(tag)

    if status is None:
        longest_component = len(str(m.components[-1]))
        for component, s in enumerate(m.component_statuses):
            click.secho(
                f'{str(component).rjust(longest_component)} {s}',
                fg = STATUS_TO_COLOR[s] if color else None,
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
    """
    Rerun (part of) a map from scratch.

    The selected components must be completed or errored.
    See the subcommands of this command group for different ways to specify
    which components to rerun.

    Any existing output of rerun components is removed; they are re-submitted
    to the HTCondor queue with their original map options (i.e., without any
    subsequent edits).
    """
    pass


@rerun.command(name = 'components')
@tag
@click.argument(
    'components',
    nargs = -1,
    type = int,
)
def rerun_components(tag, components):
    """
    Rerun selected components from a single map.

    Any existing output of re-run components is removed; they are re-submitted
    to the HTCondor queue with their original map options (i.e., without any
    subsequent edits).
    """
    m = _cli_load(tag)

    with make_spinner(f'Rerunning components {components} of map {tag} ...') as spinner:
        try:
            m.rerun(components)
        except htmap.exceptions.CannotRerunComponents as err:
            click.echo(f"ERROR: {err}", err = True)
        spinner.succeed(f'Reran components {components} of map {tag}')


@rerun.command()
@_multi_tag_args
def map(tags, pattern, all):
    """
    Rerun all of the components of any number of maps.

    Any existing output of re-run components is removed; they are re-submitted
    to the HTCondor queue with their original map options (i.e., without any
    subsequent edits).
    """
    tags = _get_tags(all, pattern, tags)

    for tag in tags:
        m = _cli_load(tag)
        with make_spinner(f'Rerunning map {tag} ...') as spinner:
            m.rerun()
            spinner.succeed(f'Reran map {tag}')


@cli.command()
@tag
@click.argument('new')
def retag(tag, new):
    """
    Change the tag of an existing map.

    Retagging a map makes it not transient.
    Maps that have never had an explicit tag given to them are transient
    and can be easily cleaned up via the clean command.
    """
    with make_spinner(f'Retagging map {tag} to {new} ...') as spinner:
        _cli_load(tag).retag(new)
        spinner.succeed(f'Retagged map {tag} to {new}')


@cli.command(short_help = "Print HTMap and HTCondor Python bindings version information.")
def version():
    """Print HTMap and HTCondor Python bindings version information."""
    click.echo(htmap.version())
    click.echo(htcondor.version())


@cli.command()
@click.option(
    '--user',
    is_flag = True,
    default = False,
    help = 'Display only user settings (the contents of ~/.htmaprc).',
)
def settings(user):
    """
    Print HTMap's current settings.

    By default, this command shows the merger of your user settings from
    ~/.htmaprc and HTMap's own default settings. To show only your user
    settings, pass the --user option.
    """
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


@cli.command(name = 'set')
@click.argument('setting')
@click.argument('value')
def set_(setting, value):
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


@transplants.command(name = 'remove')
@click.argument('index')
def remove_transplant(index):
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
    """
    Edit a map's attributes (e.g., its memory request).

    Edits do not affect components that are currently running. To "restart"
    components so that they see the new attribute value, consider vacating
    their map (see the vacate command).
    """


@edit.command()
@tag
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
    """
    Set a map's requested memory.

    Edits do not affect components that are currently running. To "restart"
    components so that they see the new attribute value, consider vacating
    their map (see the vacate command).
    """
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
@tag
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
    """
    Set a map's requested disk.

    Edits do not affect components that are currently running. To "restart"
    components so that they see the new attribute value, consider vacating
    their map (see the vacate command).
    """
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
@tag
def path(tag):
    """
    Get paths to parts of HTMap's data storage for a map.

    This command is mostly useful for debugging or interfacing with other tools.
    The tag argument is a map tag, optionally followed by a colon (:) and a
    target.

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
@click.option(
    '--view / --no-view',
    default = False,
    help = "If enabled, display the contents of the current log file instead of its path (defaults to disabled)."
)
def logs(view):
    """
    Print the path to HTMap's current log file.

    The log file rotates, so if you need to go further back in time,
    look at the rotated log files (stored next to the current log file).
    """
    log_file = Path(htmap.settings['HTMAP_DIR']) / names.LOGS_DIR / 'htmap.log'

    if view:
        with log_file.open() as f:
            click.echo_via_pager(f)
            return

    click.echo(str(log_file))


@cli.command(short_help = "Enable autocompletion for HTMap CLI commands and tags in your shell. Run this once!")
@click.option(
    "--shell",
    required = True,
    type = click.Choice(["bash", "zsh", "fish"], case_sensitive = False),
    help = "Which shell to enable autocompletion for.",
)
@click.option(
    "--force",
    is_flag = True,
    default = False,
    help = "Append the autocompletion activation command even if it already exists.",
)
@click.option(
    "--destination",
    type = click.Path(dir_okay = False, writable = True, resolve_path = True),
    default = None,
    help = "Append the autocompletion activation command to this file instead of the shell default.",
)
def autocompletion(shell, force, destination):
    """
    Enable autocompletion for HTMap CLI commands and tags in your shell.

    This command should only need to be run once.

    Note that your Python
    environment must be available (i.e., running "htmap" must work) by the time
    the autocompletion-enabling command runs in your shell configuration file.
    """
    cmd, dst = {
        "bash": (
            r'eval "$(_HTMAP_COMPLETE=source_bash htmap)"',
            Path.home() / ".bashrc",
        ),
        "zsh": (
            r'eval "$(_HTMAP_COMPLETE=source_zsh htmap)"',
            Path.home() / ".zshrc",
        ),
        "fish": (
            r"eval (env _HTMAP_COMPLETE=source_fish htmap)",
            Path.home() / ".config" / "fish" / "completions" / "htmap.fish",
        ),
    }[shell]

    if destination is not None:
        dst = Path(destination)

    if not force and cmd in dst.read_text():
        click.secho(
            f"Autocompletion already enabled for {shell} (in {dst}).",
            fg = "yellow",
        )
        return

    with dst.open(mode = "a") as f:
        f.write(f"\n# enable htmap CLI autocompletion\n{cmd}\n")

    click.secho(
        f"Autocompletion enabled for {shell} (startup command added to {dst}). Restart your shell to use it.",
        fg = "green",
    )


def _cli_load(tag: str) -> htmap.Map:
    with make_spinner(text = f'Loading map {tag}...') as spinner:
        try:
            return htmap.load(tag)
        except Exception as e:
            spinner.fail()
            logger.exception(f"Could not find a map with tag {tag}")
            click.echo(f'ERROR: could not find a map with tag {tag}', err = True)
            click.echo(f'Your map tags are:', err = True)
            click.echo(_fmt_tag_list(), err = True)
            sys.exit(1)


def _get_tags(all: bool, pattern: List[str], tags: List[str]) -> Tuple[str, ...]:
    if all:
        tags = list(htmap.get_tags())
    elif len(pattern) > 0:
        tags += _get_tags_from_patterns(pattern)

    _check_tags(tags)

    return tuple(tags)


def _get_tags_from_patterns(patterns: List[str]) -> List[str]:
    return list(set(sum((htmap.get_tags(p) for p in patterns), ())))


def _check_tags(tags: Collection[str]) -> None:
    if len(tags) == 0:
        click.echo('WARNING: no tags were found', err = True)


def _fmt_tag_list(pattern: Optional[str] = None) -> str:
    return '\n'.join(htmap.get_tags(pattern))


if __name__ == '__main__':
    cli()
