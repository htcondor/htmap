import logging
import sys

import htmap

import click

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CONTEXT_SETTINGS = dict(help_option_names = ['-h', '--help'])


@click.group(context_settings = CONTEXT_SETTINGS)
@click.option('--verbose', '-v', is_flag = True, default = False, help = 'Show log messages as the CLI runs.')
def cli(verbose):
    """HTMap command line tools."""
    if verbose:
        _start_htmap_logger()


def _start_htmap_logger():
    htmap_logger = logging.getLogger('htmap')
    htmap_logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(stream = sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    htmap_logger.addHandler(handler)

    return handler


@cli.command()
def ids():
    """Print map ids."""
    click.echo(_id_list())


@cli.command()
def status():
    """Print the status of all maps."""
    click.echo(htmap.status())


@cli.command()
def clean():
    """Remove all maps."""
    htmap.clean()


@cli.command()
@click.argument('ids', nargs = -1)
def wait(ids):
    """Wait for maps to complete."""
    for map_id in ids:
        _cli_load(map_id).wait(show_progress_bar = True)


@cli.command()
@click.argument('ids', nargs = -1)
def remove(ids):
    """Remove maps."""
    for map_id in ids:
        _cli_load(map_id).remove()


@cli.command()
@click.argument('ids', nargs = -1)
def release(ids):
    """Release maps."""
    for map_id in ids:
        _cli_load(map_id).release()


@cli.command()
@click.argument('ids', nargs = -1)
def hold(ids):
    """Hold maps."""
    for map_id in ids:
        _cli_load(map_id).hold()


@cli.command()
@click.argument('ids', nargs = -1)
def reasons(ids):
    """Print the hold reasons for maps."""
    for map_id in ids:
        click.echo(_cli_load(map_id).hold_report())


@cli.command()
@click.argument('id')
@click.argument('newid')
def rename(id, newid):
    """Rename a map."""
    _cli_load(id).rename(newid)


@cli.command()
def version():
    """Print HTMap version information."""
    click.echo(htmap.version())


def _cli_load(map_id) -> htmap.Map:
    try:
        return htmap.load(map_id)
    except Exception as e:
        click.echo(f'Error: could not find a map with map_id {map_id}')
        click.echo(f'Your map ids are...')
        click.echo(_id_list())

        sys.exit(1)


def _id_list() -> str:
    return '\n'.join(htmap.map_ids())
