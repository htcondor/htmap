import sys

import htmap

import click


@click.group()
def cli():
    """HTMap command line tools."""
    pass


@cli.command()
def ids():
    """Print map ids."""
    click.echo(id_list())


@cli.command()
def status():
    """Print the status of maps."""
    click.echo(htmap.status())


@cli.command()
def clean():
    """Remove all maps."""
    htmap.clean()


@cli.command()
@click.argument('id')
def wait(id):
    """Wait for a map to complete."""
    cli_load(id).wait(show_progress_bar = True)


@cli.command()
@click.argument('id')
def remove(id):
    """Remove a map."""
    cli_load(id).remove()


@cli.command()
@click.argument('id')
def release(id):
    """Release a map."""
    cli_load(id).release()


@cli.command()
@click.argument('id')
def hold(id):
    """Hold a map."""
    cli_load(id).hold()


@cli.command()
@click.argument('id')
def reasons(id):
    """Print the hold reasons for a map."""
    click.echo(cli_load(id).hold_reasons())


@cli.command()
@click.command('id')
@click.command('newid')
def rename(id, newid):
    cli_load(id).rename(newid)


@cli.command()
def version():
    """Print HTMap version information."""
    click.echo(htmap.version())


def cli_load(map_id) -> htmap.Map:
    try:
        return htmap.load(map_id)
    except Exception as e:
        click.echo(f'Error: could not find a map with map_id {map_id}')
        click.echo(f'Your map ids are...')
        click.echo(id_list())

        sys.exit(1)


def id_list() -> str:
    return '\n'.join(htmap.map_ids())
