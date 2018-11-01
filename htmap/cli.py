import sys

import htmap

import click


@click.group()
def cli():
    """HTMap command line tools."""
    pass


@cli.command()
def status():
    """Print the status of maps."""
    click.echo(htmap.status())


@cli.command()
@click.argument('id')
def wait(id):
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


def cli_load(map_id) -> htmap.Map:
    try:
        return htmap.load(map_id)
    except:
        click.echo(f'ERR: could not find map with map_id {map_id}')
        sys.exit(1)
