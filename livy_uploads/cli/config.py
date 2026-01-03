import json

import click

from livy_uploads.cli.helpers.formats import display_item
from livy_uploads.project import Project


@click.group(name="config")
@click.pass_context
def cli(ctx: click.Context) -> None:
    ctx.obj = Project.get()


@cli.command()
@click.option("-c", "--compact", is_flag=True, default=False, help="Output in compact format")
@click.pass_obj
def dump(project: Project, compact: bool) -> None:
    config = project.config.raw_values
    display_item(config, compact=compact)
