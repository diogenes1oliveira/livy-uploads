import json
import os
import shlex
from typing import Optional

import click

from livy_uploads.cli.helpers.formats import display_item
from livy_uploads.project.project import Project


@click.group(name="configs")
@click.pass_context
def cli(ctx: click.Context) -> None:
    ctx.ensure_object(Project)


@cli.command()
@click.option("-c", "--compact", is_flag=True, default=False, help="Output in compact format")
@click.pass_obj
def dump(project: Project, compact: bool) -> None:
    "Display the final complete config"
    project.initialize()
    project.configs.setup()

    assert project.configs.raw_configs is not None

    display_item(project.configs.raw_configs, compact=compact)


@cli.command()
@click.option("-t", "--type", "typename", type=str, required=False, default=None, help="Type to convert the value to")
@click.argument("key", required=True)
@click.pass_obj
def get(project: Project, key: str, typename: Optional[str] = None) -> None:
    "Gets a specific config value"
    project.setup()

    if typename:
        try:
            t = project.converter.get_type_by_name(typename)
        except KeyError:
            click.echo(f"Type {typename!r} not found", err=True)
            raise click.Abort()
    else:
        t = None

    try:
        value = project.configs.get(key, t=t)  # type: ignore
    except (ValueError, TypeError, KeyError, IndexError) as e:
        click.echo(f"Failed to get value for key {key!r}: {e}", err=True)
        raise click.Abort()
    click.echo(repr(value))


@cli.command()
@click.pass_obj
def envs(project: Project) -> None:
    "Display the envs overriden through the .env files"
    project.initialize()
    project.envs.setup()

    assert project.envs.overriden_env_names is not None

    click.echo("Overriden environment variables:", err=True)

    for name in sorted(project.envs.overriden_env_names):
        curr = os.environ[name]
        quoted = shlex.quote(curr)
        if quoted != curr:
            click.echo(f"{name}={json.dumps(curr)}")
        else:
            click.echo(f"{name}={curr}")
