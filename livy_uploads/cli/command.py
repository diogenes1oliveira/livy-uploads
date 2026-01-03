import logging
from typing import Optional

import click

from livy_uploads.cli.helpers.adapters import build_parser, get_session_manager
from livy_uploads.cli.helpers.formats import display_item, display_list, with_compact_option, with_format_option
from livy_uploads.cli.helpers.sessions import with_query_singlefilter
from livy_uploads.client.handle import SessionHandle
from livy_uploads.commands.helpers import get_command, get_commands
from livy_uploads.models.session import SessionQuery

LOGGER = logging.getLogger(__name__)


@click.group(name="command")
@click.pass_context
def cli(ctx: click.Context) -> None:
    "Session commands"
    pass


@cli.command(name="list")
@with_format_option
def list_(format: Optional[str]) -> None:
    "List the available commands"
    commands = [cmd.as_json() for cmd in get_commands()]
    click.echo(f"found {len(commands)} commands", err=True)
    display_list(commands, format, ["name", "description", "impl"], compactify=lambda cmd: cmd["name"])


@cli.command(
    context_settings={
        "ignore_unknown_options": True,
        "allow_interspersed_args": False,
    }
)
@click.pass_context
@with_query_singlefilter
@with_compact_option
@click.argument("command", type=str, required=False, default=None)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def run(
    ctx: click.Context,
    query: SessionQuery,
    compact: Optional[bool],
    command: Optional[str],
    args: tuple[str, ...],
) -> None:
    """
    Executes a command in the session.

    COMMAND is the name of the command to execute.

    ARGS are additional arguments to pass to the command.
    """
    if command is None:
        # If no command is provided, manually show the CLI help, that won't be shown by click automatically
        click.echo(ctx.get_help(), err=True)
        ctx.exit(0)
    else:
        # First arg is the command name
        try:
            cls = get_command(command)
        except ValueError as e:
            click.echo(f"ERROR: {e}", err=True)
            ctx.exit(1)

        # all remainder args are forwarded to build the command instance
        parser = build_parser(cls)
        ns = parser.parse_args(args)
        cmd = cls(**dict(ns._get_kwargs()))

        manager = get_session_manager()
        handle = SessionHandle.get(manager, query)

        LOGGER.info("invoking %s on %s", cmd, handle)
        result = cmd.run(handle)
        display_item(result, compact)


if __name__ == "__main__":
    cli()
