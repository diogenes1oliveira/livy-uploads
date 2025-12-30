"""Command-line interface for livy-uploads client."""

import dataclasses
import functools
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

import click
from typing_extensions import Self

from livy_uploads.client.managers import StreamSessionEventsCallback
from livy_uploads.client.sparkmagic import SparkMagic
from livy_uploads.configs.setup import save_config as setup_save_config
from livy_uploads.logs import configure_logger
from livy_uploads.models.session import SessionKind, SessionQuery, SessionState
from livy_uploads.paths import load_envfile

LOGGER = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


@dataclasses.dataclass()
class CliContext:
    basedir: Path = Path.cwd()

    @functools.cached_property
    def sparkmagic(self) -> SparkMagic:
        return SparkMagic.setup(basedir=self.basedir)

    def setup(self) -> Self:
        self.sparkmagic
        return self


def _query_singlefilter(f: F) -> F:
    for decorator in [
        click.option("--name", type=str, help="Filter by session name"),
        click.option("--id", type=int, help="Filter by session ID"),
        click.option("--app-id", type=str, help="Filter by application ID"),
    ]:
        f = decorator(f)

    return f


def _query_multifilter(f: F) -> F:
    f = _query_singlefilter(f)
    for decorator in [
        click.option("--state", type=click.Choice([s.value for s in SessionState]), help="Filter by session state"),
        click.option("--kind", type=click.Choice([k.value for k in SessionKind]), help="Filter by session kind"),
        click.option("--queue", type=str, help="Filter by queue name"),
        click.option("--owner", type=str, help="Filter by owner"),
    ]:
        f = decorator(f)
    return f


@click.group()
@click.pass_context
def cli(ctx: click.Context) -> None:
    """sparkrl: Spark Remote Layer"""
    configure_logger()
    cli_ctx: CliContext = ctx.ensure_object(CliContext)

    if env_path := load_envfile():
        cli_ctx.basedir = env_path.parent


@cli.command()
@click.option(
    "--as-post-json", is_flag=True, default=False, help="Output as a JSON object that can be used to POST to /sessions"
)
@click.pass_obj
def config(ctx: CliContext, as_post_json: bool) -> None:
    """Dump the Livy JSON configuration."""
    if as_post_json:
        data = ctx.sparkmagic.get_session_post_json()
    else:
        data = ctx.sparkmagic.config.as_json()

    click.echo(json.dumps(data, indent=2))


@cli.command()
@click.option("--env-filename", type=str, help="Environment filename override")
@click.option("--quote", is_flag=True, default=False, help="Quote the values")
@click.pass_obj
def save_config(ctx: CliContext, env_filename: Optional[str], quote: bool = False) -> None:
    """Resolves and saves the configuration to an environment file."""
    setup_save_config(env_filename=env_filename, quote=quote)


@cli.command(context_settings=dict(ignore_unknown_options=True, help_option_names=[]))
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.pass_obj
def run(ctx: CliContext, args: tuple[str, ...]) -> None:
    """Runs an arbitrary command in a properly configured shell environment."""

    if not args:
        raise click.BadArgumentUsage("no command provided")

    ctx.setup()
    LOGGER.info("running command %r in pid=%d", args[0], os.getpid())
    os.execvp(args[0], args)


@cli.command()
@_query_multifilter
@click.option(
    "--logs/--no-logs", is_flag=True, default=False, help="Include log output in the output (default: disabled)"
)
@click.option("--refresh/--no-refresh", is_flag=True, default=False, help="Refresh the session")
@click.option("--format", type=click.Choice(["json", "jsonlines"]), default="json", help="Output format")
@click.pass_obj
def list(
    ctx: CliContext,
    name: str,
    state: Optional[str],
    id: Optional[int],
    kind: Optional[str],
    app_id: Optional[str],
    queue: Optional[str],
    owner: Optional[str],
    logs: bool,
    format: str,
    refresh: bool,
) -> None:
    """List Livy sessions matching the given filters."""
    query = SessionQuery(
        name=name,
        state=SessionState.parse_optional(state),
        id=id,
        kind=SessionKind.parse_optional(kind),
        appId=app_id,
        queue=queue,
        owner=owner,
    )
    LOGGER.info("listing sessions with query: %s", query.as_dict())

    if format == "jsonlines":
        for info in ctx.sparkmagic.manager.find(query, refresh=refresh, keep_logs=logs):
            click.echo(json.dumps(info.as_json()))
    else:
        infos = ctx.sparkmagic.manager.find_all(query, refresh=refresh, keep_logs=logs)
        click.echo(json.dumps([info.as_json() for info in infos], indent=2))


@cli.command()
@click.option("--name", type=str, help="Filter by session name")
@click.option("--id", type=int, help="Filter by session ID")
@click.option("--app-id", type=str, help="Filter by application ID")
@click.option("--refresh/--no-refresh", is_flag=True, default=True, help="Refresh the session")
@click.pass_obj
def get(
    ctx: CliContext,
    name: Optional[str],
    id: Optional[int],
    app_id: Optional[str],
    refresh: bool,
) -> None:
    """Get detailed information about a single session matching the filters.

    Fails if no session or multiple sessions match the filters.

    If no filters are provided, finds the default session name in the configuration.
    """
    if not name and not id and not app_id:
        if not (name := ctx.sparkmagic.config.as_session_info().name):
            raise click.BadArgumentUsage("no filters provided and no default session name in configuration")

    query = SessionQuery(
        name=name,
        id=id,
        appId=app_id,
    )
    info = ctx.sparkmagic.manager.find_one(query, refresh=refresh, keep_logs=True)
    if info is None:
        click.echo("ERROR: no session found matching the filters", err=True)
        sys.exit(1)

    click.echo(json.dumps(info.as_json(), indent=2))


@cli.command()
@click.option("--no-wait", type=bool, default=False, help="Do not wait for the session to be ready")
@click.option("--recreate", type=bool, default=False, help="Recreate the session if it already exists")
@click.argument("NAME", type=str, required=False)
@click.pass_obj
def create(ctx: CliContext, no_wait: bool, recreate: bool, name: Optional[str]) -> None:
    """Create a new session.

    If the session already exists, it will be reused by default.
    """
    try:
        info = ctx.sparkmagic.manager.create(name=name, recreate=recreate)
    except ValueError as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)

    if not no_wait:
        ctx.sparkmagic.manager.wait(
            query=SessionQuery(id=info.id),
            state=SessionState.IDLE,
            retry_policy=ctx.sparkmagic.config.readiness_policy,
            delete=True,
            callback=StreamSessionEventsCallback(),
        )
        info = ctx.sparkmagic.manager.refresh(info)

    click.echo(json.dumps(info.as_json(), indent=2))


@cli.command()
@click.argument("ID", type=int)
@click.pass_obj
def poll(ctx: CliContext, id: int) -> None:
    """Poll the state of a session."""
    state = ctx.sparkmagic.manager.poll(id)
    click.echo(state.value)


@cli.command()
@_query_singlefilter
@click.option("--no-wait", type=bool, default=False, help="Do not wait for the session to be deleted")
@click.pass_obj
def delete(
    ctx: CliContext,
    name: Optional[str],
    id: Optional[int],
    app_id: Optional[str],
    no_wait: bool,
) -> None:
    """Delete sessions matching the filters."""
    if not name and not id and not app_id:
        if not (name := ctx.sparkmagic.config.as_session_info().name):
            raise click.BadArgumentUsage("no filters provided and no default session name in configuration")

    query = SessionQuery(
        name=name,
        id=id,
        appId=app_id,
    )
    ctx.sparkmagic.manager.delete_all(query)
    if not no_wait:
        ctx.sparkmagic.manager.wait(
            query=query,
            state=SessionState.GONE,
            retry_policy=ctx.sparkmagic.config.finish_policy,
            delete=True,
        )


@cli.command()
@_query_multifilter
@click.option("--logs/--no-logs", is_flag=True, default=True, help="Print the logs of the session (default: true)")
@click.option("--refresh/--no-refresh", is_flag=True, default=False, help="Refresh the session")
@click.pass_obj
def follow(
    ctx: CliContext,
    name: str,
    state: Optional[str],
    id: Optional[int],
    kind: Optional[str],
    app_id: Optional[str],
    queue: Optional[str],
    owner: Optional[str],
    logs: bool,
    refresh: bool,
) -> None:
    """Follow the sessions matching the given filters."""
    query = SessionQuery(
        name=name,
        state=SessionState.parse_optional(state),
        id=id,
        kind=SessionKind.parse_optional(kind),
        appId=app_id,
        queue=queue,
        owner=owner,
    )
    LOGGER.info("following sessions with query: %s", query.as_dict())
    ctx.sparkmagic.manager.follow(query, include_logs=logs, refresh=refresh)


if __name__ == "__main__":
    cli()
