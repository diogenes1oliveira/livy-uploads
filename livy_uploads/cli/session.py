import logging
import sys
from typing import Optional

import click

from livy_uploads.cli.helpers.adapters import get_session_manager
from livy_uploads.cli.helpers.formats import display_item, display_list, with_format_option
from livy_uploads.cli.helpers.sessions import with_query_multifilter, with_query_singlefilter
from livy_uploads.client.manager import SessionManager
from livy_uploads.models.session import SessionQuery
from livy_uploads.project import Project

LOGGER = logging.getLogger(__name__)


with_wait_options = click.option(
    "--wait/--no-wait", is_flag=True, default=True, help="Wait for the operation to complete"
)
with_refresh_options = click.option(
    "--refresh/--no-refresh",
    is_flag=True,
    default=True,
    help="Refresh the session state",
)


@click.group(name="session")
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Livy sessions management"""
    project = Project.get()
    ctx.obj = get_session_manager(project=project)


@cli.command()
@with_query_multifilter
@with_format_option
@click.pass_obj
def list(
    manager: SessionManager,
    query: SessionQuery,
    format: Optional[str],
) -> None:
    """List Livy sessions matching the given filters."""
    LOGGER.info("listing sessions with query: %s", query.as_dict())

    infos = manager.find_all(query, refresh=False, keep_logs=False)
    columns = ["id", "name", "state", "kind", "appId", "owner"]
    display_list(infos, format=format, columns=columns)


@cli.command()
@with_query_singlefilter
@with_refresh_options
@click.pass_obj
def get(
    manager: SessionManager,
    query: SessionQuery,
    refresh: bool,
) -> None:
    """Get detailed information about a single session matching the filters.

    Fails if no session or multiple sessions match the filters.

    If no filters are provided, finds the default session name in the configuration.
    """
    if not query.has_identifier():
        if not (default_name := manager.default_session_name):
            raise click.BadArgumentUsage("no filters provided and no default session name in configuration")
        query = SessionQuery(name=default_name)

    info = manager.find_one(query, refresh=refresh, keep_logs=True)
    if info is None:
        click.echo("ERROR: no session found matching the filters", err=True)
        sys.exit(1)

    display_item(info)


@cli.command()
@with_wait_options
@click.option("--recreate/--no-recreate", is_flag=True, default=False, help="Recreate the session if it already exists")
@click.argument("NAME", type=str, required=False)
@click.pass_obj
def create(manager: SessionManager, wait: bool, recreate: bool, name: Optional[str]) -> None:
    """Create a new session.

    If the session already exists, it will be reused by default.
    """
    try:
        info = manager.create(name=name, recreate=recreate, wait=wait)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)

    display_item(info)


@cli.command()
@click.argument("ID", type=int)
@click.pass_obj
def poll(manager: SessionManager, id: int) -> None:
    """Poll the state of a session."""
    state = manager.poll(id)
    click.echo(state.value)


@cli.command()
@with_query_singlefilter
@with_wait_options
@click.pass_obj
def delete(
    manager: SessionManager,
    query: SessionQuery,
    wait: bool,
) -> None:
    """Delete sessions matching the filters."""
    if not query.has_identifier():
        if not (default_name := manager.default_session_name):
            raise click.BadArgumentUsage("no filters provided and no default session name in configuration")
        query = SessionQuery(name=default_name)

    manager.delete_all(query, wait=wait)


@cli.command()
@with_query_multifilter
@click.option("--logs/--no-logs", is_flag=True, default=True, help="Print the logs of the session (default: true)")
@with_refresh_options
@click.pass_obj
def follow(
    manager: SessionManager,
    query: SessionQuery,
    logs: bool,
    refresh: bool,
) -> None:
    """Follow the sessions matching the given filters."""
    LOGGER.info("following sessions with query: %s", query.as_dict())
    manager.follow(query, include_logs=logs, refresh=refresh)


if __name__ == "__main__":
    cli()
