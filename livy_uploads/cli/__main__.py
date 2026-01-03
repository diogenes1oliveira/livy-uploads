import os
from typing import Optional

import click

from livy_uploads.cli.command import cli as command_cli
from livy_uploads.cli.config import cli as config_cli
from livy_uploads.cli.session import cli as session_cli
from livy_uploads.project import LOG_LEVEL_ENVVAR


@click.group()
@click.option(
    "-l", "--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]), help="Set the log level"
)
@click.pass_context
def cli(ctx: click.Context, log_level: Optional[str]) -> None:
    if log_level:
        os.environ[LOG_LEVEL_ENVVAR] = log_level


cli.add_command(config_cli)
cli.add_command(session_cli)
cli.add_command(command_cli)

if __name__ == "__main__":
    cli()
