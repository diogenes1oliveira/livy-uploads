import os
from typing import Optional

import click

from livy_uploads.cli.command import cli as command_cli
from livy_uploads.cli.configs import cli as configs_cli
from livy_uploads.cli.plugins import cli as plugins_cli
from livy_uploads.cli.session import cli as session_cli
from livy_uploads.configs.logs import LOG_LEVEL_ENVVAR


@click.group()
@click.option(
    "-l", "--log-level", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]), help="Set the log level"
)
@click.pass_context
def cli(ctx: click.Context, log_level: Optional[str]) -> None:
    if log_level:
        os.environ[LOG_LEVEL_ENVVAR] = log_level


cli.add_command(configs_cli)
cli.add_command(session_cli)
cli.add_command(command_cli)
cli.add_command(plugins_cli)

if __name__ == "__main__":
    cli()
