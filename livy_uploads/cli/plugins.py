from typing import Optional

import click

from livy_uploads.cli.helpers.formats import display_list, with_format_option
from livy_uploads.plugins import LOADER, constants


@click.group(name="plugins")
def cli() -> None:
    pass


@cli.command(name="list")
@with_format_option
@click.option("-n", "--no-resolve", is_flag=True, default=None, help="Resolve the loaders before listing")
def list_(format: Optional[str], no_resolve: Optional[bool] = None) -> None:
    "List the configured plugin loaders"

    click.echo(f"INFO: using APPNAME={constants.APPNAME}", err=True)

    if no_resolve:
        loader = LOADER.setup()
    else:
        (loader,) = LOADER.resolve()

    infos = [loader.as_json() for loader in loader.loaders]
    extra_cols = ["default_uris"] if no_resolve else []

    display_list(
        infos,
        format,
        ["type", "description", "uri", "tags", "priority", *extra_cols],
        compactify=lambda info: info["uri"],
    )


@cli.command(name="find")
@with_format_option
@click.option("--all", is_flag=True, default=False, help="Find all matches")
@click.argument("type", metavar="TYPE", type=click.Choice(["path", "class", "object"]))
@click.argument("pattern", type=str, required=False, default="*")
def find(type: str, pattern: str, all: bool, format: Optional[str]) -> None:
    "Scan the available plugins for the given pattern"

    (loader,) = LOADER.resolve()

    if type == "path":
        if all:
            items = list(loader.find_paths(pattern=pattern))
        else:
            item = next(loader.find_paths(pattern=pattern))
            items = [item]

        rows = [
            {"path": str(item.path), "uri": item.uri, "loader": item.loader.impl_typename() if item.loader else None}
            for item in items
        ]
    else:
        raise NotImplementedError(f"finding {type=!r} is not implemented yet")

    display_list(rows, format, ["path", "uri", "loader"], compactify=lambda row: row["path"])


if __name__ == "__main__":
    cli()
