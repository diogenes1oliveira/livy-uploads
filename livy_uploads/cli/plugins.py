import logging
from typing import Iterable, Optional, cast

import click

from livy_uploads.cli.helpers.formats import display_item, display_list, with_compact_option, with_format_option
from livy_uploads.plugins import Implementation, constants, implementation_as_json
from livy_uploads.project import Project

LOGGER = logging.getLogger(__name__)


@click.group(name="plugins")
@click.pass_context
def cli(ctx: click.Context) -> None:
    ctx.ensure_object(Project)


@cli.command(name="constants")
@with_compact_option
def constants_(compact: bool) -> None:
    "Display the plugin app constants"
    display_item(constants.as_json(), compact=compact)


@cli.command(name="list")
@with_format_option
@click.option("-n", "--no-resolve", is_flag=True, default=None, help="Resolve the loaders before listing")
@click.pass_obj
def list_(project: Project, format: Optional[str], no_resolve: Optional[bool] = None) -> None:
    "List the configured plugin loaders"

    LOGGER.debug("using APPNAME=%s", constants.PROJECT_APPNAME)

    if no_resolve:
        project.initialize()
    else:
        project.setup()

    infos = [loader.as_json() for loader in project.loader.loaders]
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
@click.pass_obj
def find(project: Project, type: str, pattern: str, all: bool, format: Optional[str]) -> None:
    "Scan the available plugins for the given pattern"
    project.setup()

    if type == "path":
        if all:
            items = list(project.loader.find_paths(pattern=pattern))
        else:
            item = next(project.loader.find_paths(pattern=pattern))
            items = [item]

        rows = [
            {"path": str(item.path), "uri": item.uri, "loader": item.loader.impl_typename() if item.loader else None}
            for item in items
        ]
    else:
        raise NotImplementedError(f"finding {type=!r} is not implemented yet")

    display_list(rows, format, ["path", "uri", "loader"], compactify=lambda row: row["path"])


@cli.group(name="impls", invoke_without_command=True)
@with_format_option
@click.pass_context
def impls(ctx: click.Context, format: Optional[str]) -> None:
    "List the registered implementable base classes"
    project: Project = ctx.ensure_object(Project)
    project.setup()

    all_impls = set[type[Implementation]]()
    ctx.obj = (project, all_impls, format)

    all_impls.update(project.impls.base_uris.keys())

    if ctx.invoked_subcommand is not None:
        return

    _display_impls(all_impls, format)


@impls.command(name="scan")
@click.pass_obj
def impls_scan(ctx: tuple[Project, set[type[Implementation]], Optional[str]]) -> None:
    "Scan for all available implementations across all loaders"
    project, all_impls, format = ctx

    for base_cls, uri in project.impls.base_uris.items():
        LOGGER.debug("scanning implementations in loader %s", project.impls.uri)
        for found in project.impls.find_types(base_cls, pattern="*"):
            all_impls.add(found.type)

    _display_impls(all_impls, format)


def _display_impls(impls: set[type[Implementation]], format: Optional[str]) -> None:
    impl_infos = [implementation_as_json(impl) for impl in impls]
    impl_infos = sorted(impl_infos, key=lambda info: info["uri"])
    fields = ["name", "type", "description", "tags", "priority", "group", "class", "module", "uri"]
    display_list(impl_infos, format, fields, compactify=lambda info: "{uri}".format(**info))


if __name__ == "__main__":
    cli()
