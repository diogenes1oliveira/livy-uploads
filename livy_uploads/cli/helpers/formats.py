import collections.abc
import json
import sys
from typing import Any, Callable, Iterable, Optional, Sequence, TypeVar, Union

import click

from livy_uploads.utils.datautils import jsonify

T = TypeVar("T")

JSON_FORMATS = ["json", "jsonlines"]
COMPACT_FORMAT = "compact"
TABLE_FORMATS = [
    "plain",
    "simple",
    "pretty",
    "github",
    "html",
]
ALL_FORMATS = JSON_FORMATS + TABLE_FORMATS + [COMPACT_FORMAT]

DEFAULT_FORMAT = "json"
DEFAULT_CLI_FORMAT = "pretty"


with_format_option = click.option(
    "--format",
    type=click.Choice(ALL_FORMATS),
    help="Output format",
)
with_compact_option = click.option(
    "--compact",
    is_flag=True,
    default=False,
    help="Output in compact format",
)


def display_item(
    obj: Any,
    compact: Optional[bool] = None,
) -> None:
    compact = compact or False
    if obj is None:
        return

    if isinstance(obj, (bytes, bytearray)):
        sys.stdout.buffer.write(obj)
        sys.stdout.buffer.flush()
        return

    value = jsonify(obj)
    assert not isinstance(value, collections.abc.Sequence), "display_item() not supported for sequences"

    if isinstance(value, str):
        out = value
    else:
        out = json.dumps(value, indent=2 if not compact else None)

    click.echo(out, nl=sys.stdout.isatty() and not out.endswith("\n") and not out.endswith("\r"))


def display_list(
    items: Iterable[T],
    format: Optional[str],
    columns: Sequence[str],
    compactify: Optional[Callable[[T], Union[str, int]]] = None,
) -> None:
    if not format:
        if sys.stdout.isatty():
            format = DEFAULT_CLI_FORMAT
        else:
            format = DEFAULT_FORMAT

    if format == "jsonlines":
        out = "\n".join(json.dumps(jsonify(item)) for item in items)
    elif format == "json":
        out = json.dumps([jsonify(item) for item in items], indent=2)
    elif format == "compact":
        assert compactify is not None
        out = "\n".join(str(compactify(item)) for item in items)
    else:
        from tabulate import tabulate

        rows = [extract_columns(item, columns) for item in items]
        out = tabulate(rows, headers=columns, tablefmt=format)
        out = out.rstrip("\r\n") + "\n"

    click.echo(out, nl=sys.stdout.isatty())


def extract_columns(obj: Any, columns: Sequence[str]) -> Sequence[Any]:
    if isinstance(obj, collections.abc.Mapping):
        return [obj.get(col, None) for col in columns]
    else:
        return [getattr(obj, col, None) for col in columns]
