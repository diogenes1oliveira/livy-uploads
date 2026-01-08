import base64
import json
import logging
import shlex
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, Optional, TypeVar, Union, get_args, get_origin
from uuid import uuid4

P = TypeVar("P", Path, PurePosixPath)
LOGGER = logging.getLogger(__name__)


def resolve_path_or_content(
    value: Union[str, Path, PurePosixPath],
    mode: Literal["binary", "text"],
    filename: str,
    t: type[P],
    basedir: Path,
    cachedir: Path,
) -> P:
    """
    Resolves a path-like or content string into a concrete file path.

    This function handles various input formats and converts them into a file path,
    either by resolving an existing path or by writing provided content to a cached file.

    Modes of Resolution:
    --------------------
    The resolution logic depends on the input `value` and the `mode` parameter.

    1. **Path Resolution**:
       If `value` is a `Path` object, or a string containing "/" within the first 3 characters,
       it is treated as a file path.
       - Absolute paths are returned as-is.
       - Relative paths are resolved relative to `basedir`.

    2. **Content Resolution**:
       If the input is not identified as a path, it is treated as content to be written
       to a file in `cachedir`. The content handling depends on `mode`:

       - **mode='binary'**:
         The input string is assumed to be base64-encoded data. It is decoded and
         written to the file.

       - **mode='text'**:
         The input is processed based on its format:
         - **Single-quoted string**: Parsed using `shlex.split`, allowing shell-style escaping.
         - **Double-quoted string**: Parsed as a JSON string.
         - **Prefix 'base64:'**: The suffix is decoded as base64 data.
         - **Raw string**: The string is encoded as UTF-8 directly.
    """
    if mode not in ("binary", "text"):
        raise ValueError(f"unknown mode {mode!r}")

    if isinstance(value, str) and "/" in value[:3]:
        # guessing as a path
        value = t(value)

    if isinstance(value, (Path, PurePosixPath)):
        path = t(value)
        if path.is_absolute():
            return t(path)
        else:
            return t(basedir) / path

    if mode == "binary":
        data = base64.b64decode(value)
    elif (first := value.strip()[0]) == "'":
        data = shlex.split(value)[0].encode("utf-8")
    elif first == '"':
        data = json.loads(value).encode("utf-8")
    elif value.startswith("base64:"):
        data = base64.b64decode(value.removeprefix("base64:"))
    else:
        data = value.encode("utf-8")

    cache_path = cachedir / filename.format(uuid=uuid4().hex)
    LOGGER.debug("Writing resolved %s to cache file %s", mode, cache_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.touch(0o600)
    cache_path.chmod(0o600)

    cache_path.write_bytes(data)
    return t(cache_path)


def get_path_resolve_annotation(t: type) -> Optional[tuple[Literal["binary", "text"], str]]:
    """
    >>> get_path_resolve_annotation(Annotated[Path, "sparkrl.resolve=path-or-data"])
    ('binary', '')
    >>> get_path_resolve_annotation(Annotated[PurePosixPath, "sparkrl.resolve=path-or-text"])
    ('text', '')
    >>> get_path_resolve_annotation(Annotated[PurePosixPath, "sparkrl.resolve=path-or-data:{uuid}.keytab"])
    ('binary', '{uuid}.keytab')
    >>> get_path_resolve_annotation(Annotated[Path, "other.annotation=value"]) is None
    True
    >>> get_path_resolve_annotation(str) is None
    True
    >>> get_path_resolve_annotation(Annotated[int, "sparkrl.resolve=path-or-data"]) is None
    True
    """
    if get_origin(t) is not Annotated:
        return None

    args = get_args(t)
    actual_type = args[0]
    if not issubclass(actual_type, (Path, PurePosixPath)):
        return None

    for arg in args[1:]:
        if isinstance(arg, str) and arg.startswith("sparkrl.resolve="):
            resolve = arg.removeprefix("sparkrl.resolve=")
            break
    else:
        return None

    resolve, _, name = resolve.partition(":")
    resolve = resolve.strip()
    name = name.strip()

    if name:
        try:
            name.format(uuid="example")
        except Exception:
            raise TypeError(f"Invalid path template {name!r} in {t}") from None

    if resolve == "path-or-data":
        return "binary", name
    elif resolve == "path-or-text":
        return "text", name
    else:
        raise TypeError(f"Unknown sparkrl.resolve={resolve!r} annotation in {t}")
