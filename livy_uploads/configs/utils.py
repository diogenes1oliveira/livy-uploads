import base64
import collections.abc
import hashlib
import importlib.util
import logging
import os
from pathlib import Path
from typing import Any, Mapping, Optional

from livy_uploads.utils.datautils import is_module_name

LOGGER = logging.getLogger(__name__)
DEFAULT_HASH_LENGTH = 10
CACHE_DIRNAME = __name__.partition(".")[0]


def interpolate_envvars(source: Mapping[str, Any], env: Optional[Mapping[str, str]] = None) -> dict[str, Any]:
    """
    >>> interpolate_envvars({"foo": "${BAR}_${BAR}"}, {"BAR": "baz"})
    {'foo': 'baz_baz'}
    """
    env = env if env is not None else os.environ
    from dotenv.variables import parse_variables

    result: dict[str, Any] = {}

    for k, v in source.items():
        if isinstance(v, str):
            atoms = parse_variables(v)
            result[k] = "".join(atom.resolve(env) for atom in atoms)
        elif isinstance(v, collections.abc.Mapping):
            result[k] = interpolate_envvars(v, env)
        else:
            result[k] = v

    return result


def split_envvar(value: Optional[str]) -> list[str]:
    value = (value or "").replace(",", " ").replace(";", " ").replace("|", " ")
    return list(filter(None, value.split()))


def get_default_cache_dir() -> Path:
    if value := os.getenv("XDG_CACHE_HOME"):
        basedir = Path(value)
    else:
        basedir = Path.home() / ".cache"
    return basedir / CACHE_DIRNAME


def resolve_path_or_content(
    s: str,
    *,
    filename: str,
    basedir: Optional[Path] = None,
    cache_dir: Optional[Path] = None,
    mode: Optional[int] = None,
    hash_length: Optional[int] = None,
    binary: bool = False,
) -> Path:

    if os.path.sep in s[:3]:
        path = Path(s)
        if not path.is_absolute():
            basedir = basedir or Path.cwd()
            path = basedir / path
        return path
    else:
        if binary:
            data = base64.b64decode(s)
        else:
            data = s.encode("utf8")

        mode = mode if mode is not None else 0o600
        cache_filename = get_cache_filename(s, filename=filename, hash_length=hash_length)
        cache_dir = (cache_dir or get_default_cache_dir()).absolute()
        cache_path = cache_dir / cache_filename

        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug("writing content to the cache file %s", cache_path)
        else:
            LOGGER.info("writing content to the cache file %s", cache_filename)

        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path.touch(mode)
        cache_path.chmod(mode)
        cache_path.write_bytes(data)

        return cache_path


def get_cache_filename(s: str, *, filename: str, hash_length: Optional[int] = None) -> str:
    hash_length = hash_length or DEFAULT_HASH_LENGTH
    digest = hashlib.sha256(s.encode()).hexdigest()[:hash_length]
    try:
        return filename % digest
    except TypeError:
        return filename


def resolve_module_or_path(s: str, basedir: Optional[Path] = None) -> Path:
    if os.path.sep in s[:3]:
        path = Path(s)
    elif is_module_name(s):
        spec = importlib.util.find_spec(s)
        if spec is None or not spec.origin:
            raise ImportError(f"module not found: {s!r}")
        path = Path(spec.origin)
    else:
        raise ValueError(f"invalid source type: {s!r} - should be a path or a module name")

    if not path.is_absolute():
        basedir = basedir or Path.cwd()
        path = basedir / path

    if path.name == "__init__.py":
        path = path.parent

    if not path.exists():
        raise FileNotFoundError(f"module or path not found: {path!r}")

    return path
