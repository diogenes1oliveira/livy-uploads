"""
Utilities for resolving file paths and environment files in a search path hierarchy.
"""

import importlib.util
import logging
import os
from pathlib import Path, PurePosixPath
from typing import Iterable, List, Optional, Union

LOGGER = logging.getLogger(__name__)


NBLIB_PATH_ENVVAR = "NBLIB_PATH"


class PathNotFoundError(FileNotFoundError):
    """
    Raised when none of the requested paths are found in any of the search locations.

    Attributes:
        paths: The list of paths that were searched for.
        searched_paths: The list of absolute locations that were checked.
    """

    def __init__(self, paths: List[str], searched_paths: List[str]):
        super().__init__(f"not a single one of {paths=!r} were found in any of the {searched_paths=!r}")
        self.paths = paths
        self.searched_paths = searched_paths


def find_first_in_paths(
    paths: Union[Iterable[str], Iterable[Path], Iterable[PurePosixPath]],
    pathspec: Optional[Union[str, List[str], List[Path]]] = None,
) -> Path:
    """
    Finds the first matching file in the given paths or in the configured pathspec.

    Args:
        paths: the relative paths to look for. A PurePosixPath will be converted to a regular filesystem path,
            for compatibility with Windows.
        pathspec: The pathspec to use. If None, the environment variable $NBLIB_PATH is used.

    Returns:
        Path: The absolute Path object of the first matching file found.

    Raises:
        PathNotFoundError: If none of the paths are found in any of the searched locations.
        ValueError: If no paths are provided to search for.

    Search Behavior:
        The function searches for files in the following order:

        1. Current Working Directory (CWD): Relative paths are checked first in the current directory.
            If a match is found, returns the absolute path.

        2. Search Paths (Haystacks): For each directory in the pathspec, checks all candidate paths in order.
            - Relative paths are searched as-is under each path search directory
            - Absolute paths have their leading "/" stripped and are searched as relative paths
            - Returns the first match found

        3. Absolute Path Fallback: If no match is found in the working directory or the search paths, absolute paths are
            checked verbatim as a last resort.
    """
    needles = [Path(p) if isinstance(p, str) else Path(*p.parts) for p in paths if p]

    if not needles:
        raise ValueError("no paths provided to search for")

    if pathspec is None:
        haystacks = [Path(p) for p in resolve_paths()]
    elif not pathspec:
        haystacks = []
    elif isinstance(pathspec, str):
        haystacks = [Path(p) for p in _split_pathspec(pathspec)]
    else:
        haystacks = [Path(p) for p in pathspec]

    searched_paths = []

    # relative paths should be checked first in the current directory
    for needle in needles:
        if needle.is_absolute():
            continue
        if needle.exists():
            return needle.absolute()

    # Then in the search spec, absolute paths might be relative to the search paths
    for haystack in haystacks:
        for needle in needles:
            if needle.is_absolute():
                needle = Path(*needle.parts[1:])

            candidate_path = Path(haystack) / needle
            searched_paths.append(candidate_path)
            if os.path.exists(candidate_path):
                return candidate_path

    # Fallback to just the verbatim absolute path
    for needle in needles:
        if needle.is_absolute():
            searched_paths.append(needle)
            if needle.exists():
                return needle

    raise PathNotFoundError(list(map(str, needles)), list(map(str, searched_paths)))


def resolve_pathspec(pathspec: Optional[str] = None, save: bool = False) -> str:
    """
    Resolves and re-merges a pathspec string into a colon-separated list of absolute paths.

    This is a convenience wrapper around :func:`resolve_paths` that re-joins the resolved
    paths back into a single pathspec string suitable for environment variables.

    Args:
        pathspec: A colon-separated string of paths to resolve. If None, the environment variable $NBLIB_PATH is used.
        save: Whether to save the resolved paths to the environment variable $NBLIB_PATH.

    Returns:
        A colon-separated string of resolved paths (uses os.pathsep which is ":" on Unix-like systems)

    See Also:
        :func:`resolve_paths` for detailed information on how individual paths are resolved.
    """
    result = os.pathsep.join(resolve_paths(pathspec))

    if save:
        LOGGER.info("setting NBLIB_PATH=%r", result)
        os.environ["NBLIB_PATH"] = result
    else:
        LOGGER.debug("resolved NBLIB_PATH=%r", result)

    return result


def resolve_paths(pathspec: Optional[str] = None) -> List[str]:
    """
    Resolves a pathspec string into a list of absolute directory paths.

    Args:
        pathspec: Path specification string with components separated by os.pathsep (typically ":").
            If None or empty, reads from the `$NBLIB_PATH` environment variable.

    Returns:
        A list of absolute paths

    Pathspec Format:
        The pathspec string contains colon-separated components. Each component can be either:

        - Directory Path: Any string containing a path separator (/) is treated as a directory path.
            Examples: "/usr/lib", "~/mylibs", "./local/libs"

        - Package Name: Any string without a path separator is treated as a Python package name.
            Examples: "numpy", "pandas", "some.package"

    Resolution Behavior:
        Each component in the pathspec is processed as follows:

        1. Directory Paths: Components containing a path separator (/) are treated as directory paths.
            - Tilde (~) expansion is performed for home directory references
            - Converted to absolute paths
            - Added to the result list

        2. Package Names: Components without a path separator are treated as Python package names.
            - Attempts to locate the package using importlib
            - Only packages with __init__.py files are included (the parent directory is added)
            - Module files without __init__.py are skipped
            - Import errors are silently ignored

        3. Default Package: The top-level package name (livy_uploads) is always appended to the search path,
            regardless of the input pathspec.
    """

    pathspec = (pathspec or os.getenv("NBLIB_PATH") or "").strip()

    paths = _split_pathspec(pathspec)
    paths.append(__name__.partition(".")[0])  # this top-level package name

    resolved = []

    for path in paths:
        if os.sep in path:
            resolved.append(str(Path(path).expanduser().absolute()))
        else:
            try:
                spec = importlib.util.find_spec(path)
            except Exception:
                continue
            if spec is None or not spec.origin:
                continue
            origin = Path(spec.origin).absolute()
            if origin.name == "__init__.py":
                resolved.append(str(origin.parent))
            else:
                continue

    return resolved


def _split_pathspec(pathspec: str) -> List[str]:
    return list(filter(None, pathspec.split(os.pathsep)))


def load_envfile(
    *filenames: str,
    override: bool = True,
    interpolate: bool = True,
    verbose: bool = True,
    required: bool = False,
) -> Optional[Path]:
    """
    Loads environment variables from the first found envfile in the current directory hierarchy.

    Searches for environment files starting from the current working directory, using
    :func:`find_first_in_paths` with an empty pathspec (searches only CWD and parent directories).

    Args:
        filenames: The names of the envfiles to search for. Defaults to `(".env",)` if not provided.
        override: Whether to override currently existing environment variables.
        interpolate: Whether to interpolate environment variables using `${VARNAME}` syntax.
        verbose: Whether to log files and variables being loaded.
        required: Whether to raise PathNotFoundError if no envfile is found.

    Returns:
        The absolute Path of the loaded envfile, or None if no file was found and required=False.

    Raises:
        PathNotFoundError: If no envfile is found and required=True.

    See Also:
        :func:`find_first_in_paths` for details on the search behavior.
    """
    from dotenv import dotenv_values

    if not filenames:
        filenames = (".env",)
    try:
        path = find_first_in_paths(filenames, pathspec=[Path.cwd(), *Path.cwd().parents])
    except PathNotFoundError as e:
        if required:
            raise

        print(f"no envfile found; skipping {filenames=!r} {e.searched_paths=!r}")
        LOGGER.warning(
            "no envfile found with any of the names %s; searched paths: %s",
            filenames,
            e.searched_paths,
        )
        return None

    log_level = logging.INFO if verbose else logging.DEBUG

    LOGGER.log(log_level, "loading envfile from %s", path)
    parsed_values = dotenv_values(path, interpolate=interpolate, verbose=verbose)
    values = {k: v for k, v in parsed_values.items() if v is not None}
    if not override:
        values = {k: v for k, v in values.items() if k not in os.environ}

    if values:
        LOGGER.log(
            log_level,
            "setting %d environment variables: %s",
            len(values),
            ", ".join("$" + s for s in values.keys()),
        )
        os.environ.update(values)
    else:
        LOGGER.log(log_level, "no environment variables to set from %s", path)

    return Path(path)
