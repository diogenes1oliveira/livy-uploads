__all__ = (
    "FileLoader",
    "FileLoaderMixIn",
    "scan_paths",
)

import dataclasses
import logging
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import ClassVar, Optional, TypeVar, Union, cast

from typing_extensions import Self

from livy_uploads.plugins.base import FoundPath, NoCodeMixin, PluginLoader

T = TypeVar("T")

LOGGER = logging.getLogger(__name__)


class FileLoaderMixIn:
    """
    Mixin for plugin loaders that can load files from a local path.
    """

    path: PurePosixPath
    "Path to the code file. Should be a .py, .zip or .egg file."

    def find_paths(self, *, pattern: str, basedir: Optional[Path] = None) -> Iterator[FoundPath]:
        """
        Scans the file's directory for the matching relative paths.

        >>> (loader,) = FileLoader.parse(__file__).resolve()

        >>> exact_filename_uris = sorted([f.uri for f in loader.find_paths(pattern="files.py")])  # doctest: +ELLIPSIS
        >>> exact_filename_uris  # doctest: +ELLIPSIS
        ['file://.../plugins/files.py/files.py']

        >>> partial_filename_uris = sorted([f.uri for f in loader.find_paths(pattern="*.py")])
        >>> len(partial_filename_uris) > 0
        True
        >>> any('files.py' in uri for uri in partial_filename_uris)
        True
        """
        loader = cast(PluginLoader, self)
        for filename, path in scan_paths(self.path, pattern=pattern):
            uri = loader.named_uri(filename)
            yield FoundPath(path=path, uri=uri, pattern=pattern, loader=loader)


@dataclasses.dataclass(frozen=True)
class FileLoader(FileLoaderMixIn, NoCodeMixin, PluginLoader):
    """
    Loads plugins from a local path.
    """

    __impl_typename__: ClassVar[str] = "file"
    __impl_priority__: ClassVar[int] = 20  # explicit priority (can be auto-detected), higher than ModuleLoader
    __impl_tags__: ClassVar[tuple[str, ...]] = ("file:",)
    __default_uris__: ClassVar[tuple[str, ...]] = ("file://./",)

    path: PurePosixPath
    "Path to the file or directory."

    @classmethod
    def parse(cls, value: Union[str, Path, PurePosixPath]) -> Self:
        """
        Parses an URI such as `file://<path>` or a spec like `<path>`.

        Args:
            value: the file path object or spec.
                If a spec, must have at least one `/` separator.

        >>> FileLoader.parse("path/to/plugin.py")
        FileLoader(path=PurePosixPath('path/to/plugin.py'))

        >>> FileLoader.parse("path/to/my-plugin.py")
        FileLoader(path=PurePosixPath('path/to/my-plugin.py'))

        >>> FileLoader.parse("plugin.py")
        Traceback (most recent call last):
        ...
        ValueError: invalid file path spec, no / separator: 'plugin.py'
        """
        if isinstance(value, str):
            if "/" not in value:
                raise ValueError(f"invalid file path spec, no / separator: {value!r}")
            path = PurePosixPath(value)
        elif isinstance(value, Path):
            path = PurePosixPath(value.as_posix())
        else:
            path = value

        return cls(path=path)

    def named_uri(self, name: Optional[str]) -> str:
        """
        URI in the format `file://<path>[/<name>]`.

        >>> loader = FileLoader.parse("path/to/plugin.py")

        >>> loader.named_uri(None)
        'file://path/to/plugin.py'

        >>> loader.named_uri("field")
        'file://path/to/plugin.py/field'

        >>> loader.uri
        'file://path/to/plugin.py'
        """
        suffix = f"/{name or ''}" if name else ""
        return f"file://{self.path}{suffix}"

    def resolve(self, *, basedir: Optional[Path] = None) -> tuple[Self]:
        """
        Resolves the plugin by absolutizing the paths.

        We don't check if the file exists, because this can be done later at load time in case the path shows up
        in the meantime.

        >>> (resolved_loader,) = FileLoader.parse(__file__).resolve()  # doctest: +ELLIPSIS
        >>> resolved_loader  # doctest: +ELLIPSIS
        FileLoader(path=PurePosixPath('.../plugins/files.py'))

        >>> assert resolved_loader.path.is_absolute()

        """
        basedir = basedir or Path.cwd()
        if not self.path.is_absolute():
            setup_path = Path(PurePosixPath(basedir.as_posix()) / self.path)
        else:
            setup_path = Path(self.path)

        return (dataclasses.replace(self, path=PurePosixPath(setup_path.as_posix())),)


def scan_paths(file_or_dir_path: Union[Path, PurePosixPath], *, pattern: str) -> Iterator[tuple[str, Path]]:
    """
    Scans a directory for matching paths.

    Args:
        file_or_dir_path: either a file (whose parent directory will be scanned) or a directory to scan directly
        pattern: glob pattern to match files

    >>> this_file = Path(__file__)
    >>> this_dir = this_file.parent

    >>> ffiles = sorted(dict(scan_paths(this_dir, pattern="f*.py")))
    >>> ffiles
    ['files.py']

    >>> mfiles = sorted(dict(scan_paths(this_dir, pattern="m*.py")))
    >>> mfiles
    ['modules.py']

    >>> bfiles = sorted(dict(scan_paths(this_dir.parent, pattern="plugins/b*.py")))
    >>> bfiles
    ['plugins/base.py']
    """
    path = Path(file_or_dir_path)
    if not path.is_absolute():
        return

    # If it's a file, scan its parent directory
    if path.is_file():
        path = path.parent
    elif not path.is_dir():
        return

    for result in path.glob(pattern):
        relative = result.relative_to(path).as_posix()
        yield relative, result
