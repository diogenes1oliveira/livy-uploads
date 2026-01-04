__all__ = (
    "get_loader",
    "get_loaders",
    "resolve_loaders",
)

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Optional, TypeVar, Union

from livy_uploads.plugins.base import PluginLoader
from livy_uploads.plugins.combine import CombinedLoader
from livy_uploads.plugins.entrypoints import EntryPointsLoader
from livy_uploads.plugins.files import FileLoader
from livy_uploads.plugins.modules import ModuleLoader

DEFAULT_LOADERS = (FileLoader, ModuleLoader, EntryPointsLoader, CombinedLoader)

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")

LOADER: Optional[PluginLoader] = None


def get_loader(source: Union[str, Path]) -> PluginLoader:
    """
    Builds the plugin loader for the given source spec.

    Args:
        source: the source spec or path.

    Parsing behavior:

    - Path objects are passed directly to `FileLoader`:

        >>> get_loader(Path("path/to/plugin.py"))
        FileLoader(path=PurePosixPath('path/to/plugin.py'))

    - Strings with a `://` separator are parsed as a loader URI:

        >>> get_loader("module://some.package")
        ModuleLoader(module_name='some.package')

        >>> get_loader("file://path/to/plugin.py")
        FileLoader(path=PurePosixPath('path/to/plugin.py'))

    - Strings with a `:` separator are parsed as a tagged loader spec:

        >>> get_loader("file:path/to/plugin.py")
        FileLoader(path=PurePosixPath('path/to/plugin.py'))

        >>> get_loader("entrypoint:some.group")
        EntryPointsLoader(groups=('some.group',))

    - Otherwise, try all available loaders with explicit priority order, higher first.

        >>> get_loader("some.package")
        ModuleLoader(module_name='some.package')

        >>> get_loader("some.package")
        ModuleLoader(module_name='some.package')

        >>> get_loader("./path/to/plugin.py")
        FileLoader(path=PurePosixPath('path/to/plugin.py'))

        >>> get_loader("some-file-but-with-no-slashes.py")
        Traceback (most recent call last):
        ...
        ValueError: ...
    """
    if isinstance(source, Path):
        return FileLoader.parse(str(source))

    scheme, sep, rest = source.partition("://")

    if sep:
        if not scheme:
            raise ValueError(f"no scheme in plugin source URI: {source!r}")

        try:
            loader_cls = PluginLoader.get_implementation(typename=scheme)
        except ValueError:
            raise ValueError(f"no plugin loader for {scheme=!r}: {source!r}") from None

        return loader_cls.parse(rest)

    tag, sep, spec = source.partition(":")
    if tag and sep:
        tag = tag + sep
        try:
            loader_cls = PluginLoader.get_implementation(tags=(tag,))
        except ValueError:
            pass
        else:
            return loader_cls.parse(spec)

    spec = source
    for loader_cls in PluginLoader.get_implementations(min_priority=1).values():
        try:
            return loader_cls.parse(spec)
        except ValueError:
            continue

    raise ValueError(f"no plugin loader can handle {spec=!r}")


def get_loaders(*sources: str) -> list[PluginLoader]:
    """
    Builds the plugin loaders for the given source specs, forwarding to :meth:`get_loader`.

    The default URIs for each loader are added to the end of the list. You can exclude each of them by adding a `!`
    prefix to the source spec.

    Args:
        sources: the source specs or URI negation.

    >>> with_default_loaders = get_loaders('./some/file.py')
    >>> [l.uri for l in with_default_loaders]
    ['file://some/file.py', 'entrypoint://sparkrl.plugins.*/']

    >>> no_default_loaders = get_loaders('some.package', './some/file.py', '!entrypoint://.*')
    >>> [l.uri for l in no_default_loaders]
    ['module://some.package', 'file://some/file.py']
    """
    all_sources = [s for s in sources if not s.startswith("!")]
    all_sources.extend(PluginLoader.get_default_uris())
    all_exclusions = {s.removeprefix("!") for s in sources if s.startswith("!")}

    loaders: dict[str, PluginLoader] = {}

    for s in all_sources:
        if s in all_exclusions:
            continue
        loader = get_loader(s)
        loaders[loader.uri] = loader

    return list(loaders.values())


def resolve_loaders(loaders: Sequence[PluginLoader], basedir: Optional[Path] = None) -> list[PluginLoader]:
    """
    Sets up the plugin loaders for the given source specs.

    Args:
        sources: the source specs or URI negation.
        basedir: the base directory to setup relative file paths against
    """
    resolved_loaders_dict: dict[str, PluginLoader] = {}
    failed_uris: list[str] = []

    for loader in loaders:
        try:
            for resolved_loader in loader.resolve(basedir=basedir):
                resolved_loaders_dict[resolved_loader.uri] = resolved_loader
        except FileNotFoundError:
            failed_uris.append(loader.uri)

    if failed_uris:
        LOGGER.warning("skipped %d plugins: %r", len(failed_uris), failed_uris)

    return list(resolved_loaders_dict.values())
