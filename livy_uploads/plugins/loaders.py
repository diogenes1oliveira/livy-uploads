__all__ = (
    "get_loader",
    "get_loaders",
    "resolve_loaders",
    "register_default_loaders",
    "DEFAULT_LOADERS",
    "CombinedLoader",
    "ImplementationLoader",
)

import dataclasses
import itertools
import logging
from collections.abc import Iterator, Mapping, Sequence
from fnmatch import fnmatch
from pathlib import Path
from typing import ClassVar, Optional, TypeVar, Union, cast

from typing_extensions import Self

from livy_uploads.plugins.base import FoundObject, FoundPath, FoundType, Matcher, PluginLoader, Predicate
from livy_uploads.plugins.entrypoints import EntryPointsLoader
from livy_uploads.plugins.files import FileLoader
from livy_uploads.plugins.impls import (
    USER_MIN_PRIORITY,
    Implementation,
    find_implementable_bases,
    get_implementation,
    get_implementations,
    register_type,
)
from livy_uploads.plugins.modules import ModuleLoader
from livy_uploads.plugins.utils import resolve_group, split_name_attr

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


def register_default_loaders() -> None:
    for cls in DEFAULT_LOADERS:
        register_type(cls)


def get_loader(source: Union[str, Path]) -> PluginLoader:
    """
    Builds the plugin loader for the given source spec.

    Args:
        source: the source spec or path.

    Gets a registered plugin loader by spec:

        >>> register_default_loaders()
        >>> get_loader("module://os")
        ModuleLoader(module_name='os')

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

    - Try to use all parsers with priority >= 100 (user-defined loaders), higher first.

    - Otherwise, try to detect a file by detecting a slash in the spec, otherwise try to load as a module.

        >>> get_loader("some.package")
        ModuleLoader(module_name='some.package')

        >>> get_loader("top_level_package")
        ModuleLoader(module_name='top_level_package')

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
            loader_cls = get_implementation(PluginLoader, typename=scheme)
        except ValueError:
            raise ValueError(f"no plugin loader for {scheme=!r}: {source!r}") from None

        return loader_cls.parse(rest)

    tag, sep, spec = source.partition(":")
    if tag and sep:
        tag = tag + sep
        try:
            loader_cls = get_implementation(PluginLoader, tags=(tag,))
        except ValueError:
            pass
        else:
            return loader_cls.parse(spec)

    spec = source
    for loader_cls in get_implementations(PluginLoader, min_priority=USER_MIN_PRIORITY).values():
        try:
            return loader_cls.parse(spec)
        except ValueError:
            continue

    if "/" in spec:
        return FileLoader.parse(spec)
    else:
        try:
            return ModuleLoader.parse(spec)
        except ValueError:
            pass

    raise ValueError(f"no plugin loader can handle {spec=!r}")


def get_loaders(*sources: str) -> list[PluginLoader]:
    """
    Builds the plugin loaders for the given source specs, forwarding to :meth:`get_loader`.

    The default URIs for each loader are added to the end of the list. You can exclude each of them by adding a `!`
    prefix to the source spec.

    Args:
        sources: the source specs or URI negation.

    >>> register_default_loaders()
    >>> with_default_loaders = get_loaders('./some/file.py')
    >>> [l.uri for l in with_default_loaders]
    ['file://some/file.py', 'entrypoint://.*/', 'impl://.*']

    >>> no_default_loaders = get_loaders('some.package', './some/file.py', '!entrypoint://.*/', '!impl://.*')
    >>> [l.uri for l in no_default_loaders]
    ['module://some.package', 'file://some/file.py']
    """
    spec_sources = [s for s in sources if not s.startswith("!")]
    default_sources = PluginLoader.get_default_uris()
    all_sources = [*spec_sources, *default_sources]
    exclusions = {s.removeprefix("!") for s in sources if s.startswith("!")}
    excluded = set[str]()

    LOGGER.debug(
        "loading %d sources: spec_sources=%r default_sources=%r exclusions=%r",
        len(all_sources),
        spec_sources,
        default_sources,
        exclusions,
    )
    loaders: dict[str, PluginLoader] = {}

    for s in all_sources:
        if s in exclusions:
            excluded.add(s)
            continue

        loader = get_loader(s)
        loaders[loader.uri] = loader

    LOGGER.debug("excluded %d sources %r", len(excluded), excluded)

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


@dataclasses.dataclass(frozen=True)
class CombinedLoader(PluginLoader):
    """
    A loader that combines multiple loaders.
    """

    __impl_typename__: ClassVar[str] = "combined"

    loaders: list[PluginLoader]
    "The loaders to combine."

    @classmethod
    def parse(cls, value: str) -> Self:
        """
        Parses a spec URL part like `[<name>/[:<attr>]]?loader=<spec_or_uri_1>[&loader=<spec_or_uri_2>]...` or a string list with
        ampersand-separated loader URIs or specs like `spec_or_uri_1&spec_or_uri_2&...`.

        >>> register_default_loaders()
        >>> CombinedLoader.parse("module://os&file://path/to/file.py")
        CombinedLoader(loaders=[ModuleLoader(module_name='os'), FileLoader(path=PurePosixPath('path/to/file.py'))])

        >>> CombinedLoader.parse("?loader=module://os&loader=entrypoint://.*")
        CombinedLoader(loaders=[ModuleLoader(module_name='os'), EntryPointsLoader(groups=('.*',))])
        """
        if "?" in value:
            _, _, value = value.partition("?")
            sources = [s.removeprefix("loader=") for s in value.split("&") if s.startswith("loader=")]
        else:
            sources = list(filter(None, value.split("&")))

        loaders = [get_loader(s) for s in sources]
        return cls(loaders=loaders)

    def named_uri(self, name: Optional[str]) -> str:
        """
        URI in the format `combined://[<name>/[:<attr>]]?loader=<uri1>&loader=<uri2>...`.

        >>> register_default_loaders()
        >>> loader = CombinedLoader.parse("module://os&file://path/to/file.py")
        >>> loader.named_uri(None)
        'combined://?loader=module://os&loader=file://path/to/file.py'

        >>> loader.named_uri("myfield")
        'combined://myfield/?loader=module://os&loader=file://path/to/file.py'

        >>> loader.named_uri("myfield:attr")
        'combined://myfield/:attr?loader=module://os&loader=file://path/to/file.py'
        """
        url = "combined://"

        if name:
            name, attr = split_name_attr(name)
            url += f"{name}/"
            if attr:
                url += f":{attr}"

        loaders_part = "&".join(f"loader={loader.uri}" for loader in self.loaders)
        if not loaders_part:
            return url
        return f"{url}?{loaders_part}"

    def resolve(self, *, basedir: Optional[Path] = None) -> tuple[Self]:
        """
        Resolves all underlying loaders.

        >>> register_default_loaders()
        >>> loader = CombinedLoader.parse("module://os&entrypoint://.patch*")
        >>> (resolved_loader,) = loader.resolve()
        >>> [l.uri for l in resolved_loader.loaders]
        ['module://os', 'entrypoint://sparkrl.plugins.patches/']
        """
        resolved_loaders = list(
            itertools.chain.from_iterable(loader.resolve(basedir=basedir) for loader in self.loaders)
        )
        resolved_self = dataclasses.replace(self, loaders=resolved_loaders)
        return (resolved_self,)

    def find_paths(self, *, pattern: str, basedir: Optional[Path] = None) -> Iterator[FoundPath]:
        """
        Finds paths from all underlying loaders.

        >>> register_default_loaders()
        >>> (loader,) = CombinedLoader.parse("module://os").resolve()
        >>> paths = list(loader.find_paths(pattern="*.py"))
        >>> len(paths) > 0
        True
        """
        for loader in self.loaders:
            yield from loader.find_paths(pattern=pattern, basedir=basedir)

    def find_types(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[type[T]]] = None,
        predicate: Optional[Predicate[type[T]]] = None,
    ) -> Iterator[FoundType[T]]:
        """
        Finds types from all underlying loaders.

        >>> register_default_loaders()
        >>> from livy_uploads.commands.base import SessionCommand
        >>> (loader,) = CombinedLoader.parse("entrypoint://.*").resolve()
        >>> types = list(loader.find_types(SessionCommand, pattern="*"))
        >>> len(types) > 0
        True
        """
        for loader in self.loaders:
            yield from loader.find_types(t, pattern=pattern, match=match, predicate=predicate)

    def find_objects(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[T]] = None,
        predicate: Optional[Predicate[T]] = None,
    ) -> Iterator[FoundObject[T]]:
        """
        Finds objects from all underlying loaders.

        >>> register_default_loaders()
        >>> (loader,) = CombinedLoader.parse("entrypoint://.*").resolve()
        >>> objects = list(loader.find_objects(object, pattern="*", match=Matcher.any()))
        >>> len(objects) > 0
        True
        """
        for loader in self.loaders:
            yield from loader.find_objects(t, pattern=pattern, match=match, predicate=predicate)


@dataclasses.dataclass(frozen=True)
class ImplementationLoader(PluginLoader):
    """
    A loader that loads implementations from a plugin group.
    """

    __impl_typename__: ClassVar[str] = "impl"

    __default_uris__: ClassVar[tuple[str, ...]] = ("impl://.*",)

    groups: tuple[str, ...]
    "the entrypoint groups to load implementations from"

    base_uris: Mapping[type[Implementation], str] = dataclasses.field(
        default_factory=dict, repr=False, hash=False, compare=False, init=False
    )
    "The loader URIs for each base class, once resolved."

    resolved: bool = dataclasses.field(default=False, repr=False, hash=False, compare=False, init=False)
    "Whether this loader has been resolved."

    @classmethod
    def parse(cls, value: str) -> Self:
        """
        Parse a spec like a comma-separated list of entrypoint groups.

        >>> ImplementationLoader.parse("g1,h.g2")
        ImplementationLoader(groups=('g1', 'h.g2'))
        """
        groups = EntryPointsLoader.parse(value).groups
        return cls(groups=groups)

    @property
    def uri(self) -> str:
        """
        Implementation-specific URI for this plugin loader spec.

        >>> parser = ImplementationLoader(groups=("some.package",))
        >>> parser.uri
        'impl://some.package'
        """
        return self.named_uri(None)

    @property
    def group(self) -> str:
        assert len(self.groups) == 1
        return self.groups[0]

    def named_uri(self, name: Optional[str]) -> str:
        """
        URI in the format `impl://<group1>[,<group2>...][/name]`.

        >>> multiparser = ImplementationLoader(groups=("g1", "h.g2"))
        >>> multiparser.named_uri(None)
        'impl://g1,h.g2'
        >>> multiparser.named_uri("field")
        'impl://g1,h.g2/field'

        >>> nullparser = ImplementationLoader(groups=())
        >>> nullparser.named_uri(None)
        'impl://'
        """
        url = "impl://" + ",".join(self.groups)
        if name:
            url += f"/{name}"
        return url

    def resolve(self, *, basedir: Optional[Path] = None) -> "tuple[ImplementationLoader, ...]":
        """
        Resolves the plugin modules without executing any code.

        Raises:
            FileNotFoundError: if the loader cannot be resolved.

        >>> ImplementationLoader.parse(".pat*").resolve()
        (ImplementationLoader(groups=('sparkrl.plugins.patches',)),)

        >>> ImplementationLoader.parse(".some_missing_gr*").resolve()
        Traceback (most recent call last):
        ...
        FileNotFoundError: ...
        """
        base_uris_by_group = dict[str, dict[type[Implementation], str]]()

        group_patterns = {resolve_group(g) for g in self.groups}
        bases = find_implementable_bases()

        for uri, base in bases.items():
            group = base.plugin_group()
            assert group, f"No plugin group set in implementable base class: {uri}"

            if any(fnmatch(group, pattern) for pattern in group_patterns):
                base_uris_by_group.setdefault(group, {}).setdefault(base, uri)

        if not base_uris_by_group:
            raise FileNotFoundError(f"no implementable bases found for groups {self.groups!r}")

        resolved: list[ImplementationLoader] = []

        for group, base_uris in sorted(base_uris_by_group.items(), key=lambda kv: kv[0]):
            loader = ImplementationLoader(groups=(group,))
            object.__setattr__(loader, "base_uris", base_uris)
            resolved.append(loader)

        for loader in resolved:
            to_register = dict[type[Implementation], str]()

            for entrypoint_loader in EntryPointsLoader(groups=(loader.group,)).resolve():
                LOGGER.debug("scanning loader %s", entrypoint_loader.uri)
                for found in entrypoint_loader.find_types(object, pattern="*"):
                    impl = cast(type[Implementation], found.type)
                    to_register[impl] = found.uri

            LOGGER.debug(
                "registering %d classes for group %s from loader %s: %s",
                len(to_register),
                loader.group,
                loader.uri,
                to_register,
            )
            for impl, uri in to_register.items():
                register_type(impl, uri=uri)

            object.__setattr__(loader, "resolved", True)

        return tuple(resolved)

    def find_paths(self, *, pattern: str, basedir: Optional[Path] = None) -> Iterator[FoundPath]:
        """
        Always empty because this loader is just for classes.
        """
        return iter(())

    def find_objects(
        self, t: type[T], *, pattern: str, match: Optional[Matcher[T]] = None, predicate: Optional[Predicate[T]] = None
    ) -> Iterator[FoundObject[T]]:
        """
        Always empty because this loader is just for classes.
        """
        return iter(())

    def find_types(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[type[T]]] = None,
        predicate: Optional[Predicate[type[T]]] = None,
    ) -> Iterator[FoundType[T]]:
        """
        Finds implementations from all underlying groups.

        This loader must have been resolved beforehand.

        Args:
            t: the type to find implementations for.
            pattern: a matcher for a pattern
        """
        assert self.resolved
        found = get_implementations(cast(type[Implementation], t), pattern=pattern)

        for typename, impl in found.items():
            cls = cast(type[T], impl)

            if match is not None and not match(cls):
                continue
            if predicate is not None and not predicate(cls):
                continue

            yield FoundType(uri=typename, type=cls, pattern=pattern, loader=self)


DEFAULT_LOADERS = (
    FileLoader,
    ModuleLoader,
    EntryPointsLoader,
    ImplementationLoader,
)
