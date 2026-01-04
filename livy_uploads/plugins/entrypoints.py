__all__ = (
    "EntryPointsLoader",
    "scan_entrypoints",
)

import dataclasses
import importlib.metadata
import importlib.util
import itertools
import logging
from collections.abc import Iterator, Sequence
from fnmatch import fnmatch
from importlib.metadata import EntryPoint
from pathlib import Path
from typing import ClassVar, Optional, TypeVar

from typing_extensions import Self

from livy_uploads.configs.utils import split_envvar
from livy_uploads.plugins import constants
from livy_uploads.plugins.base import FoundObject, FoundPath, FoundType, Matcher, PluginLoader, Predicate
from livy_uploads.plugins.modules import ModuleLoader, scan_module, scan_spec_paths
from livy_uploads.plugins.utils import fix_group
from livy_uploads.utils.typeutils import is_actual_class

T = TypeVar("T")

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class EntryPointsLoader(PluginLoader):
    """
    Loads plugins from the entry points in the package metadata.
    """

    __impl_typename__: ClassVar[str] = "entrypoint"
    __impl_tags__: ClassVar[tuple[str, ...]] = ("entrypoint:",)
    __default_uris__: ClassVar[tuple[str, ...]] = ("entrypoint://.*",)

    groups: tuple[str, ...]
    "The entrypoint group names or patterns with wildcards."

    entry_points: Optional[Sequence[EntryPoint]] = dataclasses.field(
        default=None, repr=False, hash=False, compare=False, init=False
    )
    "The importlib entry points for the matching group(s), once resolved."

    def __post_init__(self) -> None:
        groups = tuple(fix_group(g) for g in self.groups)
        object.__setattr__(self, "groups", groups)

    @classmethod
    def parse(cls, value: str) -> Self:
        """
        Parses a spec like `<group[,group2]>[:<pattern>]` or `<group>?select=<pattern>`.

        >>> EntryPointsLoader.parse("some.group")
        EntryPointsLoader(groups=('some.group',))

        >>> EntryPointsLoader.parse("some.group:*Loader")
        EntryPointsLoader(groups=('some.group',))

        >>> EntryPointsLoader.parse("some.group,other.group")
        EntryPointsLoader(groups=('some.group', 'other.group'))

        >>> EntryPointsLoader.parse(".*")
        EntryPointsLoader(groups=('sparkrl.plugins.*',))

        >>> EntryPointsLoader.parse("*")
        EntryPointsLoader(groups=('__all__',))

        >>> EntryPointsLoader.parse(".commands")
        EntryPointsLoader(groups=('sparkrl.plugins.commands',))

        >>> EntryPointsLoader.parse(".patches")
        EntryPointsLoader(groups=('sparkrl.plugins.patches',))

        >>> EntryPointsLoader.parse(".commands,.patches")
        EntryPointsLoader(groups=('sparkrl.plugins.commands', 'sparkrl.plugins.patches'))

        >>> EntryPointsLoader.parse("some.group?select=field")
        EntryPointsLoader(groups=('some.group',))
        """
        # Handle ?select= syntax (ignore the pattern part for now)
        if "?" in value:
            value, _, _ = value.partition("?select=")

        # Handle :pattern syntax (ignore the pattern part)
        value, _, _ = value.partition(":")

        groups = split_envvar(value)
        return cls(groups=tuple(groups))

    def named_uri(self, name: Optional[str]) -> str:
        """
        URI in the format `entrypoint://<group[,group2]>[/<name>]`.

        >>> loader = EntryPointsLoader.parse("some.group")
        >>> loader.named_uri(None)
        'entrypoint://some.group/'

        >>> loader.named_uri("field")
        'entrypoint://some.group/field'

        >>> loader = EntryPointsLoader.parse("some.group,other.group")
        >>> loader.named_uri(None)
        'entrypoint://some.group,other.group/'

        >>> loader = EntryPointsLoader.parse("*")
        >>> loader.named_uri(None)
        'entrypoint:///'

        >>> loader.uri
        'entrypoint:///'
        """
        if set(self.groups) == {"__all__"}:
            group_part = ""
        else:
            group_part = ",".join(self.groups)

        return f"entrypoint://{group_part}/{name or ''}"

    def resolve(self, *, basedir: Optional[Path] = None) -> tuple[PluginLoader, ...]:
        """
        Resolves the entrypoints by finding the matching entrypoint groups (without loading them).

        >>> (loader,) = EntryPointsLoader.parse("sparkrl.plugins.commands").resolve()
        >>> loader
        EntryPointsLoader(groups=('sparkrl.plugins.commands',))

        >>> (resolved_loader,) = EntryPointsLoader.parse("sparkrl.plugins.commands").resolve()
        >>> assert resolved_loader.entry_points is not None
        >>> len(resolved_loader.entry_points) > 0
        True

        >>> (loader,) = EntryPointsLoader.parse(".commands").resolve()
        >>> loader
        EntryPointsLoader(groups=('sparkrl.plugins.commands',))

        >>> (loader,) = EntryPointsLoader.parse(".patches").resolve()
        >>> loader
        EntryPointsLoader(groups=('sparkrl.plugins.patches',))

        >>> loaders = EntryPointsLoader.parse(".*").resolve()
        >>> [l.uri for l in loaders]
        ['entrypoint://sparkrl.plugins.commands/', 'module://livy_uploads', 'entrypoint://sparkrl.plugins.patches/']

        >>> loaders = EntryPointsLoader.parse(".commands,.patches").resolve()
        >>> [l.uri for l in loaders]
        ['entrypoint://sparkrl.plugins.commands/', 'entrypoint://sparkrl.plugins.patches/']

        >>> loaders = EntryPointsLoader.parse("sparkrl.plugins.*").resolve()
        >>> [l.uri for l in loaders]
        ['entrypoint://sparkrl.plugins.commands/', 'module://livy_uploads', 'entrypoint://sparkrl.plugins.patches/']

        >>> EntryPointsLoader.parse("group.that.will.never.exist").resolve()
        Traceback (most recent call last):
        ...
        FileNotFoundError: ...
        """
        entrypoints_loader = importlib.metadata.entry_points()
        all_entry_points: list[EntryPoint]

        if isinstance(entrypoints_loader, dict):
            # In Python 3.9, entry_points() returns a dict
            all_entry_points = list(itertools.chain.from_iterable(entrypoints_loader.values()))
        else:
            # In Python 3.10+, it returns an EntryPoints object with select() method
            all_entry_points = entrypoints_loader.select()  # type: ignore[unreachable]

        matched: dict[str, list[EntryPoint]] = {}
        for group in self.groups:
            for entry_point in all_entry_points:
                if group == "__all__" or fnmatch(entry_point.group, group):
                    matched.setdefault(entry_point.group, []).append(entry_point)

        if not matched:
            raise FileNotFoundError(f"no matched entrypoints for groups {self.groups!r}")

        groups = tuple(sorted(matched.keys()))
        loaders: list[PluginLoader] = []

        for group in groups:
            entry_points = tuple(matched[group])
            if group == constants.LOADERS_GROUP:
                for entry_point in entry_points:
                    loaders.extend(self._resolve_loaders(entry_point))
            else:
                loader = dataclasses.replace(self, groups=(group,))
                object.__setattr__(loader, "entry_points", entry_points)
                loaders.append(loader)

        return tuple(loaders)

    def _resolve_loaders(self, entry_point: EntryPoint) -> Iterator[PluginLoader]:
        yield ModuleLoader(module_name=entry_point.module)

    def find_paths(self, *, pattern: str, basedir: Optional[Path] = None) -> Iterator[FoundPath]:
        """
        Finds file paths relative to the entrypoint module directories.

        This will skip the loaders that don't come from a file path.
        """
        assert self.entry_points is not None

        for entry_point in self.entry_points:
            module = entry_point.module
            spec = importlib.util.find_spec(module)
            if spec is None:
                continue

            for filename, path in scan_spec_paths(spec, pattern=pattern):
                uri = self.named_uri(filename)
                yield FoundPath(path=path, uri=uri, pattern=pattern, loader=self)

    def find_objects(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[T]] = None,
        predicate: Optional[Predicate[T]] = None,
    ) -> Iterator[FoundObject[T]]:
        """
        Loads entrypoints and scans them for matching objects.

        >>> (resolved_loader,) = EntryPointsLoader.parse("sparkrl.plugins.commands").resolve()
        >>> objects = list(resolved_loader.find_objects(object, pattern="*", match=Matcher.any()))
        >>> len(objects) > 0
        True
        """
        assert self.entry_points is not None
        matcher = match or Matcher.instance(t)

        for name, obj, _entry_point in scan_entrypoints(
            self.entry_points, pattern=pattern, matcher=matcher, predicate=predicate
        ):
            uri = self.named_uri(name)
            yield FoundObject(object=obj, uri=uri, pattern=pattern, loader=self)

    def find_types(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[type[T]]] = None,
        predicate: Optional[Predicate[type[T]]] = None,
    ) -> Iterator[FoundType[T]]:
        """
        Loads entrypoints and scans them for matching class definitions.

        >>> from livy_uploads.commands.base import SessionCommand
        >>> (resolved_loader,) = EntryPointsLoader.parse("sparkrl.plugins.commands").resolve()
        >>> types = list(resolved_loader.find_types(SessionCommand, pattern="*"))
        >>> len(types) > 0
        True
        >>> all(issubclass(t.type, SessionCommand) for t in types)
        True
        """
        assert self.entry_points is not None
        matcher = match or Matcher.subclass(t)

        for name, cls, _entry_point in scan_entrypoints(
            self.entry_points, pattern=pattern, matcher=matcher, predicate=predicate
        ):
            uri = self.named_uri(name)
            yield FoundType(type=cls, uri=uri, pattern=pattern, loader=self)


def scan_entrypoints(
    entry_points: Sequence[EntryPoint],
    *,
    matcher: Matcher[T],
    pattern: str,
    predicate: Optional[Predicate[T]] = None,
) -> Iterator[tuple[str, T, EntryPoint]]:
    """
    Scans entrypoints for matching objects.

    The function loads each entrypoint and either:
    - If it's a module, scans it using scan_module
    - If it's a class/object directly, yields it if it matches

    >>> import importlib.metadata
    >>> all_eps = importlib.metadata.entry_points()
    >>> if isinstance(all_eps, dict):
    ...     commands_eps = all_eps.get('sparkrl.plugins.commands', [])
    ... else:
    ...     commands_eps = all_eps.select(group='sparkrl.plugins.commands')
    >>> commands_eps = list(commands_eps)
    >>> len(commands_eps) > 0
    True

    >>> from livy_uploads.commands.base import SessionCommand
    >>> scanned = list(scan_entrypoints(commands_eps, matcher=Matcher.subclass(SessionCommand), pattern="*"))
    >>> len(scanned) > 0
    True
    >>> all(isinstance(item[0], str) for item in scanned)
    True
    """
    import inspect


    pattern = pattern or "*"

    for entry_point in entry_points:
        # Check if the entrypoint name matches the pattern
        if not fnmatch(entry_point.name, pattern):
            continue

        try:
            value = entry_point.load()
        except Exception as e:
            LOGGER.warning(f"Failed to load entrypoint {entry_point.name!r}: {e}")
            continue

        # If it's a module, scan it
        if inspect.ismodule(value):
            module = value
            for name, obj in scan_module(module, pattern=pattern, matcher=matcher, predicate=predicate):
                yield name, obj, entry_point
        # If it's a direct value (class or object)
        elif matcher(value):
            if predicate is None or predicate(value):
                # Use the class name if it's a class, otherwise use the entrypoint name
                name = value.__name__ if is_actual_class(value) else entry_point.name
                yield name, value, entry_point
