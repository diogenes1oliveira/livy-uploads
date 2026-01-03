__all__ = (
    "PluginLoader",
    "ModuleLoader",
    "FileLoader",
    "EntryPointsLoader",
    "get_loader",
    "get_loaders",
    "resolve_loaders",
)

import dataclasses
import functools
import importlib
import importlib.metadata
import importlib.util
import inspect
import itertools
import logging
import os
import re
import textwrap
from abc import ABC, abstractmethod
from fnmatch import fnmatch
from importlib.machinery import ModuleSpec
from importlib.metadata import EntryPoint
from multiprocessing import Value
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import Any, ClassVar, Collection, Iterable, Mapping, Optional, Protocol, TypeVar, Union

from typing_extensions import Self

from livy_uploads.configs.impl import Implementation
from livy_uploads.configs.utils import resolve_path_or_content, split_envvar
from livy_uploads.utils.datautils import is_module_name
from livy_uploads.utils.typeutils import is_actual_class, is_actual_subclass, is_concrete

PLUGINS_ENVVARS = "SPARKRL_PLUGINS"
T = TypeVar("T")

LOGGER = logging.getLogger(__name__)

PACKAGE = __name__.partition(".")[0]
PREFIX = f"{PACKAGE}.plugins."
DEFAULT_GROUPS = (f"{PACKAGE}.plugins.commands", f"{PACKAGE}.plugins.patches")


@dataclasses.dataclass(frozen=True)
class PluginLoader(Implementation):
    """
    Base class for all plugin loaders.

    Implementations should define the `__impl_typename__` class attribute matching the scheme of the loader URI.
    """

    __impl_typename__: ClassVar[str]
    "The scheme of this plugin loader."

    __default_uris__: ClassVar[Optional[tuple[str, ...]]] = None
    "The default URIs for this plugin loader."

    attrs: str
    "A named attribute, a wildcard pattern, or `__all__` to select all attributes."

    def __post_init__(self) -> None:
        """
        Checks the validity of the instance and fixes the `attrs` field.

        Raises:
            ValueError: if this instance is invalid.
        """
        object.__setattr__(self, "attrs", _fix_attrs(self.attrs or "*"))

    @classmethod
    @abstractmethod
    def parse(cls, value: str) -> Self:
        """
        Parse a plugin loader spec.

        Args:
            value: the loader-specific spec or the scheme-specific part of the URI.
        """
        raise NotImplementedError

    @property
    def uri(self) -> str:
        """
        Implementation-specific URI for this plugin loader spec.
        """
        return self.named_uri(self.attrs if self.attrs != "__all__" else None)

    @abstractmethod
    def named_uri(self, name: Optional[str]) -> str:
        """
        Implementation-specific URI for a specific attribute loaded by this plugin loader.
        """
        raise NotImplementedError

    @abstractmethod
    def resolve(self, *, basedir: Optional[Path] = None) -> Self:
        """
        Resolves the plugin modules without executing any code.
        """
        raise NotImplementedError

    @abstractmethod
    def load(self, t: type[T]) -> dict[str, type[T]]:
        """
        Imports or executes the module and selects the matching declared classes.
        """
        raise NotImplementedError

    @classmethod
    def get_default_uris(cls) -> tuple[str, ...]:
        """
        Returns the default URIs for all implementations of this loader type.

        >>> PluginLoader.get_default_uris()
        ('entrypoint://.*',)
        """
        all_default_uris = set[str]()

        for impl in cls.get_implementations().values():
            default_uris = impl.__default_uris__ or ()
            all_default_uris.update(default_uris)

        return tuple(sorted(all_default_uris))


@dataclasses.dataclass(frozen=True)
class ModuleLoader(PluginLoader):
    """
    Loads plugins from a Python module.
    """

    __impl_typename__: ClassVar[str] = "module"
    __impl_priority__: ClassVar[int] = 20  # explicit priority (can be auto-detected), higher than FileLoader

    module_name: str
    "Python module name (!)"

    spec: Optional[ModuleSpec] = dataclasses.field(default=None, repr=False, hash=False, compare=False)
    "The importlib spec for the module, if resolved."

    def __post_init__(self) -> None:
        super().__post_init__()
        if not is_module_name(self.module_name):
            raise ValueError(f"invalid module name: {self.module_name!r}")

    @classmethod
    def parse(cls, value: str) -> Self:
        """
        Parses an URI such as `module://<module_name>/[<name>]` or a spec like `<module_name>/[<name>]`.

        >>> ModuleLoader.parse("some.package")
        ModuleLoader(attrs='__all__', module_name='some.package')

        >>> ModuleLoader.parse("some.package:field")
        ModuleLoader(attrs='field', module_name='some.package')

        >>> ModuleLoader.parse("some.package/field")
        ModuleLoader(attrs='field', module_name='some.package')

        >>> ModuleLoader.parse("some.package/")
        ModuleLoader(attrs='__all__', module_name='some.package')
        """
        module_name, _, attrs = value.replace("/", ":").partition(":")
        return cls(module_name=module_name, attrs=attrs)

    def named_uri(self, name: Optional[str]) -> str:
        """
        URI in the format `module://<module_name>/[<name>]`.

        >>> ModuleLoader.parse("some.package:field").named_uri(None)
        'module://some.package/'

        >>> ModuleLoader.parse("some.package:field").named_uri("other_field")
        'module://some.package/other_field'

        >>> ModuleLoader.parse("some.package").named_uri(None)
        'module://some.package/'

        >>> ModuleLoader.parse("some.package").named_uri("field")
        'module://some.package/field'
        """
        return f"module://{self.module_name}/{name or ''}"

    def resolve(self, *, basedir: Optional[Path] = None) -> Self:
        """
        Resolves the plugin by finding the module spec (without importing it)

        >>> loader = ModuleLoader.parse(__name__).resolve()
        >>> loader
        ModuleLoader(attrs='__all__', module_name='livy_uploads.configs.plugins')
        >>> assert loader.spec is not None
        >>> assert loader.spec.loader is not None
        >>> assert loader.spec.origin is not None

        >>> from pathlib import Path
        >>> assert Path(loader.spec.origin).resolve() == Path(__file__).resolve()
        """
        spec = importlib.util.find_spec(self.module_name)
        if spec is None or spec.loader is None:
            raise FileNotFoundError(f"module not found: {self.module_name!r}")
        return dataclasses.replace(self, spec=spec)

    def load(self, t: type[T]) -> dict[str, type[T]]:
        """
        Imports the module and selects the matching declared classes.

        - If the module has a `__all__` attribute, use it to filter the candidates.
        - Otherwise, scan the module namespace to find all top-level classes.

        >>> all_loader = ModuleLoader.parse(__name__).resolve()
        >>> all_names = sorted([c.__name__ for c in all_loader.load(PluginLoader).values()])
        >>> all_names
        ['EntryPointsLoader', 'FileLoader', 'ModuleLoader', 'PluginLoader']

        >>> filtered_loader = ModuleLoader.parse(f"{__name__}:*eLoader").resolve()
        >>> filtered_names = sorted([c.__name__ for c in filtered_loader.load(PluginLoader).values()])
        >>> filtered_names
        ['FileLoader', 'ModuleLoader']
        """
        assert self.spec is not None
        module = importlib.import_module(self.module_name)

        return self.filter_module(module, self.attrs, t)

    @classmethod
    def filter_module(cls, module: ModuleType, attrs: str, t: type[T]) -> dict[str, type[T]]:
        all_attr = getattr(module, "__all__", None)
        if isinstance(all_attr, str):
            all_attr = [all_attr]

        results: dict[str, type[T]] = {}
        loader = ModuleLoader(module_name=module.__name__, attrs="__all__")

        if all_attr is not None:
            for name in all_attr:
                if attrs != "__all__" and not fnmatch(name, attrs):
                    continue
                try:
                    value = getattr(module, name)
                except AttributeError:
                    continue
                if is_actual_subclass(value, t):
                    attr_cls = value
                    results[loader.named_uri(name)] = attr_cls
        else:
            for name in dir(module):
                if name.startswith("_") or (attrs != "__all__" and not fnmatch(name, attrs)):
                    continue
                try:
                    value = getattr(module, name)
                except AttributeError:
                    continue
                if not is_actual_class(value):
                    continue
                attr_cls = value
                if attr_cls.__module__ != module.__name__ or not isinstance(attr_cls, t):
                    continue
                results[loader.named_uri(name)] = attr_cls

        return results


@dataclasses.dataclass(frozen=True)
class FileLoader(PluginLoader):
    """
    Loads plugins from a local code file.
    """

    __impl_typename__: ClassVar[str] = "file"
    __impl_priority__: ClassVar[int] = 10  # explicit priority (can be auto-detected), lower than ModuleLoader
    __impl_tags__: ClassVar[tuple[str, ...]] = ("file:",)

    path: PurePosixPath
    "Path to the code file. Should be a .py, .zip or .egg file."

    module_name: str
    "The assigned module name for the code file."

    spec: Optional[ModuleSpec] = dataclasses.field(default=None, repr=False, hash=False, compare=False)
    "The importlib spec for the code file, if resolved."

    def __post_init__(self) -> None:
        if not any(self.path.name.endswith(suffix) for suffix in (".py", ".zip", ".egg")):
            raise ValueError(f"plugin path must be a Python module file, got {self.path!r} instead")

        super().__post_init__()

    @classmethod
    def parse(cls, value: Union[str, Path, PurePosixPath]) -> Self:
        """
        Parses an URI such as `file://<path>[#<attrs>]` or a spec like `<path>[#<attrs>]`.

        Args:
            value: the file path object or spec.
                If a spec, must have at least one `/` separator.

        >>> FileLoader.parse("path/to/plugin.py")
        FileLoader(attrs='__all__', path=PurePosixPath('path/to/plugin.py'), module_name='plugin')

        >>> FileLoader.parse("path/to/plugin.py#field")
        FileLoader(attrs='field', path=PurePosixPath('path/to/plugin.py'), module_name='plugin')

        >>> FileLoader.parse("path/to/my-plugin.py")
        FileLoader(attrs='__all__', path=PurePosixPath('path/to/my-plugin.py'), module_name='my_plugin')

        >>> FileLoader.parse("path/to/plugin.py#")
        FileLoader(attrs='__all__', path=PurePosixPath('path/to/plugin.py'), module_name='plugin')

        >>> FileLoader.parse("plugin.py")
        Traceback (most recent call last):
        ...
        ValueError: invalid file path spec, no / separator: 'plugin.py'

        >>> FileLoader.parse(Path("path/to/plugin.docx"))
        Traceback (most recent call last):
        ...
        ValueError: plugin path must be a Python module file, got PurePosixPath('path/to/plugin.docx') instead
        """
        if isinstance(value, str):
            if "/" not in value:
                raise ValueError(f"invalid file path spec, no / separator: {value!r}")
            path = PurePosixPath(value)
        elif isinstance(value, Path):
            path = PurePosixPath(value.as_posix())
        else:
            path = value

        filename, sep, attrs = path.name.replace("#", ":").partition(":")
        if sep:
            path = path.with_name(filename)

        module_name = re.sub(r"[.-]", "_", path.stem)
        return cls(path=path, module_name=module_name, attrs=attrs)

    def named_uri(self, name: Optional[str]) -> str:
        """
        URI in the format `file://<path>[#<name>]`.

        >>> FileLoader.parse("path/to/plugin.py#field").named_uri(None)
        'file://path/to/plugin.py'

        >>> FileLoader.parse("path/to/plugin.py#field").named_uri("other_field")
        'file://path/to/plugin.py#other_field'

        >>> FileLoader.parse("path/to/plugin.py").named_uri(None)
        'file://path/to/plugin.py'

        >>> FileLoader.parse("path/to/plugin.py").named_uri("field")
        'file://path/to/plugin.py#field'
        """
        suffix = "" if not name else f"#{name}"
        return f"file://{self.path}{suffix}"

    def resolve(self, *, basedir: Optional[Path] = None) -> Self:
        """
        Resolves the plugin by trying to find a loader for the code file path, without importing it.

        >>> loader = FileLoader.parse(__file__).resolve()  # doctest: +ELLIPSIS
        >>> loader
        FileLoader(attrs='__all__', path=PurePosixPath('.../configs/plugins.py'), module_name='plugins')
        >>> assert loader.path.is_absolute()

        >>> FileLoader.parse("./this_file_will_never_exist.py").resolve()
        Traceback (most recent call last):
        ...
        FileNotFoundError: ...
        """
        basedir = basedir or Path.cwd()
        if not self.path.is_absolute():
            resolved_path = Path(PurePosixPath(basedir.as_posix()) / self.path)
        else:
            resolved_path = Path(self.path)

        if not resolved_path.exists():
            raise FileNotFoundError(f"plugin path not found: {resolved_path!r}")

        spec = importlib.util.spec_from_file_location(self.module_name, str(resolved_path))
        if spec is None or spec.loader is None:
            raise ValueError(f"cannot load module from {resolved_path!r}")

        return dataclasses.replace(self, path=PurePosixPath(resolved_path.as_posix()), spec=spec)

    def load(self, t: type[T]) -> dict[str, type[T]]:
        """
        Imports the code file as a module and selects the matching declared classes.
        """
        assert self.spec is not None
        assert self.spec.loader is not None
        module = importlib.util.module_from_spec(self.spec)
        self.spec.loader.exec_module(module)

        return ModuleLoader.filter_module(module, self.attrs, t)


@dataclasses.dataclass(frozen=True)
class EntryPointsLoader(PluginLoader):
    """
    Loads plugins from the entry points in the package metadata.
    """

    __impl_typename__: ClassVar[str] = "entrypoint"
    __impl_tags__: ClassVar[tuple[str, ...]] = ("entrypoint:",)

    __default_uris__: ClassVar[tuple[str, ...]] = ("entrypoint://.*",)

    groups: tuple[str, ...]
    "The entrypoint group names or `('__all__',)` to select all groups."

    entry_points: Optional[Collection[EntryPoint]] = dataclasses.field(
        default=None, repr=False, hash=False, compare=False
    )
    "The importlib entry points for the matching group(s), if resolved."

    def __post_init__(self) -> None:
        groups = [_fix_group(g) for g in self.groups]
        object.__setattr__(self, "groups", tuple(groups))
        super().__post_init__()

    @classmethod
    def parse(cls, value: str) -> Self:
        """
        Parses an URI part such as `<group[,group2,group3]>?select=<attrs>` or a spec like `[group1[,group2]][:attrs]`.

        >>> EntryPointsLoader.parse("some.group?select=field")
        EntryPointsLoader(attrs='field', groups=('some.group',))

        >>> EntryPointsLoader.parse("some.group:field")
        EntryPointsLoader(attrs='field', groups=('some.group',))

        >>> EntryPointsLoader.parse("some.group")
        EntryPointsLoader(attrs='__all__', groups=('some.group',))

        >>> EntryPointsLoader.parse(".*")
        EntryPointsLoader(attrs='__all__', groups=('livy_uploads.plugins.*',))

        >>> EntryPointsLoader.parse("some.group,other.group?select=field")
        EntryPointsLoader(attrs='field', groups=('some.group', 'other.group'))
        """
        if "?" in value:
            group, _, select = value.partition("?select=")
        else:
            group, _, select = value.partition(":")

        groups = split_envvar(group)
        return cls(groups=tuple(groups), attrs=select)

    def named_uri(self, name: Optional[str], *, override_group: Optional[str] = None) -> str:
        """
        URI in the format `entrypoint://[<group>][?select=<name>]`.

        >>> EntryPointsLoader.parse("some.group:field").named_uri(None)
        'entrypoint://some.group'

        >>> EntryPointsLoader.parse("some.group:field").named_uri(name="other_field")
        'entrypoint://some.group?select=other_field'

        >>> EntryPointsLoader.parse("some.group").named_uri(None)
        'entrypoint://some.group'

        >>> EntryPointsLoader.parse("some.group").named_uri("field")
        'entrypoint://some.group?select=field'

        >>> EntryPointsLoader.parse("*").named_uri("field")
        'entrypoint://?select=field'

        >>> EntryPointsLoader.parse("some.group,other.group").named_uri(None)
        'entrypoint://some.group,other.group'
        """
        groups: list[str] = list(self.groups) if override_group is None else [override_group]

        if set(groups) == {"__all__"}:
            group = ""
        else:
            group = ",".join(groups)

        suffix = f"?select={name}" if name is not None else ""
        return f"entrypoint://{group}{suffix}"

    def resolve(self, *, basedir: Optional[Path] = None) -> Self:
        """
        Resolves the entrypoints by finding the matching entrypoint groups (without importing them).

        Raises:
            FileNotFoundError: if no entrypoint is found.

        >>> EntryPointsLoader.parse(".commands:session_info").resolve()
        EntryPointsLoader(attrs='session_info', groups=('livy_uploads.plugins.commands',))

        >>> EntryPointsLoader.parse(".*").resolve()
        EntryPointsLoader(attrs='__all__', groups=('livy_uploads.plugins.commands', 'livy_uploads.plugins.patches'))

        >>> EntryPointsLoader.parse("livy_uploads.plugins.commands:command_that_will_never_exist").resolve()
        EntryPointsLoader(attrs='command_that_will_never_exist', groups=('livy_uploads.plugins.commands',))

        >>> EntryPointsLoader.parse("group.that.will.never.exist").resolve()
        Traceback (most recent call last):
        ...
        FileNotFoundError: ...

        >>> loader = EntryPointsLoader.parse(".commands").resolve()
        >>> loader
        EntryPointsLoader(attrs='__all__', groups=('livy_uploads.plugins.commands',))

        >>> assert loader.entry_points is not None
        >>> infos_entrypoint = next(ep for ep in loader.entry_points if ep.name == "infos")
        >>> infos_entrypoint
        EntryPoint(name='infos', value='livy_uploads.commands.infos', group='livy_uploads.plugins.commands')
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
            groups_s = ", ".join(repr(g) for g in self.groups)
            raise FileNotFoundError(f"no matched entrypoints for groups {groups_s}")

        groups = tuple(sorted(matched.keys()))
        entry_points = list(itertools.chain.from_iterable(matched[group] for group in groups))
        return dataclasses.replace(self, entry_points=entry_points, groups=groups)

    def load(self, t: type[T]) -> dict[str, type[T]]:
        """
        Selects and imports the matching entrypoints.

        >>> from livy_uploads.commands.base import SessionCommand
        >>> from livy_uploads.patches.base import Patch

        >>> commands_loader = EntryPointsLoader.parse(".commands:infos").resolve()
        >>> patches_loader = EntryPointsLoader.parse(".patches:sparkmagic*").resolve()

        >>> patch_impls = patches_loader.load(Patch)
        >>> sorted([c.__name__ for c in patch_impls.values()])
        ['SparkMagicReloadPatch']
        >>> sorted(patch_impls.keys())
        ['module://livy_uploads.patches.sparkmagic/SparkMagicReloadPatch']

        >>> command_impls = commands_loader.load(SessionCommand)
        >>> sorted([c.__name__ for c in command_impls.values()])
        ['SessionInfoCommand']
        >>> sorted(command_impls.keys())
        ['module://livy_uploads.commands.infos/SessionInfoCommand']
        """
        assert self.entry_points is not None
        candidates: dict[str, type] = {}

        for entry_point in self.entry_points:
            if self.attrs != "__all__" and not fnmatch(entry_point.name, self.attrs):
                continue

            loader = ModuleLoader(module_name=entry_point.module, attrs="__all__")
            value = entry_point.load()

            if is_actual_class(value):
                cls = value
                uri = loader.named_uri(cls.__name__)
                candidates[uri] = cls
            elif inspect.ismodule(value):
                module = value

                attrs_all = getattr(module, "__all__", None) or []
                if isinstance(attrs_all, str):
                    attrs_all = [attrs_all]

                for attr in attrs_all:
                    value = getattr(module, attr)
                    if is_actual_class(value):
                        cls = value
                        uri = loader.named_uri(attr)
                        candidates[uri] = cls
            else:
                raise TypeError(
                    f"bad entrypoint {entry_point!r}: expected a class or module, got {type(value)=!r} instead"
                )

        return {uri: cls for uri, cls in candidates.items() if issubclass(cls, t)}


def _fix_attrs(attrs: str, spec: Optional[str] = None) -> str:
    """
    >>> _fix_attrs("foo")
    'foo'
    >>> [_fix_attrs("*"), _fix_attrs("__all__")]
    ['__all__', '__all__']
    >>> _fix_attrs("foo*")
    'foo*'
    >>> _fix_attrs("")
    Traceback (most recent call last):
    ...
    ValueError: ...
    >>> _fix_attrs("123invalid")
    Traceback (most recent call last):
    ...
    ValueError: ...
    """
    if not attrs:
        if spec is not None:
            raise ValueError(f"empty attribute spec: {spec!r}")
        else:
            raise ValueError("empty attribute spec")

    if attrs == "*":
        return "__all__"

    if attrs.count("*") > 1:
        raise ValueError(f"too many wildcards in attribute spec: {attrs!r}")

    if not attrs.replace("*", "").isidentifier():
        raise ValueError(f"invalid attribute spec: {attrs!r}")

    return attrs


def _fix_group(group: Optional[str]) -> str:
    """
    >>> _fix_group("foo.bar")
    'foo.bar'
    >>> _fix_group(".foo")
    'livy_uploads.plugins.foo'
    >>> _fix_group(".foo.*")
    'livy_uploads.plugins.foo.*'
    >>> _fix_group("foo.bar.*")
    'foo.bar.*'
    >>> [_fix_group("*"), _fix_group(""), _fix_group(None), _fix_group("__all__")]
    ['__all__', '__all__', '__all__', '__all__']
    >>> _fix_group("123invalid")
    Traceback (most recent call last):
    ...
    ValueError: ...
    """
    group = group or ""
    if group.startswith("."):
        group = PREFIX + group.removeprefix(".")
    if not group or group == "*":
        return "__all__"
    if not is_module_name(group.removesuffix("*").rstrip(".")):
        raise ValueError(f"invalid entrypoint group name: {group!r}")
    return group


def get_loader(source: Union[str, Path]) -> PluginLoader:
    """
    Builds the plugin loader for the given source spec.

    Args:
        source: the source spec or path.

    Parsing behavior:

    - Path objects are passed directly to `FileLoader`:

        >>> get_loader(Path("path/to/plugin.py"))
        FileLoader(attrs='__all__', path=PurePosixPath('path/to/plugin.py'), module_name='plugin')

    - Strings with a `://` separator are parsed as a loader URI:

        >>> get_loader("module://some.package")
        ModuleLoader(attrs='__all__', module_name='some.package')

        >>> get_loader("file://path/to/plugin.py#field")
        FileLoader(attrs='field', path=PurePosixPath('path/to/plugin.py'), module_name='plugin')

    - Strings with a `:` separator are parsed as a tagged loader spec:

        >>> get_loader("file:path/to/plugin.py")
        FileLoader(attrs='__all__', path=PurePosixPath('path/to/plugin.py'), module_name='plugin')

        >>> get_loader("entrypoint:some.group:field")
        EntryPointsLoader(attrs='field', groups=('some.group',))

    - Otherwise, try all available loaders with explicit priority order, higher first.

        >>> get_loader("some.package")
        ModuleLoader(attrs='__all__', module_name='some.package')

        >>> get_loader("some.package:field")
        ModuleLoader(attrs='field', module_name='some.package')

        >>> get_loader("./path/to/plugin.py")
        FileLoader(attrs='__all__', path=PurePosixPath('path/to/plugin.py'), module_name='plugin')

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
    ['file://some/file.py', 'entrypoint://livy_uploads.plugins.*']

    >>> no_default_loaders = get_loaders('some.package', '!entrypoint://.*')
    >>> [l.uri for l in no_default_loaders]
    ['module://some.package/']
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


def resolve_loaders(
    value: Optional[str] = None,
    *,
    basedir: Optional[Path] = None,
    save: bool = True,
) -> list[PluginLoader]:
    """
    Resolves the plugin loaders for the given source specs.

    Args:
        value: the merged source specs
        basedir: the base directory to resolve relative file paths against
        save: whether to save the resolved loaders to the `$SPARKRL_PLUGINS` environment variable.
    """
    sources = split_envvar(value)

    declared_loaders = get_loaders(*sources)
    resolved_loaders: dict[str, PluginLoader] = {}
    failed_uris: list[str] = []

    for plugin in declared_loaders:
        try:
            resolved = plugin.resolve(basedir=basedir)
            resolved_loaders[resolved.uri] = resolved
        except FileNotFoundError:
            failed_uris.append(plugin.uri)

    if failed_uris:
        LOGGER.warning("skipped %d plugins: %r", len(failed_uris), failed_uris)

    if save:
        os.environ[PLUGINS_ENVVARS] = ",".join(resolved_loaders.keys())

    return list(resolved_loaders.values())
