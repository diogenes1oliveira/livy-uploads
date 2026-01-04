__all__ = (
    "ModuleLoader",
    "ModuleLoaderMixIn",
    "scan_module",
)

import copy
import dataclasses
import importlib
import importlib.util
from collections.abc import Iterator, Sequence
from fnmatch import fnmatch
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType
from typing import Any, ClassVar, Optional, TypeVar, cast, get_origin

from typing_extensions import Self

from livy_uploads.plugins.base import FoundObject, FoundPath, FoundType, Matcher, PluginLoader, Predicate
from livy_uploads.plugins.files import scan_paths
from livy_uploads.utils.datautils import is_module_name
from livy_uploads.utils.typeutils import is_actual_class

T = TypeVar("T")

MISSING: Any = object()


class ModuleLoaderMixIn:
    """
    Mixin for plugin loaders that can load modules from a Python module.
    """

    module_name: str
    "Python module name (!)"

    spec: Optional[ModuleSpec] = dataclasses.field(default=None, repr=False, hash=False, compare=False, init=False)
    "The importlib spec for the module, once resolved."

    def __post_init__(self) -> None:
        if not is_module_name(self.module_name):
            raise ValueError(f"invalid module name: {self.module_name!r}")

    def find_objects(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[T]] = None,
        predicate: Optional[Predicate[T]] = None,
    ) -> Iterator[FoundObject[T]]:
        """
        Imports the module and scans it for the matching declared objects.

        >>> (loader,) = ModuleLoader(__name__).resolve()
        >>> uris = sorted([f.uri for f in loader.find_objects(object, pattern="scan_*")])
        >>> uris
        ['module://livy_uploads.plugins.modules/scan_module']
        """
        matcher = match or Matcher.instance(t)

        assert self.spec is not None
        module = importlib.import_module(self.module_name)
        loader = cast(PluginLoader, self)

        for name, obj in scan_module(module, pattern=pattern, matcher=matcher, predicate=predicate):
            uri = loader.named_uri(name)
            yield FoundObject(object=obj, uri=uri, pattern=pattern, loader=loader)

    def find_types(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[type[T]]] = None,
        predicate: Optional[Predicate[type[T]]] = None,
    ) -> Iterator[FoundType[T]]:
        """
        Imports the module and scans it for the matching class definitions.

        >>> (loader,) = ModuleLoader(__name__).resolve()
        >>> uris = sorted([f.uri for f in loader.find_types(PluginLoader, pattern="*")])
        >>> uris
        ['module://livy_uploads.plugins.modules/ModuleLoader']
        """
        matcher = match or Matcher.subclass(t)

        assert self.spec is not None
        module = importlib.import_module(self.module_name)
        loader = cast(PluginLoader, self)

        for name, cls in scan_module(module, pattern=pattern, matcher=matcher, predicate=predicate):
            uri = loader.named_uri(name)
            yield FoundType(type=cls, uri=uri, pattern=pattern, loader=loader)

    def find_paths(self, *, pattern: str, basedir: Optional[Path] = None) -> Iterator[FoundPath]:
        """
        Scans the module directory for the matching relative paths.

        >>> (loader,) = ModuleLoader(__name__).resolve()

        >>> exact_filename_uris = sorted([f.uri for f in loader.find_paths(pattern="modules.py")])
        >>> exact_filename_uris
        ['module://livy_uploads.plugins.modules/modules.py']

        >>> partial_filename_uris = sorted([f.uri for f in loader.find_paths(pattern="b*.py")])
        >>> partial_filename_uris
        ['module://livy_uploads.plugins.modules/base.py']

        >>> root = __name__.partition(".")[0]
        >>> (resolved_root_loader,) = ModuleLoader(root).resolve()
        >>> resolved_root_loader.module_name
        'livy_uploads'

        >>> root_uris = sorted(f.uri for f in resolved_root_loader.find_paths(pattern='plugins/m*.py'))
        >>> root_uris
        ['module://livy_uploads/plugins/modules.py']
        """
        assert self.spec is not None
        loader = cast(PluginLoader, self)
        for filename, path in scan_spec_paths(self.spec, pattern=pattern):
            uri = loader.named_uri(filename)
            yield FoundPath(path=path, uri=uri, pattern=pattern, loader=loader)


@dataclasses.dataclass(frozen=True)
class ModuleLoader(ModuleLoaderMixIn, PluginLoader):
    """
    Loads plugins from a Python module.
    """

    __impl_typename__: ClassVar[str] = "module"
    __impl_priority__: ClassVar[int] = 10  # explicit priority (can be auto-detected), lower than FileLoader

    module_name: str
    "Python module name (!)"

    @classmethod
    def parse(cls, value: str) -> Self:
        """
        Parses a spec or URI part like `<module_name>[/|/*]`.

        >>> ModuleLoader.parse("some.package")
        ModuleLoader(module_name='some.package')

        >>> ModuleLoader.parse(".")
        ModuleLoader(module_name='livy_uploads')

        >>> ModuleLoader.parse(".plugins/")
        ModuleLoader(module_name='livy_uploads.plugins')

        >>> ModuleLoader.parse("other_package/")
        ModuleLoader(module_name='other_package')

        >>> ModuleLoader.parse("some.package/*")
        ModuleLoader(module_name='some.package')

        >>> ModuleLoader.parse("./some/path.py")
        Traceback (most recent call last):
        ...
        ValueError: ...
        """
        module_name = value.removesuffix("/").removesuffix("/*")
        if module_name.startswith("."):
            this_module = __name__.partition(".")[0]
            module_name = this_module if module_name == "." else this_module + module_name
        return cls(module_name=module_name)

    def named_uri(self, name: Optional[str]) -> str:
        """
        URI in the format `module://<module_name>/[<name>]`.

        >>> loader = ModuleLoader("some.package")

        >>> loader.named_uri(None)
        'module://some.package'

        >>> loader.named_uri("field")
        'module://some.package/field'

        >>> loader.uri
        'module://some.package'
        """
        suffix = "" if not name else f"/{name}"
        return f"module://{self.module_name}{suffix}"

    def resolve(self, *, basedir: Optional[Path] = None) -> tuple[Self]:
        """
        Resolves the plugin by finding the module spec (without importing it)

        >>> (resolved_loader,) = ModuleLoader.parse(__name__).resolve()
        >>> resolved_loader
        ModuleLoader(module_name='livy_uploads.plugins.modules')

        >>> assert resolved_loader.spec is not None
        >>> assert resolved_loader.spec.loader is not None
        >>> assert resolved_loader.spec.origin is not None

        >>> assert Path(resolved_loader.spec.origin).resolve() == Path(__file__).resolve()
        """
        try:
            spec = importlib.util.find_spec(self.module_name)
        except ModuleNotFoundError:
            raise FileNotFoundError(f"module not found: {self.module_name!r}") from None

        if spec is None or spec.loader is None:
            raise FileNotFoundError(f"module not found: {self.module_name!r}")

        setup_loader = copy.deepcopy(self)
        object.__setattr__(setup_loader, "spec", spec)

        return (setup_loader,)


def scan_module(
    module: ModuleType,
    *,
    matcher: Matcher[T],
    pattern: str,
    predicate: Optional[Predicate[T]] = None,
    all_names: Optional[Sequence[str]] = MISSING,
) -> Iterator[tuple[str, T]]:
    """
    Scans a module for the matching declared objects.

    - If the module has an `__all__` attribute, use it to filter the candidates.
    - Otherwise, scan the module namespace to find all top-level classes, functions or strings.

    >>> import string
    >>> assert hasattr(string, '__all__')
    >>> scanned_ascii = dict(scan_module(string, matcher=Matcher.instance(str), pattern='ascii_*case'))
    >>> sorted(scanned_ascii.keys())
    ['ascii_lowercase', 'ascii_uppercase']

    >>> import inspect
    >>> assert not hasattr(inspect, '__all__')
    >>> scanned_is = dict(scan_module(inspect, matcher=callable, pattern='is*'))
    >>> len(scanned_is) > 10  # should find many is* functions
    True
    >>> 'isfunction' in scanned_is
    True

    >>> this_module = importlib.import_module(__name__)
    >>> scanned_loaders = dict(scan_module(this_module, matcher=Matcher.subclass(PluginLoader), pattern="*eLoader"))
    >>> sorted(scanned_loaders.keys())
    ['ModuleLoader']

    >>> scanned_objects = dict(scan_module(this_module, matcher=Matcher.any(), pattern="Found*", all_names=None))
    >>> sorted(scanned_objects.keys())
    ['FoundObject', 'FoundPath', 'FoundType']
    """
    pattern = pattern or "*"
    names = all_names if all_names is not MISSING else getattr(module, "__all__", None)

    if names is not None:
        # check only the names explicitly listed in __all__
        it = (
            name
            for name in names
            # match the pattern
            if fnmatch(name, pattern)
        )
    else:
        # find all top-level classes in the module
        it = (
            name
            for name in dir(module)
            # exclude private names
            if not name.startswith("_")
            # match the pattern
            and fnmatch(name, pattern)
            # attribute exists and is not None
            and ((value := getattr(module, name)) is not None)
            # value is not a generic type
            and get_origin(value) is None
            # # simple string, class or function
            and (isinstance(value, str) or is_actual_class(value) or callable(value))
            # class or function was defined in the same module
            and (
                (all_names is None or getattr(value, "__module__", None) == module.__name__)
                if callable(value)
                else True
            )
        )

    for name in it:
        try:
            value = getattr(module, name)
        except AttributeError:
            # print(f"AttributeError: {name}")
            continue

        if not matcher(value):
            # print(f"not matcher: {name} {value} {matcher}")
            continue

        obj = value
        if predicate is not None and not predicate(obj):
            # print(f"not predicate: {name} {obj} {predicate}")
            continue

        yield name, obj


def scan_spec_paths(spec: ModuleSpec, *, pattern: str) -> Iterator[tuple[str, Path]]:
    """
    Scans the module spec for the matching relative paths.
    """
    if spec.origin is None:
        return

    origin = Path(spec.origin)
    if not origin.is_file():
        return

    yield from scan_paths(origin, pattern=pattern)
