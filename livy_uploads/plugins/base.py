__all__ = (
    "Matcher",
    "Predicate",
    "FoundPath",
    "FoundObject",
    "FoundType",
    "Loader",
    "PluginLoader",
    "NoCodeMixin",
)

import dataclasses
import functools
from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, ClassVar, Generic, Optional, Protocol, TypeVar, runtime_checkable

from typing_extensions import Self, TypeGuard

from livy_uploads.configs.impl import Implementation
from livy_uploads.utils.typeutils import get_short_description, is_actual_subclass

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)


class Matcher(Protocol, Generic[T_co]):
    """
    Type guard function to assert the type of a value.
    """

    def __call__(self, value: Any) -> TypeGuard[T_co]:
        raise NotImplementedError

    @staticmethod
    def subclass(cls: type[T]) -> "Matcher[type[T]]":
        """
        A matcher that checks if a value is a subclass of the given class.

        >>> plugin_matcher = Matcher.subclass(PluginLoader)
        >>> plugin_matcher(PluginLoader)
        True
        >>> subclass = type('DummyLoader', (PluginLoader,), {})
        >>> plugin_matcher(subclass)
        True
        >>> [plugin_matcher(str), plugin_matcher(dict), plugin_matcher(list[int]), plugin_matcher(42)]
        [False, False, False, False]

        >>> from collections.abc import Mapping
        >>> dict_matcher = Matcher.subclass(Mapping)
        >>> dict_matcher(dict)
        True
        >>> dict_matcher(dict[str, str])
        False
        >>> [dict_matcher(42), dict_matcher(list)]
        [False, False]
        """
        return functools.partial(is_actual_subclass, t=cls)  # type: ignore

    @staticmethod
    def any() -> "Matcher[Any]":
        """
        A matcher that checks if a value is any type.

        >>> any_matcher = Matcher.any()
        >>> any_matcher(42)
        True
        >>> any_matcher(list)
        True
        """
        return lambda _: True  # type: ignore

    @staticmethod
    def instance(cls: type[T]) -> "Matcher[T]":
        """
        A matcher that checks if a value is an instance of the given class.

        >>> from collections.abc import Collection, Sequence
        >>> collection_matcher = Matcher.instance(Collection)
        >>> sequence_matcher = Matcher.instance(Sequence)

        >>> value = [1, 2, 3]
        >>> [collection_matcher(value), sequence_matcher(value)]
        [True, True]

        >>> value = (42, 43)
        >>> [collection_matcher(value), sequence_matcher(value)]
        [True, True]

        >>> value = {'k': 'v'}
        >>> [collection_matcher(value), sequence_matcher(value)]
        [True, False]

        >>> value = 42
        >>> [collection_matcher(value), sequence_matcher(value)]
        [False, False]
        """
        return lambda value: isinstance(value, cls)  # type: ignore


class Predicate(Protocol, Generic[T_contra]):
    """
    Filter out scanned objects.
    """

    def __call__(self, value: T_contra) -> bool:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class FoundPath:
    """
    A path that was found by a plugin loader.
    """

    path: Path
    "local absolute path"
    uri: str
    "the plugin URI the path was found at"
    pattern: str
    "the pattern that matched the path"
    loader: Optional["PluginLoader"] = dataclasses.field(default=None, repr=False, hash=False, compare=False)
    "a reference to the plugin loader that found the path"


@dataclasses.dataclass(frozen=True)
class FoundObject(Generic[T]):
    """
    An object that was found by a plugin loader.
    """

    object: T
    "the object that was found"
    uri: str
    "the plugin URI the object was found at"
    pattern: str
    "the pattern that matched the object"
    loader: Optional["PluginLoader"] = dataclasses.field(default=None, repr=False, hash=False, compare=False)
    "a reference to the plugin loader that found the object"


@dataclasses.dataclass(frozen=True)
class FoundType(Generic[T]):
    """
    A type that was found by a plugin loader.
    """

    type: type[T]
    "the type that was found"
    uri: str
    "the plugin URI the type was found at"
    pattern: str
    "the pattern that matched the type"
    loader: Optional["PluginLoader"] = dataclasses.field(default=None, repr=False, hash=False, compare=False)
    "a reference to the plugin loader that found the type"


@runtime_checkable
class Loader(Protocol):
    """
    An object that can find paths, classes and plain Python objects.
    """

    @abstractmethod
    def find_paths(self, *, pattern: str, basedir: Optional[Path] = None) -> Iterator[FoundPath]:
        """
        Finds relative paths that match the pattern.
        """
        raise NotImplementedError

    @abstractmethod
    def find_types(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[type[T]]] = None,
        predicate: Optional[Predicate[type[T]]] = None,
    ) -> Iterator[FoundType[T]]:
        """
        Finds class definitions that match the pattern.
        """
        raise NotImplementedError

    @abstractmethod
    def find_objects(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[T]] = None,
        predicate: Optional[Predicate[T]] = None,
    ) -> Iterator[FoundObject[T]]:
        """
        Finds objects that match the pattern.
        """
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class PluginLoader(Loader, Implementation, ABC):
    """
    Base class for all plugin-based loaders.

    Implementations should define the `__impl_typename__` class attribute matching the scheme of the loader URI.
    """

    __impl_typename__: ClassVar[str]
    "The scheme of this plugin loader."

    __impl_tags__: ClassVar[tuple[str, ...]] = ()
    """
    The tags of this plugin loader.

    Used to match a spec prefixed by just `prefix:` instead of the full URI.
    """

    __impl_priority__: ClassVar[int] = 0
    """
    The priority of this plugin loader, higher first.

    If set, this plugin loader can be tried automatically as a fallback.
    """

    __default_uris__: ClassVar[Optional[tuple[str, ...]]] = None
    "The default URIs for this plugin loader to be added after the user-defined URIs."

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
        return self.named_uri(None)

    @abstractmethod
    def named_uri(self, name: Optional[str]) -> str:
        """
        Implementation-specific URI for possibly a specific attribute loaded by this plugin loader.
        """
        raise NotImplementedError

    @abstractmethod
    def resolve(self, *, basedir: Optional[Path] = None) -> "Sequence[PluginLoader]":
        """
        Resolves the plugin modules without executing any code.

        Raises:
            FileNotFoundError: if the loader cannot be resolved.
        """
        raise NotImplementedError

    @classmethod
    def get_default_uris(cls) -> tuple[str, ...]:
        """
        Returns the default URIs for all implementations of this loader type.
        """
        all_default_uris = set[str]()

        for impl in cls.get_implementations().values():
            default_uris = impl.__default_uris__ or ()
            all_default_uris.update(default_uris)

        return tuple(sorted(all_default_uris))

    def as_json(self) -> dict[str, Any]:
        return {
            "type": self.impl_typename(),
            "description": get_short_description(type(self)),
            "uri": self.uri,
            "tags": self.impl_tags(),
            "default_uris": self.get_default_uris(),
            "priority": self.impl_priority(),
        }


class NoCodeMixin:
    """
    Mixin for plugin loaders that do not support executing code (generally for security reasons).
    """

    def find_objects(
        self,
        t: type[T],
        *,
        pattern: str,
        match: Optional[Matcher[T]] = None,
        predicate: Optional[Predicate[T]] = None,
    ) -> Iterator[FoundObject[T]]:
        """
        Always returns an empty iterator: this loader does not support executing code.
        []
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
        Always returns an empty iterator: this loader does not support executing code.
        """
        return iter(())
