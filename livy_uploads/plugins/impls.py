__all__ = (
    "Implementation",
    "get_implementation",
    "get_implementations",
    "get_interfaces",
    "register_type",
    "find_implementable_bases",
    "implementation_as_json",
)

import inspect
import logging
from collections.abc import Collection, Mapping
from fnmatch import fnmatch
from typing import Any, Optional, Protocol, TypeVar, cast, runtime_checkable

from livy_uploads.plugins import constants
from livy_uploads.utils.typeutils import get_short_description, is_concrete

LOGGER = logging.getLogger(__name__)

_impl_registry: dict[type["Implementation"], dict[str, type["Implementation"]]] = {}


Impl = TypeVar("Impl", bound="Implementation")
USER_MIN_PRIORITY = 100


@runtime_checkable
class Implementation(Protocol):
    """Protocol for plugin implementations with automatic registration.

    When subclassed, abstract classes are registered as interfaces and concrete
    classes are registered as implementations for all their abstract base classes.

    If a class inherits from multiple implementations, it will be registered as an implementation for all of them.
    """

    @classmethod
    def impl_typename(cls) -> str:
        """
        Get the typename for this implementation.

        Returns:
            The value of `__impl_typename__` class attribute if defined, otherwise the class name.
        """
        try:
            return cls.__dict__.get("__impl_typename__", None) or cls.__name__
        except KeyError:
            return cls.__name__

    @classmethod
    def impl_priority(cls) -> int:
        """
        Get the priority of this implementation.

        Returns:
            The value of `__impl_priority__` class attribute if defined, otherwise 0.
        """
        try:
            return cls.__dict__.get("__impl_priority__", None) or 0
        except KeyError:
            return 0

    @classmethod
    def impl_tags(cls) -> tuple[str, ...]:
        """
        Get the tags of this implementation.

        Returns:
            The value of `__impl_tags__` class attribute if defined, otherwise an empty tuple.
        """
        try:
            return cls.__dict__.get("__impl_tags__", None) or ()
        except KeyError:
            return ()

    @classmethod
    def plugin_group(cls) -> Optional[str]:
        """
        Get the plugin group name for this interface.

        Returns:
            The value of the closest `__impl_group__` class attribute if defined, otherwise None.
        """
        return getattr(cls, "__plugin_group__", None) or None

    @classmethod
    def impl_uri(cls) -> Optional[str]:
        """
        Gets the URI for this implementation.

        Returns:
            The value of the `__impl_uri__` class attribute if set
        """
        return getattr(cls, "__impl_uri__", None)


def get_interfaces(cls: type[Impl]) -> tuple[type[Impl], ...]:
    """
    Get all registered interfaces that are subclasses of this interface.

    Must be called on abstract classes only.

    Returns:
        All abstract classes in the registry that inherit from the calling class.

    Raises:
        AssertionError: If called on a concrete class.
    """
    assert not is_concrete(cls), f".get_interfaces() invoked in the non-interface class {cls.__name__}"
    return tuple(base for base in _impl_registry.keys() if issubclass(base, cls))


def get_implementations(
    cls: type[Impl],
    pattern: Optional[str] = None,
    tags: Optional[Collection[str]] = None,
    min_priority: Optional[int] = None,
) -> Mapping[str, type[Impl]]:
    """
    Get all registered implementations for this interface, sorted by priority.

    Args:
        pattern: Optional pattern to filter the implementations by.
            If provided, only implementations that match the pattern will be returned.
        tags: Optional collection of tags to filter the implementations by.
            If provided, only implementations that declare all of those tags will be returned.
        min_priority: Optional minimum priority threshold.
            If provided, only implementations with priority >= min_priority will be returned.

    Returns:
        Mapping of typename to matching implementation class. Empty dict if no matching implementations found.
    """
    pattern = pattern or "*"

    try:
        impls = dict(_impl_registry[cls])
    except KeyError:
        impls = {}

    if tags is not None:
        impls = {typename: impl for typename, impl in impls.items() if all(tag in impl.impl_tags() for tag in tags)}

    if min_priority is not None:
        impls = {typename: impl for typename, impl in impls.items() if impl.impl_priority() >= min_priority}

    if pattern != "*":
        impls = {typename: impl for typename, impl in impls.items() if fnmatch(typename, pattern)}

    return {
        typename: cast(type[Impl], impl)
        for typename, impl in sorted(impls.items(), key=lambda item: item[1].impl_priority(), reverse=True)
    }


def get_implementation(
    cls: type[Impl],
    typename: Optional[str] = None,
    tags: Optional[Collection[str]] = None,
) -> type[Impl]:
    """
    Get a specific implementation by typename, or the only implementation if unique.

    Args:
        typename: Optional typename to look up.
            If not provided, will try to find a single implementation.
        tags: Optional collection of tags to filter the implementations by.
            If provided, only implementations that declare all of those tags will be matched.

    Returns:
        The implementation class matching the typename, or the sole implementation
        if typename is None and exactly one exists.

    Raises:
        ValueError: no matching implementation found, or multiple implementations found.
    """
    impls = get_implementations(cls, tags=tags)
    if not tags:
        suffix = ""
    elif len(tags) == 1:
        suffix = f"tag {next(iter(tags))!r}"
    else:
        suffix = f"tags {tuple(tags)!r}"

    if typename:
        try:
            return impls[typename]
        except KeyError:
            suffix = f" and {suffix}" if suffix else ""
            raise ValueError(f"no implementation found for {cls.__name__} with {typename=!r}{suffix}") from None
    elif len(impls) > 1:
        names = ", ".join(impls.keys())
        suffix = f" with {suffix}" if suffix else ""
        raise ValueError(f"multiple implementations found for {cls.__name__}{suffix}: {names!r}")
    else:
        try:
            return next(iter(impls.values()))
        except StopIteration:
            suffix = f" and {suffix}" if suffix else ""
            raise ValueError(f"no implementations found for {cls.__name__}{suffix}") from None


def register_type(cls: type, uri: Optional[str] = None) -> None:
    """
    Register abstract classes as interfaces and concrete classes as implementations.
    """
    if not issubclass(cls, Implementation):
        raise TypeError(f"Class {cls.__name__} is not a subclass of Implementation")

    direct_impls = [base for base in cls.__bases__ if issubclass(base, Implementation)]
    if len(direct_impls) > 1:
        raise TypeError(f"Multiple inheritance of Implementation: {cls.__name__} inherits from {direct_impls}")

    if uri:
        cls.__impl_uri__ = uri

    if not is_concrete(cls):
        _impl_registry.setdefault(cls, {})
        return

    for base in inspect.getmro(cls):
        if base is object or not issubclass(base, Implementation):
            continue
        impls = _impl_registry.setdefault(base, {})
        impls[cls.impl_typename()] = cls


def find_implementable_bases() -> Mapping[str, type["Implementation"]]:
    """
    Finds all implementable base classes declared in the entrypoints.

    This will scan all the groups in `sparkrl.plugins.*`, looking for the entrypoints named `base`.
    """
    # inline import to avoid circular shenanigans
    from livy_uploads.plugins.entrypoints import EntryPointsLoader

    found_types = dict[str, type["Implementation"]]()
    no_groups = list[str]()

    loaders = EntryPointsLoader.parse(f"{constants.GROUP_PREFIX}*").resolve()
    for loader in loaders:
        if not isinstance(loader, EntryPointsLoader):
            continue

        for found in loader.find_types(Implementation, pattern="base"):
            if not found.type.plugin_group():
                no_groups.append(found.uri)
                continue
            found_types[found.uri] = found.type

    uris = " ".join(found_types.keys())
    LOGGER.debug("Discovered %d implementable base classes: %s", len(found_types), uris)
    if no_groups:
        LOGGER.warning("No plugin group set in %d implementable base classes: %s", len(no_groups), " ".join(no_groups))
    return dict(sorted(found_types.items(), key=lambda kv: (kv[0], kv[1].impl_priority()), reverse=True))


def implementation_as_json(impl: type["Implementation"]) -> dict[str, Any]:
    """
    Get the JSON representation of an implementation.
    """
    return {
        "name": impl.impl_typename(),
        "type": "concrete" if is_concrete(impl) else "interface",
        "description": get_short_description(impl),
        "tags": impl.impl_tags(),
        "priority": impl.impl_priority(),
        "group": impl.plugin_group(),
        "class": impl.__name__,
        "module": impl.__module__,
        "uri": impl.impl_uri(),
    }
