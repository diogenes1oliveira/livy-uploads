import inspect
from collections.abc import Collection, Mapping
from typing import Any, Optional, Protocol, cast, runtime_checkable

from typing_extensions import Self

from livy_uploads.utils.typeutils import is_concrete

_impl_registry: dict[type["Implementation"], dict[str, type["Implementation"]]] = {}


@runtime_checkable
class Implementation(Protocol):
    """Protocol for plugin implementations with automatic registration.

    When subclassed, abstract classes are registered as interfaces and concrete
    classes are registered as implementations for all their abstract base classes.

    If a class inherits from multiple implementations, it will be registered as an implementation for all of them.
    """

    def __init_subclass__(cls: type["Implementation"], **kwargs: Any) -> None:
        """
        Register abstract classes as interfaces and concrete classes as implementations.
        """
        super().__init_subclass__(**kwargs)

        direct_impls = [base for base in cls.__bases__ if issubclass(base, Implementation)]
        if len(direct_impls) > 1:
            raise TypeError(f"Multiple inheritance of Implementation: {cls.__name__} inherits from {direct_impls}")

        if not is_concrete(cls):
            _impl_registry.setdefault(cls, {})
            return

        for base in inspect.getmro(cls):
            if base is object or not issubclass(base, Implementation):
                continue
            impls = _impl_registry.setdefault(base, {})
            impls[cls.impl_typename()] = cls

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

        Must be called on interface classes only.

        Returns:
            The value of the closest `__impl_group__` class attribute if defined, otherwise None.
        """
        assert not is_concrete(cls), f".plugin_group() invoked in the non-interface class {cls.__name__}"

        return getattr(cls, "__plugin_group__", None) or None

    @classmethod
    def get_interfaces(cls) -> tuple[type[Self], ...]:
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

    @classmethod
    def get_implementations(
        cls, tags: Optional[Collection[str]] = None, min_priority: Optional[int] = None
    ) -> Mapping[str, type[Self]]:
        """
        Get all registered implementations for this interface, sorted by priority.

        Args:
            tags: Optional collection of tags to filter the implementations by.
                If provided, only implementations that declare all of those tags will be returned.
            min_priority: Optional minimum priority threshold.
                If provided, only implementations with priority >= min_priority will be returned.

        Returns:
            Mapping of typename to matching implementation class. Empty dict if no matching implementations found.
        """
        try:
            impls = dict(_impl_registry[cls])
        except KeyError:
            impls = {}

        if tags is not None:
            impls = {typename: impl for typename, impl in impls.items() if all(tag in impl.impl_tags() for tag in tags)}

        if min_priority is not None:
            impls = {typename: impl for typename, impl in impls.items() if impl.impl_priority() >= min_priority}

        return {
            typename: cast(type[Self], impl)
            for typename, impl in sorted(impls.items(), key=lambda item: item[1].impl_priority(), reverse=True)
        }

    @classmethod
    def get_implementation(cls, typename: Optional[str] = None, tags: Optional[Collection[str]] = None) -> type[Self]:
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
        impls = cls.get_implementations(tags=tags)
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
