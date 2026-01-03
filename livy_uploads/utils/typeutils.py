import collections.abc
import inspect
from abc import ABC
from typing import TYPE_CHECKING, Any, Literal, Optional, Protocol, TypeVar, Union, get_origin, overload

from typing_extensions import TypeGuard

if TYPE_CHECKING:
    from requests import Response
else:
    Response = Any

T = TypeVar("T")


def try_decode(response: Response) -> Any:
    """
    Tries to decode the response as JSON or text.
    """
    from requests.exceptions import JSONDecodeError

    try:
        return response.json()
    except JSONDecodeError:
        try:
            return response.text
        except UnicodeDecodeError:
            return response.content.decode("utf8", errors="replace")


def assert_type(value: Any, expected_type: type[T]) -> T:
    """
    Type assertion utility function.
    """
    try:
        origin = expected_type.__origin__  # type: ignore[attr-defined]
        if origin is Union:
            args = expected_type.__args__  # type: ignore[attr-defined]
            if len(args) == 2 and args[1] is type(None):
                nullable = True
                expected_type = args[0]
    except AttributeError:
        nullable = False

    if nullable and value is None:
        return value  # type: ignore[return-value]

    if not isinstance(value, expected_type):
        raise ValueError(f"Expected {expected_type}, got {type(value)}")

    return value


@overload
def is_type(obj: None, t: type[T], nullable: Optional[Literal[False]] = None) -> TypeGuard[T]: ...
@overload
def is_type(obj: None, t: type[T], nullable: Literal[True]) -> TypeGuard[Optional[T]]: ...
def is_type(obj: Any, t: type[T], nullable: Optional[bool] = None) -> bool:
    if obj is None:
        return nullable is True

    try:
        return isinstance(obj, t)
    except TypeError:
        return False


@overload
def as_type(obj: None, t: type[T], nullable: Optional[Literal[False]] = None) -> T: ...
@overload
def as_type(obj: None, t: type[T], nullable: Literal[True]) -> Optional[T]: ...
def as_type(obj: Any, t: type[T], nullable: Optional[bool] = None) -> Optional[T]:
    if not is_type(obj, t, nullable):  # type: ignore
        raise ValueError(f"Expected {t}, got {type(obj)}")
    return obj


def is_list(obj: Any) -> TypeGuard[collections.abc.Sequence[Any]]:
    """
    >>> is_list(1)
    False

    >>> is_list([1, 2, 3])
    True

    >>> is_list("hello")
    False

    >>> is_list(b"data")
    False

    >>> is_list({"k": "v"})
    False

    >>> is_list((1, 2, 3))
    True

    >>> is_list({1, 2, 3})
    False

    >>> is_list(range(3))
    True

    >>> is_list(iter(range(3)))
    False

    >>> is_list(None)
    False
    """
    if isinstance(obj, (str, bytes, bytearray)):
        return False

    return isinstance(obj, collections.abc.Sequence) and isinstance(obj, collections.abc.Collection)


def is_actual_class(obj: Any) -> TypeGuard[type]:
    try:
        return inspect.isclass(obj) and get_origin(obj) is None
    except TypeError:
        return False


def is_actual_subclass(obj: Any, t: type[T]) -> TypeGuard[type[T]]:
    return is_actual_class(obj) and issubclass(obj, t)


@overload
def is_concrete(obj: Any, t: None = None) -> bool: ...
@overload
def is_concrete(obj: Any, t: type[T]) -> TypeGuard[type[T]]: ...
def is_concrete(obj: Any, t: Optional[type[T]] = None) -> bool:
    """Check if obj is a concrete class (optionally of type t).

    A class is concrete if:
        - It is a class (not an instance)
        - It is not a generic type
        - It is not abstract
        - It is not ABC or Protocol themselves
        - It does not directly inherit from ABC
        - It does not directly inherit from Protocol

    Args:
        obj: Object to check.
        t: Optional parent type to verify inheritance.

    Returns:
        True if obj is a concrete class, False otherwise.

    Examples:
        >>> is_concrete(int)
        True

        >>> is_concrete(42)
        False

        >>> from abc import ABC, abstractmethod
        >>> class SomeABC(ABC): pass
        >>> is_concrete(SomeABC)
        False

        >>> class SomeAbstract(ABC):
        ...     @abstractmethod
        ...     def some_method(self): pass
        >>> is_concrete(SomeAbstract)
        False

        >>> class SomeImpl(SomeAbstract):
        ...     def some_method(self): pass
        >>> is_concrete(SomeImpl)
        True

    """
    try:
        if (
            inspect.isclass(obj)
            and obj is not ABC
            and obj is not Protocol
            and get_origin(obj) is None
            and not inspect.isabstract(obj)
            and ABC not in obj.__bases__
            and Protocol not in obj.__bases__
        ):
            cls = obj
        else:
            return False
    except TypeError:
        return False

    if t is not None:
        return issubclass(cls, t)
    else:
        return True
