import collections.abc
from typing import TYPE_CHECKING, Any, Literal, Optional, TypeVar, Union, overload

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
