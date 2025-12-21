from typing import Any, Type, TypeVar, Union

import requests

T = TypeVar("T")


def try_decode(response: requests.Response) -> Any:
    """
    Tries to decode the response as JSON or text.
    """
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        try:
            return response.text
        except UnicodeDecodeError:
            return response.content.decode("utf8", errors="replace")


def assert_type(value: Any, expected_type: Type[T]) -> T:
    """
    Type assertion utility function.
    """
    try:
        origin = getattr(expected_type, "__origin__")
        if origin is Union:
            args = expected_type.__args__  # type: ignore
            if len(args) == 2 and args[1] is type(None):
                nullable = True
                expected_type = args[0]
    except AttributeError:
        nullable = False

    if nullable and value is None:
        return value  # type: ignore

    if not isinstance(value, expected_type):
        raise ValueError(f"Expected {expected_type}, got {type(value)}")

    return value
