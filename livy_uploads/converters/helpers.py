import dataclasses
from typing import Any, ClassVar, Protocol, TypeVar, cast

from typing_extensions import TypeGuard

T = TypeVar("T")


class HasDataclassFields(Protocol):
    """
    Marker protocol for dataclass instances.
    """

    __dataclass_fields__: ClassVar[dict[str, dataclasses.Field]]
    "The dataclass fields metadata."


def has_dataclass_fields(t: Any) -> TypeGuard[HasDataclassFields]:
    """
    Checks if the object is a dataclass type.
    """
    try:
        return hasattr(t, "__dataclass_fields__") and isinstance(t.__dataclass_fields__, dict)
    except TypeError:
        return False
