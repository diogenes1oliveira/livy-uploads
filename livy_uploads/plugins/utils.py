from typing import Any, Optional, Protocol

from typing_extensions import TypeGuard

from livy_uploads.plugins import constants
from livy_uploads.utils.datautils import is_module_name


def fix_attrs(attrs: str, spec: Optional[str] = None) -> str:
    """
    >>> fix_attrs("foo")
    'foo'
    >>> [fix_attrs("*"), fix_attrs("__all__")]
    ['__all__', '__all__']
    >>> fix_attrs("foo*")
    'foo*'
    >>> fix_attrs("")
    Traceback (most recent call last):
    ...
    ValueError: ...
    >>> fix_attrs("123invalid")
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


def fix_group(group: Optional[str]) -> str:
    """
    >>> fix_group("foo.bar")
    'foo.bar'
    >>> fix_group(".foo")
    'sparkrl.plugins.foo'
    >>> fix_group(".foo.*")
    'sparkrl.plugins.foo.*'
    >>> fix_group("foo.bar.*")
    'foo.bar.*'
    >>> [fix_group("*"), fix_group(""), fix_group(None), fix_group("__all__")]
    ['__all__', '__all__', '__all__', '__all__']
    >>> fix_group("123invalid")
    Traceback (most recent call last):
    ...
    ValueError: ...
    """
    group = group or ""
    if group.startswith("."):
        group = constants.GROUP_PREFIX + group.removeprefix(".")
    if not group or group == "*":
        return "__all__"
    if not is_module_name(group.removesuffix("*").rstrip(".")):
        raise ValueError(f"invalid entrypoint group name: {group!r}")
    return group


class SupportsClose(Protocol):
    def close(self) -> None:
        pass


def is_closeable(obj: Any) -> TypeGuard[SupportsClose]:
    return callable(getattr(obj, "close", None))
