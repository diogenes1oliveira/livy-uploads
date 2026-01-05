from typing import Any, Optional, Protocol

from typing_extensions import TypeGuard

from livy_uploads.plugins import constants


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
    '.foo'
    >>> fix_group(".foo.*")
    '.foo.*'
    >>> fix_group("foo.bar.*")
    'foo.bar.*'
    >>> [fix_group("*"), fix_group(""), fix_group(None), fix_group("__all__")]
    ['*', '*', '*', '*']
    >>> fix_group("123invalid")
    Traceback (most recent call last):
    ...
    ValueError: ...
    """
    if not group or group == "__all__":
        return "*"

    if not group.replace("*", "_").replace(".", "_").isidentifier():
        raise ValueError(f"invalid entrypoint group name: {group!r}")

    return group


def resolve_group(group: str) -> str:
    if not group.startswith("."):
        return group
    else:
        return constants.GROUP_PREFIX + group.removeprefix(".")


def split_name_attr(spec: str) -> tuple[str, Optional[str]]:
    """
    >>> split_name_attr("name:attr")
    ('name', 'attr')

    >>> split_name_attr("name")
    ('name', None)

    >>> split_name_attr(":attr")
    Traceback (most recent call last):
    ...
    ValueError: ...

    >>> split_name_attr("name:")
    Traceback (most recent call last):
    ...
    ValueError: ...
    """
    name, sep, attr = spec.partition(":")
    if not name:
        raise ValueError(f"missing name in spec: {spec!r}")
    if sep and not attr:
        raise ValueError(f"missing attribute in spec: {spec!r}")
    return name, attr or None


class SupportsClose(Protocol):
    def close(self) -> None:
        pass


def is_closeable(obj: Any) -> TypeGuard[SupportsClose]:
    return callable(getattr(obj, "close", None))


def _split_spec_parts(spec: str) -> list[str]:
    """
    >>> _split_spec_parts("group/name:attr?query")
    ['group', '/', 'name', ':', 'attr', '?', 'query']
    >>> _split_spec_parts("simple")
    ['simple']
    >>> _split_spec_parts("group/name")
    ['group', '/', 'name']
    >>> _split_spec_parts(":attr")
    ['', ':', 'attr']
    >>> _split_spec_parts("group:")
    ['group', ':']
    >>> _split_spec_parts("a#bb:ccc")
    ['a', '#', 'bb', ':', 'ccc']
    >>> _split_spec_parts("//::??##")
    ['', '/', '', '/', '', ':', '', ':', '', '?', '', '?', '', '#', '', '#']
    >>> _split_spec_parts("")
    []
    """
    parts = list[str]()

    start = 0
    for i, c in enumerate(spec):
        if c in "/#:?":
            parts.append(spec[start:i])
            parts.append(c)
            start = i + 1

    if rest := spec[start:]:
        parts.append(rest)

    return parts


# def spec_to_pattern(spec: str) -> Pattern[str]:
#     """
#     >>> spec_to_pattern("foo/bar:baz?qux")
#     re.compile('^foo/bar:baz\\?qux$')
#     >>> spec_to_pattern("foo*/bar?:baz#qux")
#     re.compile('^foo.*/bar.:baz#qux$')
#     >>> spec_to_pattern("foo/bar")
#     re.compile('^foo/bar$')
#     >>> spec_to_pattern("foo*bar")
#     re.compile('^foo.*bar$')
#     >>> spec_to_pattern("")
#     re.compile('^$')
#     """
#     parts = _split_spec_parts(spec)
#     regex_parts = list[str]()

#     special_chars = "/#:?"
#     any_char = r"[a-zA-Z0-9_.,-]"

#     for part in parts:
#         if part in special_chars:
#             regex_parts.append("\\" + part)
#         else:
#             mapped = part.replace(".", r"\.").replace("?", any_char).replace("*", f"{any_char}*")
#             regex_parts.append(mapped)

#     regex = "^" + "".join(regex_parts) + "$"
#     return re.compile(regex)
