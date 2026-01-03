import base64
import collections.abc
import functools
from collections.abc import Mapping
from typing import Any, Callable, Iterable, NamedTuple, Optional, TypeVar

from livy_uploads.utils.typeutils import is_list

T = TypeVar("T")


@functools.total_ordering
class DeltaItem(NamedTuple):
    keys: tuple[str, ...]
    current: Any = None
    override: Any = None

    @property
    def key(self) -> str:
        return "." + ".".join(self.keys)

    def __str__(self) -> str:
        current = repr(self.current) if self.current is not ... else "..."
        return f"{self.key}: {current} -> {self.override!r}"

    def __hash__(self) -> int:
        return hash(self.keys)

    def __eq__(self, other: Any) -> bool:
        if not is_list(other):
            return False

        other_keys = other[0]
        return self.keys == other_keys  # type: ignore

    def __lt__(self, other: Any) -> bool:
        if not is_list(other):
            return False

        other_keys = other[0]
        for a, b in zip(self.keys, other_keys):
            if a < b:
                return True
            elif a > b:
                return False

        return len(self.keys) < len(other_keys)


def delta_patch(
    target: Any,
    override: Any,
    *,
    _key_paths: Optional[tuple[str, ...]] = None,
) -> dict[str, DeltaItem]:
    """
    Computes a delta patch between two dictionaries.

    Args:
        target: The target dictionary.
        override: The override dictionary.

    Returns:
        A set with the delta items.

    >>> items = delta_patch(
    ...     target={
    ...         "a": {"b": 1},
    ...         "c": 2,
    ...     },
    ...     override={
    ...         "a": {"b": 2, "d": 3},
    ...         "c": 4,
    ...     },
    ... )
    >>> list(map(str, sorted(items.values())))
    ['.a.b: 1 -> 2', '.a.d: None -> 3', '.c: 2 -> 4']
    """
    key_paths: tuple[str, ...] = _key_paths or ()
    if target is None and override is None:
        return {}

    if not isinstance(override, collections.abc.Mapping):
        # got to a primitive value in the override

        if isinstance(target, collections.abc.Mapping):
            # type changed: was a map, now it isn't
            prev = None
            changed = True
        else:
            prev = target
            # value possibly changed
            changed = prev != override

        if changed:
            item = DeltaItem(keys=key_paths, current=prev, override=override)
            return {item.key: item}
        else:
            return {}
    else:
        target_fields: Mapping[str, Any]

        if not isinstance(target, collections.abc.Mapping):
            # type changed to a map
            target_fields = {}
        else:
            target_fields = target

        deltas = {}

        # changed fields
        for k, override_value in override.items():
            field_deltas = delta_patch(
                target=target_fields.get(k),
                override=override_value,
                _key_paths=(*key_paths, k),
            )
            deltas.update(field_deltas)

        # removed fields
        for k in target_fields:
            if k in override:
                continue
            item = DeltaItem(keys=(*key_paths, k), current=..., override=None)
            deltas[item.key] = item

        return deltas


def delta_rolling_list(target: Iterable[T], override: Iterable[T]) -> list[T]:
    """
    Computes a sliding delta patch between two lists.

    >>> delta_rolling_list([1, 2, 3], [1, 2, 3, 4, 5])
    [4, 5]
    >>> delta_rolling_list([1, 2, 3], [1, 2, 3])
    []
    >>> delta_rolling_list([1, 2, 3, 4], [3, 4, 5, 6])
    [5, 6]
    >>> delta_rolling_list([1, 2, 3, 4], [3, 5, 6])
    [3, 5, 6]

    Raises:
        TypeError: If the target and override items are not hashable.
    """
    targets = list(target)
    overrides = list(override)

    try:
        hash(tuple(targets))
        hash(tuple(overrides))
    except TypeError:
        return overrides

    # starting from the first element in the override, find the longest common subsequence
    # in the target.
    for i in range(len(targets)):
        suffix = targets[i:]
        if overrides[: len(suffix)] == suffix:
            return overrides[len(suffix) :]

    return overrides


def deep_merge(target: Mapping[str, Any], overrides: Mapping[str, Any]) -> dict[str, Any]:
    """
    Merges two dictionaries deeply, recursively combining nested mappings.

    Args:
        target: The base dictionary to merge into.
        overrides: The dictionary containing values to override or add to the target.

    Returns:
        A new dictionary with the merged result. The target dictionary is not modified.

    Merge Behavior:
        The function processes each key-value pair in the overrides dictionary:

        1. Nested Dictionaries: If both target and override values are mappings, they are
            recursively merged.

        2. Null Override: If the override value is None and the target value is a mapping,
            the key is removed from the result (treated as a deletion).

        3. Simple Override: In all other cases (primitive values, type changes, or new keys),
            the override value replaces the target value.

    Examples:
        Simple merge with primitive values:

        >>> deep_merge({"a": 1, "b": 2}, {"b": 3, "c": 4})
        {'a': 1, 'b': 3, 'c': 4}

        Nested dictionary merge:

        >>> deep_merge(
        ...     {"config": {"host": "localhost", "port": 8080}},
        ...     {"config": {"port": 9000, "debug": True}}
        ... )
        {'config': {'host': 'localhost', 'port': 9000, 'debug': True}}

        Deeply nested merge:

        >>> deep_merge(
        ...     {"a": {"b": {"c": 1, "d": 2}, "e": 3}},
        ...     {"a": {"b": {"c": 10}, "f": 4}}
        ... )
        {'a': {'b': {'c': 10, 'd': 2}, 'e': 3, 'f': 4}}

        Remove a key with None override:

        >>> deep_merge(
        ...     {"config": {"host": "localhost", "port": 8080}},
        ...     {"config": None}
        ... )
        {}

        >>> deep_merge(
        ...     {"a": 1, "config": {"host": "localhost"}},
        ...     {"config": None}
        ... )
        {'a': 1}

        Type change (dict to primitive):

        >>> deep_merge(
        ...     {"value": {"nested": "data"}},
        ...     {"value": "simple"}
        ... )
        {'value': 'simple'}

        Empty dictionaries:

        >>> deep_merge({}, {"a": 1})
        {'a': 1}

        >>> deep_merge({"a": 1}, {})
        {'a': 1}
    """
    result = dict(target)

    for key, override in overrides.items():
        if (
            key in target
            and isinstance(target[key], collections.abc.Mapping)
            and isinstance(override, collections.abc.Mapping)
        ):
            # recursively merge the target and override dictionaries
            result[key] = deep_merge(result[key], override)
        elif key in target and isinstance(target[key], collections.abc.Mapping) and override is None:
            # a null override to a target dict implies removing the key
            del result[key]
        else:
            # override the target value with the override value
            result[key] = override

    return result


def keep_only(target: Mapping[str, Any], keys: Iterable[str]) -> dict[str, Any]:
    """
    Keeps only the keys in the target dictionary.
    """
    return {k: v for k, v in target.items() if k in keys}


def jsonify(obj: Any) -> Any:
    if obj is None:
        return None
    elif hasattr(obj, "as_json"):
        return jsonify(obj.as_json())
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(obj).decode("ascii")
    elif isinstance(obj, collections.abc.Sequence):
        return [jsonify(item) for item in obj]
    elif isinstance(obj, collections.abc.Mapping):
        return {jsonify(k): jsonify(v) for k, v in obj.items()}
    else:
        raise RuntimeError(f"Cannot serialize {type(obj)} to JSON")


def get_nested_key(obj: Any, key: str) -> Any:
    parts = (key or ".").removeprefix(".").split(".")
    curr = obj

    for i, k in enumerate(parts):
        if not isinstance(curr, collections.abc.Mapping):
            path = "." + ".".join(parts[: i + 1])
            raise TypeError(f"expected a dictionary at .{path}, got {type(curr)} instead")
        try:
            curr = curr[k]
        except KeyError:
            path = "." + ".".join(parts[: i + 1])
            raise KeyError(f"key {k} not found at {path}") from None

    return curr


def is_module_name(s: str) -> bool:
    """
    Checks if a string is a valid module name.
    """
    return False if not s else all(part.isidentifier() for part in s.split("."))
