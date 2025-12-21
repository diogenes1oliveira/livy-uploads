import collections.abc
import functools
from typing import Any, Callable, Mapping, NamedTuple, Optional

from livy_uploads.utils.typeutils import is_list


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
    list_diff: Optional[Callable[[Any, Any], Optional[list[Any]]]] = None,
    _key_paths: Optional[tuple[str, ...]] = None,
) -> dict[str, DeltaItem]:
    """
    Computes a delta patch between two dictionaries.

    Args:
        target: The target dictionary.
        override: The override dictionary.
        list_diff: A function that computes the difference between two lists. Should raise a TypeError if the lists
            can't be compared.

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

    >>> items = delta_patch(
    ...     target={"indexes": [1, 2, 3]},
    ...     override={"indexes": [2, 3, 4, 5]},
    ...     list_diff=delta_rolling_list,
    ... )
    >>> list(map(str, sorted(items.values())))
    ['.indexes: None -> [4, 5]']
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
            if (is_list(prev) or is_list(override)) and list_diff:
                try:
                    diff_items = list_diff(prev, override)
                except TypeError:
                    # uncompatible types, so just compare the lists as if they were primitive values
                    changed = prev != override
                else:
                    if diff_items:
                        changed = True
                        prev = None
                        override = diff_items
                    else:
                        changed = False
            else:
                # primitive value possibly changed
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
                list_diff=list_diff,
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


def delta_rolling_list(target: Any, override: Any) -> Optional[list[Any]]:
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
    """
    if override is None:
        return None

    if not is_list(override):
        raise TypeError("override must be a list")

    overrides: list[Any] = list(override)

    if target is None:
        return overrides

    if not is_list(target):
        raise TypeError("target must be a list")

    targets: list[Any] = list(target)

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
