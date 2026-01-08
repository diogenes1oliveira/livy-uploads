import base64
import collections.abc
import functools
from collections.abc import Iterable, Mapping
from typing import Any, Iterable, NamedTuple, Optional, TypeVar, Union

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


def deep_merge(
    target: Mapping[str, Any],
    overrides: Mapping[str, Any],
    *,
    key: Optional[Iterable[str]] = None,
) -> dict[str, Any]:
    """
    Merges two dictionaries deeply, recursively combining nested mappings.

    Args:
        target: The base dictionary to merge into.
        overrides: The dictionary containing values to override or add to the target.
        key: A sequence of keys defining a nested path in ``target`` where ``overrides``
            should be merged. Intermediate keys will be created as dictionaries if they
            don't exist or are not mappings.

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

        Merging into a specific key:

        >>> deep_merge(
        ...     {"a": 1},
        ...     {"b": 2},
        ...     key=["nested", "path"]
        ... )
        {'a': 1, 'nested': {'path': {'b': 2}}}

        Overwriting non-dict intermediates:

        >>> deep_merge(
        ...     {"a": [1, 2]},
        ...     {"b": 3},
        ...     key=["a", "c"]
        ... )
        {'a': {'c': {'b': 3}}}
    """
    if key is not None:
        key_list = list(key)
        if key_list:
            # We are merging deep
            root = dict(target)
            curr = root
            # Traverse all but the last part of the key
            for k in key_list[:-1]:
                if k not in curr or not isinstance(curr[k], collections.abc.Mapping):
                    curr[k] = {}
                curr = curr[k]

            # Now handle the last key part
            last_k = key_list[-1]
            if last_k not in curr or not isinstance(curr[last_k], collections.abc.Mapping):
                curr[last_k] = {}

            # Perform the merge at the leaf
            curr[last_k] = deep_merge(curr[last_k], overrides)
            return root

    result = dict(target)

    for key_name, override in overrides.items():
        if (
            key_name in target
            and isinstance(target[key_name], collections.abc.Mapping)
            and isinstance(override, collections.abc.Mapping)
        ):
            # recursively merge the target and override dictionaries
            result[key_name] = deep_merge(result[key_name], override)
        elif key_name in target and isinstance(target[key_name], collections.abc.Mapping) and override is None:
            # a null override to a target dict implies removing the key
            del result[key_name]
        else:
            # override the target value with the override value
            result[key_name] = override

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


def get_nested_key(obj: Any, key: Union[str, Iterable[str]]) -> Any:
    if isinstance(key, str):
        parts = (key or ".").removeprefix(".").split(".")
    else:
        parts = list(key)
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


    >>> is_module_name("foo.bar")
    True
    >>> is_module_name("123invalid")
    False
    >>> is_module_name("foo..invalid")
    False
    >>> is_module_name("")
    False
    >>> is_module_name(".foo")
    False
    """
    return False if not s else all(part.isidentifier() for part in s.split("."))


def resolve_local_ref(root: Any, ref: str) -> Any:
    """
    Resolves a JSON pointer-like reference (starting with #) within a root structure.
    Example: #/key1/key2/0
    """
    if not ref.startswith("#"):
        raise ValueError(f"Reference must start with '#': {ref!r}")

    # Remove the leading #
    path = ref[1:]
    if not path:
        return root

    # Split by / but handle empty parts if needed (though #/ usually implies root)
    # Filter out empty strings from splitting #/a/b -> ['', 'a', 'b']
    parts = [p for p in path.split("/") if p]

    curr = root
    for i, part in enumerate(parts):
        # Try as list index first
        if isinstance(curr, collections.abc.Sequence) and not isinstance(curr, (str, bytes)):
            try:
                idx = int(part)
                if idx < 0:
                    # JSON pointers don't usually support negative indices, but python does.
                    # Let's support standard nonnegative for now, or just try conversion.
                    pass
                curr = curr[idx]
                continue
            except (ValueError, IndexError):
                # If it's a list, we expect an integer index. If conversion fails or out of bounds:
                raise IndexError(f"List index out of range or invalid: {part} at path {'/'.join(parts[:i])}") from None

        # Try as dict key
        if isinstance(curr, collections.abc.Mapping):
            if part in curr:
                curr = curr[part]
                continue
            else:
                raise KeyError(f"Key {part!r} not found at path {'/'.join(parts[:i])}")

        # If we are here, we couldn't resolve 'part' on 'curr'
        raise TypeError(f"Cannot resolve {part!r} on {type(curr)} at path {'/'.join(parts[:i])}")

    return curr


def resolve_local_includes(config: Any, root: Any = None) -> Any:
    """
    Recursively scans for .include keys that contain local references (starting with #).
    Resolves them against the 'root' (defaulting to 'config' if not provided)
    and merges them into the current dictionary.

    Existing file includes are left in .include.
    """
    if root is None:
        root = config

    if isinstance(config, list):
        return [resolve_local_includes(x, root=root) for x in config]

    if not isinstance(config, dict):
        return config

    # Process children first to ensure deep resolution?
    # Or process self first?
    # Usually we want to resolve includes at this level, then recurse,
    # OR recurse then resolve.
    # Given that an included part might itself have includes, we probably want to RESOLVE first, then Recurse.
    # BUT wait, if we resolve a reference to another part of the tree, that part should probably be fully resolved?
    # This can get circular. Let's stick to:
    # 1. Recurse into children (to resolve their includes)
    # 2. Then resolve current level includes.
    # OR:
    # 1. Resolve current level includes.
    # 2. Recurse.

    # If I resolve #/definitions/base which itself has an include, I want that to be handled.
    # User requirement is "recurse in the loaded file keys and look for this include"

    # Let's do: Recurse first (modify children in place or replacement)
    for k, v in list(config.items()):
        if k == ".include":
            continue
        config[k] = resolve_local_includes(v, root=root)

    if ".include" in config:
        include_val = config[".include"]
        paths: list[str] = []
        if isinstance(include_val, str):
            paths = [include_val]
        elif isinstance(include_val, list):
            paths = include_val

        local_refs = []
        file_refs = []

        for p in paths:
            if isinstance(p, str) and p.startswith("#"):
                local_refs.append(p)
            else:
                file_refs.append(p)

        # If we have local refs, resolve and merge them
        if local_refs:
            merged_included: dict[str, Any] = {}
            for ref in local_refs:
                try:
                    included_content = resolve_local_ref(root, ref)
                    # We should probably also resolve includes inside the included content?
                    # The resolved content comes from 'root' which is already being processed or is the full doc.
                    # If we blindly reference it, we get it as is.
                    # Deep merge it.
                    if isinstance(included_content, collections.abc.Mapping):
                        merged_included = deep_merge(merged_included, included_content)
                    else:
                        # What if it's not a dict? e.g. including a list?
                        # Usually .include mixes into the current dict.
                        # If the included thing is not a dict, deep_merge might fail or behavior is undefined for "mixing in".
                        # For now, assume it's a dict as per config patterns.
                        pass
                except (KeyError, IndexError, TypeError, ValueError):
                    # For now we might want to fail hard or log?
                    # Given it's config loading, failing hard usually better.
                    raise

            # Update config with merged content
            # We want headers/values from included to be defaults, overridden by current config.
            # But deep_merge(target, override) applies overrides ON TOP of target.
            # So: result = deep_merge(merged_included, config_without_include)

            # Remove the local refs from .include
            if file_refs:
                config[".include"] = file_refs if len(file_refs) > 1 else file_refs[0]
            else:
                config.pop(".include")

            # Merge the current config ON TOP of the included stuff
            # Note: we already recursed into config's children.
            return deep_merge(merged_included, config)

    return config
