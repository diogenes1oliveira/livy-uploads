import collections.abc
import importlib
import importlib.metadata
import importlib.util
import inspect
import os
from pathlib import Path
from types import ModuleType
from typing import Type, TypeVar, Union

T = TypeVar("T")


def load_plugins(t: Type[T], *sources: Union[str, Path, ModuleType]) -> list[Type[T]]:
    """
    Loads plugins from entry points and optional sources.

    First loads plugins from project.entry-points."livy_uploads.plugins",
    then loads from the provided sources (module names, paths to .py files).

    The module `__all__` is used to determine the plugins to load from sources.

    Args:
        t: The type of the plugins to load.
        sources: Optional source module names or paths to .py files.

    Returns:
        A list of plugins.
    """
    plugin_classes = []

    # Load from entry points first
    try:
        entry_points_dict = importlib.metadata.entry_points()
        # In Python 3.9, entry_points() returns a dict
        # In Python 3.10+, it returns an EntryPoints object with select() method
        if isinstance(entry_points_dict, dict):
            entry_points = entry_points_dict.get("livy_uploads.plugins", [])
        else:
            # Python 3.10+ with group parameter
            entry_points = entry_points_dict.select(group="livy_uploads.plugins")  # type: ignore[unreachable]

        for entry_point in entry_points:
            try:
                plugin_class = entry_point.load()
                if inspect.isclass(plugin_class) and issubclass(plugin_class, t):
                    plugin_classes.append(plugin_class)
            except Exception:
                continue
    except Exception:
        pass

    # Then load from provided sources
    for source in sources:
        plugin_classes.extend(_load_from_source(source, t))

    return plugin_classes


def _load_from_source(source: Union[str, Path, ModuleType], t: Type[T]) -> list[Type[T]]:
    """
    Loads plugins from a single source.

    Args:
        source: the source module name, module object or path to a .py file.
        t: The type of the plugins to load.

    Returns:
        A list of plugins.
    """
    module: ModuleType

    if isinstance(source, str):
        if os.path.sep in source:
            source = Path(source)
        elif is_module_name(source):
            module = importlib.import_module(source)
        else:
            raise ValueError(f"invalid source type: {source!r}")

    if isinstance(source, Path):
        source = source.absolute()
        if source.suffix != ".py":
            raise ValueError(f"source must be a .py file: {source!r}")
        module_name = source.stem
        if not is_module_name(module_name):
            raise ValueError(f"source filename is not a valid Python module name: {source!r}")

        spec = importlib.util.spec_from_file_location(module_name, source)
        if spec is None or spec.loader is None:
            raise ValueError(f"could not load module from {source!r}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    if isinstance(source, ModuleType):
        module = source

    candidates = getattr(module, "__all__", [])
    if isinstance(candidates, str) or not isinstance(candidates, collections.abc.Sequence):
        raise ValueError(f"__all__ must be a sequence: {candidates!r}")

    plugin_classes = []

    for name in candidates:
        name = str(name)
        try:
            value = getattr(module, name)
            if inspect.isclass(value) and issubclass(value, t):
                plugin_classes.append(value)
        except (TypeError, AttributeError):
            continue

    return plugin_classes


def is_module_name(s: str) -> bool:
    """
    Checks if a string is a valid module name.
    """
    return all(part.isidentifier() for part in s.split("."))
