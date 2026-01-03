# mypy: disable-error-code=no-untyped-def
import collections.abc
from pathlib import Path
from typing import Any

import pytest

from livy_uploads.commands.base import SessionCommand
from livy_uploads.configs.plugins import DEFAULT_GROUPS, EntryPointsLoader, get_loaders, resolve_loaders
from livy_uploads.patches.base import Patch

# Test plugin implementations for collections.abc.Mapping and collections.abc.Sequence
PLUGIN_CODE = """
from collections.abc import Mapping, Sequence

class MappingPluginOne(Mapping):
    def __getitem__(self, key):
        pass
    
    def __iter__(self):
        pass
    
    def __len__(self):
        pass


class SequencePluginOne(Sequence):
    def __getitem__(self, index):
        pass
    
    def __len__(self):
        pass


class SequencePluginTwo(Sequence):
    def __getitem__(self, index):
        pass
    
    def __len__(self):
        pass
"""
PLUGIN_MODULE = "dev_test_livy_uploads_plugin_file"
PLUGIN_FILENAME = f"{PLUGIN_MODULE}.py"


@pytest.fixture(autouse=True)
def plugin_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Write the plugin module to a temporary path
    module_path = tmp_path / PLUGIN_FILENAME
    module_path.write_text(PLUGIN_CODE)

    monkeypatch.syspath_prepend(str(tmp_path))
    return module_path


@pytest.fixture(autouse=True)
def setup_plugin_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SPARKRL_PLUGINS", f"")


class TestPluginLoader:
    @pytest.mark.parametrize(
        ["spec", "base", "expected_classes"],
        [
            (f"file://{PLUGIN_FILENAME}#SequenceP*", object, ["SequencePluginOne", "SequencePluginTwo"]),
            (f"{PLUGIN_MODULE}:SequenceP*", object, ["SequencePluginOne", "SequencePluginTwo"]),
            (f"{PLUGIN_MODULE}:SequenceP*", collections.abc.Mapping, []),
            (f"{PLUGIN_MODULE}:NoSuchClass", object, []),
        ],
    )
    def test_load_specs(self, spec: str, base: type[Any], expected_classes: list[str], plugin_file: Path):
        loaders = resolve_loaders(spec + ",entrypoint:missing", basedir=plugin_file.parent)

        loader = loaders[0]
        assert len(loaders) == 1
        loaded = loader.load(base)

        loaded_classes = [cls.__name__ for cls in loaded.values()]
        assert loaded_classes == expected_classes


class TestEntryPointsLoader:
    def test_load_default_entrypoints(self):
        loaders = resolve_loaders("")

        loader = loaders[0]
        assert len(loaders) == 1
        assert isinstance(loader, EntryPointsLoader)
        assert loader.entry_points is not None
        for entry_point in loader.entry_points:
            assert entry_point.group.startswith("livy_uploads.plugins."), f"extra group: {entry_point.group!r}"

        loaded_commands = [l.__name__ for l in loader.load(SessionCommand).values()]
        assert loaded_commands == ["SessionInfoCommand"]

        loaded_patches = [l.__name__ for l in loader.load(Patch).values()]
        assert loaded_patches == ["SparkMagicReloadPatch"]


@pytest.mark.parametrize(
    "sources,expected_uris",
    [
        # Module:// scheme tests
        (
            (f"module://{PLUGIN_MODULE}",),
            [f"module://{PLUGIN_MODULE}/__all__"],
        ),
        (
            (f"module://{PLUGIN_MODULE}:MappingPluginOne",),
            [f"module://{PLUGIN_MODULE}/MappingPluginOne"],
        ),
        (
            (f"module://{PLUGIN_MODULE}:Sequence*",),
            [f"module://{PLUGIN_MODULE}/Sequence*"],
        ),
        (
            (f"module://{PLUGIN_MODULE}", f"module://{PLUGIN_MODULE}:MappingPluginOne"),
            [
                f"module://{PLUGIN_MODULE}/__all__",
                f"module://{PLUGIN_MODULE}/MappingPluginOne",
            ],
        ),
        (
            (
                f"module://{PLUGIN_MODULE}:Sequence*",
                f"module://{PLUGIN_MODULE}:MappingPluginOne",
            ),
            [
                f"module://{PLUGIN_MODULE}/Sequence*",
                f"module://{PLUGIN_MODULE}/MappingPluginOne",
            ],
        ),
        # Entrypoint:// scheme tests
        (
            ("entrypoint://livy_uploads.test",),
            ["entrypoint://livy_uploads.test?select=__all__"],
        ),
        (
            ("entrypoint://livy_uploads.test:MyPlugin",),
            ["entrypoint://livy_uploads.test?select=MyPlugin"],
        ),
        # File:// scheme tests
        (
            (f"file://{PLUGIN_FILENAME}",),
            [f"file://{PLUGIN_FILENAME}#__all__"],
        ),
        (
            (f"file://{PLUGIN_FILENAME}#MappingPluginOne",),
            [f"file://{PLUGIN_FILENAME}#MappingPluginOne"],
        ),
        (
            (f"file://{PLUGIN_FILENAME}#Sequence*",),
            [f"file://{PLUGIN_FILENAME}#Sequence*"],
        ),
        # Auto-detection: file paths with os.path.sep in first 3 chars
        (
            (f"./{PLUGIN_FILENAME}",),
            [f"file://{PLUGIN_FILENAME}#__all__"],
        ),
        (
            (f"./{PLUGIN_FILENAME}#SequencePluginOne",),
            [f"file://{PLUGIN_FILENAME}#SequencePluginOne"],
        ),
        # Auto-detection: module names (no path separator in first 3 chars)
        (
            (PLUGIN_MODULE,),
            [f"module://{PLUGIN_MODULE}/__all__"],
        ),
        (
            (f"{PLUGIN_MODULE}:MappingPluginOne",),
            [f"module://{PLUGIN_MODULE}/MappingPluginOne"],
        ),
        (
            (f"{PLUGIN_MODULE}:Sequence*",),
            [f"module://{PLUGIN_MODULE}/Sequence*"],
        ),
        # Auto-detection: entrypoint groups
        (
            ("entrypoint:livy_uploads.plugins.commands",),
            ["entrypoint://livy_uploads.plugins.commands?select=__all__"],
        ),
        (
            ("entrypoint:livy_uploads.plugins.commands:MyPlugin",),
            ["entrypoint://livy_uploads.plugins.commands?select=MyPlugin"],
        ),
        # Mixed sources
        (
            (PLUGIN_MODULE, f"file://{PLUGIN_FILENAME}#MappingPluginOne"),
            [
                f"module://{PLUGIN_MODULE}/__all__",
                f"file://{PLUGIN_FILENAME}#MappingPluginOne",
            ],
        ),
    ],
)
def test_get_loaders(sources, expected_uris):
    loaders = get_loaders(*sources)

    assert len(loaders) == len(expected_uris)
    actual_uris = [loader.uri for loader in loaders]
    assert actual_uris == expected_uris


def test_get_loaders_with_absolute_paths(plugin_file: Path):
    expected_uri = f"file://{plugin_file}#__all__"

    assert get_loaders(plugin_file)[0].uri == expected_uri
    assert get_loaders(f"file://{plugin_file}")[0].uri == expected_uri
    assert get_loaders(str(plugin_file))[0].uri == expected_uri


@pytest.mark.parametrize(
    ["specs", "expected_uris"],
    [
        [f"./{PLUGIN_FILENAME}", ["file://{plugin_file}#__all__"]],
        [PLUGIN_MODULE, [f"module://{PLUGIN_MODULE}/__all__"]],
        [f"file://./{PLUGIN_FILENAME}:WhateverClass", ["file://{plugin_file}#WhateverClass"]],
    ],
)
def test_resolve_loaders(specs: str, expected_uris: list[str], plugin_file: Path):
    specs = "entrypoint:missing," + specs  # skip the default entrypoint stuff
    expected_uris = [_try_format(s, plugin_file=plugin_file) for s in expected_uris]

    loaders = resolve_loaders(specs, basedir=plugin_file.parent)

    actual_uris = [loader.uri for loader in loaders]
    assert actual_uris == expected_uris


def _try_format(s: str, **kwargs: Any) -> str:
    try:
        return s.format(**kwargs)
    except (KeyError, TypeError):
        return s
