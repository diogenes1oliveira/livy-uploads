# mypy: disable-error-code=no-untyped-def
import collections.abc
from pathlib import Path

import pytest

from livy_uploads.plugins.base import Matcher
from livy_uploads.plugins.combine import CombinedLoader

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


# Some actual object instances for testing find_objects
test_string = "hello world"
test_number = 42
test_list = [1, 2, 3]
test_dict = {"key": "value"}
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
    monkeypatch.setenv("SPARKRL_PLUGINS", "")


class TestCombineLoader:
    @pytest.fixture
    def combined_loader(self, plugin_file: Path):
        (loader,) = CombinedLoader.parse(f"module://{PLUGIN_MODULE},file://{plugin_file}").resolve()
        return loader

    def test_find_paths(self):
        test_file = Path(__file__)
        (loader,) = CombinedLoader.parse(f"file://{test_file}").resolve()

        all_py_paths = list(loader.find_paths(pattern="*.py"))
        assert len(all_py_paths) > 0
        assert all(hasattr(p, "path") for p in all_py_paths)
        assert all(hasattr(p, "uri") for p in all_py_paths)
        assert all(p.path.suffix == ".py" for p in all_py_paths)

        assert any(p.path.name == test_file.name for p in all_py_paths)

        test_py_paths = list(loader.find_paths(pattern="test_*.py"))
        assert len(test_py_paths) > 0
        assert all(p.path.name.startswith("test_") for p in test_py_paths)
        assert all(p.path.suffix == ".py" for p in test_py_paths)

    def test_find_paths_nested(self):
        top_level_pkg = __name__.partition(".")[0]
        (loader,) = CombinedLoader.parse(f"module://{top_level_pkg}").resolve()

        py_paths = list(loader.find_paths(pattern="*.py"))
        assert len(py_paths) > 0
        assert all(hasattr(p, "path") for p in py_paths)
        assert all(p.path.suffix == ".py" for p in py_paths)

        nested_paths = list(loader.find_paths(pattern="plugins/*.py"))
        assert len(nested_paths) > 0
        assert all("plugins" in str(p.path) for p in nested_paths)
        assert all(p.path.suffix == ".py" for p in nested_paths)

    def test_find_types(self, combined_loader: CombinedLoader):
        types = list(combined_loader.find_types(collections.abc.Sequence, pattern="*"))

        type_names = {t.type.__name__ for t in types}
        assert type_names == {"SequencePluginOne", "SequencePluginTwo"}

    def test_find_objects(self):
        (loader,) = CombinedLoader.parse(f"module://{PLUGIN_MODULE}").resolve()

        all_objects = list(loader.find_objects(object, pattern="*", match=Matcher.any()))
        assert len(all_objects) > 0
        assert all(hasattr(o, "object") for o in all_objects)
        assert all(hasattr(o, "uri") for o in all_objects)

        string_objects = list(
            loader.find_objects(
                str,
                pattern="test_string",
                match=Matcher.instance(str),
            )
        )
        assert len(string_objects) == 1
        assert string_objects[0].object == "hello world"
        assert string_objects[0].pattern == "test_string"

        string_wildcard = list(
            loader.find_objects(
                str,
                pattern="test_*",
                match=Matcher.instance(str),
            )
        )
        assert len(string_wildcard) >= 1
        assert any(o.object == "hello world" for o in string_wildcard)

        all_matching_test = list(
            loader.find_objects(
                object,
                pattern="test_*",
                match=Matcher.any(),
            )
        )
        assert len(all_matching_test) >= 1
        test_names = [str(o.object) for o in all_matching_test]
        assert any("hello" in name for name in test_names)
