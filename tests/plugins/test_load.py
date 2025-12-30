import tempfile
from collections.abc import Mapping
from pathlib import Path
from textwrap import dedent
from types import ModuleType

import pytest

from livy_uploads.plugins.base import SetupPlugin
from livy_uploads.plugins.load import is_module_name, load_plugins

# mypy: disable-error-code="no-untyped-def,attr-defined"


def filter_entry_point_plugins(plugins):
    """Filter out entry point plugins from results for testing."""
    from livy_uploads.configs.sparkmagic_conf import SparkMagicConfSetup
    from livy_uploads.configs.user_ids import UserIdsSetup

    return [p for p in plugins if p not in (UserIdsSetup, SparkMagicConfSetup)]


class TestIsModuleName:
    """Tests for the is_module_name helper function."""

    @pytest.mark.parametrize(
        "name",
        [
            "module",
            "my_module",
            "module123",
            "my.module",
            "my.nested.module",
            "a.b.c.d.e",
            "_private",
            "__dunder__",
        ],
    )
    def test_valid_module_names(self, name):
        assert is_module_name(name) is True

    @pytest.mark.parametrize(
        "name",
        [
            "123invalid",
            "my-module",
            "my module",
            "my.123invalid",
            "my..module",
            ".module",
            "module.",
            "",
            "my/module",
            "my\\module",
        ],
    )
    def test_invalid_module_names(self, name):
        assert is_module_name(name) is False


class TestLoadPlugins:
    """Tests for the load_plugins function."""

    def test_load_from_module_object(self):
        """Test loading plugins from a ModuleType object."""
        # Create a mock module
        module = ModuleType("test_module")

        class TestPlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        class AnotherPlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        class NotAPlugin:
            pass

        module.TestPlugin = TestPlugin  # type: ignore
        module.AnotherPlugin = AnotherPlugin  # type: ignore
        module.NotAPlugin = NotAPlugin  # type: ignore
        module.__all__ = ["TestPlugin", "AnotherPlugin", "NotAPlugin"]  # type: ignore

        result = filter_entry_point_plugins(load_plugins(SetupPlugin, module))
        assert len(result) == 2
        assert TestPlugin in result
        assert AnotherPlugin in result
        assert NotAPlugin not in result

    def test_load_from_module_name(self):
        """Test loading plugins from a module name string."""
        # Use an actual module from the project
        result = load_plugins(SetupPlugin, "livy_uploads.plugins")
        # Should return empty list since there are no plugins defined
        assert isinstance(result, list)

    def test_load_from_file_path_string(self):
        """Test loading plugins from a file path string."""
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin_file = Path(tmpdir) / "test_plugin.py"
            plugin_file.write_text(
                dedent(
                    """
                    from pathlib import Path
                    from typing import Mapping

                    class MyPlugin:
                        def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                            return {}

                    class AnotherPlugin:
                        def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                            return {}

                    __all__ = ["MyPlugin", "AnotherPlugin"]
                    """
                )
            )

            result = filter_entry_point_plugins(load_plugins(SetupPlugin, str(plugin_file)))
            assert len(result) == 2
            assert all(cls.__name__ in ["MyPlugin", "AnotherPlugin"] for cls in result)

    def test_load_from_path_object(self):
        """Test loading plugins from a Path object."""
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin_file = Path(tmpdir) / "my_plugin.py"
            plugin_file.write_text(
                dedent(
                    """
                    from pathlib import Path
                    from typing import Mapping

                    class PluginA:
                        def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                            return {}

                    __all__ = ["PluginA"]
                    """
                )
            )

            result = filter_entry_point_plugins(load_plugins(SetupPlugin, plugin_file))
            assert len(result) == 1
            assert result[0].__name__ == "PluginA"

    def test_load_with_no_all_attribute(self):
        """Test loading when module has no __all__ attribute."""
        module = ModuleType("test_module")

        class TestPlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        module.TestPlugin = TestPlugin  # type: ignore
        # No __all__ attribute

        result = filter_entry_point_plugins(load_plugins(SetupPlugin, module))
        assert len(result) == 0

    def test_load_with_invalid_all_string(self):
        """Test that __all__ as a string raises ValueError."""
        module = ModuleType("test_module")
        module.__all__ = "invalid"  # type: ignore

        with pytest.raises(ValueError, match="__all__ must be a sequence"):
            load_plugins(SetupPlugin, module)

    def test_load_with_invalid_all_non_sequence(self):
        """Test that __all__ as a non-sequence raises ValueError."""
        module = ModuleType("test_module")
        module.__all__ = 123  # type: ignore

        with pytest.raises(ValueError, match="__all__ must be a sequence"):
            load_plugins(SetupPlugin, module)

    def test_load_with_missing_attributes(self):
        """Test loading when __all__ references non-existent attributes."""
        module = ModuleType("test_module")

        class TestPlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        module.TestPlugin = TestPlugin  # type: ignore
        module.__all__ = ["TestPlugin", "NonExistent", "AlsoMissing"]  # type: ignore

        # Should not raise, just skip missing ones
        result = filter_entry_point_plugins(load_plugins(SetupPlugin, module))
        assert len(result) == 1
        assert result[0] == TestPlugin

    def test_load_with_non_class_items(self):
        """Test loading when __all__ contains non-class items."""
        module = ModuleType("test_module")

        class TestPlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        module.TestPlugin = TestPlugin  # type: ignore
        module.some_function = lambda: None  # type: ignore
        module.some_constant = 42  # type: ignore
        module.__all__ = ["TestPlugin", "some_function", "some_constant"]  # type: ignore

        result = filter_entry_point_plugins(load_plugins(SetupPlugin, module))
        assert len(result) == 1
        assert result[0] == TestPlugin

    def test_invalid_source_type(self):
        """Test with invalid source string (not path or module name)."""
        with pytest.raises(ValueError, match="invalid source type"):
            load_plugins(SetupPlugin, "not a path and not-a-module-name!")

    def test_invalid_file_extension(self):
        """Test with non-.py file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            invalid_file = Path(tmpdir) / "test.txt"
            invalid_file.write_text("not python code")

            with pytest.raises(ValueError, match="source must be a .py file"):
                load_plugins(SetupPlugin, invalid_file)

    def test_invalid_module_filename(self):
        """Test with .py file that has an invalid module name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            invalid_file = Path(tmpdir) / "123-invalid.py"
            invalid_file.write_text("# python code")

            with pytest.raises(ValueError, match="source filename is not a valid Python module name"):
                load_plugins(SetupPlugin, invalid_file)

    def test_file_with_syntax_error(self):
        """Test loading a file with syntax errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_file = Path(tmpdir) / "bad_syntax.py"
            bad_file.write_text("this is not valid python syntax !!!")

            with pytest.raises(SyntaxError):
                load_plugins(SetupPlugin, bad_file)

    def test_load_filters_by_type(self):
        """Test that only classes matching the specified type are loaded."""
        module = ModuleType("test_module")

        class CorrectPlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        class WrongPlugin:
            def different_method(self):
                pass

        class BaseClass:
            pass

        module.CorrectPlugin = CorrectPlugin  # type: ignore
        module.WrongPlugin = WrongPlugin  # type: ignore
        module.BaseClass = BaseClass  # type: ignore
        module.__all__ = ["CorrectPlugin", "WrongPlugin", "BaseClass"]  # type: ignore

        result = filter_entry_point_plugins(load_plugins(SetupPlugin, module))
        assert len(result) == 1
        assert result[0] == CorrectPlugin

    def test_empty_all_list(self):
        """Test loading when __all__ is an empty list."""
        module = ModuleType("test_module")

        class TestPlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        module.TestPlugin = TestPlugin  # type: ignore
        module.__all__ = []  # type: ignore

        result = filter_entry_point_plugins(load_plugins(SetupPlugin, module))
        assert len(result) == 0

    def test_path_normalization(self):
        """Test that relative paths are converted to absolute."""
        with tempfile.TemporaryDirectory() as tmpdir:
            plugin_file = Path(tmpdir) / "plugin.py"
            plugin_file.write_text(
                dedent(
                    """
                    from pathlib import Path
                    from typing import Mapping

                    class Plugin:
                        def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                            return {}

                    __all__ = ["Plugin"]
                    """
                )
            )

            # Should work with absolute path
            result = filter_entry_point_plugins(load_plugins(SetupPlugin, plugin_file.absolute()))
            assert len(result) == 1

    def test_numeric_items_in_all(self):
        """Test that numeric items in __all__ are converted to strings."""
        module = ModuleType("test_module")

        class TestPlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        # Set attribute with numeric name (weird but possible)
        setattr(module, "123", TestPlugin)
        module.__all__ = [123]  # type: ignore

        # Should convert to string and try to get attribute
        result = filter_entry_point_plugins(load_plugins(SetupPlugin, module))
        assert len(result) == 1

    def test_subclass_filtering(self):
        """Test that subclasses are properly identified."""
        module = ModuleType("test_module")

        class BasePlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        class DerivedPlugin(BasePlugin):
            pass

        class UnrelatedClass:
            pass

        module.BasePlugin = BasePlugin  # type: ignore
        module.DerivedPlugin = DerivedPlugin  # type: ignore
        module.UnrelatedClass = UnrelatedClass  # type: ignore
        module.__all__ = ["BasePlugin", "DerivedPlugin", "UnrelatedClass"]  # type: ignore

        result = filter_entry_point_plugins(load_plugins(SetupPlugin, module))
        # Both BasePlugin and DerivedPlugin should match SetupPlugin
        assert len(result) == 2
        assert BasePlugin in result
        assert DerivedPlugin in result

    def test_load_from_entry_points(self):
        """Test that plugins are loaded from entry points."""
        from livy_uploads.configs.user_ids import UserIdsSetup

        # Load without any sources - should get entry points
        result = load_plugins(SetupPlugin)

        # Should include UserIdsSetup from entry points
        assert UserIdsSetup in result
        assert len(result) >= 1

    def test_load_entry_points_then_sources(self):
        """Test that entry points are loaded first, then sources."""
        from livy_uploads.configs.user_ids import UserIdsSetup

        # Create a module with additional plugins
        module = ModuleType("test_module")

        class CustomPlugin:
            def setup(self, basedir: Path, env_filename: str, env: Mapping[str, str]) -> Mapping[str, str]:
                return {}

        module.CustomPlugin = CustomPlugin  # type: ignore
        module.__all__ = ["CustomPlugin"]  # type: ignore

        # Load from entry points + module
        result = load_plugins(SetupPlugin, module)

        # Should include both entry point plugin and module plugin
        assert UserIdsSetup in result
        assert CustomPlugin in result
