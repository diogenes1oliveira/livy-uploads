# mypy: disable-error-code=no-untyped-def
from pathlib import Path

import pytest

from livy_uploads.configs.configfiles import ConfigFileLoader


class TestConfigFileLoaderFindPaths:
    @pytest.fixture
    def loader(self, tmp_path: Path):
        # We use a custom basename to make testing easier
        (loader,) = ConfigFileLoader(basenames=("app.toml",), profiles=("prod",)).resolve(basedir=tmp_path)
        return loader

    def test_find_all_standard_files(self, tmp_path: Path, loader):
        """Pattern='*' should return all standard config files defined by profiles."""
        # Files don't strictly need to exist for '*' pattern as per ProfileFileLoader design,
        # but let's create them to be realistic.
        (tmp_path / "app.toml").touch()
        (tmp_path / "app-prod.toml").touch()

        found = list(loader.find_paths(pattern="*"))

        paths = {f.path.name for f in found}
        assert "app.toml" in paths
        assert "app-prod.toml" in paths

        # Verify keys are None
        assert all(f.key is None for f in found)

    def test_find_namespaced_files_checks_order(self, tmp_path: Path, loader):
        """Pattern='*.*' should yield files in interleaved order."""
        # Create standard files
        (tmp_path / "app.toml").touch()
        (tmp_path / "app-prod.toml").touch()

        # Create nested files
        (tmp_path / "app.database.toml").touch()
        (tmp_path / "app.kafka.toml").touch()
        (tmp_path / "app-prod.extra.toml").touch()
        (tmp_path / "app-prod.logging.toml").touch()

        found = list(loader.find_paths(pattern="*.*"))
        filenames = [f.path.name for f in found]

        # Expected indices
        idx_default = filenames.index("app.toml")
        idx_prod = filenames.index("app-prod.toml")

        # Standard file should come first
        assert idx_default < idx_prod

        # Nested files for default should come immediately after default
        idx_db = filenames.index("app.database.toml")
        idx_kafka = filenames.index("app.kafka.toml")

        # They should be after app.toml but before app-prod.toml
        assert idx_default < idx_db < idx_prod
        assert idx_default < idx_kafka < idx_prod

        # Nested files for prod should come after prod
        idx_extra = filenames.index("app-prod.extra.toml")
        idx_logging = filenames.index("app-prod.logging.toml")

        assert idx_prod < idx_extra
        assert idx_prod < idx_logging

    def test_find_namespaced_files(self, tmp_path: Path, loader):
        """Pattern='*.*' should find namespaced config files via globbing."""
        # These MUST exist to be found
        (tmp_path / "app.database.toml").touch()
        (tmp_path / "app.kafka.toml").touch()
        (tmp_path / "app-prod.extra.toml").touch()

        # Regular files should NOT be matched by *.* logic in this implementation
        (tmp_path / "app.toml").touch()

        found = list(loader.find_paths(pattern="*.*"))

        found_map = {f.path.name: f.key for f in found}

        # Check default profile files
        assert found_map["app.database.toml"] == "database"
        assert found_map["app.kafka.toml"] == "kafka"

        # Check prod profile files
        assert found_map["app-prod.extra.toml"] == "extra"

        # The base file IS included now with *.* due to interleaved loading
        assert "app.toml" in found_map
        assert found_map["app.toml"] is None

    def test_find_namespaced_files_ignores_invalid_structure(self, tmp_path: Path, loader):
        """Should ignore files that don't match the namespaced pattern."""
        # e.g. app.toml (no namespace) - although already tested above
        # e.g. unrelated.toml
        (tmp_path / "unrelated.toml").touch()
        (tmp_path / "app-prod.toml").touch()

        found = list(loader.find_paths(pattern="*.*"))
        paths = {f.path.name for f in found}

        assert "unrelated.toml" not in paths
        # app-prod.toml IS expected now
        assert "app-prod.toml" in paths

    def test_invalid_pattern(self, loader):
        with pytest.raises(ValueError, match="Invalid pattern"):
            list(loader.find_paths(pattern="*.json"))


class TestConfigFileLoaderSetup:
    @pytest.fixture
    def loader(self, tmp_path: Path):
        (loader,) = ConfigFileLoader(basenames=("app.toml",), profiles=("prod",)).resolve(basedir=tmp_path)
        return loader

    def test_setup_merges_simple_files(self, tmp_path: Path, loader):
        f1 = tmp_path / "app.toml"
        f1.write_text('key = "default"', encoding="utf-8")
        f2 = tmp_path / "app-prod.toml"
        f2.write_text('key = "prod"', encoding="utf-8")

        loaded = loader.setup()

        assert loader.raw_configs["key"] == "prod"
        assert set(loaded) == {f1, f2}

    def test_setup_merges_nested_files(self, tmp_path: Path, loader):
        f1 = tmp_path / "app.toml"
        f1.write_text('key = "default"', encoding="utf-8")
        f2 = tmp_path / "app.database.toml"
        f2.write_text('host = "localhost"', encoding="utf-8")
        f3 = tmp_path / "app-prod.database.toml"
        f3.write_text('host = "prod-db"', encoding="utf-8")

        loaded = loader.setup()

        assert loader.raw_configs["key"] == "default"
        assert loader.raw_configs["database"]["host"] == "prod-db"
        assert set(loaded) == {f1, f2, f3}

    def test_setup_nested_keys_creation(self, tmp_path: Path, loader):
        """Verify that deep_merge creates intermediate keys for nested configs."""
        f1 = tmp_path / "app.toml"
        f1.write_text("[section]\nval = 1", encoding="utf-8")
        # this should create section.deep.nested = {val = 2}
        f2 = tmp_path / "app.section.deep.nested.toml"
        f2.write_text("val = 2", encoding="utf-8")

        loaded = loader.setup()

        assert loader.raw_configs["section"]["val"] == 1
        assert loader.raw_configs["section"]["deep"]["nested"]["val"] == 2
        assert set(loaded) == {f1, f2}

    def test_setup_load_error_invalid_extension(self, tmp_path: Path):
        (loader,) = ConfigFileLoader(basenames=("app.txt",), profiles=()).resolve(basedir=tmp_path)
        (tmp_path / "app.txt").touch()

        with pytest.raises(ValueError, match="Unsupported file extension"):
            loader.setup()

    def test_setup_load_error_not_dict(self, tmp_path: Path):
        (loader,) = ConfigFileLoader(basenames=("app.json",), profiles=()).resolve(basedir=tmp_path)
        (tmp_path / "app.json").write_text("[]", encoding="utf-8")

        with pytest.raises(ValueError, match="not a top-level dict"):
            loader.setup()

    def test_setup_interpolates_env_vars(self, tmp_path: Path, loader, monkeypatch):
        """Verify that environment variables are interpolated in loaded configs."""
        monkeypatch.setenv("MY_VAR", "interpolated_value")
        (tmp_path / "app.toml").write_text('key = "${MY_VAR}"', encoding="utf-8")

        # Test nested interpolation too
        (tmp_path / "app.section.toml").write_text('val = "prefix_${MY_VAR}_suffix"', encoding="utf-8")

        loader.setup()

        assert loader.raw_configs["key"] == "interpolated_value"
        assert loader.raw_configs["section"]["val"] == "prefix_interpolated_value_suffix"


class TestIncludedConfigs:
    @pytest.fixture
    def loader(self, tmp_path: Path):
        (loader,) = ConfigFileLoader(basenames=("app.toml",), profiles=()).resolve(basedir=tmp_path)
        return loader

    def test_include_simple_file(self, tmp_path: Path, loader):
        """Verify .include loads and merges another file."""
        # Using json for main file to avoid toml list of dicts issue if tricky, but toml is fine.
        (tmp_path / "app.toml").write_text('key = "main"\n[section]\n".include" = "included.toml"', encoding="utf-8")
        (tmp_path / "included.toml").write_text('included_key = "included_value"', encoding="utf-8")

        loader.setup()

        assert loader.raw_configs["key"] == "main"
        assert loader.raw_configs["section"]["included_key"] == "included_value"
        assert ".include" not in loader.raw_configs["section"]

    def test_include_relative_path(self, tmp_path: Path, loader):
        """Verify .include resolves paths relative to basedir."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (tmp_path / "app.toml").write_text('".include" = "./subdir/included.toml"', encoding="utf-8")
        (subdir / "included.toml").write_text('key = "val"', encoding="utf-8")

        loader.setup()

        assert loader.raw_configs["key"] == "val"

    def test_include_list_of_files(self, tmp_path: Path, loader):
        """Verify .include supports a list of files, merging them in order."""
        (tmp_path / "app.toml").write_text('val = "local"\n".include" = ["inc1.toml", "inc2.toml"]', encoding="utf-8")
        (tmp_path / "inc1.toml").write_text('key1 = "v1"\ncommon = "from_1"', encoding="utf-8")
        (tmp_path / "inc2.toml").write_text('key2 = "v2"\ncommon = "from_2"', encoding="utf-8")

        loader.setup()

        assert loader.raw_configs["val"] == "local"
        assert loader.raw_configs["key1"] == "v1"
        assert loader.raw_configs["key2"] == "v2"
        # Later includes should override earlier ones
        assert loader.raw_configs["common"] == "from_2"

    def test_include_local_ref(self, tmp_path: Path, loader):
        """Verify .include supports local references starting with #."""
        (tmp_path / "app.toml").write_text("root = {val = 1}\n" 'nested = {".include" = "#/root"}\n', encoding="utf-8")

        loader.setup()

        assert loader.raw_configs["root"]["val"] == 1
        assert loader.raw_configs["nested"]["val"] == 1
