# mypy: disable-error-code=no-untyped-def
from pathlib import Path

import pytest

from livy_uploads.plugins import constants
from livy_uploads.plugins.profiles import ProfileFileLoader, _fix_profiles, get_current_profiles, try_relativize


@pytest.fixture
def test_files(tmp_path: Path):
    project_dir = tmp_path / "project"
    project_dir.mkdir()

    (project_dir / ".env").write_text("DEFAULT_CONFIG=1")
    (project_dir / ".prod.env").write_text("PROD_CONFIG=1")
    (project_dir / ".override.env").write_text("OVERRIDE_CONFIG=1")
    (project_dir / "config.txt").write_text("CONFIG_TXT=1")

    subdir = project_dir / "subdir"
    subdir.mkdir()
    (subdir / ".env").write_text("SUBDIR_CONFIG=1")

    return project_dir


class TestProfileFileLoaderInit:
    def test_deduplicates_profiles(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=("prod", "dev", "prod", "override"))

        assert loader.profiles == ("prod", "dev", "override")

    def test_preserves_order_when_deduplicating(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=("a", "b", "a", "c", "b", "d"))

        assert loader.profiles == ("a", "b", "c", "d")

    def test_none_profiles_not_deduplicated(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=None)

        assert loader.profiles is None

    def test_empty_profiles_preserved(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=())

        assert loader.profiles == ()


class TestProfileFileLoaderParse:
    def test_parse_single_basename_with_profiles(self):
        loader = ProfileFileLoader.parse(".env?profiles=prod+override")

        assert loader.basenames == (".env",)
        assert loader.profiles == ("prod", "override")

    def test_parse_multiple_basenames_with_profiles(self):
        loader = ProfileFileLoader.parse(".env,env?profiles=prod+override")

        assert loader.basenames == (".env", "env")
        assert loader.profiles == ("prod", "override")

    def test_parse_with_empty_profiles(self):
        loader = ProfileFileLoader.parse(".env?profiles=")

        assert loader.basenames == (".env",)
        assert loader.profiles == ()

    def test_parse_without_profiles(self):
        loader = ProfileFileLoader.parse(".env")

        assert loader.basenames == (".env",)
        assert loader.profiles is None

    def test_parse_multiple_basenames_no_profiles(self):
        loader = ProfileFileLoader.parse(".env,config.txt")

        assert loader.basenames == (".env", "config.txt")
        assert loader.profiles is None


class TestProfileFileLoaderUri:
    def test_uri_with_profiles(self):
        loader = ProfileFileLoader(basenames=(".env", "env"), profiles=("prod", "override"))

        assert loader.uri == "profile://.env,env?profiles=prod+override"

    def test_uri_with_empty_profiles(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=())

        assert loader.uri == "profile://.env?profiles="

    def test_uri_without_profiles(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=None)

        assert loader.uri == "profile://.env"

    def test_uri_multiple_basenames(self):
        loader = ProfileFileLoader(basenames=(".env", "config.txt", ".prod.env"), profiles=("prod",))

        assert loader.uri == "profile://.env,config.txt,.prod.env?profiles=prod"


class TestProfileFileLoaderNamedUri:
    def test_named_uri_with_custom_name(self):
        loader = ProfileFileLoader(basenames=(".env", ".prod.env"), profiles=("prod",))

        result = loader.named_uri(".hm.env")

        assert result == "profile://.hm.env?profiles=prod"

    def test_named_uri_with_none_name_uses_basenames(self):
        loader = ProfileFileLoader(basenames=(".env", ".prod.env"), profiles=("prod",))

        result = loader.named_uri(None)

        assert result == "profile://.env,.prod.env?profiles=prod"

    def test_named_uri_with_custom_profiles(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=("prod",))

        result = loader.named_uri(".test.env", profiles=("dev", "staging"))

        assert result == "profile://.test.env?profiles=dev+staging"

    def test_named_uri_with_empty_profiles(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=("prod",))

        result = loader.named_uri(".test.env", profiles=())

        assert result == "profile://.test.env?profiles="

    def test_named_uri_with_none_profiles(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=("prod",))

        result = loader.named_uri(".test.env", profiles=None)

        assert result == "profile://.test.env"


class TestProfileFileLoaderResolve:
    def test_resolve_adds_default_profile(self, test_files: Path):
        loader = ProfileFileLoader(basenames=(".env",), profiles=("prod",))

        (resolved,) = loader.resolve(basedir=test_files)

        assert resolved.profiles == ("default", "prod")

    def test_resolve_with_none_profiles_gets_current_profiles(self, test_files: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv(constants.PROFILES_ENV, raising=False)
        loader = ProfileFileLoader(basenames=(".env",), profiles=None)

        (resolved,) = loader.resolve(basedir=test_files)

        assert resolved.profiles == ("default", "override")

    def test_resolve_with_empty_profiles_gets_only_default(self, test_files: Path):
        loader = ProfileFileLoader(basenames=(".env",), profiles=())

        (resolved,) = loader.resolve(basedir=test_files)

        assert resolved.profiles == ("default",)

    def test_resolve_relativizes_absolute_path_in_project(self, test_files: Path):
        absolute_path = test_files / ".env"
        loader = ProfileFileLoader(basenames=(str(absolute_path),), profiles=("prod",))

        (resolved,) = loader.resolve(basedir=test_files)

        assert resolved.basenames == (".env",)

    def test_resolve_relativizes_absolute_path_in_subdir(self, test_files: Path):
        absolute_path = test_files / "subdir" / ".env"
        loader = ProfileFileLoader(basenames=(str(absolute_path),), profiles=("prod",))

        (resolved,) = loader.resolve(basedir=test_files)

        assert resolved.basenames == ("subdir/.env",)

    def test_resolve_keeps_relative_path(self, test_files: Path):
        loader = ProfileFileLoader(basenames=("config/.env",), profiles=("prod",))

        (resolved,) = loader.resolve(basedir=test_files)

        assert resolved.basenames == ("config/.env",)

    def test_resolve_sets_basedir(self, test_files: Path):
        loader = ProfileFileLoader(basenames=(".env",), profiles=("prod",))

        (resolved,) = loader.resolve(basedir=test_files)

        assert resolved.basedir == test_files

    def test_resolve_uses_cwd_when_basedir_not_provided(self, test_files: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.chdir(test_files)
        loader = ProfileFileLoader(basenames=(".env",), profiles=("prod",))

        (resolved,) = loader.resolve()

        assert resolved.basedir == test_files


class TestProfileFileLoaderFindPaths:
    @pytest.fixture
    def resolved_loader(self, test_files: Path):
        loader = ProfileFileLoader(basenames=(".env", "config.txt"), profiles=("prod", "override"))
        (resolved,) = loader.resolve(basedir=test_files)
        return resolved

    def test_find_paths_with_wildcard_pattern(self, resolved_loader: ProfileFileLoader):
        paths = list(resolved_loader.find_paths(pattern="*.env"))

        assert len(paths) == 3
        assert all(p.path.name.endswith(".env") for p in paths)
        assert all(p.pattern == "*.env" for p in paths)
        assert resolved_loader.profiles == ("default", "prod", "override")

    def test_find_paths_with_specific_pattern(self, resolved_loader: ProfileFileLoader):
        paths = list(resolved_loader.find_paths(pattern=".env"))

        assert len(paths) == 3
        assert all(p.path.name == ".env" for p in paths)

    def test_find_paths_txt_pattern(self, resolved_loader: ProfileFileLoader):
        paths = list(resolved_loader.find_paths(pattern="*.txt"))

        assert len(paths) == 3
        assert all(p.path.suffix == ".txt" for p in paths)

    def test_find_paths_generates_all_combinations(self, test_files: Path):
        loader = ProfileFileLoader(basenames=(".env", "nonexistent.env"), profiles=("prod",))
        (resolved,) = loader.resolve(basedir=test_files)

        paths = list(resolved.find_paths(pattern="*.env"))

        assert len(paths) == 4
        path_names = [p.path.name for p in paths]
        assert path_names.count(".env") == 2
        assert path_names.count("nonexistent.env") == 2

    def test_find_paths_sets_uri_for_each_profile(self, resolved_loader: ProfileFileLoader):
        paths = list(resolved_loader.find_paths(pattern="*.env"))

        uris = [p.uri for p in paths]
        assert "profile://.env?profiles=prod" in uris
        assert "profile://.env?profiles=override" in uris

    def test_find_paths_sets_loader_reference(self, resolved_loader: ProfileFileLoader):
        paths = list(resolved_loader.find_paths(pattern="*.env"))

        assert all(p.loader == resolved_loader for p in paths)

    def test_find_paths_returns_absolute_paths(self, resolved_loader: ProfileFileLoader):
        paths = list(resolved_loader.find_paths(pattern="*.env"))

        assert all(p.path.is_absolute() for p in paths)

    def test_find_paths_with_empty_profiles_adds_default(self, test_files: Path):
        loader = ProfileFileLoader(basenames=(".env",), profiles=())
        (resolved,) = loader.resolve(basedir=test_files)

        paths = list(resolved.find_paths(pattern="*.env"))

        assert len(paths) == 1
        assert resolved.profiles == ("default",)

    def test_find_paths_asserts_when_not_resolved(self):
        loader = ProfileFileLoader(basenames=(".env",), profiles=("prod",))

        with pytest.raises(AssertionError, match="not resolved yet"):
            list(loader.find_paths(pattern="*.env"))


class TestGetCurrentProfiles:
    def test_get_current_profiles_from_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv(constants.PROFILES_ENV, "prod,staging,test")

        result = get_current_profiles()

        assert result == ("default", "prod", "staging", "test")

    def test_get_current_profiles_with_plus_separator(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv(constants.PROFILES_ENV, "prod+staging+test")

        result = get_current_profiles()

        assert result == ("default", "prod", "staging", "test")

    def test_get_current_profiles_with_semicolon_separator(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv(constants.PROFILES_ENV, "prod;staging;test")

        result = get_current_profiles()

        assert result == ("default", "prod", "staging", "test")

    def test_get_current_profiles_with_pipe_separator(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv(constants.PROFILES_ENV, "prod|staging|test")

        result = get_current_profiles()

        assert result == ("default", "prod", "staging", "test")

    def test_get_current_profiles_strips_whitespace(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv(constants.PROFILES_ENV, "  prod , staging , test  ")

        result = get_current_profiles()

        assert result == ("default", "prod", "staging", "test")

    def test_get_current_profiles_default_when_env_not_set(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv(constants.PROFILES_ENV, raising=False)

        result = get_current_profiles()

        assert result == ("default", "override")

    def test_get_current_profiles_removes_default_duplicates(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv(constants.PROFILES_ENV, "default,prod,default,staging")

        result = get_current_profiles()

        assert result == ("default", "prod", "staging")


class TestFixProfiles:
    def test_fix_profiles_adds_default_first(self):
        result = _fix_profiles(("prod", "staging"))

        assert result == ("default", "prod", "staging")

    def test_fix_profiles_removes_duplicate_default(self):
        result = _fix_profiles(("default", "prod", "default", "staging"))

        assert result == ("default", "prod", "staging")

    def test_fix_profiles_with_empty_tuple(self):
        result = _fix_profiles(())

        assert result == ("default",)

    def test_fix_profiles_deduplicates_non_default(self):
        result = _fix_profiles(("prod", "staging", "prod", "test"))

        assert result == ("default", "prod", "staging", "test")

    def test_fix_profiles_preserves_order(self):
        result = _fix_profiles(("c", "a", "b"))

        assert result == ("default", "c", "a", "b")


class TestTryRelativize:
    def test_try_relativize_path_in_basedir(self, tmp_path: Path):
        basedir = tmp_path / "project"
        basedir.mkdir()
        filepath = basedir / "config" / ".env"

        result = try_relativize(str(filepath), basedir)

        assert result == "config/.env"

    def test_try_relativize_path_in_basedir_absolute(self, tmp_path: Path):
        basedir = tmp_path / "project"
        basedir.mkdir()
        subdir = basedir / "config"
        subdir.mkdir()
        filepath = subdir / ".env"
        filepath.write_text("test")

        result = try_relativize(str(filepath.absolute()), basedir)

        assert result == "config/.env"

    def test_try_relativize_relative_path(self, tmp_path: Path):
        basedir = tmp_path / "project"
        basedir.mkdir()

        result = try_relativize("config/.env", basedir)

        assert result == "config/.env"

    def test_try_relativize_path_outside_basedir_returns_absolute(self, tmp_path: Path):
        basedir = tmp_path / "project"
        basedir.mkdir()
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        filepath = other_dir / ".env"
        filepath.write_text("test")

        result = try_relativize(str(filepath.absolute()), basedir)

        assert result == str(filepath.absolute())

    def test_try_relativize_path_in_home_uses_tilde(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        fake_home = tmp_path / "home" / "user"
        fake_home.mkdir(parents=True)
        monkeypatch.setattr(Path, "home", lambda: fake_home)

        basedir = tmp_path / "project"
        basedir.mkdir()
        filepath = fake_home / ".bashrc"

        result = try_relativize(str(filepath), basedir)

        assert result == "~/.bashrc"

    def test_try_relativize_file_already_relative(self, tmp_path: Path):
        basedir = tmp_path / "project"
        basedir.mkdir()

        result = try_relativize(".env", basedir)

        assert result == ".env"
