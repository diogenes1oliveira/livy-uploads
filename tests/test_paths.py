# mypy: disable-error-code=no-untyped-def
import os
from pathlib import Path, PurePosixPath
from unittest.mock import Mock, patch

import pytest

from livy_uploads.paths import PathNotFoundError, find_first_in_paths, resolve_paths


class TestResolvePaths:
    @pytest.fixture
    def mock_env(self, monkeypatch):
        """Fixture to manage environment variables."""
        monkeypatch.delenv("NBLIB_PATH", raising=False)
        return monkeypatch

    def test_resolve_paths_with_no_pathspec_and_no_env_variable(self, mock_env):
        result = resolve_paths()

        assert isinstance(result, list)
        assert len(result) >= 1
        assert any("livy_uploads" in path for path in result)

    def test_resolve_paths_with_directory_path(self, tmp_path, mock_env):
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()
        pathspec = str(test_dir)

        result = resolve_paths(pathspec)

        assert str(test_dir.absolute()) in result
        assert any("livy_uploads" in path for path in result)

    def test_resolve_paths_with_multiple_directory_paths(self, tmp_path, mock_env):
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()
        pathspec = f"{dir1}{os.pathsep}{dir2}"

        result = resolve_paths(pathspec)

        assert str(dir1.absolute()) in result
        assert str(dir2.absolute()) in result

    def test_resolve_paths_with_tilde_expansion(self, tmp_path, mock_env, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        pathspec = "~/test_dir"

        result = resolve_paths(pathspec)

        expected_path = str(tmp_path / "test_dir")
        assert expected_path in result

    def test_resolve_paths_with_valid_package_name(self, mock_env):
        pathspec = "pytest"

        result = resolve_paths(pathspec)

        assert any("pytest" in path for path in result)
        assert any("livy_uploads" in path for path in result)

    def test_resolve_paths_with_invalid_package_name(self, mock_env):
        pathspec = "nonexistent_package_xyz123"

        result = resolve_paths(pathspec)

        assert not any("nonexistent_package_xyz123" in path for path in result)
        assert any("livy_uploads" in path for path in result)

    def test_resolve_paths_from_environment_variable(self, tmp_path, mock_env):
        test_dir = tmp_path / "env_dir"
        test_dir.mkdir()
        mock_env.setenv("NBLIB_PATH", str(test_dir))

        result = resolve_paths()

        assert str(test_dir.absolute()) in result

    def test_resolve_paths_empty_string_pathspec(self, mock_env):
        result = resolve_paths("")

        assert isinstance(result, list)
        assert any("livy_uploads" in path for path in result)

    def test_resolve_paths_whitespace_only_pathspec(self, mock_env):
        result = resolve_paths("   ")

        assert isinstance(result, list)
        assert any("livy_uploads" in path for path in result)

    def test_resolve_paths_mixed_directories_and_packages(self, tmp_path, mock_env):
        test_dir = tmp_path / "mixed_dir"
        test_dir.mkdir()
        pathspec = f"{test_dir}{os.pathsep}pytest"

        result = resolve_paths(pathspec)

        assert str(test_dir.absolute()) in result
        assert any("pytest" in path for path in result)

    def test_resolve_paths_filters_empty_components(self, tmp_path, mock_env):
        test_dir = tmp_path / "filter_dir"
        test_dir.mkdir()
        pathspec = f"{test_dir}{os.pathsep}{os.pathsep}{os.pathsep}"

        result = resolve_paths(pathspec)

        assert str(test_dir.absolute()) in result

    def test_resolve_paths_skips_package_with_no_origin(self, mock_env):
        with patch("importlib.util.find_spec") as mock_find_spec:
            mock_spec = Mock()
            mock_spec.origin = None
            mock_find_spec.return_value = mock_spec
            pathspec = "some_package"

            result = resolve_paths(pathspec)

            assert not any("some_package" in path for path in result)

    def test_resolve_paths_skips_package_with_exception(self, mock_env):
        with patch("importlib.util.find_spec", side_effect=ImportError("test error")):
            pathspec = "error_package"

            result = resolve_paths(pathspec)

            assert not any("error_package" in path for path in result)

    def test_resolve_paths_handles_package_without_init(self, mock_env):
        with patch("importlib.util.find_spec") as mock_find_spec:
            mock_spec = Mock()
            mock_spec.origin = "/some/path/module.py"
            mock_find_spec.return_value = mock_spec
            pathspec = "some_module"

            result = resolve_paths(pathspec)

            assert "/some/path" not in result

    def test_resolve_paths_includes_package_with_init(self, mock_env):
        pathspec = "livy_uploads"

        result = resolve_paths(pathspec)

        assert any("livy_uploads" in path for path in result)

    def test_resolve_paths_always_includes_own_package(self, mock_env):
        result = resolve_paths("")

        assert any("livy_uploads" in path for path in result)

    def test_resolve_paths_returns_absolute_paths(self, tmp_path, mock_env, monkeypatch):
        test_dir = tmp_path / "abs_test"
        test_dir.mkdir()
        monkeypatch.chdir(tmp_path)
        pathspec = "./abs_test"

        result = resolve_paths(pathspec)

        for path in result:
            assert Path(path).is_absolute()


class TestFindFirstInPaths:
    @pytest.fixture
    def mock_env(self, monkeypatch):
        """Fixture to manage environment variables."""
        monkeypatch.delenv("NBLIB_PATH", raising=False)
        return monkeypatch

    @pytest.fixture
    def test_structure(self, tmp_path):
        """Create a test directory structure with files."""
        base1 = tmp_path / "base1"
        base2 = tmp_path / "base2"
        base1.mkdir()
        base2.mkdir()

        (base1 / "file1.txt").write_text("content1")
        (base1 / "subdir").mkdir()
        (base1 / "subdir" / "nested.txt").write_text("nested1")

        (base2 / "file2.txt").write_text("content2")
        (base2 / "subdir").mkdir()
        (base2 / "subdir" / "nested.txt").write_text("nested2")

        return {"base1": base1, "base2": base2, "tmp_path": tmp_path}

    def test_find_first_in_paths_with_single_path_in_pathspec(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = ["file1.txt"]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "file1.txt"

    def test_find_first_in_paths_returns_path_object(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = ["file1.txt"]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert isinstance(result, Path)

    def test_find_first_in_paths_searches_multiple_haystacks(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        base2 = test_structure["base2"]
        paths = ["file2.txt"]
        pathspec = f"{base1}{os.pathsep}{base2}"

        result = find_first_in_paths(paths, pathspec)

        assert result == base2 / "file2.txt"

    def test_find_first_in_paths_returns_first_needle_found(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        base2 = test_structure["base2"]
        paths = ["file2.txt", "file1.txt"]
        pathspec = f"{base1}{os.pathsep}{base2}"

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "file1.txt"

    def test_find_first_in_paths_checks_all_needles_in_each_haystack(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = ["nonexistent.txt", "file1.txt"]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "file1.txt"

    def test_find_first_in_paths_with_nested_path(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = ["subdir/nested.txt"]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "subdir" / "nested.txt"

    def test_find_first_in_paths_with_pureposixpath(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = [PurePosixPath("file1.txt")]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "file1.txt"

    def test_find_first_in_paths_with_path_object(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = [Path("file1.txt")]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "file1.txt"

    def test_find_first_in_paths_with_absolute_path(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        base2 = test_structure["base2"]
        file1_abs = base1 / "file1.txt"
        paths = [str(file1_abs)]
        pathspec = str(base2)

        result = find_first_in_paths(paths, pathspec)

        assert result == file1_abs

    def test_find_first_in_paths_absolute_path_checked_after_relative_search(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        base2 = test_structure["base2"]
        file2_abs = base2 / "file2.txt"
        paths = ["file1.txt", str(file2_abs)]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "file1.txt"

    def test_find_first_in_paths_with_none_pathspec_uses_resolve_paths(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = ["file1.txt"]
        mock_env.setenv("NBLIB_PATH", str(base1))

        result = find_first_in_paths(paths, pathspec=None)

        assert result == base1 / "file1.txt"

    def test_find_first_in_paths_with_empty_string_pathspec(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        file1_abs = base1 / "file1.txt"
        paths = [str(file1_abs)]
        pathspec = ""

        result = find_first_in_paths(paths, pathspec)

        assert result == file1_abs

    def test_find_first_in_paths_with_list_pathspec(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        base2 = test_structure["base2"]
        paths = ["file2.txt"]
        pathspec = [str(base1), str(base2)]

        result = find_first_in_paths(paths, pathspec)

        assert result == base2 / "file2.txt"

    def test_find_first_in_paths_with_path_objects_in_pathspec(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        base2 = test_structure["base2"]
        paths = ["file2.txt"]
        pathspec = [base1, base2]

        result = find_first_in_paths(paths, pathspec)

        assert result == base2 / "file2.txt"

    def test_find_first_in_paths_raises_path_not_found_error(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = ["nonexistent.txt"]
        pathspec = str(base1)

        with pytest.raises(PathNotFoundError) as exc_info:
            find_first_in_paths(paths, pathspec)

        assert "nonexistent.txt" in str(exc_info.value)

    def test_find_first_in_paths_error_includes_all_needles(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = ["nonexistent1.txt", "nonexistent2.txt"]
        pathspec = str(base1)

        with pytest.raises(PathNotFoundError) as exc_info:
            find_first_in_paths(paths, pathspec)

        error_message = str(exc_info.value)
        assert "nonexistent1.txt" in error_message
        assert "nonexistent2.txt" in error_message

    def test_find_first_in_paths_error_has_paths_attribute(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = ["nonexistent.txt"]
        pathspec = str(base1)

        with pytest.raises(PathNotFoundError) as exc_info:
            find_first_in_paths(paths, pathspec)

        assert hasattr(exc_info.value, "paths")
        assert hasattr(exc_info.value, "searched_paths")

    def test_find_first_in_paths_with_string_pathspec_splits_by_pathsep(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        base2 = test_structure["base2"]
        paths = ["file2.txt"]
        pathspec = f"{base1}{os.pathsep}{base2}"

        result = find_first_in_paths(paths, pathspec)

        assert result == base2 / "file2.txt"

    def test_find_first_in_paths_returns_first_haystack_match(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        base2 = test_structure["base2"]
        paths = ["subdir/nested.txt"]
        pathspec = f"{base1}{os.pathsep}{base2}"

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "subdir" / "nested.txt"

    def test_find_first_in_paths_checks_cwd_first_for_relative_paths(self, test_structure, mock_env, monkeypatch):
        base1 = test_structure["base1"]
        tmp_path = test_structure["tmp_path"]
        cwd_dir = tmp_path / "cwd"
        cwd_dir.mkdir()
        monkeypatch.chdir(cwd_dir)
        (cwd_dir / "file1.txt").write_text("cwd content")
        paths = ["file1.txt"]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == cwd_dir / "file1.txt"

    def test_find_first_in_paths_returns_absolute_path_from_cwd(self, test_structure, mock_env, monkeypatch):
        tmp_path = test_structure["tmp_path"]
        cwd_dir = tmp_path / "cwd"
        cwd_dir.mkdir()
        monkeypatch.chdir(cwd_dir)
        (cwd_dir / "test.txt").write_text("content")
        paths = ["test.txt"]
        pathspec = ""

        result = find_first_in_paths(paths, pathspec)

        assert result.is_absolute()
        assert result == cwd_dir / "test.txt"

    def test_find_first_in_paths_absolute_path_searched_in_haystacks(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        base2 = test_structure["base2"]
        paths = ["/file2.txt"]
        pathspec = f"{base1}{os.pathsep}{base2}"

        result = find_first_in_paths(paths, pathspec)

        assert result == base2 / "file2.txt"

    def test_find_first_in_paths_absolute_path_strips_leading_slash(self, test_structure, mock_env):
        base1 = test_structure["base1"]
        paths = ["/subdir/nested.txt"]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "subdir" / "nested.txt"

    def test_find_first_in_paths_cwd_checked_before_haystacks(self, test_structure, mock_env, monkeypatch):
        base1 = test_structure["base1"]
        tmp_path = test_structure["tmp_path"]
        cwd_dir = tmp_path / "cwd"
        cwd_dir.mkdir()
        (cwd_dir / "subdir").mkdir()
        (cwd_dir / "subdir" / "nested.txt").write_text("cwd nested")
        monkeypatch.chdir(cwd_dir)
        paths = ["subdir/nested.txt"]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == cwd_dir / "subdir" / "nested.txt"

    def test_find_first_in_paths_absolute_path_not_checked_in_cwd(self, test_structure, mock_env, monkeypatch):
        base1 = test_structure["base1"]
        tmp_path = test_structure["tmp_path"]
        cwd_dir = tmp_path / "cwd"
        cwd_dir.mkdir()
        monkeypatch.chdir(cwd_dir)
        (cwd_dir / "file1.txt").write_text("should not find this")
        paths = ["/file1.txt"]
        pathspec = str(base1)

        result = find_first_in_paths(paths, pathspec)

        assert result == base1 / "file1.txt"
