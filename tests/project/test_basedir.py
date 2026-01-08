# mypy: disable-error-code=no-untyped-def

import pytest

from livy_uploads.project import basedir, constants


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    monkeypatch.delenv(constants.BASEDIR_ENV, raising=False)


def test_find_basedir_from_env(tmp_path, monkeypatch):
    target_dir = tmp_path / "custom_project"
    target_dir.mkdir()
    monkeypatch.setenv(constants.BASEDIR_ENV, str(target_dir))

    result = basedir.find_basedir()

    assert result == target_dir


def test_find_basedir_from_current_dir(tmp_path, monkeypatch):
    (tmp_path / "sparkrl.toml").touch()
    monkeypatch.chdir(tmp_path)

    result = basedir.find_basedir()

    assert result == tmp_path


def test_find_basedir_from_parent_dir(tmp_path, monkeypatch):
    (tmp_path / "pyproject.toml").touch()
    subdir = tmp_path / "subdir" / "deep"
    subdir.mkdir(parents=True)
    monkeypatch.chdir(subdir)

    result = basedir.find_basedir()

    assert result == tmp_path


def test_find_basedir_fallback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = basedir.find_basedir()

    assert result == tmp_path
