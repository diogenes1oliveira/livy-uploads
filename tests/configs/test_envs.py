# mypy: disable-error-code=no-untyped-def
import os
from pathlib import Path

import pytest

from livy_uploads.configs.envs import EnvFileLoader


@pytest.fixture(autouse=True)
def clean_environ():
    old_environ = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(old_environ)


class TestEnvFileLoaderSetup:
    def test_setup_loads_env_file(self, tmp_path: Path):
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_KEY=test_value\n")

        (loader,) = EnvFileLoader.parse(".env").resolve(basedir=tmp_path)
        loader.setup()

        assert os.environ.get("TEST_KEY") == "test_value"

    def test_setup_with_interpolation(self, tmp_path: Path):
        env_file = tmp_path / ".env"
        env_file.write_text("BASE=value\nDERIVED=${BASE}_suffixed\n")

        (loader,) = EnvFileLoader.parse(".env").resolve(basedir=tmp_path)
        loader.setup()

        assert os.environ.get("BASE") == "value"
        assert os.environ.get("DERIVED") == "value_suffixed"

    def test_setup_no_interpolation(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("SPARKRL_ENVFILE_NO_INTERPOLATE", "true")
        env_file = tmp_path / ".env"
        env_file.write_text("BASE=value\nDERIVED=${BASE}_suffixed\n")

        (loader,) = EnvFileLoader.parse(".env").resolve(basedir=tmp_path)
        loader.setup()

        assert os.environ.get("BASE") == "value"
        assert os.environ.get("DERIVED") == "${BASE}_suffixed"

    def test_setup_no_override_existing(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("EXISTING_KEY", "original_value")
        env_file = tmp_path / ".env"
        env_file.write_text("EXISTING_KEY=new_value\nNEW_KEY=new_value\n")

        (loader,) = EnvFileLoader.parse(".env").resolve(basedir=tmp_path)
        loader.setup()

        assert os.environ.get("EXISTING_KEY") == "original_value"
        assert os.environ.get("NEW_KEY") == "new_value"

    def test_setup_with_override(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("SPARKRL_ENVFILE_OVERRIDE", "true")
        monkeypatch.setenv("EXISTING_KEY", "original_value")

        env_file = tmp_path / ".env"
        env_file.write_text("EXISTING_KEY=new_value\n")

        (loader,) = EnvFileLoader.parse(".env").resolve(basedir=tmp_path)
        loader.setup()

        assert os.environ.get("EXISTING_KEY") == "new_value"

    def test_setup_profiles_loading_order(self, tmp_path: Path):
        (tmp_path / ".env").write_text("COMMON=default\nONLY_DEFAULT=1\n")
        (tmp_path / ".prod.env").write_text("COMMON=prod\nONLY_PROD=1\n")

        (loader,) = EnvFileLoader.parse(".env?profiles=prod").resolve(basedir=tmp_path)
        loader.setup()

        # Late load overrides early load (.prod.env > .env)
        # Merging logic should prefer 'prod' because it is loaded last.
        assert os.environ.get("COMMON") == "prod"
        assert os.environ.get("ONLY_DEFAULT") == "1"
        assert os.environ.get("ONLY_PROD") == "1"

    def test_setup_profiles_with_override(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("SPARKRL_ENVFILE_OVERRIDE", "true")

        (tmp_path / ".env").write_text("COMMON=default\n")
        (tmp_path / ".prod.env").write_text("COMMON=prod\n")

        (loader,) = EnvFileLoader.parse(".env?profiles=prod").resolve(basedir=tmp_path)
        loader.setup()

        assert os.environ.get("COMMON") == "prod"
