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


class TestEnvFileLoaderSaveEnvs:
    @pytest.fixture
    def loader(self, tmp_path):
        (loader,) = EnvFileLoader.parse(".env").resolve(basedir=tmp_path)
        return loader

    def test_save_envs_creates_file(self, tmp_path, loader):
        loader.save_envs({"KEY": "value"}, quote="none")

        assert (tmp_path / ".env").read_text() == "KEY=value"

    def test_save_envs_updates_existing(self, tmp_path, loader):
        (tmp_path / ".env").write_text("KEY=old\n")

        loader.save_envs({"KEY": "new"}, quote="none")

        assert (tmp_path / ".env").read_text() == "KEY=new"

    def test_save_envs_appends_new(self, tmp_path, loader):
        (tmp_path / ".env").write_text("KEY1=value1\n")

        loader.save_envs({"KEY2": "value2"}, quote="none")

        # Order isn't strictly guaranteed by dict iteration but for a single append it usually is appended at end
        content = (tmp_path / ".env").read_text()
        assert "KEY1=value1" in content
        assert "KEY2=value2" in content

    def test_save_envs_removes_keys(self, tmp_path, loader):
        (tmp_path / ".env").write_text("KEY1=value1\nKEY2=value2\n")

        loader.save_envs({"KEY1": None}, quote="none")

        content = (tmp_path / ".env").read_text()
        assert "KEY1" not in content
        assert "KEY2=value2" in content

    def test_save_envs_mixed_operations(self, tmp_path, loader):
        (tmp_path / ".env").write_text("KEEP=keep\nUPDATE=old\nDELETE=bye\n")

        loader.save_envs({"UPDATE": "new", "DELETE": None, "NEW": "created"}, quote="none")

        content = (tmp_path / ".env").read_text()
        assert "KEEP=keep" in content
        assert "UPDATE=new" in content
        assert "DELETE" not in content
        assert "NEW=created" in content

    def test_save_envs_preserves_comments_and_structure(self, tmp_path, loader):
        initial_content = "# initial comment\nKEY=old\n\n# another comment"
        (tmp_path / ".env").write_text(initial_content)

        loader.save_envs({"KEY": "new"}, quote="none")

        content = (tmp_path / ".env").read_text()
        assert "# initial comment" in content
        assert "KEY=new" in content
        assert "# another comment" in content
