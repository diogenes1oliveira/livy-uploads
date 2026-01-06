import os
import pytest


@pytest.fixture(autouse=True, scope="session")
def reset_plugin_envs():
    os.environ.pop("PROJECT_APPNAME", None)
    os.environ.pop("SPARKRL_PLUGINS", None)
    os.environ.pop("SPARKRL_PROFILES", None)
