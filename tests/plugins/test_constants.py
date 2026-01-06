import pytest

from livy_uploads.plugins import constants


@pytest.fixture(autouse=True)
def reload_constants(monkeypatch: pytest.MonkeyPatch):
    yield
    monkeypatch.undo()
    constants.reload()


def test_plugins_constants(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PROJECT_APPNAME", "dummy1")
    constants.reload()
    assert constants.PROJECT_APPNAME == "dummy1"

    monkeypatch.setenv("PROJECT_APPNAME", "dummy2")
    constants.reload()

    assert constants.as_json() == {
        "PROJECT_APPNAME": "dummy2",
        "PLUGINS_ENV": "DUMMY2_PLUGINS",
        "PROFILES_ENV": "DUMMY2_PROFILES",
        "GROUP_PREFIX": "dummy2.plugins.",
        "LOADERS_GROUP": "dummy2.plugins.loaders",
    }
