from livy_uploads.plugins import LOADER
from livy_uploads.plugins.base import Loader
from livy_uploads.plugins.loader import GlobalLoader


def test_singleton() -> None:
    assert isinstance(LOADER, Loader)

    assert LOADER is GlobalLoader()
    assert GlobalLoader() is GlobalLoader()
    assert GlobalLoader() is LOADER
