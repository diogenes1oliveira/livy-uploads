import logging

import pytest

from livy_uploads.configs.logs import LoggingConfigurator

# mypy: disable-error-code=no-untyped-def

LOGGER = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def reset_loggers(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOG_FORMAT", "<time> %(levelname)s %(name)s: %(message)s")


@pytest.fixture()
def configurator():
    return LoggingConfigurator()


def test_logging_setup(configurator: LoggingConfigurator, capsys: pytest.CaptureFixture[str]):
    configurator.setup()

    capsys.readouterr()
    LOGGER.info("hello world!")
    _, err = capsys.readouterr()
    assert err == f"<time> INFO {__name__}: hello world!\n"
