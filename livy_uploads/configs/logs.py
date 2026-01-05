__all__ = (
    "LoggingConfigurator",
    "FlushStreamHandler",
    "RecreateLoggersPatch",
    "configure_logger",
    "parse_log_levels",
    "get_level_by_name",
)

import logging
import os
import sys
from typing import Any, ClassVar, Mapping, NamedTuple, Optional, TextIO, Union

from typing_extensions import Self

from livy_uploads.configs.base import Configurable
from livy_uploads.configs.utils import split_envvar
from livy_uploads.patches.base import Patch

LOG_LEVEL_ENVVAR = "LOG_LEVEL"
"The environment variable that specifies the log levels."

LOG_FORMAT_ENVVAR = "LOG_FORMAT"
"The environment variable that specifies the log format."

LOG_DEFAULT_LEVEL = "INFO"
LOG_DEFAULT_SPEC = f"{LOG_DEFAULT_LEVEL},urllib3:WARNING"
LOG_DEFAULT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


class LoggingConfigurator(Configurable):
    """
    A singleton instance for configuring the logging system.
    """

    _instance: ClassVar[Optional["LoggingConfigurator"]] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance  # type: ignore

    @property
    def log_levels(self) -> tuple["LevelSpec", ...]:
        """
        Gets the log levels for each logger name.

        The root level is returned with an empty string.
        """
        return parse_log_levels(os.getenv(LOG_LEVEL_ENVVAR) or "")

    @property
    def logger(self) -> logging.Logger:
        "A logger for operations in the log setup itself."
        return logging.getLogger(__name__)

    def setup(self, *, handler: Optional[logging.Handler] = None) -> None:
        """
        Configures the logging system based on the log levels in `$LOG_LEVEL`.
        """
        for level, name in self.log_levels:
            configure_logger(level_name=level, logger=name)

        if self.logger.isEnabledFor(logging.DEBUG):
            specs = " ".join(str(level) for level in self.log_levels)
            self.logger.debug("configured log levels: %s", specs)


class LevelSpec(NamedTuple):
    """
    A dummy container for a log level and name, mostly for pretty-printing.
    """

    level: str
    name: Optional[str] = None

    def __str__(self) -> str:
        return f"{self.name or '<root>'}:{self.level}"


class FlushStreamHandler(logging.Handler):
    """
    A logging handlers that immediately flushes the output to a stream (defaulting to stdout).

    Useful for avoiding selective buffering of log messages, e.g. in Livy sessions.
    """

    def __init__(self, stream: Optional[TextIO] = None) -> None:
        self._stream = stream

    @property
    def stream(self) -> TextIO:
        return self._stream or sys.stdout

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record) + "\n"
        self.stream.write(msg)
        self.stream.flush()


class RecreateLoggersPatch(Patch):
    """
    A patch that explicitly sets the level on existing loggers in primary packages.
    """

    recreate_names: ClassVar[tuple[str, ...]] = (f"{__name__.partition('.')[0]}.", "sparkmagic.")

    def apply_patch(self) -> None:
        # print(f"Configuring root logger to level {level} ({logging.getLevelName(level)})")
        for name, existing_logger in logging.Logger.manager.loggerDict.items():
            if isinstance(existing_logger, logging.Logger) and any(
                name.startswith(prefix) for prefix in self.recreate_names
            ):
                # print(f"Resetting logger {name} level to NOTSET and enabling it")
                existing_logger.setLevel(logging.NOTSET)  # Use root's level
                existing_logger.disabled = False  # Enable the logger!


def configure_logger(
    level_name: str,
    logger: Optional[Union[logging.Logger, str]] = None,
    handler: Optional[logging.Handler] = None,
    replace: bool = True,
    fmt: Optional[str] = None,
) -> None:
    """
    Configures a logger with a formatted handler at the specified log level.

    Sets up a logger with a consistent format (timestamp, level, name, message) and
    attaches a handler. By default, configures the root logger with a StreamHandler.

    Args:
        level_name: The log level name (e.g., "INFO", "DEBUG").
        logger: The logger to configure. If None, uses the root logger.
        handler: The handler to attach. If None, creates a new StreamHandler.
        replace: Whether to clear existing handlers before adding the new one.
        fmt: The log format string. If None, uses the default format.
    """
    level = get_level_by_name(level_name)
    handler = handler or logging.StreamHandler()
    fmt = fmt or os.getenv(LOG_FORMAT_ENVVAR) or LOG_DEFAULT_FORMAT
    formatter = logging.Formatter(fmt=fmt, datefmt=LOG_DATEFMT)
    handler.setFormatter(formatter)

    if logger is None:
        # Configuring root logger: handler should not filter, only logger level controls output
        handler.setLevel(logging.NOTSET)
        logger = logging.getLogger()
    else:
        # Configuring specific logger: both logger and handler levels control output
        handler.setLevel(level)
        if isinstance(logger, str):
            assert all(p.isidentifier() for p in logger.split(".")), f"bad logger name {logger=!r}"
            logger = logging.getLogger(logger)

    logger.setLevel(level)

    if replace:
        logger.handlers.clear()

    logger.handlers.append(handler)


def parse_log_levels(value: str) -> tuple[LevelSpec, ...]:
    """
    Parses the log levels spec into a mapping of logger names to log levels.

    - The log levels are sorted by name.
    - The root level is returned with an empty string.
    - For duplicate names, the last level is used.

    >>> parse_log_levels(LOG_DEFAULT_SPEC)
    (LevelSpec(level='INFO', name=None), LevelSpec(level='WARNING', name='urllib3'))

    >>> parse_log_levels("")
    (LevelSpec(level='INFO', name=None), LevelSpec(level='WARNING', name='urllib3'))

    >>> parse_log_levels("urllib3:WARNING")
    (LevelSpec(level='INFO', name=None), LevelSpec(level='WARNING', name='urllib3'))

    >>> parse_log_levels("INFO,urllib3:WARNING,requests:DEBUG,urllib3:INFO")
    (LevelSpec(level='INFO', name=None), LevelSpec(level='DEBUG', name='requests'), LevelSpec(level='INFO', name='urllib3'))

    >>> parse_log_levels("NO_SUCH_LEVEL,requests:DEBUG")
    Traceback (most recent call last):
    ...
    ValueError: ...
    """
    results = {}
    specs = value.replace(",", " ").split() or LOG_DEFAULT_SPEC.split(",")

    for spec in specs:
        name, _, level = spec.rpartition(":")
        get_level_by_name(level)
        if name and not all(p.isidentifier() for p in name.split(".")):
            raise ValueError(f"bad logger name {name=!r}")

        results[name or ""] = LevelSpec(name=name or None, level=level)

    if "" not in results:
        results[""] = LevelSpec(name=None, level=LOG_DEFAULT_LEVEL)

    return tuple(sorted(results.values(), key=lambda level: level.name or ""))


def get_level_by_name(level_name: str) -> int:
    """
    Parses a log level name string into its corresponding logging module constant.

    >>> get_level_by_name("INFO")
    20

    >>> get_level_by_name("NO_SUCH_LEVEL")
    Traceback (most recent call last):
    ...
    ValueError: unknown log level name: 'NO_SUCH_LEVEL'

    >>> get_level_by_name("123invalid")
    Traceback (most recent call last):
    ...
    ValueError: invalid log level name: '123invalid'
    """
    if not level_name.isidentifier():
        raise ValueError(f"invalid log level name: {level_name!r}")

    try:
        return getattr(logging, level_name.upper())  # type: ignore
    except AttributeError:
        raise ValueError(f"unknown log level name: {level_name!r}") from None
