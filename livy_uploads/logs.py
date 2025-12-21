"""
Utilities for configuring logging handlers and parsing log levels.
"""

import logging
import os
from typing import Optional, Iterable
import sys
from logging import Logger

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL_NAME = (os.getenv("LOG_LEVEL") or "INFO").upper()


class FlushStreamHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record) + "\n"
        sys.stdout.write(msg)
        sys.stdout.flush()


def configure_logger(
    logger: Optional[logging.Logger] = None,
    level_name: Optional[str] = None,
    handler: Optional[logging.Handler] = None,
    replace: bool = True,
    recreate_names: Optional[Iterable[str]] = (),
) -> None:
    """
    Configures a logger with a formatted handler at the specified log level.

    Sets up a logger with a consistent format (timestamp, level, name, message) and
    attaches a handler. By default, configures the root logger with a StreamHandler.

    Args:
        logger: The logger to configure. If None, uses the root logger.
        level_name: The log level name (e.g., "INFO", "DEBUG"). If None, reads from
            the $LOG_LEVEL environment variable or defaults to "INFO".
        handler: The handler to attach. If None, creates a new StreamHandler.
        replace: Whether to clear existing handlers before adding the new one.

    Configuration Behavior:
        1. Parses the level name using :func:`parse_log_level_name`
        2. Creates or uses the provided handler
        3. Formats the handler with the module's standard format and date format
        4. Sets both handler and logger to the specified level
        5. Optionally clears existing handlers if replace=True
        6. Attaches the handler to the logger

    See Also:
        :func:`parse_log_level_name` for log level name parsing.
    """
    if recreate_names == ():
        recreate_names = ["livy_uploads.", "sparkmagic."]

    level = parse_log_level_name(level_name or LOG_LEVEL_NAME)
    handler = handler or logging.StreamHandler()
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATEFMT)
    handler.setFormatter(formatter)

    if logger is None:
        # Configuring root logger: handler should not filter, only logger level controls output
        handler.setLevel(logging.NOTSET)
        logger = logging.getLogger()
    else:
        # Configuring specific logger: both logger and handler levels control output
        handler.setLevel(level)

    logger.setLevel(level)

    if replace:
        logger.handlers.clear()

    logger.handlers.append(handler)

    # Workaround for Jupyter: explicitly set level on all existing livy_uploads loggers
    if logger.name == "root" and recreate_names:
        # print(f"Configuring root logger to level {level} ({logging.getLevelName(level)})")
        for name, existing_logger in logging.Logger.manager.loggerDict.items():
            if isinstance(existing_logger, logging.Logger) and any(
                name.startswith(prefix) for prefix in recreate_names
            ):
                # print(f"Resetting logger {name} level to NOTSET and enabling it")
                existing_logger.setLevel(logging.NOTSET)  # Use root's level
                existing_logger.disabled = False  # Enable the logger!


def parse_log_level_name(level_name: str) -> int:
    """
    Parses a log level name string into its corresponding logging module constant.

    Converts a case-insensitive log level name (e.g., "info", "DEBUG") into the
    integer constant from the logging module (e.g., logging.INFO, logging.DEBUG).

    Args:
        level_name: The log level name to parse. Case-insensitive. Must be a valid
            Python identifier matching a logging module constant (INFO, DEBUG, WARNING, ERROR, CRITICAL).

    Returns:
        The integer value of the logging level constant.

    Raises:
        ValueError: If level_name is not a valid identifier.
        AttributeError: If level_name doesn't correspond to a logging module constant.
    """
    level_name = level_name.upper()
    if not level_name.isidentifier():
        raise ValueError(f"invalid log level name: {level_name!r}")
    return getattr(logging, level_name)
