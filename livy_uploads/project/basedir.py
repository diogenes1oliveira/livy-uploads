__all__ = ("find_basedir",)

import logging
import os
from pathlib import Path

from livy_uploads.project import constants

PROJECT_ROOT_FILENAMES = ("sparkrl.toml", "pyproject.toml", ".env.example", "README.md")

LOGGER = logging.getLogger(__name__)


def find_basedir() -> Path:
    if value := os.getenv(constants.BASEDIR_ENV):
        path = Path(value).absolute().resolve()
        LOGGER.info("Using basedir from $%s: %s", constants.BASEDIR_ENV, path)
        return path

    curr = Path.cwd()

    for candidate_dir in [curr, *curr.parents]:
        for filename in PROJECT_ROOT_FILENAMES:
            marker_file = candidate_dir / filename
            if marker_file.is_file():
                path = candidate_dir.resolve()
                cwd = Path.cwd().resolve()
                level = logging.DEBUG if path == cwd else logging.INFO
                LOGGER.log(level, "using basedir from detected %r: %s", filename, path)
                return path

    LOGGER.info("falling back to current director as basedir: %s", curr)
    return curr
