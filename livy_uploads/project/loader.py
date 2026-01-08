__all__ = ("GlobalEnvLoader",)

import logging
import os
from pathlib import Path
from typing import Optional

from typing_extensions import Self

from livy_uploads.configs.utils import split_envvar
from livy_uploads.plugins import constants, register_default_loaders
from livy_uploads.plugins.loaders import CombinedLoader, get_loaders, resolve_loaders

LOGGER = logging.getLogger(__name__)


DEFAULT_FILE_LOADER = "file://./"


class GlobalEnvLoader(CombinedLoader):
    """
    A global loader for the whole app that uses the `{APPNAME}_PLUGINS` environment variable.
    """

    def __init__(self) -> None:
        super().__init__(loaders=[])

    def setup(self) -> Self:
        """
        Recreates all loaders specified in the environment variable.
        """
        constants.reload()
        sources = split_envvar(os.getenv(constants.PLUGINS_ENV, ""))

        if DEFAULT_FILE_LOADER not in sources:
            sources.append(DEFAULT_FILE_LOADER)

        register_default_loaders()

        loaders = get_loaders(*sources)
        object.__setattr__(self, "loaders", loaders)

        LOGGER.debug("got %d loaders: %s", len(loaders), " ".join(l.uri for l in loaders))
        return self

    def resolve(self, *, basedir: Optional[Path] = None) -> tuple[Self]:
        """
        Resolves the plugin modules without executing any code.

        Raises:
            FileNotFoundError: if the loader cannot be resolved.
        """
        loaders = resolve_loaders(self.loaders, basedir=basedir)
        object.__setattr__(self, "loaders", loaders)

        sources = [loader.uri for loader in loaders]
        LOGGER.debug("using %d plugin loaders: %r", len(loaders), sources)
        os.environ[constants.PLUGINS_ENV] = ",".join(sources)

        return (self,)
