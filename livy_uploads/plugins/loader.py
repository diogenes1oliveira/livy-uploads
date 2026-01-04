import logging
import os
from pathlib import Path
from typing import Any, Optional

from typing_extensions import Self

from livy_uploads.plugins import constants
from livy_uploads.plugins.combine import CombinedLoader
from livy_uploads.plugins.load import get_loaders, resolve_loaders
from livy_uploads.configs.utils import split_envvar

LOGGER = logging.getLogger(__name__)


class GlobalLoader(CombinedLoader):
    """
    A singleton loader for the whole app.
    """

    _instance: Optional["GlobalLoader"] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "GlobalLoader":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        super().__init__(loaders=[])

    def setup(self) -> Self:
        constants.reload()
        sources = split_envvar(os.getenv(constants.PLUGINS_ENV, ""))
        loaders = get_loaders(*sources)
        object.__setattr__(self, "loaders", loaders)
        return self

    def resolve(self, *, basedir: Optional[Path] = None) -> tuple[Self]:
        """
        Resolves the plugin modules without executing any code.

        Raises:
            FileNotFoundError: if the loader cannot be resolved.
        """
        self.setup()
        loaders = resolve_loaders(self.loaders, basedir=basedir)
        object.__setattr__(self, "loaders", loaders)

        sources = [loader.uri for loader in loaders]
        LOGGER.debug("using %d plugin loaders: %r", len(loaders), sources)
        os.environ[constants.PLUGINS_ENV] = ",".join(sources)

        return (self,)
