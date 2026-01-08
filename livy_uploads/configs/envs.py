import dataclasses
import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import dotenv_values
from typing_extensions import Self

from livy_uploads.configs.base import Configurable
from livy_uploads.configs.utils import split_envvar, unique_values
from livy_uploads.helpers import dataclass_transient
from livy_uploads.plugins import constants
from livy_uploads.plugins.profiles import ProfileFileLoader

LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class EnvFileLoader(ProfileFileLoader, Configurable):
    """
    Loads environment variable files based on a set of profiles.
    """

    overriden_env_names: Optional[set[str]] = dataclasses.field(
        init=False, repr=False, compare=False, hash=False, default=None
    )
    "The applied env names"

    @property
    def envfile_names_envvar(self) -> str:
        """
        >>> EnvFileLoader().envfile_names_envvar
        'SPARKRL_ENVFILE_NAMES'
        """
        return f"{constants.PROJECT_APPNAME.upper()}_ENVFILE_NAMES"

    @property
    def default_basenames(self) -> tuple[str, ...]:
        """
        >>> EnvFileLoader().default_basenames
        ('.env',)
        """
        if value := os.getenv(self.envfile_names_envvar):
            return tuple(unique_values(split_envvar(value)))

        return (".env",)

    @property
    def envfile_override_envvar(self) -> str:
        """
        >>> EnvFileLoader().envfile_override_envvar
        'SPARKRL_ENVFILE_OVERRIDE'
        """
        return f"{constants.PROJECT_APPNAME.upper()}_ENVFILE_OVERRIDE"

    @property
    def envfile_no_interpolate_envvar(self) -> str:
        """
        >>> EnvFileLoader().envfile_no_interpolate_envvar
        'SPARKRL_ENVFILE_NO_INTERPOLATE'
        """
        return f"{constants.PROJECT_APPNAME.upper()}_ENVFILE_NO_INTERPOLATE"

    @property
    def envfile_no_interpolate(self) -> bool:
        """
        >>> EnvFileLoader().envfile_no_interpolate
        False
        """
        try:
            value = os.environ[self.envfile_no_interpolate_envvar]
        except KeyError:
            return False

        return value.lower() in ("true", "1", "yes", "y")

    @property
    def envfile_override(self) -> bool:
        """
        >>> EnvFileLoader().envfile_override
        False
        """
        try:
            value = os.environ[self.envfile_override_envvar]
        except KeyError:
            return False

        return value.lower() in ("true", "1", "yes", "y")

    def get_filenames(self, basename: str, profile: str) -> tuple[str, ...]:
        """
        >>> EnvFileLoader().get_filenames('.env', profile='prod')
        ('.prod.env',)

        >>> EnvFileLoader().get_filenames('.env', profile='default')
        ('.env',)
        """
        if profile != "default":
            return (f".{profile}{basename}",)

        return (basename,)

    def setup(self) -> list[Path]:
        """
        Finds and loads the dotenv files, merging them in order.
        """

        assert self.basedir is not None, ".basedir not resolved yet"

        # Load all values first, merging them in order
        merged_values: dict[str, Optional[str]] = {}
        candidate_paths = [f.path for f in self.find_paths(pattern="*")]
        LOGGER.debug("loading environment variables from %s", " ".join(map(str, candidate_paths)))

        interpolate = not self.envfile_no_interpolate
        found_paths = []
        applied_envs: list[str] = []

        for path in candidate_paths:
            # We merge values on top of each other, so the last loaded file wins
            # (e.g. .prod.env overrides .env)
            if not path.exists():
                continue

            file_values = dotenv_values(dotenv_path=path, interpolate=interpolate)
            merged_values.update(file_values)
            found_paths.append(path)

        # Then apply to os.environ
        override = self.envfile_override
        for key, value in merged_values.items():
            if value is None:
                continue

            if override or key not in os.environ:
                os.environ[key] = value
                applied_envs.append(key)

        if self.overriden_env_names is None:
            object.__setattr__(self, "overriden_env_names", set(applied_envs))
        else:
            self.overriden_env_names.update(applied_envs)

        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug("overriden environment variables: %s", " ".join(sorted(applied_envs)))

        return found_paths
