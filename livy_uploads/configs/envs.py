import dataclasses
import json
import logging
import os
import shlex
from pathlib import Path
from typing import Literal, Mapping, Optional

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

    def save_envs(
        self,
        updates: Mapping[str, Optional[str]],
        quote: Literal["shell", "none", "json", "auto"] = "auto",
    ) -> None:
        """
        Saves or replaces the environment variables in the first .env file in the list of default basenames.

        Args:
            updates: A mapping of environment variable names to their values. `None` values will be removed.
        """

        assert self.basedir is not None, ".basedir not resolved yet"

        try:
            filename = self.default_basenames[0]
        except IndexError:
            raise ValueError("no default basenames to save envs to") from None

        path = self.basedir / filename
        if not path.exists():
            path.touch(0o600)
        path.chmod(0o600)

        updates = dict(updates)

        content = path.read_text()
        new_lines = []
        for line in content.splitlines():
            name, sep, _ = line.partition("=")
            if not name or not sep:
                new_lines.append(line)
                continue
            try:
                update = updates.pop(name)
            except KeyError:
                # non-matching lines are kept as-is
                new_lines.append(line)
                continue

            if update is None:
                # explicit deletion of a variable
                continue

            new_lines.append(f"{name}={env_quote(update, quote=quote)}")

        # add in the non-popped updates
        if updates:
            for name, value in updates.items():
                if value is None:
                    continue
                new_lines.append(f"{name}={env_quote(value, quote=quote)}")

        path.write_text("\n".join(new_lines))


def env_quote(value: str, quote: Literal["shell", "none", "json", "auto"]) -> str:
    r"""
    >>> [env_quote("foo", quote="shell"), env_quote("foo bar", quote="shell")]
    ['foo', "'foo bar'"]
    >>> [env_quote("foo", quote="json"), env_quote("foo bar", quote="json")]
    ['"foo"', '"foo bar"']
    >>> [env_quote("foo", quote="none"), env_quote("foo bar", quote="none")]
    ['foo', 'foo bar']
    >>> [env_quote("foo", quote="auto"), env_quote("foo bar", quote="auto")]
    ['"foo"', '"foo bar"']
    """
    if quote == "auto":
        dumped = json.dumps(value)
        if dumped == value:
            quote = "none"
        else:
            quote = "json"

    if quote == "shell":
        return shlex.quote(value)
    elif quote == "json":
        return json.dumps(value)
    elif quote == "none":
        if "\r" in value or "\n" in value:
            raise ValueError(f"value {value!r} contains newline or carriage return characters")
        return value
    else:
        raise ValueError(f"invalid quote mode {quote!r}")
