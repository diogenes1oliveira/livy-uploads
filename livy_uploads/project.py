import functools
import importlib.metadata
import inspect
import logging
import os
from pathlib import Path
from typing import Any, ClassVar, Optional, TypeVar

from typing_extensions import Self

from livy_uploads.configs.config import Config
from livy_uploads.logs import configure_logger
from livy_uploads.patches.base import Patch

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")


NO_OVERRIDE_ENVVAR = "ENVFILE_NO_OVERRIDE"
LOG_LEVEL_ENVVAR = "LOG_LEVEL"
PROFILES_ENVVAR = "PROFILES"
PLUGINS_GROUP = "livy_uploads.plugins"


def find_project_root(start: Optional[Path] = None) -> Path:
    start = (start or Path.cwd()).absolute()

    for path in [start, *start.parents]:
        for filename in ("sparkrl.toml", "pyproject.toml", ".env.example", "README.md"):
            if (path / filename).exists():
                return path

    raise FileNotFoundError("no project root file found")


class Project:
    _current: ClassVar[Optional["Project"]] = None

    def __init__(self, rootdir: Path):
        self.rootdir = rootdir.absolute()
        self._config: Optional[dict[str, Any]] = None

    @classmethod
    def get(cls) -> "Project":
        if cls._current is not None:
            return cls._current

        rootdir = find_project_root()
        LOGGER.info("using project root directory %s", rootdir)
        cls._current = cls(rootdir).reload()
        return cls._current

    def reload(self) -> Self:
        try:
            del self.envfile_values
        except AttributeError:
            pass

        try:
            del self.config
        except AttributeError:
            pass

        self._setup_logs()
        self._apply_patches()
        self._setup_env()
        self.config.reload()
        return self

    @property
    def profiles(self) -> tuple[str, ...]:
        return _split_envvar(os.environ.get(PROFILES_ENVVAR)) or ("override",)

    @property
    def config_filenames(self) -> tuple[str, ...]:
        return ("sparkrl.toml", *[f"sparkrl-{profile}.toml" for profile in self.profiles])

    @functools.cached_property
    def config(self) -> Config:
        return Config(basedir=self.rootdir, filenames=self.config_filenames, env=self.envfile_values)

    @property
    def cache_dir(self) -> Path:
        return self.rootdir / "var" / "cache"

    @property
    def log_level(self) -> str:
        return (os.getenv(LOG_LEVEL_ENVVAR) or "INFO").upper()

    @property
    def envfile_path(self) -> Path:
        return self.rootdir / ".env"

    @functools.cached_property
    def envfile_values(self) -> dict[str, str]:
        from dotenv import dotenv_values

        LOGGER.info("loading environment variables from %s", self.envfile_path)

        values = dotenv_values(self.envfile_path)
        return {k: v for k, v in values.items() if v is not None}

    @property
    def envfile_no_override(self) -> bool:
        for s in (self.envfile_values.get(NO_OVERRIDE_ENVVAR, None), os.environ.get(NO_OVERRIDE_ENVVAR, None)):
            if s:
                return s.lower() in ("1", "true", "yes", "y")
        return False

    def load_plugins(self, t: type[T], subgroup: str) -> list[type[T]]:
        group = PLUGINS_GROUP + "." + subgroup
        entry_points_dict = importlib.metadata.entry_points()
        if isinstance(entry_points_dict, dict):
            # In Python 3.9, entry_points() returns a dict
            entry_points = entry_points_dict.get(group, [])
        else:
            # In Python 3.10+, it returns an EntryPoints object with select() method
            entry_points = entry_points_dict.select(group=group)  # type: ignore[unreachable]

        classes: list[type[T]] = []
        for entry_point in entry_points:
            try:
                spec = entry_point.load()
                if inspect.isclass(spec) and issubclass(spec, t):
                    cls = spec
                    classes.append(cls)
                else:
                    raise TypeError(f"expected a {t.__name__}, got {type(spec)}")
            except TypeError as e:
                if LOGGER.isEnabledFor(logging.DEBUG):
                    LOGGER.debug("failed to load plugins from entry point %s", entry_point, exc_info=True)
                else:
                    LOGGER.warning("failed to load plugins from entry point %s: %s", entry_point, e)

        LOGGER.info(
            "loaded %d plugins from group %r: %s",
            len(classes),
            group,
            ", ".join(cls.__name__ for cls in classes),
        )
        return classes

    def _apply_patches(self) -> None:
        patches = self.load_plugins(Patch, "patches")
        for cls in patches:
            patch = cls()
            LOGGER.info("applying patch %s", cls.__name__)
            patch.apply(self)

    def _setup_logs(self) -> None:
        configure_logger(level_name=self.log_level)

    def _setup_env(self) -> None:
        values = self.envfile_values
        values.pop(NO_OVERRIDE_ENVVAR, None)

        varnames = set[str]()

        if not values:
            LOGGER.debug("no environment variables to set from %s", self.envfile_path)
            return

        for k, v in values.items():
            if k in os.environ and self.envfile_no_override:
                LOGGER.debug("skipping already set environment variable %s", k)
                continue

            os.environ[k] = v
            varnames.add(k)

        self._setup_logs()  # in case this got overriden by the .env

        if not varnames:
            LOGGER.info("no environment variables were overriden")
        else:
            LOGGER.info("set %d environment variables: %s", len(varnames), ", ".join("$" + s for s in varnames))


def _split_envvar(value: Optional[str]) -> tuple[str, ...]:
    value = (value or "").replace(",", " ")
    return tuple(filter(None, value.split(",")))
