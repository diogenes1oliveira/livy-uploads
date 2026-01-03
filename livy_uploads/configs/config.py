import copy
import dataclasses
import functools
import logging
import os
from pathlib import Path
from typing import Any, Collection, Literal, Mapping, Optional, TypeVar, overload

from typing_extensions import Self

from livy_uploads.configs.utils import get_default_cache_dir, interpolate_envvars, resolve_path_or_content
from livy_uploads.utils.datautils import deep_merge, get_nested_key

DEFAULT_FILENAMES = ("sparkrl.toml", "sparkrl-override.toml")
LOGGER = logging.getLogger(__name__)

T = TypeVar("T")

_MISSING = object()


@dataclasses.dataclass(frozen=True)
class Config:
    basedir: Path
    cache_dir: Path
    filenames: tuple[str, ...]
    env: Mapping[str, str]
    _overrides: Mapping[str, Any] = dataclasses.field(
        default_factory=dict, init=False, repr=False, compare=False, hash=False
    )

    def __init__(
        self,
        basedir: Optional[Path] = None,
        cache_dir: Optional[Path] = None,
        filenames: Optional[Collection[str]] = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> None:
        basedir = (basedir or Path.cwd()).absolute()
        filenames = filenames if filenames is not None else DEFAULT_FILENAMES
        env = env if env is not None else os.environ
        cache_dir = cache_dir or get_default_cache_dir()

        object.__setattr__(self, "basedir", basedir)
        object.__setattr__(self, "cache_dir", cache_dir)
        object.__setattr__(self, "filenames", filenames)
        object.__setattr__(self, "env", env)
        object.__setattr__(self, "_overrides", {})

    def reload(self, overrides: Optional[Mapping[str, Any]] = None) -> Self:
        try:
            del self.raw_values
        except AttributeError:
            pass

        if overrides is not None:
            object.__setattr__(self, "_overrides", overrides)

        self.raw_values  # trigger the cached property again
        return self

    @functools.cached_property
    def raw_values(self) -> Mapping[str, Any]:
        if not self.filenames:
            LOGGER.debug("no configuration files to load, returning empty dictionary")
            return {}

        import toml

        LOGGER.info("loading configuration files: %s", " ".join(self.filenames))
        merged_configs: dict[str, Any] = {}

        for filename in self.filenames:
            path = self.basedir / filename
            try:
                with path.open("r") as fp:
                    configs = toml.load(fp)

            except FileNotFoundError:
                LOGGER.debug("configuration file %s not found, skipping", path)
                continue
            except ValueError:
                LOGGER.warning("failed to load configuration file %s, skipping", path, exc_info=True)
                continue

            merged_configs = deep_merge(merged_configs, configs)

        merged_configs = deep_merge(merged_configs, self._overrides)
        return interpolate_envvars(merged_configs, env=self.env)

    def get_raw(self, key: str, *, nullable: Optional[bool] = False) -> Any:
        try:
            return get_nested_key(self.raw_values, key)
        except KeyError:
            if nullable:
                return None
            raise

    @overload
    def get(self, key: str, t: type[T]) -> T: ...

    @overload
    def get(self, key: str, t: type[T], *, nullable: Literal[False]) -> T: ...

    @overload
    def get(self, key: str, t: type[T], *, nullable: Literal[True]) -> Optional[T]: ...

    @overload
    def get(self, key: str, t: type[T], *, nullable: bool) -> Optional[T]: ...

    @overload
    def get(self, key: str, t: type[T], *, default: T) -> T: ...

    def get(self, key: str, t: type[T], *, nullable: Optional[bool] = False, default: Any = _MISSING) -> Optional[T]:
        assert not (nullable and default is not _MISSING), "cannot specify both nullable and default"

        try:
            raw_value = get_nested_key(self.raw_values, key)
            if raw_value is None:
                raise KeyError(f"null value found for key .{key}")
        except KeyError:
            if default is not _MISSING:
                return copy.deepcopy(default)  # type: ignore
            elif nullable:
                return None
            raise

        if hasattr(t, "parse"):
            return t.parse(raw_value)  # type: ignore

        import cattrs

        try:
            return cattrs.structure(raw_value, t)
        except cattrs.errors.BaseValidationError as e:
            raise ValueError(f"failed to parse configuration value {key} as {t} - {e}") from e

    def resolve_file(self, value: str, *, filename: str, binary: bool = False) -> Path:
        return resolve_path_or_content(
            value,
            filename=filename,
            basedir=self.basedir,
            cache_dir=self.cache_dir,
            binary=binary,
        )
