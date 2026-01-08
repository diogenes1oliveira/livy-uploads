import collections.abc
import dataclasses
import logging
import os
from collections.abc import Iterator, Mapping
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Literal, Optional, TypeVar, Union, overload

from livy_uploads.configs.base import Configurable
from livy_uploads.configs.converters import NO_DEFAULT, Converter
from livy_uploads.configs.utils import interpolate_envvars
from livy_uploads.plugins import constants
from livy_uploads.plugins.base import FoundPath
from livy_uploads.plugins.profiles import ProfileFileLoader
from livy_uploads.utils.datautils import deep_merge, resolve_local_includes

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")


def load_config_file(path: Path) -> dict[str, Any]:
    if path.suffix == ".toml":
        import toml

        with path.open("r") as fp:
            raw_values = toml.load(path)
    elif path.suffix in (".yaml", ".yml"):
        import yaml

        with path.open("r") as fp:
            raw_values = yaml.safe_load(fp)
    elif path.suffix == ".json":
        import json

        with path.open("r") as fp:
            raw_values = json.load(fp)
    else:
        raise ValueError(f"Unsupported file extension {path.suffix!r} in {path}")

    if not isinstance(raw_values, collections.abc.Mapping):
        raise ValueError(f"Unsupported configuration in {path}: not a top-level dict")

    # Resolve local includes (starting with #) within the file
    values_dict = dict(raw_values)
    resolved = resolve_local_includes(values_dict)
    if not isinstance(resolved, dict):
        # Should not happen if input is dict, but for type safety
        return dict(resolved)
    return resolved


@dataclasses.dataclass(frozen=True)
class ConfigFile(FoundPath):
    key: Optional[str] = None
    "Key to nest the config under"

    def load(self) -> dict[str, Any]:
        return load_config_file(self.path)


@dataclasses.dataclass(frozen=True)
class ConfigFileLoader(ProfileFileLoader, Configurable):
    """
    Loads configuration files based on a set of profiles.
    """

    raw_configs: Optional[Mapping[str, Any]] = dataclasses.field(
        default=None, init=False, repr=False, compare=False, hash=False
    )
    "The full merged raw values, once configured"

    converter: Optional[Converter] = dataclasses.field(default=None, init=False, repr=False, compare=False, hash=False)
    "The converter instance once configured"

    @property
    def default_basenames(self) -> tuple[str, ...]:
        """
        >>> ConfigFileLoader().default_basenames
        ('sparkrl.toml', 'sparkrl.yaml', 'sparkrl.json')
        """
        app = constants.PROJECT_APPNAME
        return (f"{app}.toml", f"{app}.yaml", f"{app}.json")

    def get_filenames(self, basename: str, profile: str) -> tuple[str, ...]:
        """
        >>> ConfigFileLoader().get_filenames('app.toml', profile='prod')
        ('app-prod.toml',)

        >>> ConfigFileLoader().get_filenames('app.yaml', profile='default')
        ('app.yaml',)
        """
        if profile == "default":
            return (basename,)

        name, ext = os.path.splitext(basename)
        return (f"{name}-{profile}{ext}",)

    def set_converter(self, converter: Converter) -> None:
        object.__setattr__(self, "converter", converter)

    def find_paths(self, *, pattern: str) -> Iterator[ConfigFile]:
        """
        Finds configuration files matching the given pattern.

        Supported patterns:
        - "*": Finds standard configuration files defined by basenames and profiles.
          Example: `sparkrl.toml`, `sparkrl-prod.toml`.
          Returns ConfigFile objects with key=None.
        - "*.*": Finds BOTH standard configuration files AND nested configuration files
          that are namespaced by a dot-separated key.

          The finding order is interleaved: for each standard file (e.g. `sparkrl.toml`),
          it yields the file itself first, and then immediately scans and yields any
          nested files associated with it (e.g. `sparkrl.database.toml`). Only then
          does it proceed to the next profile/basename (e.g. `sparkrl-prod.toml`).

          Example order:
          1. `sparkrl.toml` (key=None)
          2. `sparkrl.database.toml` (key="database")
          3. `sparkrl.kafka.toml` (key="kafka")
          4. `sparkrl-prod.toml` (key=None)
          5. `sparkrl-prod.extra.toml` (key="extra")

          Behavior:
          - Standard files (key=None): Found via profile/basename expansion.
          - Nested files (key="..."): Found by scanning the directory for `{filename}.*.{ext}`.
            Key is extracted from the match.
        """
        if pattern not in ("*", "*.*"):
            raise ValueError(f"Invalid pattern: {pattern!r}")

        # *.* will try to load files like app.some.key.toml, app.key1.yaml, app-prod.a.b.json
        # i.e., a key to nest the config under
        # This requires scanning the directories
        assert self.basedir is not None

        if pattern == "*":
            # just load regular config files and profiles
            for found_path in super().find_paths(pattern="*"):
                yield ConfigFile(found_path.path, found_path.uri, found_path.pattern, loader=self)
            return

        for found_file in super().find_paths(pattern="*"):
            # yield the main file first
            yield ConfigFile(found_file.path, found_file.uri, found_file.pattern, loader=self)

            # then yield all found namespaced correspondences for this one file
            name = found_file.path.name
            root, ext = os.path.splitext(name)
            # We are looking for {root}.{key}{ext}
            glob_pattern = f"{root}.*{ext}"

            for match in self.basedir.glob(glob_pattern):
                # match is a Path object
                # Extract key: match.name = "root.key.ext" -> key = "key"
                # We know match.name starts with root + "." and ends with ext
                key_part = match.name[len(root) + 1 : -len(ext)]
                if not key_part:
                    continue

                # extract the profiles from the parent file URI to reuse them
                # (e.g. if parent is .prod.env, the nested file should likely be associated with prod too)
                profiles = self.parse(found_file.uri).profiles
                uri = self.named_uri(match.name, profiles=profiles)
                yield ConfigFile(
                    path=match,
                    uri=uri,
                    pattern=pattern,
                    loader=self,
                    key=key_part,
                )

    def _resolve_path(self, filename: str) -> Path:
        """Helper to resolve a filename to an absolute path based on self.basedir"""
        # basedir is guaranteed to be resolved by the time find_paths is called
        assert self.basedir is not None

        path = Path(PurePosixPath(filename)).expanduser()
        if not path.is_absolute():
            path = (self.basedir / path).absolute()
        return path

    def _process_includes(self, config: Any) -> Any:
        if isinstance(config, list):
            return [self._process_includes(x) for x in config]

        if not isinstance(config, dict):
            return config

        # Recurse first
        for key, value in list(config.items()):
            if key == ".include":
                continue
            config[key] = self._process_includes(value)

        if ".include" in config:
            include_val = config.pop(".include")
            paths: list[Any] = []
            if isinstance(include_val, str):
                paths = [include_val]
            elif isinstance(include_val, list):
                paths = include_val

            merged_included: dict[str, Any] = {}
            for path_str in paths:
                if not isinstance(path_str, str):
                    continue
                path = self._resolve_path(path_str)
                included = load_config_file(path)
                merged_included = deep_merge(merged_included, included)

            # Local overrides included (implied best practice)
            return deep_merge(merged_included, config)

        return config

    def setup(self) -> list[Path]:
        """
        Finds and loads the configuration files, interpolating the environment variables at the end.
        """
        merged_config: dict[str, Any] = {}
        loaded_paths: list[Path] = []

        for config_file in self.find_paths(pattern="*.*"):
            try:
                key = config_file.key.split(".") if config_file.key else None
                merged_config = deep_merge(merged_config, config_file.load(), key=key)
            except FileNotFoundError:
                continue
            else:
                loaded_paths.append(config_file.path)

        merged_config = self._process_includes(merged_config)
        interpolated_config = interpolate_envvars(merged_config, env=os.environ)

        final_config = interpolated_config
        object.__setattr__(self, "raw_configs", final_config)

        LOGGER.debug("loaded config files: %s", " ".join(map(str, loaded_paths)))
        return loaded_paths

    # When type T is provided
    @overload
    def get(self, key: Iterable[str], t: type[T], *, nullable: Literal[True]) -> Optional[T]: ...

    @overload
    def get(self, key: Iterable[str], t: type[T], *, nullable: Literal[False]) -> T: ...

    @overload
    def get(self, key: Iterable[str], t: type[T], *, default: None) -> Optional[T]: ...

    @overload
    def get(self, key: Iterable[str], t: type[T], *, default: T) -> T: ...

    @overload
    def get(self, key: Iterable[str], t: type[T]) -> T: ...

    # When type is not provided
    @overload
    def get(self, key: Iterable[str], *, nullable: Literal[True]) -> Optional[Any]: ...

    @overload
    def get(self, key: Iterable[str], *, default: Any) -> Any: ...

    @overload
    def get(self, key: Iterable[str]) -> Any: ...

    def get(
        self,
        key: Union[str, Iterable[str]],
        t: Optional[type[T]] = None,
        *,
        nullable: Optional[bool] = None,
        default: Any = NO_DEFAULT,
    ) -> Any:
        """
        Gets a value from the raw config

        Args:
            key: the key path or the dot-separated key to get the value from
            t: the type to convert the value to. If not given, the value is returned as is.
            nullable: whether the value can be null or missing.
            default: the default value to return if the key is not found or the value is null.

        Raises:
            KeyError: if the key is not found or the value is null for a required config.
            ValueError: if there's a value, but it cannot be converted to the given type.
            TypeError: if both nullable and default are specified.
        """

        if isinstance(key, str):
            keys = key.removeprefix("$").removeprefix(".").split(".")
        else:
            keys = list(key)

        assert self.converter is not None, "converter not set yet"
        assert self.raw_configs is not None, "raw configs not set yet"

        return self.converter(raw=self.raw_configs, keys=keys, t=t, nullable=nullable, default=default)  # type: ignore
