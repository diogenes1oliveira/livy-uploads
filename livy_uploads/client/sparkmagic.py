__all__ = ("SparkMagic",)

import functools
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from typing_extensions import Self

from livy_uploads.client.managers import SessionManager
from livy_uploads.endpoint import LivyEndpoint
from livy_uploads.models.sparkmagic import SPARKMAGIC_CONFIG_ENVVAR, SPARKMAGIC_PROFILES_ENVVAR, SparkMagicConfig
from livy_uploads.paths import find_first_in_paths
from livy_uploads.utils.datautils import deep_merge

LOGGER = logging.getLogger(__name__)


class SparkMagic:
    def __init__(self, config: SparkMagicConfig, basedir: Optional[Path] = None) -> None:
        """
        Initializes a new SparkMagic instance.
        """
        self.config = config
        self.basedir = basedir or Path.cwd()

    @functools.cached_property
    def endpoint(self) -> LivyEndpoint:
        if self.config.kerberos_config is not None:
            from livy_uploads.auth import Authenticator

            authenticator = Authenticator.from_config(self.config.kerberos_config.as_json())
        else:
            authenticator = None

        return LivyEndpoint(
            url=self.config.livy_url,
            default_headers=self.config.custom_headers,
            proxy=self.config.http_config.proxy,
            verify=self.config.http_config.verify,
            authenticator=authenticator,
        )

    @functools.cached_property
    def manager(self) -> SessionManager:
        return SessionManager(endpoint=self.endpoint, config=self.config)

    @classmethod
    def setup(cls, conf_dir: Optional[str] = None, profiles: Optional[tuple[str, ...]] = None) -> Self:
        """
        Loads and configures SparkMagic.
        """
        if conf_dir := os.getenv(SPARKMAGIC_CONFIG_ENVVAR):
            conf_dir_path = find_first_in_paths([conf_dir], pathspec=[".dev", "."])
        else:
            conf_dir_path = find_first_in_paths(["conf" + os.path.sep + "sparkmagic"], pathspec=[".dev", "."])

        if profiles is None:
            profiles = tuple((os.getenv(SPARKMAGIC_PROFILES_ENVVAR) or "defaults").replace(",", " ").split())

        config = cls.write_sparkmagic_config(conf_dir_path, profiles)
        return cls(config=config, basedir=conf_dir_path)

    @classmethod
    def write_sparkmagic_config(cls, conf_dir: Path, profiles: tuple[str, ...]) -> SparkMagicConfig:
        conf_dir = conf_dir.absolute()
        profile = ",".join(profiles or ("defaults",))

        LOGGER.info("setting $%s=%s", SPARKMAGIC_CONFIG_ENVVAR, conf_dir)
        LOGGER.info("setting $%s=%s", SPARKMAGIC_PROFILES_ENVVAR, profile)
        os.environ[SPARKMAGIC_CONFIG_ENVVAR] = str(conf_dir)
        os.environ[SPARKMAGIC_PROFILES_ENVVAR] = profile

        merged: dict[str, Any] = {}

        for profile in profiles:
            candidates = [
                f"config.{profile}.toml",
                f"config.{profile}.yaml",
                f"config.{profile}.json",
            ]
            for candidate in candidates:
                conf_input_path = conf_dir / candidate
                try:
                    with conf_input_path.open("r") as fp:
                        LOGGER.info("loading SparkMagic configuration from %s", conf_input_path)
                        if conf_input_path.suffix == ".toml":
                            import toml

                            merged = deep_merge(merged, toml.load(fp))
                        elif conf_input_path.suffix == ".yaml":
                            import yaml

                            merged = deep_merge(merged, yaml.load(fp, Loader=yaml.SafeLoader))
                        elif conf_input_path.suffix == ".json":
                            merged = deep_merge(merged, json.load(fp))
                        else:
                            raise ValueError(f"unsupported configuration file format: {conf_input_path.suffix}")
                except FileNotFoundError:
                    pass

        config = SparkMagicConfig.parse(merged, basedir=conf_dir)
        conf_output_path = conf_dir / "config.json"
        LOGGER.info("writing merged SparkMagic configuration to %s", conf_output_path)
        with conf_output_path.open("w") as fp:
            json.dump(merged, fp, indent=2)

        return config

    def get_session_post_json(self) -> dict[str, Any]:
        """
        Returns a JSON object that can be used to POST to /sessions.
        """
        configs = deep_merge(self.config.session_configs_defaults, self.config.session_configs)
        configs.pop("owner", None)
        return configs
