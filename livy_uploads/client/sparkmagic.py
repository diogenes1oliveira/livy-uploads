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
from livy_uploads.models.sparkmagic import SPARKMAGIC_CONFIG_ENVVAR, SparkMagicConfig
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
    def setup(cls, basedir: Path) -> Self:
        """
        Loads and configures SparkMagic.
        """
        value = os.getenv(SPARKMAGIC_CONFIG_ENVVAR)
        conf_dir = Path(value) if value else Path.home() / ".config" / "sparkmagic"
        LOGGER.info("loading sparkmagic config from %s", conf_dir)
        json_path = conf_dir / "config.json"
        if not json_path.exists():
            raise FileNotFoundError(f"SparkMagic config file not found: {json_path}")
        with json_path.open("r") as fp:
            config = SparkMagicConfig.parse(json.load(fp))

        return cls(config=config, basedir=basedir)

    def get_session_post_json(self) -> dict[str, Any]:
        """
        Returns a JSON object that can be used to POST to /sessions.
        """
        configs = deep_merge(self.config.session_configs_defaults, self.config.session_configs)
        configs.pop("owner", None)
        return configs
