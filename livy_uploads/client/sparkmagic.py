__all__ = ("SparkMagic",)

import functools
import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

from typing_extensions import Self

from livy_uploads.client.managers import SessionManager
from livy_uploads.client.models.sparkmagic import SPARKMAGIC_CONFIG_ENVVAR, SparkMagicConfig
from livy_uploads.endpoint import LivyEndpoint
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
    def setup(cls, conf_dir: Optional[str] = None) -> Self:
        """
        Loads and configures SparkMagic.
        """
        if conf_dir:
            conf_dir_path = find_first_in_paths([conf_dir], pathspec=[".dev", "."])
        else:
            conf_dir_path = find_first_in_paths(["conf/sparkmagic"], pathspec=[".dev", "."])

        conf_dir_path = conf_dir_path.absolute()
        conf_path = conf_dir_path / "config.json"
        LOGGER.info("loading SparkMagic configuration from %s", conf_path)

        with conf_path.open("r") as fp:
            body = json.load(fp)

        config = SparkMagicConfig.parse(body, basedir=conf_dir_path)
        LOGGER.info("setting $%s=%s", SPARKMAGIC_CONFIG_ENVVAR, conf_dir_path)
        os.environ[SPARKMAGIC_CONFIG_ENVVAR] = str(conf_dir_path)

        return cls(config=config, basedir=conf_dir_path)

    def get_session_post_json(self) -> dict[str, Any]:
        """
        Returns a JSON object that can be used to POST to /sessions.
        """
        configs = deep_merge(self.config.session_configs_defaults, self.config.session_configs)
        configs.pop("owner", None)
        return configs
