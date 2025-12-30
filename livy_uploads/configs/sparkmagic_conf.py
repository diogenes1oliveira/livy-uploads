import json
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from livy_uploads.configs.setup import load_configs
from livy_uploads.plugins.base import SetupPlugin

LOGGER = logging.getLogger(__name__)


class SparkMagicConfSetup(SetupPlugin):
    """
    Sets up the SparkMagic configuration into $SPARKMAGIC_CONF_DIR.
    """

    def __init__(self, basedir: Optional[os.PathLike] = None, env: Optional[Mapping[str, str]] = None):
        self.basedir = Path(basedir or Path.cwd()).absolute()
        self.env = env if env is not None else os.environ

    def setup(self, basedir: os.PathLike, env_filename: str, env: Mapping[str, str]) -> dict[str, str]:
        self.basedir = Path(basedir).absolute()
        self.env = env

        if (config_file := self.config_file) is not None:
            merged_config = self.load_config()
            LOGGER.info("writing merged config to %s", config_file)
            with config_file.open("w") as fp:
                json.dump(merged_config, fp, indent=2)

        return {
            "SPARKMAGIC_CONF_DIR": str(self.config_dir) if self.config_dir else "",
            "SPARKMAGIC_CONF_PROFILES": ",".join(self.config_profiles),
        }

    def load_config(self) -> dict[str, Any]:
        files = self.config_input_files
        LOGGER.info("loading SparkMagic configuration from %d files: %s", len(files), list(map(str, files)))
        return load_configs(self.env, *files)

    @property
    def config_dir(self) -> Optional[Path]:
        value = self.env.get("SPARKMAGIC_CONF_DIR")
        if not value:
            return None
        path = Path(value)
        if not path.is_absolute():
            path = self.basedir / path
        return path.absolute()

    @property
    def config_profiles(self) -> list[str]:
        value = (self.env.get("SPARKMAGIC_CONF_PROFILES") or "").strip()
        values = value.replace(",", " ").split()

        return values or ["defaults"]

    @property
    def config_input_files(self) -> list[Path]:
        if self.config_dir is None:
            return []

        candidates: list[Path] = []
        for profile in self.config_profiles:
            for format in ("toml", "yaml", "json"):
                candidates.append(self.config_dir / f"config-{profile}.{format}")
                candidates.append(self.config_dir / f"config-{profile}.j2.{format}")

        return [path for path in candidates if path.exists()]

    @property
    def config_file(self) -> Optional[Path]:
        if self.config_dir is None:
            return None

        return self.config_dir / "config.json"
