__all__ = (
    "SparkMagicConfigurator",
    "SparkMagicConfig",
)

import dataclasses
import json
import logging
import os
from pathlib import Path
from typing import Annotated

from livy_uploads.configs.base import Configurable
from livy_uploads.project import Project

LOGGER = logging.getLogger(__name__)

SPARKMAGIC_ENVVAR = "SPARKMAGIC_CONF_DIR"


@dataclasses.dataclass
class SparkMagicConfig:
    conf_dir: Annotated[Path, "sparkr.resolve=path"]


class SparkMagicConfigurator(Configurable):
    """
    Configures sparkmagic.
    """

    def setup(self) -> None:
        """
        Sets the SPARKMAGIC_CONF_DIR environment variable to the basedir.
        """
        project = Project.get()

        conf_dir = project.configs.get(["sparkmagic"], SparkMagicConfig).conf_dir
        confs = project.configs.get(["sparkmagic"], dict)
        confs.pop("conf_dir")

        LOGGER.debug("writing final sparkmagic config to %s", conf_dir)
        conf_path = conf_dir / "config.json"
        conf_path.parent.mkdir(parents=True, exist_ok=True)
        conf_path.write_text(json.dumps(confs, indent=2))

        LOGGER.info("setting %s=%s", SPARKMAGIC_ENVVAR, conf_dir)
        os.environ[SPARKMAGIC_ENVVAR] = str(conf_dir)
