import json
import logging
import os
from collections.abc import Mapping
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Optional

from livy_uploads.configs.envfile import envfile_load, envfile_save
from livy_uploads.configs.templates import JinjaRenderer
from livy_uploads.plugins.base import SetupPlugin
from livy_uploads.plugins.load import load_plugins
from livy_uploads.utils.datautils import deep_merge

LOGGER = logging.getLogger(__name__)
jinja_renderer = JinjaRenderer()


def save_config(env_filename: Optional[str] = None, quote: bool = False, appname: Optional[str] = None) -> None:
    env_filename = env_filename or ".env"

    env_path, envs = envfile_load(env_filename)
    if env_path is None:
        LOGGER.warning("no %s or %s.example found, skipping", env_filename, env_filename)
        return

    plugin_cls_list = load_plugins(SetupPlugin)
    if not plugin_cls_list:
        LOGGER.info("no setup plugins found, skipping")
        return

    overrides: dict[str, str] = {}
    basedir = env_path.parent

    for i, plugin_cls in enumerate(plugin_cls_list):
        LOGGER.info("loading plugin %s (%d/%d)", plugin_cls.__name__, i + 1, len(plugin_cls_list))
        plugin = plugin_cls()  # type: ignore

        plugin_overrides = plugin.setup(basedir, env_filename, envs)
        LOGGER.info(
            "plugin %s provided %d overrides: %s",
            plugin_cls.__name__,
            len(plugin_overrides),
            ", ".join(plugin_overrides.keys()),
        )
        overrides.update(plugin_overrides)

    if not overrides:
        LOGGER.info("no overrides provided by any plugins, skipping")
        return

    LOGGER.info("saving overrides to %s", env_path)
    envfile_save(env_path, overrides, quote=quote, name=appname)

    os.environ.update(overrides)


def load_configs(env: Mapping[str, str], *paths: os.PathLike) -> dict[str, Any]:
    """
    Loads configuration files from the given paths and returns a dictionary of the loaded configurations.

    Each path may be an YAML, TOML or JSON file. If the filename contains a `.j2` suffix, the file is treated
    as a Jinja2 template, receiving the `env` dictionary as the context.

    Args:
        env: The environment variables to use for interpolation.
        paths: The paths to the configuration files to load.

    Returns:
        A dictionary of the loaded configurations.
    """

    all_configs: dict[str, Any] = {}
    loaded_paths: list[Path] = []

    for path in paths:
        path = Path(path).absolute()
        with ExitStack() as stack:
            try:
                if ".j2" in path.name:
                    fp = jinja_renderer.render(path.read_text(), env=env)
                    suffix = path.with_name(path.name.replace(".j2", "")).suffix
                    stack.enter_context(fp)
                else:
                    fp = stack.enter_context(path.open("r"))  # type: ignore
                    suffix = path.suffix
            except FileNotFoundError:
                continue

            if suffix == ".toml":
                import toml

                configs = toml.load(fp)
            elif suffix == ".yaml":
                import yaml

                configs = yaml.load(fp, Loader=yaml.SafeLoader)
            elif suffix == ".json":
                configs = json.load(fp)
            else:
                raise ValueError(f"unsupported configuration file format: {suffix!r}")

            all_configs = deep_merge(all_configs, configs)
            loaded_paths.append(path)

    LOGGER.info("loaded %d config files: %s", len(loaded_paths), ", ".join(str(p) for p in loaded_paths))
    return all_configs
