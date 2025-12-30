import json
import logging
import os
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Optional

from livy_uploads.plugins.base import SetupPlugin
from livy_uploads.utils.typeutils import as_type

LOGGER = logging.getLogger(__name__)


# mypy: disable-error-code="import-untyped"


def install_sparkmagic_uv_kernel(
    basename: str,
    project_root: Path,
    env_filename: str,
    display_name: Optional[str] = None,
    user: bool = True,
) -> None:
    from sparkmagic import __file__ as sparkmagic_file

    display_name = display_name or basename
    kernel_name = re.sub(r"[^a-zA-Z0-9]", "-", display_name).lower()
    kernel_name = re.sub(r"-+", "-", kernel_name).strip("-")

    kernel_basedir = Path(sparkmagic_file).parent / "kernels" / basename
    kernel_base_json = kernel_basedir / "kernel.json"
    if not kernel_base_json.is_file():
        raise ValueError(f"kernel.json not found at {kernel_basedir}")

    LOGGER.info("installing sparkmagic server extension")
    args = [
        "jupyter-serverextension",
        "enable",
        "--sys-prefix",
        "--py",
        "sparkmagic",
    ]
    subprocess.run(args, check=True)

    LOGGER.info("installing kernel %r from %s", kernel_name, kernel_basedir)
    args = [
        "jupyter-kernelspec",
        "install",
        "--replace",
        *(["--user"] if user else []),
        f"--name={kernel_name}",
        str(kernel_basedir),
    ]
    subprocess.run(args, check=True)
    patch_uv_kernel(kernel_name, display_name, project_root, env_filename)


def patch_uv_kernel(
    kernel_name: str,
    display_name: str,
    project_root: Path,
    env_filename: str,
) -> None:
    LOGGER.info("getting path to installed kernel %s", kernel_name)
    proc = subprocess.run(
        ["jupyter-kernelspec", "list"],
        check=True,
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith(kernel_name):
            continue
        kernel_path = line.split(maxsplit=1)[1]
        kernel_json = Path(kernel_path) / "kernel.json"
        break
    else:
        raise ValueError(f"failed to find installed kernel {kernel_name!r}")

    LOGGER.info("loading kernel config from %s", kernel_json)
    with kernel_json.open("r") as fp:
        kernel_config = as_type(json.load(fp), dict)

    prefix = [
        "uv",
        "--project",
        str(project_root.absolute()),
        "run",
        "--env-file",
        str((project_root / env_filename).absolute()),
    ]
    kernel_config["argv"] = prefix + kernel_config["argv"]
    kernel_config["display_name"] = display_name
    kernel_tmp_json = kernel_json.with_name(f".{kernel_json.name}.tmp")

    LOGGER.info("writing final uv-prefixed kernel config to %s", kernel_json)
    with kernel_tmp_json.open("w") as fp:
        json.dump(kernel_config, fp, indent=2)
    kernel_tmp_json.replace(kernel_json)


class SparkMagicUvPysparkSetup(SetupPlugin):
    """
    Sets up the SparkMagic configuration into $SPARKMAGIC_CONF_DIR.
    """

    def setup(self, basedir: os.PathLike, env_filename: str, env: Mapping[str, str]) -> dict[str, str]:
        basedir = Path(basedir).absolute()
        if not (basedir / "pyproject.toml").is_file():
            LOGGER.warning("no pyproject.toml found in %s, skipping", basedir)
            return {}

        display_name = env.get("SPARKMAGIC_PYSPARK_KERNEL_NAME")
        if not display_name:
            LOGGER.warning("$SPARKMAGIC_PYSPARK_KERNEL_NAME is not set, skipping")
            return {}

        root_mode = (env.get("SPARKMAGIC_PYSPARK_ROOT_MODE") or "").lower() in ("true", "1", "yes", "y")
        user = not root_mode

        install_sparkmagic_uv_kernel(
            basename="pysparkkernel",
            project_root=basedir,
            env_filename=env_filename,
            display_name=display_name,
            user=user,
        )
        return {
            "SPARKMAGIC_PYSPARK_KERNEL_NAME": display_name,
            "SPARKMAGIC_PYSPARK_ROOT_MODE": "false" if user else "true",
        }
