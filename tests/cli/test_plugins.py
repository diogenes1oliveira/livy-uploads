import json
from collections.abc import Mapping
from typing import Any

import pytest
from click.testing import CliRunner

from livy_uploads.cli.__main__ import cli

# mypy: disable-error-code=no-untyped-def


@pytest.fixture(autouse=True)
def setup_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure plugins are loaded from the test environment
    monkeypatch.setenv("SPARKRL_PLUGINS", "!file://./")


@pytest.mark.parametrize(
    ["args", "expected_lines"],
    [
        (
            ["list", "--no-resolve", "--format=compact"],
            [
                "entrypoint://.*/",
                "impl://.*",
            ],
        ),
        (
            ["list", "--format=compact"],
            [
                "entrypoint://sparkrl.plugins.commands/",
                "entrypoint://sparkrl.plugins.configurables/",
                "module://livy_uploads",
                "entrypoint://sparkrl.plugins.patches/",
                "impl://sparkrl.plugins.commands",
                "impl://sparkrl.plugins.configurables",
                "impl://sparkrl.plugins.patches",
            ],
        ),
    ],
)
def test_plugins_list(args, expected_lines):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, args=["plugins", *args])

    assert result.exit_code == 0, result.output

    output_lines = result.stdout.strip().splitlines()
    assert output_lines == expected_lines


@pytest.mark.parametrize(
    ["args", "expected_lines"],
    [
        (
            ("impls", "--format=compact"),
            [
                "entrypoint://sparkrl.plugins.commands/base#SessionCommand",
                "entrypoint://sparkrl.plugins.configurables/base#Configurable",
                "entrypoint://sparkrl.plugins.patches/base#Patch",
            ],
        ),
        (
            ("impls", "--format=compact", "scan"),
            [
                "entrypoint://sparkrl.plugins.commands/base#SessionCommand",
                "entrypoint://sparkrl.plugins.commands/infos#SessionInfoCommand",
                "entrypoint://sparkrl.plugins.configurables/base#Configurable",
                "entrypoint://sparkrl.plugins.patches/base#Patch",
                "entrypoint://sparkrl.plugins.patches/sparkmagic_reload#SparkMagicReloadPatch",
            ],
        ),
    ],
)
def test_plugins_impls(args, expected_lines):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, args=["plugins", *args])

    assert result.exit_code == 0, result.output

    output_lines = result.stdout.strip().splitlines()
    assert output_lines == expected_lines


def test_plugins_one_impl():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, args=["plugins", "impls", "--format=json"])

    assert result.exit_code == 0, result.output

    infos_by_name = {item["name"]: _keep_only(item, "name", "type", "group") for item in json.loads(result.stdout)}

    command_info = infos_by_name["SessionCommand"]
    assert command_info == {
        "name": "SessionCommand",
        "type": "interface",
        "group": "sparkrl.plugins.commands",
    }


def test_plugins_constants():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, args=["plugins", "constants"])

    assert result.exit_code == 0, result.output

    actual = json.loads(result.stdout)
    assert actual == {
        "PROJECT_APPNAME": "sparkrl",
        "PLUGINS_ENV": "SPARKRL_PLUGINS",
        "PROFILES_ENV": "SPARKRL_PROFILES",
        "GROUP_PREFIX": "sparkrl.plugins.",
        "LOADERS_GROUP": "sparkrl.plugins.loaders",
    }


def _keep_only(info: Mapping[str, Any], *attrs: str) -> dict[str, Any]:
    return {k: v for k, v in info.items() if k in attrs}
