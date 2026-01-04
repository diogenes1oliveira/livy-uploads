import json

import pytest
from click.testing import CliRunner

from livy_uploads.cli.__main__ import cli

# mypy: disable-error-code=no-untyped-def


@pytest.fixture(autouse=True)
def setup_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SPARKRL_PLUGINS", "")


@pytest.mark.parametrize(
    ["args", "expected_lines"],
    [
        (
            ["list", "--no-resolve", "--format=compact"],
            [
                "entrypoint://sparkrl.plugins.*/",
            ],
        ),
        (
            ["list", "--format=compact"],
            [
                "entrypoint://sparkrl.plugins.commands/",
                "module://livy_uploads",
                "entrypoint://sparkrl.plugins.patches/",
            ],
        ),
    ],
)
def test_plugins_cli(args, expected_lines):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, args=["plugins", *args])

    assert result.exit_code == 0, result.output

    output_lines = result.stdout.strip().splitlines()
    assert output_lines == expected_lines


def test_plugins_constants():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, args=["plugins", "constants"])

    assert result.exit_code == 0, result.output

    actual = json.loads(result.stdout)
    assert actual == {
        "APPNAME": "sparkrl",
        "PLUGINS_ENV": "SPARKRL_PLUGINS",
        "GROUP_PREFIX": "sparkrl.plugins.",
        "LOADERS_GROUP": "sparkrl.plugins.loaders",
    }
