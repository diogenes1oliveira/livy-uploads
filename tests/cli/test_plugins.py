from click.testing import CliRunner
import pytest

from livy_uploads.cli.__main__ import cli


# mypy: ignore-error-code=no-untyped-def


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
                "module://livy_uploads",
            ],
        ),
        (
            ["list", "--format=compact"],
            [
                "entrypoint://sparkrl.plugins.commands/",
                "entrypoint://sparkrl.plugins.patches/",
                "module://livy_uploads",
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
