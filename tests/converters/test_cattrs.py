# mypy: disable-error-code="no-untyped-def"
import base64
from pathlib import Path, PurePosixPath
from typing import Annotated
from unittest.mock import MagicMock

import pytest

from livy_uploads.converters.cattrs import CattrsConverter, register_cattrs_path_resolver


class TestRegisterCattrsPathResolver:
    @pytest.fixture
    def basedir(self, tmp_path: Path):
        d = tmp_path / "basedir"
        d.mkdir()
        return d

    @pytest.fixture
    def cachedir(self, tmp_path: Path):
        d = tmp_path / "cachedir"
        d.mkdir()
        return d

    @pytest.fixture
    def mock_project(self, basedir: Path, cachedir: Path, monkeypatch):
        from livy_uploads.project import project

        mock = MagicMock()
        mock.basedir = basedir
        mock.cachedir = cachedir

        monkeypatch.setattr(project.Project, "get", lambda: mock)

        return mock

    @pytest.fixture
    def converter(self, mock_project):
        conv = CattrsConverter()
        conv.setup()
        register_cattrs_path_resolver(conv)
        return conv

    @pytest.mark.parametrize(
        "value,annotated_type,expected_content",
        [
            pytest.param(
                base64.b64encode(b"binary data").decode("utf-8"),
                Annotated[Path, "sparkrl.resolve=path-or-data:file_{uuid}.bin"],
                b"binary data",
                id="binary_base64",
            ),
            pytest.param(
                "'single quoted text'",
                Annotated[Path, "sparkrl.resolve=path-or-text:file_{uuid}.txt"],
                b"single quoted text",
                id="text_single_quoted",
            ),
            pytest.param(
                '"double quoted json"',
                Annotated[Path, "sparkrl.resolve=path-or-text:file_{uuid}.txt"],
                b"double quoted json",
                id="text_json",
            ),
            pytest.param(
                "base64:" + base64.b64encode(b"base64 prefixed").decode("utf-8"),
                Annotated[Path, "sparkrl.resolve=path-or-text:file_{uuid}.txt"],
                b"base64 prefixed",
                id="text_base64_prefix",
            ),
            pytest.param(
                "raw string content",
                Annotated[Path, "sparkrl.resolve=path-or-text:file_{uuid}.txt"],
                b"raw string content",
                id="text_raw_string",
            ),
        ],
    )
    def test_content_resolution(
        self, converter: CattrsConverter, cachedir: Path, value: str, annotated_type, expected_content: bytes
    ):
        assert converter.instance is not None
        result = converter.instance.structure(value, annotated_type)

        assert isinstance(result, Path)
        assert result.parent == cachedir
        assert result.read_bytes() == expected_content

    @pytest.mark.parametrize(
        "path_value,annotated_type,expected_path",
        [
            pytest.param(
                "./another/file.txt",
                Annotated[Path, "sparkrl.resolve=path-or-text:{uuid}.txt"],
                lambda basedir: basedir / "./another/file.txt",
                id="relative_dotslash_Path",
            ),
            pytest.param(
                Path("rel/path.txt"),
                Annotated[Path, "sparkrl.resolve=path-or-text:{uuid}.txt"],
                lambda basedir: basedir / "rel/path.txt",
                id="relative_Path_object",
            ),
            pytest.param(
                "/absolute/path.txt",
                Annotated[Path, "sparkrl.resolve=path-or-text:{uuid}.txt"],
                lambda basedir: Path("/absolute/path.txt"),
                id="absolute_path_Path",
            ),
            pytest.param(
                Path("/abs/file.txt"),
                Annotated[Path, "sparkrl.resolve=path-or-text:{uuid}.txt"],
                lambda basedir: Path("/abs/file.txt"),
                id="absolute_Path_object",
            ),
            pytest.param(
                PurePosixPath("rel/path.txt"),
                Annotated[PurePosixPath, "sparkrl.resolve=path-or-text:{uuid}.txt"],
                lambda basedir: basedir / "rel/path.txt",
                id="relative_PurePosixPath_object",
            ),
        ],
    )
    def test_path_resolution(
        self, converter: CattrsConverter, basedir: Path, path_value, annotated_type, expected_path
    ):
        expected = expected_path(basedir)

        assert converter.instance is not None
        result = converter.instance.structure(path_value, annotated_type)

        assert result == expected
