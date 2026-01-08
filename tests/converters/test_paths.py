# mypy: disable-error-code="no-untyped-def"
import base64
from pathlib import Path, PurePosixPath

import pytest

from livy_uploads.converters.paths import resolve_path_or_content


class TestResolvePathOrContent:
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

    def test_path_resolution_absolute_path(self, basedir: Path, cachedir: Path):
        abs_path = basedir / "test.txt"
        abs_path.touch()

        result = resolve_path_or_content(
            value=abs_path, mode="text", filename="test.txt", t=Path, basedir=basedir, cachedir=cachedir
        )
        assert result == abs_path

    def test_path_resolution_relative_path_string_with_slash(self, basedir: Path, cachedir: Path):
        # String with "/" in first 3 chars -> treated as path
        rel_path_str = "./sub/test.txt"
        (basedir / "sub").mkdir()
        (basedir / "sub" / "test.txt").touch()

        result = resolve_path_or_content(
            value=rel_path_str, mode="text", filename="test.txt", t=Path, basedir=basedir, cachedir=cachedir
        )
        assert result == basedir / "sub" / "test.txt"

    def test_path_resolution_relative_path_object(self, basedir: Path, cachedir: Path):
        rel_path = Path("sub/test.txt")

        result = resolve_path_or_content(
            value=rel_path, mode="text", filename="test.txt", t=Path, basedir=basedir, cachedir=cachedir
        )
        assert result == basedir / "sub" / "test.txt"

    def test_content_resolution_binary_mode(self, basedir: Path, cachedir: Path):
        content = b"some binary data"
        encoded = base64.b64encode(content).decode("utf-8")

        result = resolve_path_or_content(
            value=encoded, mode="binary", filename="binary_{uuid}.dat", t=Path, basedir=basedir, cachedir=cachedir
        )

        assert result.parent == cachedir
        assert result.read_bytes() == content
        # Check permissions (0o600 is 384 in decimal)
        assert (result.stat().st_mode & 0o777) == 0o600

    def test_content_resolution_text_mode_single_quoted(self, basedir: Path, cachedir: Path):
        value = "'quoted text'"

        result = resolve_path_or_content(
            value=value, mode="text", filename="text_{uuid}.txt", t=Path, basedir=basedir, cachedir=cachedir
        )

        assert result.parent == cachedir
        assert result.read_text("utf-8") == "quoted text"

    def test_content_resolution_text_mode_double_quoted_json(self, basedir: Path, cachedir: Path):
        value = '"json string"'

        result = resolve_path_or_content(
            value=value, mode="text", filename="json_{uuid}.txt", t=Path, basedir=basedir, cachedir=cachedir
        )

        assert result.parent == cachedir
        assert result.read_text("utf-8") == "json string"

    def test_content_resolution_text_mode_base64_prefix(self, basedir: Path, cachedir: Path):
        content = b"decoded content"
        encoded = "base64:" + base64.b64encode(content).decode("utf-8")

        result = resolve_path_or_content(
            value=encoded, mode="text", filename="b64_{uuid}.txt", t=Path, basedir=basedir, cachedir=cachedir
        )

        assert result.parent == cachedir
        assert result.read_bytes() == content

    def test_content_resolution_text_mode_raw_string(self, basedir: Path, cachedir: Path):
        value = "raw string content"

        result = resolve_path_or_content(
            value=value, mode="text", filename="raw_{uuid}.txt", t=Path, basedir=basedir, cachedir=cachedir
        )

        assert result.parent == cachedir
        assert result.read_text("utf-8") == "raw string content"

    def test_invalid_mode_raises_error(self, basedir: Path, cachedir: Path):
        with pytest.raises(ValueError, match="unknown mode 'invalid'"):
            resolve_path_or_content(
                value="stuff",
                mode="invalid",  # type: ignore
                filename="out.txt",
                t=Path,
                basedir=basedir,
                cachedir=cachedir,
            )

    def test_pure_posix_path_return_type(self, basedir: Path, cachedir: Path):
        # Even if we request PurePosixPath, resolved paths on local fs will effectively be Paths used as PurePosixPath
        # but let's verify the return type cast behavior or at least that it runs

        result = resolve_path_or_content(
            value="content",
            mode="text",
            filename="posix_{uuid}.txt",
            t=PurePosixPath,
            basedir=basedir,
            cachedir=cachedir,
        )

        assert isinstance(result, PurePosixPath)
