import hashlib
import os
import shlex
import signal
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import pytest

from conftest import LIVY_TEST_SESSION_READINESS_TIMEOUT, LIVY_TEST_SESSION_TTL
from livy_uploads.commands import LivyRunCode, LivyRunShell, LivyUploadDir, LivyUploadFile
from livy_uploads.exceptions import LivyStatementError
from livy_uploads.retry_policy import TimeoutRetryPolicy
from livy_uploads.session import LivyEndpoint, LivySession

readiness_policy = TimeoutRetryPolicy(LIVY_TEST_SESSION_READINESS_TIMEOUT, 1.0)
stop_policy = TimeoutRetryPolicy(10.0, 1.0)


# mypy: disable-error-code="no-untyped-def"


class TestCommands:
    endpoint = LivyEndpoint("http://localhost:8998")
    session: LivySession

    @classmethod
    def setup_class(cls):
        cls.session = LivySession.create(
            cls.endpoint,
            name="test-" + str(uuid4()),
            ttl=LIVY_TEST_SESSION_TTL,
            heartbeatTimeoutInSecond=60,
        )
        cls.session.wait_ready(readiness_policy)

    def test_run(self):
        now = datetime.now().astimezone()

        code_cmd = LivyRunCode(
            vars=dict(
                now=now,
            ),
            code="""
                from datetime import timedelta
                _ = now + timedelta(days=1)
            """,
        )
        lines, out = code_cmd.run(self.session)
        assert not lines
        assert isinstance(out, datetime)

        actual: datetime = out
        assert actual == now + timedelta(days=1)

    def test_run_exception(self):
        code_cmd = LivyRunCode(
            code="""
                int('invalid')
            """,
        )

        with pytest.raises(LivyStatementError) as e:
            code_cmd.run(self.session)

        assert isinstance(e.value.as_builtin(), ValueError)

    def test_run_return_unpickleable(self):
        code_cmd = LivyRunCode(
            code="""
                import socket
                _ = socket.socket()
            """,
        )

        with pytest.raises(LivyStatementError) as e:
            code_cmd.run(self.session)

        assert isinstance(e.value.as_builtin(), TypeError)

    def test_upload_file(self, tmp_path: Path):
        data = os.urandom(4096)
        path = tmp_path / "data.bin"
        path.write_bytes(data)

        mock = Mock()
        dest_path = f"tmp/data-{uuid4()}.bin"
        upload_cmd = LivyUploadFile(
            source_path=path,
            dest_path=dest_path,
            chunk_size=len(data) // 4,
            progress_func=mock,
        )
        actual_path = upload_cmd.run(self.session)
        assert mock.call_count == 4

        test_cmd = LivyRunCode(
            vars=dict(
                dest_path=dest_path,
            ),
            code="""
                import hashlib
                import os
                import stat

                _ = (
                    os.getcwd(),
                    hashlib.md5(open(dest_path, 'rb').read()).hexdigest(),
                    oct(stat.S_IMODE(os.stat(dest_path).st_mode)),
                )
            """,
        )
        _, (chdir, actual_md5, mode) = test_cmd.run(self.session)

        expected_md5 = hashlib.md5(data).hexdigest()

        assert actual_path == chdir + "/" + dest_path
        assert actual_md5 == expected_md5
        assert mode == "0o600"

    def test_upload_dir(self, tmp_path: Path):
        (tmp_path / "foo").write_text("foo")
        (tmp_path / "inner").mkdir()
        (tmp_path / "inner" / "bar").write_text("bar")

        dest_path = f"tmp/dir-{uuid4()}"
        upload_cmd = LivyUploadDir(
            source_path=tmp_path,
            dest_path=dest_path,
        )
        upload_cmd.run(self.session)

        test_cmd = LivyRunShell(f"find {shlex.quote(dest_path)} -type f")
        pid, output, returncode = test_cmd.run(self.session)
        lines = list(sorted(output.splitlines()))

        assert returncode == 0
        assert lines[0].endswith("/foo")
        assert lines[1].endswith("/inner/bar")
        assert len(lines) == 2

    def test_shell_timeout(self):
        test_cmd = LivyRunShell(f"sleep 10", run_timeout=3, stop_timeout=2)
        t0 = time.monotonic()
        _, output, returncode = test_cmd.run(self.session)
        dt = time.monotonic() - t0
        lines = list(sorted(output.splitlines()))

        assert lines == []
        assert returncode == -int(signal.SIGTERM)
        assert 3 <= dt < 10
