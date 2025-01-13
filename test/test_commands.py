from datetime import datetime, timedelta
import hashlib
import os
from pathlib import Path
import shlex
import signal
import time
from uuid import uuid4

import pytest
from unittest.mock import Mock


from livy_uploads.exceptions import LivyStatementError
from livy_uploads.session import LivyEndpoint, LivySessionEndpoint
from livy_uploads.retry_policy import LinearRetryPolicy
from livy_uploads.commands import (
    LivyRunCode,
    LivyUploadFile,
    LivyUploadDir,
    LivyRunShell,
)

class TestCommands:
    endpoint = LivyEndpoint('http://localhost:8998')
    session: LivySessionEndpoint

    @classmethod
    def setup_class(cls):
        cls.session = LivySessionEndpoint.create_session(
            cls.endpoint,
            name='test-' + str(uuid4()),
            ttl='60s',
            heartbeatTimeoutInSecond=60,
        )
        cls.session.wait_ready(LinearRetryPolicy(30, 1.0))

    def test_run(self):
        now = datetime.now().astimezone()

        code_cmd = LivyRunCode(
            vars=dict(
                now=now,
            ),
            code='''
                from datetime import timedelta
                return now + timedelta(days=1)
            ''',
        )
        lines, out = code_cmd.run(self.session)
        assert not lines
        assert isinstance(out, datetime)

        actual: datetime = out
        assert actual == now + timedelta(days=1)

    def test_run_exception(self):
        code_cmd = LivyRunCode(
            code='''
                int('invalid')
            ''',
        )

        with pytest.raises(ValueError) as e:
            code_cmd.run(self.session)

        assert isinstance(e.value.__cause__, LivyStatementError)

    def test_run_return_unpickleable(self):
        code_cmd = LivyRunCode(
            code='''
                import socket
                return socket.socket()
            ''',
        )

        with pytest.raises(TypeError) as e:
            code_cmd.run(self.session)

        assert isinstance(e.value.__cause__, LivyStatementError)

    def test_upload_file(self, tmp_path: Path):
        data = os.urandom(4096)
        path = tmp_path / 'data.bin'
        path.write_bytes(data)

        mock = Mock()
        dest_path = f'tmp/data-{uuid4()}.bin'
        upload_cmd = LivyUploadFile(
            source_path=path,
            dest_path=dest_path,
            chunk_size=len(data) // 4,
            progress_func=mock,
        )
        _, actual_path = upload_cmd.run(self.session)
        assert mock.call_count == 4

        test_cmd = LivyRunCode(
            vars=dict(
                dest_path=dest_path,
            ),
            code='''
                import hashlib
                import os
                import stat

                return (
                    os.getcwd(),
                    hashlib.md5(open(dest_path, 'rb').read()).hexdigest(),
                    oct(stat.S_IMODE(os.stat(dest_path).st_mode)),
                )
            ''',
        )
        _, (chdir, actual_md5, mode) = test_cmd.run(self.session)

        expected_md5 = hashlib.md5(data).hexdigest()

        assert actual_path == chdir + '/' + dest_path
        assert actual_md5 == expected_md5
        assert mode == '0o600'

    def test_upload_dir(self, tmp_path: Path):
        (tmp_path / 'foo').write_text('foo')
        (tmp_path / 'inner').mkdir()
        (tmp_path / 'inner' / 'bar').write_text('bar')

        dest_path = f'tmp/dir-{uuid4()}'
        upload_cmd = LivyUploadDir(
            source_path=tmp_path,
            dest_path=dest_path,
        )
        upload_cmd.run(self.session)

        test_cmd = LivyRunShell(f'find {shlex.quote(dest_path)} -type f')
        output, returncode = test_cmd.run(self.session)
        lines = list(sorted(output.splitlines()))

        assert returncode == 0
        assert lines[0].endswith('/foo')
        assert lines[1].endswith('/inner/bar')
        assert len(lines) == 2

    def test_shell_timeout(self):
        test_cmd = LivyRunShell(f'sleep 10', run_timeout=3, stop_timeout=2)
        t0 = time.monotonic()
        output, returncode = test_cmd.run(self.session)
        dt = time.monotonic() - t0
        lines = list(sorted(output.splitlines()))

        assert lines == []
        assert returncode == -int(signal.SIGTERM)
        assert 3 <= dt < 10
