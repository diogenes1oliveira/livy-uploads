from datetime import datetime, timezone
import time
from typing import List, Tuple
from uuid import uuid4

import pytest


from livy_uploads.process.popen import PopenProcess
from livy_uploads.process import popen

from livy_uploads.process.manager import ProcessManager
from livy_uploads.exceptions import LivyError
from livy_uploads.session import LivyEndpoint, LivySessionEndpoint
from livy_uploads.retry_policy import LinearRetryPolicy


process_class = PopenProcess.__name__
process_spec = popen.__name__ + ':' + process_class


class TestProcessManager:
    endpoint = LivyEndpoint('http://localhost:8998')
    session: LivySessionEndpoint

    def setup_method(self, m):
        self.session = LivySessionEndpoint.create_session(
            self.__class__.endpoint,
            name='test-' + m.__name__ + '-' + str(uuid4()),
            ttl='60s',
            heartbeatTimeoutInSecond=60,
        )
        self.session.wait_ready(LinearRetryPolicy(30, 1.0))

    def test_stream(self):
        manager = ProcessManager(self.session)
        manager.initialize()
        manager.register(process_spec)

        pid = manager.start(
            'PopenProcess',
            args=[
                'bash',
                '-c',
                'for i in $(seq 1 5); do echo "$(date --utc +%s): hello #$i from $$"; sleep 4; done; exit 3',
            ],
        )
        assert pid > 0

        logs: List[Tuple[datetime, str]] = []

        def on_log(line: str):
            logs.append((datetime.now().astimezone(), line))

        t0 = time.monotonic()
        returncode = manager.follow(pid, pause=0.1, println=on_log)
        elapsed = time.monotonic() - t0

        assert returncode == 3
        assert 15 <= elapsed < 40

        for i, (local_dt, line) in enumerate(logs):
            remote_ts, _, line = line.partition(': ')
            remote_dt = datetime.fromtimestamp(float(remote_ts), tz=timezone.utc)

            assert line == f'hello #{i + 1} from {pid}'

            diff = abs((local_dt - remote_dt).total_seconds())
            assert diff < 5.0

    def test_register(self):
        manager = ProcessManager(self.session)
        manager.initialize()
        manager.register(process_spec)

        self.session.close()
        self.session.wait_closed(LinearRetryPolicy(30, 1.0))

        # shouldn't raise an error, it's already registered in the local cache
        assert manager.register(process_spec) == process_class
        assert manager.initialize() is None

        # force=True should raise an error now, since the session is closed
        with pytest.raises(LivyError):
            manager.register(process_spec, force=True)
        with pytest.raises(LivyError):
            manager.initialize(force=True)
