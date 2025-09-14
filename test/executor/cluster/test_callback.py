import logging
import json
import threading
import time
from urllib.request import urlopen, Request
from pathlib import Path

import pytest

from livy_uploads.executor.cluster import WorkerServer, CallbackServer, WorkerInfo
from livy_uploads.executor.cluster.callback import CallbackClient


LOGGER = logging.getLogger(__name__)


class TestCallbackServer:
    @pytest.fixture
    def server(self):
        server = CallbackServer(
            hostname='localhost',
            port=0,
            pause=0.1,
            timeout=2.0,
        )

        server.start()
        try:
            yield server
        finally:
            server.close()

    def test_happy_register(self, server: CallbackServer):
        t0 = time.monotonic()
        assert server.get_info('test') is None
        assert time.monotonic() - t0 >= 2.0

        def register():
            time.sleep(0.5)
            info = WorkerInfo(
                name='test',
                pid=1234,
                url=f'http://example.com:1234',
            )
            request = Request(
                url=f'{server.url}/info',
                method='POST',
                data=json.dumps(info.asdict()).encode('utf8'),
                headers={'Content-Type': 'application/json'},
            )
            with urlopen(request) as response:
                pass

        thread = threading.Thread(daemon=True, target=register)
        thread.start()

        t0 = time.monotonic()
        info = server.get_info('test')
        dt = time.monotonic() - t0
        assert info is not None
        assert info.name == 'test'
        assert 0.5 <= dt < 2.0

    def test_start_does_register(self, server: CallbackServer, tmp_path: Path):
        worker = WorkerServer(
            name='test2',
            command='false',
            hostname='localhost',
            log_dir=str(tmp_path),
            pause=0.5,
            callback=CallbackClient(server.url),
        )

        assert server.get_info('test2') is None
        worker.start()

        info = server.get_info('test2')
        assert info is not None
        assert info.name == 'test2'
        assert info.pid == worker._process.pid
