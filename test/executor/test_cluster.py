import base64
import hashlib
import io
import re
import logging
import select
import json
import signal
import secrets
import subprocess
import socket
import threading
import time
import os
from typing import Union
from urllib.request import urlopen, Request
from uuid import uuid4
from pathlib import Path

import pytest

from livy_uploads.executor.cluster import WorkerServer, WorkerClient, CallbackServer, WorkerInfo, PollResult, get_free_port

LOGGER = logging.getLogger(__name__)


class TestWorkerHTTPServer:
    def test_happy_output(self, tmp_path: Path):
        name = str(uuid4())
        started = threading.Event()
        worker = WorkerServer(
            name=name,
            command='bash',
            args=[
                '-c',
                '''
                    TRAPPED=
                    trap 'TRAPPED=1' USR1
                    echo "started"
                    while [ -z "$TRAPPED" ]; do
                        sleep 0.1
                    done
                    echo "got signal SIGUSR1"
                    TRAPPED=
                    while [ -z "$TRAPPED" ]; do
                        sleep 0.1
                    done
                    echo "finished"
                    exit 42
                '''
            ],
            hostname='localhost',
            log_dir=str(tmp_path),
            pause=0.5,
            callback=lambda _: started.set(),
        )
        thread = threading.Thread(daemon=True, target=worker.run)
        thread.start()

        if not started.wait(timeout=5):
            pytest.fail('worker did not start')

        client = WorkerClient(worker.url)

        # check the info
        info = client.get_info()
        assert info.name == name
        assert info.pid > 0
        assert info.url == worker.url

        LOGGER.info('testing first output line')
        assert client.poll() == (b'started\n', None)

        LOGGER.info('should have nothing in the output for a while')
        time.sleep(0.5)
        assert client.poll() == (b'', None)

        LOGGER.info('sending signal for the first time')
        client.send_signal(int(signal.SIGUSR1))
        time.sleep(0.5)
        assert client.poll() == (b'got signal SIGUSR1\n', None)

        LOGGER.info('should have nothing in the output for a while once more')
        time.sleep(0.5)
        assert client.poll() == (b'', None)

        LOGGER.info('sending signal for the second time')
        client.send_signal(int(signal.SIGUSR1))
        time.sleep(0.5)
        assert client.poll() == (b'finished\n', None)

        LOGGER.info('polling for the returncode')
        time.sleep(0.5)
        assert client.poll() == (b'', 42)

        LOGGER.info('thread should take a while to die')
        assert thread.is_alive()
        thread.join(timeout=5)
        assert not thread.is_alive()

        # should still have the returncode
        assert client.poll() == (b'', 42)


    def test_happy_input(self, tmp_path: Path):
        name = str(uuid4())
        started = threading.Event()
        worker = WorkerServer(
            name=name,
            command='bash',
            args=[
                '-c',
                '''
                    read -r LINE
                    printf '%s' "$LINE" | tr -d '\r\n' | md5sum | awk '{print $1}'
                    md5sum | awk '{print $1}'
                ''',
            ],
            hostname='localhost',
            log_dir=str(tmp_path),
            pause=0.5,
            callback=lambda _: started.set(),
        )
        thread = threading.Thread(daemon=True, target=worker.run)
        thread.start()

        if not started.wait(timeout=5):
            pytest.fail('worker did not start')

        client = WorkerClient(worker.url)

        # check the info
        info = client.get_info()
        assert info.name == name
        assert info.pid > 0
        assert info.url == worker.url

        # random single line
        line = secrets.token_urlsafe(64)
        expected_md5 = md5hex(line.encode('utf8'))
        client.write_stdin(line.encode('utf8') + b'\n')
        time.sleep(0.5)
        assert client.poll() == (expected_md5.encode('utf8') + b'\n', None)

        # long random binary input
        data = secrets.token_bytes(1024 * 1024)
        expected_md5 = md5hex(data)
        client.write_stdin(data)
        time.sleep(0.5)
        assert client.poll() == (b'', None)

        # EOF marker to close the stdin
        client.write_stdin(b'')
        time.sleep(1.0)
        assert client.poll() == (expected_md5.encode('utf8') + b'\n', None)

        time.sleep(0.5)
        assert client.poll() == (b'', 0)

        # thread should take a while to die
        assert thread.is_alive()
        thread.join(timeout=5)
        assert not thread.is_alive()


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
            callback=f'{server.url}/info',
        )

        assert server.get_info('test2') is None
        worker.start()

        info = server.get_info('test2')
        assert info is not None
        assert info.name == 'test2'
        assert info.pid == worker._process.pid


def md5hex(data: Union[str, bytes]) -> str:
    if isinstance(data, str):
        data = data.encode('utf8')
    return hashlib.md5(data).hexdigest()
