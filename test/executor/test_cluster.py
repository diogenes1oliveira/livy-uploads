import base64
import hashlib
import re
import logging
import select
import signal
import secrets
import subprocess
import socket
import threading
import time
from typing import Union

import pytest

from livy_uploads.executor.cluster import WsWorker, get_free_port

LOGGER = logging.getLogger(__name__)


class TestWsWorker:
    def test_happy_output(self, ws_server: str, sock_port: int, sock_server: socket.socket):
        ws_worker = WsWorker(
            ws_port=sock_port,
            ws_url=ws_server,
            command='bash',
            args=[
                '-c',
                '''
                    TRAPPED=
                    trap 'TRAPPED=1' USR1
                    echo "started"
                    while [ -z "$TRAPPED" ]; do
                        sleep 1
                    done
                    echo "got signal SIGUSR1"
                    TRAPPED=
                    while [ -z "$TRAPPED" ]; do
                        sleep 1
                    done
                    echo "finished"
                    exit 42
                '''
            ],
        )
        thread = threading.Thread(daemon=True, target=ws_worker.run)
        thread.start()

        sock, readline, writeline = self.accept(sock_server)
        sock.settimeout(3)
        assert thread.is_alive()

        # First message is the worker sending its info
        assert re.match(r'info hostname=.* pid=\d+', readline())

        # Client needs to send a heartbeat first
        writeline('signal 0')

        # First output line in the dummy script
        assert readline() == 'stdout ' + b64dumps('started\n')

        # Should have nothing in the output for a while
        rlist, _, _ = select.select([sock], [], [], 1)
        assert not rlist

        # Now send the signal for the first time
        writeline(f'signal {int(signal.SIGUSR1)}')
        assert readline() == 'stdout ' + b64dumps('got signal SIGUSR1\n')

        # Should have nothing in the output for a while
        rlist, _, _ = select.select([sock], [], [], 1)
        assert not rlist

        # Now send the signal for the second time
        writeline(f'signal {int(signal.SIGUSR1)}')
        assert readline() == 'stdout ' + b64dumps('finished\n')

        # Now we should get the return code
        assert readline() == 'returncode 42'

        # thread should still be alive, we didn't send the ack yet
        time.sleep(1)
        assert thread.is_alive()

        # Do send the ack, should finish the thread
        writeline('ack')
        thread.join(timeout=5)
        assert not thread.is_alive()

    def test_happy_input(self, ws_server: str, sock_port: int, sock_server: socket.socket):
        ws_worker = WsWorker(
            ws_port=sock_port,
            ws_url=ws_server,
            command='bash',
            args=[
                '-c',
                '''
                    read -r LINE
                    printf '%s' "$LINE" | tr -d '\r\n' | md5sum | awk '{print $1}'
                    md5sum | awk '{print $1}'
                ''',
            ],
        )
        thread = threading.Thread(daemon=True, target=ws_worker.run)
        thread.start()

        sock, readline, writeline = self.accept(sock_server)
        sock.settimeout(3)
        assert thread.is_alive()

        # First message is the worker sending its info
        assert re.match(r'info hostname=.* pid=\d+', readline())

        # Client needs to send a heartbeat first
        writeline('signal 0')

        # random single line
        line = secrets.token_urlsafe(64)
        line = 'oi'
        expected_md5 = md5hex(line.encode('utf8'))
        writeline('stdin ' + b64dumps(line + '\n'))
        # time.sleep(5)
        # return
        assert readline() == 'stdout ' + b64dumps(expected_md5 + '\n')

        # long random binary input
        data = secrets.token_bytes(1024 * 1024)
        expected_md5 = md5hex(data)
        writeline('stdin ' + b64dumps(data))

        # EOF marker to close the stdin
        writeline('stdin ' + b64dumps(''))
        assert readline() == 'stdout ' + b64dumps(expected_md5 + '\n')

        # Now we should get the return code
        assert readline() == 'returncode 0'

        # thread should still be alive, we didn't send the ack yet
        time.sleep(1)
        assert thread.is_alive()

        # Do send the ack, should finish the thread
        writeline('ack')
        thread.join(timeout=5)
        assert not thread.is_alive()

    @pytest.fixture(scope='class')
    def ws_server(self):
        port = get_free_port()
        url = f'wss://localhost:{port}'
        process = subprocess.Popen(
            ['wstunnel', '--log-lvl', 'WARN', 'server', '--remote-to-local-server-idle-timeout=5s', url],
            stdin=subprocess.DEVNULL,
        )
        try:
            yield url
        finally:
            process.kill()
            process.wait(timeout=2)

    @pytest.fixture
    def sock_port(self):
        return get_free_port()

    @pytest.fixture
    def sock_server(self, sock_port: int):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(5.0)
        server.bind(('localhost', sock_port))
        server.listen(1)
        return server

    def accept(self, server: socket.socket):
        sock, _ = server.accept()
        rfile = sock.makefile(mode='r')
        readline = lambda: rfile.readline().rstrip('\r\n')
        def writeline(line: str):
            data = (line.rstrip('\r\n') + '\n').encode('utf8')
            LOGGER.debug('writeline %r', data if len(data) < 50 else data[:50] + b'...')
            sock.sendall(data)
        return sock, readline, writeline


def b64dumps(s: Union[str, bytes]) -> str:
    if isinstance(s, str):
        s = s.encode('utf8')
    return base64.b64encode(s).decode('utf8')


def md5hex(data: Union[str, bytes]) -> str:
    if isinstance(data, str):
        data = data.encode('utf8')
    return hashlib.md5(data).hexdigest()
