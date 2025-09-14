import hashlib
import logging
import signal
import secrets
import threading
import time
from typing import Union
from uuid import uuid4
from pathlib import Path

import pytest

from livy_uploads.executor.cluster import WorkerServer, WorkerClient


LOGGER = logging.getLogger(__name__)


class TestWorkerServer:
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

    def test_tty(self, tmp_path: Path):
        name = str(uuid4())
        ps1 = f'({name}) >'
        prefix = ps1.encode('utf8')
        started = threading.Event()
        worker = WorkerServer(
            name=name,
            command='bash',
            args=['--norc', '--noprofile', '-i'],
            env={
                'PS1': ps1,
                'TERM': 'xterm-256color',
                'INPUTRC': '/dev/null',
            },
            hostname='localhost',
            log_dir=str(tmp_path),
            tty_size=(22, 42),
            callback=lambda _: started.set(),
        )

        thread = threading.Thread(daemon=True, target=worker.run)
        thread.start()

        if not started.wait(timeout=5):
            pytest.fail('worker did not start')

        client = WorkerClient(worker.url)

        LOGGER.info('preparing the prompt')
        client.write_stdin(b"stty -echo && bind 'set enable-bracketed-paste off'\r\n")
        buf = b''
        while True:
            output, returncode = client.poll()
            assert returncode is None
            if not output:
                break
            buf += output

        LOGGER.info('sending a test command')
        client.write_stdin(b'true\n')
        assert client.poll() == (prefix, None)

        LOGGER.info('testing the window size')
        client.write_stdin(b'echo "Window size: $(tput cols)x$(tput lines)"\n')
        time.sleep(1)
        assert client.poll() == (b'Window size: 42x22\r\n' + prefix, None)

        LOGGER.info('resizing the window')
        client.send_signal(int(signal.SIGWINCH), (23, 43))
        time.sleep(1)
        client.write_stdin(b'echo "Window size: $(tput cols)x$(tput lines)"\n')
        time.sleep(1)
        assert client.poll() == (b'Window size: 43x23\r\n' + prefix, None)

        LOGGER.info('closing the stdin')
        client.write_stdin(b'')
        time.sleep(1)
        assert client.poll() == (b'', -1)

        # thread should take a while to die
        assert thread.is_alive()
        thread.join(timeout=5)
        assert not thread.is_alive()


    def test_heartbeat_timeout(self, tmp_path: Path):
        name = str(uuid4())
        started = threading.Event()
        worker = WorkerServer(
            name=name,
            command='bash',
            args=['-c', 'sleep 10'],
            hostname='localhost',
            log_dir=str(tmp_path),
            heartbeat_timeout=2.0,
            callback=lambda _: started.set(),
        )

        thread = threading.Thread(daemon=True, target=worker.run)
        thread.start()

        if not started.wait(timeout=5):
            pytest.fail('worker did not start')

        client = WorkerClient(worker.url)

        time.sleep(1)
        assert client.get_returncode() is None
        time.sleep(1)
        assert client.get_returncode() is None

        time.sleep(3)
        assert worker._process.poll() == -9

        # thread should take a while to die
        thread.join(timeout=1)
        assert not thread.is_alive()



def md5hex(data: Union[str, bytes]) -> str:
    if isinstance(data, str):
        data = data.encode('utf8')
    return hashlib.md5(data).hexdigest()

