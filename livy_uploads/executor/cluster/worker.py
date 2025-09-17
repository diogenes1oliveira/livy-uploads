#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Code to execute a command in a remote cluster worker and serve the output over HTTP.
'''



from ast import Call
import fcntl
import json
import logging
import os
from pathlib import Path
import pty
import re
import select
import shlex
import socket
import struct
import subprocess
import termios
import threading
import time
from typing import Any, BinaryIO, Optional, List, Mapping, Callable, Tuple, TypeVar, Union, Type
from urllib.request import Request, urlopen

from livy_uploads.executor.cluster.http import HttpBaseServer, HttpBaseHandler, HttpBuiltinClient, HttpBaseClient, HttpRequestsClient
from livy_uploads.executor.cluster.model import WorkerInfo, PollResult, WorkerCert, ServerCert
from livy_uploads.executor.cluster.utils import assert_type
from livy_uploads.executor.cluster.callback import CallbackClient
from livy_uploads.executor.cluster.http import parse_entity
from livy_uploads.executor.cluster.certs import CertManager

T = TypeVar('T')

LOGGER = logging.getLogger('livy_uploads.executor.cluster')

ENV_DISABLE_MAIN = 'LIVY_UPLOADS_EXECUTOR_DISABLE_MAIN'
'''
Environment variable to disable the main function even if the script is run directly.
'''

HOSTNAME_PATTERN = re.compile(r'^[a-z0-9.-]+$')



class WorkerServer(HttpBaseServer):
    """
    Runs a process and serves the output over HTTP.
    """

    def __init__(
        self,
        name: str,
        command: str,
        args: Optional[List[str]] = None,
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
        port: Optional[int] = 0,
        bind_address: Optional[str] = '0.0.0.0',
        hostname: Optional[str] = None,
        pause: Optional[float] = None,
        kill_timeout: Optional[float] = None,
        log_dir: Optional[str] = 'var/log',
        callback: Optional[Union[Callable[[WorkerInfo], Any], CallbackClient]] = None,
        stdin: Optional[bool] = True,
        tty_size: Optional[Tuple[int, int]] = None,
        bufsize: Optional[int] = 4096,
        heartbeat_timeout: Optional[float] = None,
        cert_manager: Optional[CertManager] = None,
    ):
        '''
        Args:
            name: The name of the worker. Used to identify the worker in the cluster and to set the output filenames.
            command: The command to run.
            args: The arguments to pass to the command.
            env: Override environment variables for the command.
            cwd: The working directory to run the command in. If the directory does not exist, it will be created.
            port: The port to listen on. If 0, a free port will be chosen.
            bind_address: The address to bind to. If not provided, defaults to `0.0.0.0`.
            hostname: The advertised hostname. If not provided, the FQDN will be used.
            bufsize: The buffer size to use for reading the command output.
            pause: The pause time to wait for data in the command output.
            kill_timeout: The timeout to wait for the process to terminate.
            log_dir: The directory to write the logs to. If not provided, uses a `var/log` directory.
            pause: Small pause to wait for data consistency.
            callback: An optional callback to send this worker info to when it starts.
            stdin: Whether to enable stdin in the process.
            tty_size: The size of the TTY to allocate for the process.
            heartbeat_timeout: The maximum timeout between two polls for the worker server.
        '''
        super().__init__(
            RequestHandlerClass=WorkerHandler,
            port=port,
            bind_address=bind_address,
            hostname=hostname,
        )

        self.name = name
        self.command = command
        self.args = list(args or [])
        self.env = {**os.environ, **(env or {})}
        self.cwd = Path(cwd or '.')
        self.pause = pause or 1.0
        self.log_file = Path(log_dir or 'var/log') / f'{name}.log'
        self.callback = callback
        self.stdin = stdin if stdin is not None else True
        self.tty_size = tty_size or None
        self.bufsize = bufsize or 4096
        self.kill_timeout = kill_timeout or 2.0
        self.heartbeat_timeout = heartbeat_timeout or 15.0
        self._process: Optional[subprocess.Popen] = None
        self._server_is_open = False
        self._fp: Optional[BinaryIO] = None
        self._done = threading.Event()
        self._stdin_lock = threading.Lock()
        self._input: Optional[BinaryIO] = None
        self._output: Optional[BinaryIO] = None
        self._out_thread: Optional[threading.Thread] = None
        self._last_poll_time: Optional[float] = None
        self.cert_manager = cert_manager

    @property
    def info(self) -> WorkerInfo:
        if not self._process:
            raise RuntimeError('Process is not running')

        return WorkerInfo(
            name=self.name,
            pid=self._process.pid,
            url=self.url,
        )

    def setup(self) -> None:
        """
        Starts the server and the process.
        """

        self._setup_process()
        super().setup()

        try:
            self._send_info()
        except Exception:
            self._kill()
            raise

    def _setup_process(self):
        LOGGER.info('preparing the files and directories')
        try:
            self.log_file.unlink()
        except FileNotFoundError:
            pass

        self.log_file.absolute().parent.mkdir(parents=True, exist_ok=True)
        self.log_file.touch(mode=0o600)
        self.cwd.absolute().mkdir(parents=True, exist_ok=True)
        self._fp = open(self.log_file, 'wb')

        if not self.tty_size:
            kwargs = dict(
                stdin=subprocess.PIPE if self.stdin else subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        else:
            master, slave = pty.openpty()
            rows, cols = self.tty_size
            winsize = struct.pack('HHHH', rows, cols, 0, 0)
            fcntl.ioctl(slave, termios.TIOCSWINSZ, winsize)

            kwargs = dict(
                stdin=slave if self.stdin else subprocess.DEVNULL,
                stdout=slave,
                stderr=slave,
                preexec_fn=os.setsid,
            )

        LOGGER.info('starting the process')
        self._process = subprocess.Popen(
            [*shlex.split(self.command), *self.args],
            env=self.env,
            cwd=self.cwd,
            **kwargs,
        )

        if not self.tty_size:
            self._output = self._process.stdout
            if self.stdin:
                self._input = self._process.stdin
        else:
            self._output = os.fdopen(master, 'rb')
            if self.stdin:
                self._input = os.fdopen(master, 'wb')

        self._last_poll_time = time.monotonic()
        self._out_thread = threading.Thread(target=self._write_output)
        self._out_thread.start()

    def _send_info(self):
        if self.cert_manager:
            self.cert_manager.setup()
            csr_path, _, conf_path, _ = self.cert_manager.make_request(name=self.hostname)
            worker_cert = WorkerCert(
                name=self.hostname,
                csr=csr_path.read_text(),
                conf=conf_path.read_text(),
            )
        else:
            worker_cert = None

        info = WorkerInfo(
            name=self.name,
            pid=self._process.pid,
            url=self.url,
            cert=worker_cert,
        )
        if self.callback:
            if isinstance(self.callback, CallbackClient):
                info = self.callback.send_info(info)
                if worker_cert:
                    if not info.cert or not info.cert.cert:
                        raise RuntimeError('Worker certificate is not available')
                    self.cert = self.cert_manager.get_mtls_cert(ServerCert, name=self.hostname, cert=info.cert.cert)
            else:
                self.callback(info)

    def close(self) -> None:
        """
        Closes the server and the process.
        """

        if self._process:
            LOGGER.info('killing the process')
            self._kill()

        if self._out_thread:
            self._out_thread.join(timeout=self.kill_timeout)
            if self._out_thread.is_alive():
                raise RuntimeError('out thread did not shut down')
            self._out_thread = None

        if self._fp:
            LOGGER.info('removing the log file')
            self._fp.close()
            try:
                self.log_file.unlink()
            except FileNotFoundError:
                pass

        super().close()

    def wait(self) -> None:
        """
        Waits until the process is done and its returncode is polled.
        """
        LOGGER.info('waiting for returncode to be polled')
        self._done.wait()
        LOGGER.info('returncode was polled')

    def run(self) -> int:
        """
        Starts the server and the process and waits until the process is done and its returncode is polled.
        """
        self.start()
        try:
            self.wait()
            returncode = self._process.poll()
            if returncode is None:
                raise RuntimeError('Process did not finish')
            return returncode
        finally:
            self.close()

    def _write_output(self):
        assert self._process
        assert self._last_poll_time is not None

        os.set_blocking(self._output.fileno(), False)

        while True:
            if time.monotonic() - self._last_poll_time > self.heartbeat_timeout:
                LOGGER.warning('heartbeat timeout reached, forcing process to exit')
                self._kill()
                break

            rlist, _, _ = select.select([self._output], [], [], self.pause)
            if rlist:
                try:
                    data = self._output.read(self.bufsize)
                except BlockingIOError:
                    data = None
                except OSError as e:
                    # check if it's a bad file descriptor error
                    if e.errno == 9:
                        LOGGER.info('process %d closed stdout', self._process.pid)
                        break
                    raise
            else:
                data = None

            if not data:
                if self._process.poll() is not None:
                    break
                continue

            self._fp.write(data)
            self._fp.flush()

    def _kill(self) -> None:
        if self._process:
            try:
                self._process.kill()
            except ProcessLookupError:
                pass
            self._process.wait(self.pause)
        self._done.set()

    def _set_polled(self) -> None:
        self._last_poll_time = time.monotonic()

    def get_stdout(self, start: int = 0, size: int = 1024) -> bytes:
        self._set_polled()

        with self.log_file.open('rb') as fp:
            fp.seek(start)
            data = fp.read(size)
            log_data = data if len(data) < 50 else data[:50] + b'...'
            LOGGER.debug('read %d bytes from stdout: %s', len(data), log_data)
            return data

    def get_returncode(self) -> Optional[int]:
        if not self._process:
            raise IOError('Process is not running')
        self._set_polled()
        returncode =  self._process.poll()
        if returncode is not None:
            LOGGER.info('process %d finished with returncode %d', self._process.pid, returncode)
            def delayed_done():
                time.sleep(self.pause)
                self._done.set()
            threading.Thread(daemon=True, target=delayed_done).start()
        return returncode

    def send_signal(self, signum: int, tty_size: Optional[Tuple[int, int]] = None) -> None:
        if not self._process:
            raise IOError('Process is not running')
        if tty_size:
            if not self.tty_size:
                raise ValueError('TTY is not enabled')
            if not self._input:
                raise IOError('Input is not open')
            LOGGER.info('setting TTY size to %d x %d', *tty_size)
            self._set_polled()
            rows, cols = tty_size
            winsize = struct.pack('HHHH', rows, cols, 0, 0)
            fcntl.ioctl(self._input.fileno(), termios.TIOCSWINSZ, winsize)
        else:
            self._set_polled()

        LOGGER.info('sending signal %d to process %d', signum, self._process.pid)
        self._process.send_signal(signum)

    def write_stdin(self, data: bytes) -> None:
        if not self._process:
            raise IOError('Process is not running')
        if not self._input:
            raise IOError('Input is not open')

        self._set_polled()

        with self._stdin_lock:
            if data:
                log_data = data if len(data) < 50 else data[:50] + b'...'
                LOGGER.debug('writing %d bytes to stdin: %s', len(data), log_data)
                self._input.write(data)
                self._input.flush()
            else:
                LOGGER.info('closing stdin')
                self._input.close()


class WorkerHandler(HttpBaseHandler):
    """
    Handles HTTP requests for a worker.

    The routes are:

    - `GET /ping`: Gets a 200 OK pong response.
    - `GET /stdout?start=<offset>&size=<bytes>`: Gets stdout binary data in the body of the response.
    - `GET /poll`: Gets the returncode of the worker.
    - `GET /info`: Gets the info of the worker.
    - `POST /signal?signum=<signal>`: Sends a signal to the worker.
    - `POST /stdin`: Writes te body as binary data to the stdin of the worker.
    """

    server: WorkerServer

    def do_GET(self) -> None:
        if self.url.path == '/ping':
            self.send_entity('pong')
        elif self.url.path == '/stdout':
            start = int(self.params.get('start') or '0')
            size = int(self.params.get('size') or '4096')
            data = self.server.get_stdout(start, size)
            self.send_entity(data)
        elif self.url.path == '/poll':
            returncode = self.server.get_returncode()
            if returncode is not None:
                body = str(returncode)
            else:
                body = None
            self.send_entity(body)
        elif self.url.path == '/info':
            self.send_entity(self.server.info.asdict())
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        if self.url.path == '/signal':
            signum = int(self.params.get('signum') or '0')
            rows = int(self.params.get('rows') or '0')
            cols = int(self.params.get('cols') or '0')
            if rows and cols:
                tty_size = (rows, cols)
            else:
                tty_size = None
            self.server.send_signal(signum, tty_size)
            self.send_entity(None)
        elif self.url.path == '/stdin':
            data = self.read_entity(bytes) or b''
            self.server.write_stdin(data)
            self.send_entity(None)
        else:
            self.send_error(404)


class WorkerClient:
    """
    A client for polling the status of the worker.
    """

    def __init__(
        self,
        url: str,
        name: Optional[str] = None,
        bufsize: int = 4096,
        pause: Optional[float] = 0.5,
        timeout: Optional[float] = None,
        start_timeout: Optional[float] = None,
        proxy: Optional[str] = None,
        http_client_class: Type[HttpRequestsClient] = HttpRequestsClient,
        cert_manager: Optional[CertManager] = None,
        basedir: Optional[Union[str, Path]] = None,
        client_name: str = 'test@localhost',
    ):
        self.url = url
        self.bufsize = bufsize
        self.name = name or None
        self.start_timeout = start_timeout or 15.0
        self.pause = pause or 0.5

        if cert_manager:
            cert_manager.setup()
            cert = cert_manager.make_client_cert(client_name, mtls=True)
        else:
            cert = None

        self.http_client = http_client_class(
            timeout=timeout or 3.0,
            proxy=proxy or None,
            cert=cert,
            basedir=basedir or None,
        )
        self._lock = threading.Lock()
        self._stdout_offset = 0
        self._returncode = None

    def poll(self) -> PollResult:
        with self._lock:
            if self._returncode is not None:
                return PollResult(stdout=b'', returncode=self._returncode)

            data = self.get_stdout(start=self._stdout_offset, size=self.bufsize)
            self._stdout_offset += len(data)
            returncode = None

            if len(data) == 0:
                returncode = self.get_returncode()
                if returncode is not None:
                    self._returncode = returncode

            return PollResult(stdout=data, returncode=returncode)

    def ping(self) -> None:
        url = f'{self.url}/ping'
        try:
            status, data = self.http_client.get(url)
            if status != 200 or data != b'pong':
                LOGGER.error('Bad HTTP response: status=%d data=%s', status, data)
                raise IOError
        except IOError:
            raise ConnectionError(f'Failed to ping {self.url}')

    def wait(self) -> None:
        t0 = time.monotonic()
        while time.monotonic() - t0 < self.start_timeout:
            try:
                self.ping()
                return
            except ConnectionError:
                time.sleep(self.pause)

        raise TimeoutError(f'Failed to ping {self.url} in {self.start_timeout} seconds')

    def get_stdout(self, start: int = 0, size: Optional[int] = None) -> bytes:
        size = size or self.bufsize
        url = f'{self.url}/stdout?start={start}&size={size}'
        _, data = self.http_client.get(url)
        return data or b''

    def get_returncode(self) -> Optional[int]:
        url = f'{self.url}/poll'
        status, data = self.http_client.get(url)
        if status == 200:
            text = parse_entity(data, str) or ''
            return int(text.strip())
        elif status == 204:
            return None
        else:
            raise IOError(f'Failed to get returncode from {self.url}: {status}')

    def get_info(self) -> WorkerInfo:
        url = f'{self.url}/info'
        response = self.http_client.get(url)
        if not response.ok:
            raise IOError(f'Failed to get info from {self.url}: {response.status}')

        body = parse_entity(response.data, dict) or {}
        return WorkerInfo.fromdict(body)

    def send_signal(self, signum: int, tty_size: Optional[Tuple[int, int]] = None) -> None:
        url = f'{self.url}/signal?signum={signum}'
        if tty_size:
            url += f'&rows={tty_size[0]}&cols={tty_size[1]}'

        response = self.http_client.post(url)
        if not response.ok:
            raise IOError(f'Failed to send signal {signum} to {self.url}')

    def write_stdin(self, data: bytes) -> None:
        url = f'{self.url}/stdin'
        response = self.http_client.post(url, data=data)
        if not response.ok:
            raise IOError(f'Failed to write stdin to {self.url}')



def get_winsize(fd: int) -> Tuple[int, int]:
    """
    Get the terminal size of the given file descriptor.
    """
    s = struct.pack("HHHH", 0, 0, 0, 0)
    rows, cols, _, _ = struct.unpack("HHHH", fcntl.ioctl(fd, termios.TIOCGWINSZ, s))
    return rows, cols


def get_free_port() -> int:
    '''
    Returns a free port on the local machine.
    '''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', 0))
        return s.getsockname()[1]

