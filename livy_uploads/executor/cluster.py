#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Code to execute a command in a remote cluster worker.

This is module meant to be sent to a remote cluster and executed there, so don't import non-standard libraries.
'''

__all__ = ('WorkerServer', 'WorkerClient', 'CallbackServer', 'WorkerInfo', 'PollResult', 'get_free_port')

import argparse
import collections.abc
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import os
from pathlib import Path
import queue
import select
import shlex
import signal
import socket
from socketserver import ThreadingMixIn
import subprocess
import sys
import threading
import time
from typing import Any, BinaryIO, Dict, Optional, List, Mapping, Callable, NamedTuple, Type, TypeVar, Union
from urllib.parse import ParseResult, urlparse, parse_qs
from urllib.request import Request, urlopen, build_opener, ProxyHandler


T = TypeVar('T')

LOGGER = logging.getLogger('livy_uploads.executor.cluster')

ENV_DISABLE_MAIN = 'LIVY_UPLOADS_EXECUTOR_DISABLE_MAIN'
'''
Environment variable to disable the main function even if the script is run directly.
'''



class WorkerInfo(NamedTuple):
    """
    Worker process information.
    """
    name: str
    pid: int
    url: str

    @classmethod
    def fromdict(cls, kwargs: Mapping) -> 'WorkerInfo':
        return cls(
            name=assert_type(kwargs['name'], str),
            pid=assert_type(kwargs['pid'], int),
            url=assert_type(kwargs['url'], str),
        )

    def asdict(self) -> dict:
        return dict(self._asdict())


class PollResult(NamedTuple):
    stdout: bytes
    returncode: Optional[int]


class BaseServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True

    def __init__(
        self,
        RequestHandlerClass: Type[BaseHTTPRequestHandler],
        port: Optional[int] = 0,
        hostname: Optional[str] = None,
        bind_address: Optional[str] = '0.0.0.0',
    ):
        super().__init__(
            server_address=(bind_address or '0.0.0.0', port or 0),
            RequestHandlerClass=RequestHandlerClass,
            bind_and_activate=False,
        )
        self._hostname = hostname or None
        self._serve_thread: Optional[threading.Thread] = None

    @property
    def hostname(self) -> str:
        return self._hostname or socket.getfqdn()

    @property
    def url(self) -> str:
        if not self.server_address:
            raise RuntimeError('Server port is not set yet')

        port = self.server_address[1]
        return f'http://{self.hostname}:{port}'

    def start(self) -> None:
        # from the original constructor
        LOGGER.info('binding server')
        try:
            self.server_bind()
            self.server_activate()
        except:
            self.server_close()
            raise

        LOGGER.info('serving on %s', self.url)
        thread = threading.Thread(daemon=True, target=self.serve_forever)
        thread.start()
        self._serve_thread = thread
        time.sleep(1.0)

    def close(self) -> None:
        if self._serve_thread:
            LOGGER.info('shutting down the server')
            self.shutdown()
            self._serve_thread.join(timeout=2.0)
            self._serve_thread = None


class BaseHandler(BaseHTTPRequestHandler):
    """
    Base class for HTTP handlers.
    """

    def send_entity(self, result: Optional[Union[str, bytes, Mapping]], status: Optional[int] = None) -> None:
        if result is None:
            status = status or 204
            self.send_response(status)
            self.end_headers()
            return

        status = status or 200
        if isinstance(result, str):
            data = result.encode('utf-8')
            content_type = 'text/plain; charset=utf-8'
        elif isinstance(result, bytes):
            data = result
            content_type = 'application/octet-stream'
        elif isinstance(result, collections.abc.Mapping):
            data = json.dumps(result).encode('utf-8')
            content_type = 'application/json'
        else:
            raise TypeError(f'Invalid type for result: {type(result)}')

        self.send_response(status)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Content-Type', content_type)
        self.end_headers()
        self.wfile.write(data)

    @property
    def url(self) -> ParseResult:
        return urlparse(self.path)


class WorkerServer(BaseServer):
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
        log_dir: Optional[str] = 'var/log',
        callback: Optional[Union[str, Callable[[WorkerInfo], None]]] = None,
        stdin: Optional[bool] = True,
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
            log_dir: The directory to write the logs to. If not provided, uses a `var/log` directory.
            pause: Small pause to wait for data consistency.
            callback: An optional callback to call when the worker starts. Might be a URL to POST the info to or a callable.
            stdin: Whether to enable stdin in the process.
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
        self._process: Optional[subprocess.Popen] = None
        self._server_is_open = False
        self._fp: Optional[BinaryIO] = None
        self._done = threading.Event()
        self._stdin_lock = threading.Lock()

    @property
    def info(self) -> WorkerInfo:
        if not self._process:
            raise RuntimeError('Process is not running')

        return WorkerInfo(
            name=self.name,
            pid=self._process.pid,
            url=self.url,
        )

    def start(self) -> None:
        """
        Starts the server and the process.
        """

        super().start()

        LOGGER.info('preparing the files and directories')
        try:
            self.log_file.unlink()
        except FileNotFoundError:
            pass

        self.log_file.absolute().parent.mkdir(parents=True, exist_ok=True)
        self.log_file.touch(mode=0o600)
        self.cwd.absolute().mkdir(parents=True, exist_ok=True)
        self._fp = open(self.log_file, 'wb')

        LOGGER.info('starting the process')
        self._process = subprocess.Popen(
            [*shlex.split(self.command), *self.args],
            env=self.env,
            cwd=self.cwd,
            stdin=subprocess.PIPE if self.stdin else subprocess.DEVNULL,
            stdout=self._fp,
            stderr=self._fp,
        )

        try:
            info = self.info
            LOGGER.info('started worker: %s', info)
            if self.callback:
                if callable(self.callback):
                    self.callback(info)
                elif isinstance(self.callback, str):
                    request = Request(self.callback, data=json.dumps(info.asdict()).encode('utf-8'))
                    request.add_header('Content-Type', 'application/json')
                    with urlopen(request) as response:
                        if response.status != 204:
                            raise IOError(f'Failed to send callback to {self.callback}')
                else:
                    raise ValueError(f'Invalid callback: {self.callback}')
        except Exception:
            self._kill()
            raise

        LOGGER.info('worker started with server running on %s', self.url)

    def close(self) -> None:
        """
        Closes the server and the process.
        """

        if self._process:
            LOGGER.info('killing the process')
            self._kill()

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
            returncode = self._process.returncode
            if returncode is None:
                raise RuntimeError('Process did not finish')
            return returncode
        finally:
            self.close()

    def _kill(self) -> None:
        if self._process:
            self._process.kill()
            self._process.wait(self.pause)

    def get_stdout(self, start: int = 0, size: int = 1024) -> bytes:
        with self.log_file.open('rb') as fp:
            fp.seek(start)
            data = fp.read(size)
            log_data = data if len(data) < 50 else data[:50] + b'...'
            LOGGER.debug('read %d bytes from stdout: %s', len(data), log_data)
            return data

    def get_returncode(self) -> Optional[int]:
        if not self._process:
            raise IOError('Process is not running')
        returncode =  self._process.poll()
        if returncode is not None:
            LOGGER.info('process %d finished with returncode %d', self._process.pid, returncode)
            def delayed_done():
                time.sleep(self.pause)
                self._done.set()
            threading.Thread(daemon=True, target=delayed_done).start()
        return returncode

    def send_signal(self, signum: int) -> None:
        if not self._process:
            raise IOError('Process is not running')
        LOGGER.info('sending signal %d to process %d', signum, self._process.pid)
        self._process.send_signal(signum)

    def write_stdin(self, data: bytes) -> None:
        if not self._process:
            raise IOError('Process is not running')

        with self._stdin_lock:
            if data:
                log_data = data if len(data) < 50 else data[:50] + b'...'
                LOGGER.debug('writing %d bytes to stdin: %s', len(data), log_data)
                self._process.stdin.write(data)
                self._process.stdin.flush()
            else:
                LOGGER.info('closing stdin')
                self._process.stdin.close()


class WorkerHandler(BaseHandler):
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
            qs = parse_qs(self.url.query)
            start = int((qs.get('start') or ['0'])[0])
            size = int((qs.get('size') or ['4096'])[0])
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
            qs = parse_qs(self.url.query)
            signum = int((qs.get('signum') or ['0'])[0])
            self.server.send_signal(signum)
            self.send_entity(None)
        elif self.url.path == '/stdin':
            data = self.rfile.read(int(self.headers['Content-Length']))
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
        bufsize: int = 4096,
        pause: Optional[float] = None,
        tty: Optional[bool] = None,
        stop_timeout: Optional[float] = None,
        proxy: Optional[str] = None,
    ):
        self.url = url
        self.bufsize = bufsize
        self.pause = pause or 0.5
        self.stop_timeout = stop_timeout or 10.0
        self.proxy = proxy
        self._stdout_offset = 0
        self._tty = tty
        self._signals_queue = queue.Queue()

        handler = ProxyHandler({'http': self.proxy, 'https': self.proxy} if self.proxy else {})
        self._opener = build_opener(handler)

    def poll(self) -> PollResult:
        data = self.get_stdout(start=self._stdout_offset, size=self.bufsize)
        self._stdout_offset += len(data)
        returncode = None

        if len(data) == 0:
            time.sleep(self.pause)
            data = self.get_stdout(start=self._stdout_offset, size=self.bufsize)
            self._stdout_offset += len(data)

            if len(data) == 0:
                returncode = self.get_returncode()

        return PollResult(stdout=data, returncode=returncode)

    def run(
        self,
        stdout: BinaryIO,
        stdin: Optional[BinaryIO] = None,
    ) -> int:
        r, w = os.pipe()
        done = threading.Event()

        LOGGER.info('bound to remote process')

        if stdin:
            stdin_thread = threading.Thread(daemon=True, target=self._read_stdin, args=(stdin, r))
            stdin_thread.start()
        else:
            stdin_thread = None

        signals_thread = threading.Thread(daemon=True, target=self._send_signals, args=(done,))
        signals_thread.start()

        try:
            while True:
                try:
                    result = self.poll()
                    stdout.write(result.stdout)
                    stdout.flush()

                    if result.returncode is not None:
                        LOGGER.info('process finished with returncode %d', result.returncode)
                        return result.returncode

                    if len(result.stdout) < self.bufsize:
                        time.sleep(self.pause)
                except KeyboardInterrupt:
                    LOGGER.info('received KeyboardInterrupt')
                    self.send_signal(int(signal.SIGINT))
        finally:
            done.set()
            if stdin_thread and stdin_thread.is_alive():
                LOGGER.info('waiting for the stdin thread to finish')
                os.close(w)
                stdin_thread.join(timeout=self.stop_timeout)
                if stdin_thread.is_alive():
                    raise TimeoutError('stdin thread did not finish in time')

    def _send_signals(self, done: threading.Event) -> None:
        try:
            while not done.is_set():
                try:
                    signum, t = self._signals_queue.get(timeout=1.0)
                    LOGGER.info('forwarding signal %d to process (dt=%f)', signum, time.monotonic() - t)
                except queue.Empty:
                    continue
                self.send_signal(signum)
        except:
            LOGGER.exception('error in _send_signals')

    def _read_stdin(self, stdin: BinaryIO, stop_fd: int):
        try:
            if self._tty is None:
                tty = stdin.isatty()
            else:
                tty = self._tty

            while True:
                readables, _, _ = select.select([stdin, stop_fd], [], [], 1.0)
                if not readables:
                    continue

                if stop_fd in readables:
                    LOGGER.warning('requested to stop reading stdin before EOF')
                    os.read(stop_fd, 1)
                    break

                if tty:
                    data = stdin.readline()
                else:
                    data = stdin.read(self.bufsize)

                self.write_stdin(data)
                if not data:
                    LOGGER.info('EOF in stdin')
                    break
        except:
            LOGGER.exception('error in _read_stdin')

    def get_stdout(self, start: int = 0, size: Optional[int] = None) -> bytes:
        size = size or self.bufsize
        url = f'{self.url}/stdout?start={start}&size={size}'
        with self._opener.open(url) as response:
            return response.read()

    def get_returncode(self) -> Optional[int]:
        url = f'{self.url}/poll'
        with self._opener.open(url) as response:
            if response.status == 204:
                return None
            else:
                return int(response.read().decode('utf-8'))

    def get_info(self) -> WorkerInfo:
        url = f'{self.url}/info'
        with self._opener.open(url) as response:
            body = assert_type(json.loads(response.read().decode('utf-8')), dict)
            return WorkerInfo.fromdict(body)

    def enqueue_signal(self, signum: int) -> None:
        self._signals_queue.put((signum, time.monotonic()))

    def send_signal(self, signum: int) -> None:
        url = f'{self.url}/signal?signum={signum}'
        with self._opener.open(url, data=b'') as response:
            if response.status != 204:
                raise IOError(f'Failed to send signal {signum} to {self.url}')

    def write_stdin(self, data: bytes) -> None:
        url = f'{self.url}/stdin'
        with self._opener.open(url, data=data) as response:
            if response.status != 204:
                raise IOError(f'Failed to write stdin to {self.url}')


class CallbackServer(BaseServer):
    """
    A server to receive callbacks from dynamically created workers.
    """

    def __init__(
        self,
        port: Optional[int] = 0,
        bind_address: Optional[str] = '0.0.0.0',
        hostname: Optional[str] = None,
        pause: Optional[float] = None,
        timeout: Optional[float] = None,
    ):
        '''
        Args:
            port: The port to listen on. If 0, a free port will be chosen.
            bind_address: The address to bind to. If not provided, defaults to `0.0.0.0`.
            hostname: The advertised hostname. If not provided, the FQDN will be used.
            pause: The pause time to wait between polling the worker info.
            timeout: The timeout to wait for the worker to be ready.
        '''
        super().__init__(
            RequestHandlerClass=CallbackHandler,
            port=port,
            bind_address=bind_address,
            hostname=hostname,
        )
        self.infos: Dict[str, WorkerInfo] = {}
        self.pause = pause or 0.3
        self.timeout = timeout or 20.0

    def handle_info(self, info: WorkerInfo) -> None:
        LOGGER.info('received callback info from %s: %s', info.name, info)
        self.infos[info.name] = info

    def get_info(self, name: str) -> Optional[WorkerInfo]:
        t0 = time.time()
        while True:
            if time.time() - t0 > self.timeout:
                return None
            info = self.infos.get(name)
            if info:
                return info
            time.sleep(self.pause)


class CallbackHandler(BaseHandler):
    """
    Handles HTTP requests for a callback server.

    Routes:
    - `POST /info`: Receives the info from the worker.
    - `GET /ping`: Gets a 200 OK pong response.
    - `GET /info/<name>`: Gets the info of the worker.
    """

    server: CallbackServer

    def do_GET(self) -> None:
        if self.url.path == '/ping':
            self.send_entity('pong')
        elif self.url.path.startswith('/info/'):
            name = self.url.path[len('/info/'):]
            info = self.server.infos.get(name)
            if info:
                self.send_entity(info.asdict())
            else:
                self.send_entity({'error': f'Worker {name} not found'}, status=404)
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        if self.url.path == '/info':
            data = self.rfile.read(int(self.headers['Content-Length']))
            body = assert_type(json.loads(data), dict)
            info = WorkerInfo.fromdict(body)
            self.server.handle_info(info)
            self.send_entity(None)
        else:
            self.send_error(404)


def get_free_port() -> int:
    '''
    Returns a free port on the local machine.
    '''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', 0))
        return s.getsockname()[1]


def assert_type(value: Any, expected_type: Type[T]) -> T:
    try:
        origin = getattr(expected_type, '__origin__')
        if origin is Union:
            args = expected_type.__args__
            if len(args) == 2 and args[1] is type(None):
                nullable = True
                expected_type = args[0]
    except AttributeError:
        nullable = False

    if nullable and value is None:
        return value

    if not isinstance(value, expected_type):
        raise ValueError(f'Expected {expected_type}, got {type(value)}')

    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log-level', choices=['DEBUG', 'INFO', 'WARNING'], default='INFO')
    subparsers = parser.add_subparsers(dest='subcommand')

    run_parser = subparsers.add_parser('run')
    run_parser.add_argument('-n', '--name', type=str, required=True)
    run_parser.add_argument('-e', '--env', type=str, nargs='*', action='append')
    run_parser.add_argument('--cwd', type=str)
    run_parser.add_argument('-p', '--port', type=int)
    run_parser.add_argument('--bind-address', type=str)
    run_parser.add_argument('--hostname', type=str)
    run_parser.add_argument('-k', '--kill-timeout', type=float)
    run_parser.add_argument('-d', '--log-dir', type=str)
    run_parser.add_argument('command', type=str)
    run_parser.add_argument('args', type=str, nargs='*')

    client_parser = subparsers.add_parser('client')
    client_parser.add_argument('-u', '--url', type=str, required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    if args.subcommand == 'run':
        env = {}
        for pair in args.env or []:
            key, sep, value = pair.partition('=')
            if not sep:
                value = os.environ[key]
            env[key] = value

        server = WorkerServer(
            name=args.name,
            command=args.command,
            args=args.args,
            env=env,
            cwd=args.cwd,
            port=args.port,
            bind_address=args.bind_address,
            hostname=args.hostname,
            log_dir=args.log_dir,
        )
        server.run()
    elif args.subcommand == 'client':
        client = WorkerClient(
            url=args.url,
        )
        returncode = client.run()
        sys.exit(returncode)
    else:
        raise ValueError(f'Invalid subcommand: {args.subcommand!r}')


if __name__ == '__main__' and not os.environ.get(ENV_DISABLE_MAIN):
    main()
