#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
This file contains all the necessary code for the cluster executor.

Make sure not to import any non-standard libraries here: the contents of this file will be
sent as is to the cluster.
'''

__all__ = ('WsWorker', 'get_free_port', 'ENV_DISABLE_MAIN')

import argparse
from abc import abstractmethod
from base64 import b64encode, b64decode
import os
import socket
import logging
import subprocess
import shlex
import shutil
import select
import traceback
import re
import time
import threading
import queue
from typing import BinaryIO, ClassVar, Dict, List, Mapping, Optional, Tuple, Type, TypeVar, NamedTuple, Union, TYPE_CHECKING
try:
    from typing import Protocol
except ImportError:
    # Python 3.6 didn't have the Protocol type yet
    from abc import ABC
    Protocol = ABC

from urllib.parse import urlparse, urlunparse


# name it explicitly because it will change when submitting directly to Livy
LOGGER = logging.getLogger('livy_uploads.executor.worker')
INPUT_LOGGER = LOGGER.getChild('input')
OUTPUT_LOGGER = LOGGER.getChild('output')

ENV_DISABLE_MAIN = 'LIVY_UPLOADS_EXECUTOR_DISABLE_MAIN'
'''
Environment variable to disable the main function even if the script is run directly.
'''


def get_free_port() -> int:
    '''
    Returns a free port on the local machine.
    '''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', 0))
        return s.getsockname()[1]


T = TypeVar('T', bound='Message')

class Message(Protocol):
    # Ideally this should be a dataclass following a Protocol but Python 3.6 didn't have those yet
    @classmethod
    @abstractmethod
    def parse(cls: Type[T], chunk: bytes) -> T:
        raise NotImplementedError

    @abstractmethod
    def handle(self, proc: subprocess.Popen, protocol: 'WsProtocol'):
        raise NotImplementedError


class StdinMessage(NamedTuple):
    data: bytes

    @property
    def eof(self) -> bool:
        return not self.data

    @classmethod
    def parse(cls, chunk: bytes) -> 'StdinMessage':
        return cls(data=b64decode(chunk.encode('utf8')))

    def handle(self, proc: subprocess.Popen, protocol: 'WsProtocol'):
        if INPUT_LOGGER.isEnabledFor(logging.DEBUG):
            if len(self.data) > 50:
                log_data = self.data[:50] + b'...'
            else:
                log_data = self.data
            INPUT_LOGGER.debug('got %d bytes of stdin data: %r', len(self.data), log_data)

        if proc.poll() is not None:
            INPUT_LOGGER.warning("can't write to stdin because the command has finished already")
        elif protocol.stdin_closed.is_set():
            INPUT_LOGGER.warning("can't write to stdin because it has been closed already")
        elif self.eof:
            INPUT_LOGGER.info('Closing stdin')
            protocol.stdin_closed.set()
            proc.stdin.close()
        else:
            proc.stdin.write(self.data)
            proc.stdin.flush()


class AckMessage(NamedTuple):
    @classmethod
    def parse(cls, chunk: bytes) -> 'AckMessage':
        if chunk:
            raise ValueError('Ack message does not expect any data')
        return cls()

    def handle(self, proc: subprocess.Popen, protocol: 'WsProtocol'):
        if proc.poll() is None:
            INPUT_LOGGER.warning('Ignoring ack before the command has finished')
            return

        INPUT_LOGGER.info('Received ack')
        protocol.acked.set()


class SignalMessage(NamedTuple):
    signum: int

    @property
    def heartbeat(self) -> bool:
        return self.signum == 0

    @classmethod
    def parse(cls, chunk: bytes) -> 'SignalMessage':
        signum = int(chunk)
        return cls(signum=signum)

    def handle(self, proc: subprocess.Popen, protocol: 'WsProtocol'):
        if self.heartbeat:
            INPUT_LOGGER.info('Heartbeat received')
            protocol.last_heartbeat = time.monotonic()
        elif proc.poll() is not None:
            INPUT_LOGGER.warning("can't send signal %s because the command has finished already", self.signum)
        else:
            INPUT_LOGGER.info('Sending signal %s', self.signum)
            proc.send_signal(self.signum)


class WsProtocol:
    TYPES: ClassVar[Dict[str, Type[Message]]] = {
        'stdin': StdinMessage,
        'signal': SignalMessage,
        'ack': AckMessage,
    }

    def __init__(self):
        self.last_heartbeat: float = -1.0
        self.acked = threading.Event()
        self.stdin_closed = threading.Event()

    def parse(self, line: Union[bytes, str]) -> Message:
        if isinstance(line, bytes):
            line = line.decode('utf8')

        prefix, _, chunk = line.rstrip('\r\n').partition(' ')
        try:
            cls: Type[Message] = self.TYPES[prefix]
        except KeyError:
            raise ValueError('Unknown prefix: %r', prefix)

        return cls.parse(chunk)

    def handle(self, line: Union[bytes, str], proc: subprocess.Popen):
        try:
            message = self.parse(line)
        except ValueError:
            INPUT_LOGGER.warning('bad input data', exc_info=True)
            return

        message.handle(proc, self)


# _info_pattern = re.compile(r'hostname=(.*) pid=(\d+)')

# class InfoMessage(NamedTuple):
#     hostname: str
#     pid: int

#     @classmethod
#     def parse(cls, chunk: bytes) -> 'InfoMessage':
#         match = _info_pattern.match(chunk.decode('utf8'))
#         if not match:
#             raise ValueError('Invalid info message')
#         return cls(hostname=match.group(1), pid=int(match.group(2)))


# class WsProtocol:
#     def 
class WsWorker:
    def __init__(
        self,
        ws_port: int,
        command: str,
        ws_url: Optional[str] = None,
        args: Optional[List[str]] = None,
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
        ws_bin: Optional[str] = None,
        ws_args: Optional[List[str]] = None,
        bufsize: int = 1024,
        pause: float = 1.0,
        heartbeat_timeout: float = 10.0,
        kill_timeout: float = 5.0,
    ):
        '''
        Args:
            ws_port: The port on the master to connect stdin and stdout to.
            command: The command to run.
            args: The arguments to pass to the command.
            env: Override environment variables for the command.
            cwd: The working directory to run the command in. If the directory does not exist, it will be created.
            ws_url: The URL of the master WebSocket server. Defaults to wss://localhost:12345/v1
            ws_bin: The path to the wstunnel binary.
            ws_args: Extra shell arguments to pass to the wstunnel binary.
            bufsize: The buffer size to use for reading the command output.
            pause: The pause time to wait for data in the command output.
            heartbeat_timeout: The timeout to wait for the heartbeats to be received.
            kill_timeout: The timeout to wait for the command to finish.
        '''
        url = urlparse(ws_url or 'wss://localhost:12345/v1')
        path = url.path.lstrip('/') or 'v1'
        url = url._replace(path='', query=None, fragment=None)
        self.ws_url = urlunparse(url)
        self.ws_port = ws_port
        self.command = command
        self.args = list(args or []) + ['--http-upgrade-path-prefix', path]
        self.env = env or {}
        self.cwd = cwd or None
        self.ws_bin = ws_bin or shutil.which('wstunnel') or '/usr/bin/wstunnel'
        self.ws_args = list(ws_args or [])
        self.bufsize = bufsize
        self.pause = pause
        self.heartbeat_timeout = heartbeat_timeout
        self.kill_timeout = kill_timeout

    @property
    def worker_address(self) -> Tuple[str, int]:
        url = urlparse(self.ws_url)
        return url.hostname, self.ws_port

    def run(self) -> int:
        LOGGER.info('preparing the wstunnel binary')
        try:
            os.makedirs('bin')
        except FileExistsError:
            pass

        ws_exe = os.path.abspath('bin/wstunnel')
        if os.path.exists(ws_exe):
            LOGGER.info('checking already copied wstunnel binary')
            process = subprocess.run(
                [ws_exe, '--version'],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if process.returncode != 0:
                LOGGER.warning('wstunnel binary is not working, removing it')
                os.unlink(ws_exe)

        if not os.path.exists(ws_exe):
            LOGGER.info('copying wstunnel binary to %r', ws_exe)
            shutil.copy(self.ws_bin, ws_exe)
            os.chmod(ws_exe, 0o755)

        LOGGER.info('starting the wstunnel client connected to server at %r', self.ws_url)
        ws_log_level = 'INFO' if LOGGER.isEnabledFor(logging.DEBUG) else 'WARN'
        ws_proc = subprocess.Popen(
            [ws_exe, '--log-lvl', ws_log_level, 'client', '-L', f'stdio://localhost:{self.ws_port}', *self.ws_args, self.ws_url],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            # universal_newlines=True,
        )

        LOGGER.info('starting the command %r', self.command)

        env = {**os.environ, **self.env}
        if self.cwd is not None:
            os.makedirs(self.cwd, exist_ok=True)
        cmd_proc = subprocess.Popen(
            [*shlex.split(self.command), *self.args],
            env=env,
            cwd=self.cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        LOGGER.info('command started with pid %s', cmd_proc.pid)

        protocol = WsProtocol()
        thread = threading.Thread(
            daemon=True,
            target=self._receive_input,
            kwargs=dict(
                fp=ws_proc.stdout,
                proc=cmd_proc,
                protocol=protocol,
            ),
        )
        thread.start()

        try:
            self._send_output(
                proc=cmd_proc,
                fp=ws_proc.stdin,
                protocol=protocol,
            )
        finally:
            if not protocol.acked.wait(timeout=self.kill_timeout):
                ws_proc.kill()
                ws_proc.wait(timeout=self.pause)

            thread.join(timeout=self.pause)

        if thread.is_alive():
            raise RuntimeError('Command input polling thread did not finish')

        if cmd_proc.poll() is None:
            raise RuntimeError('Command did not finish yet')

        return cmd_proc.returncode

    def _receive_input(self, fp: BinaryIO, proc: subprocess.Popen, protocol: WsProtocol):
        '''
        Receives input from the master and writes it to the subprocess stdin
        '''
        logger = LOGGER.getChild('input')
        logger.info('polling the command input')

        try:
            while not protocol.acked.is_set():
                logger.debug('waiting next input line')
                data = fp.readline()

                if not data:
                    logger.info('input channel is closed, quitting input loop')
                    break

                protocol.handle(data, proc)
        except Exception:
            logger.exception('Command input polling failed')
            if proc.poll() is not None:
                chunk = b64encode(traceback.format_exc().encode('utf8')).decode('utf8')
                line = f'stdout: {chunk}\n'
                fp.write(line)
                fp.flush()
                proc.terminate()
        finally:
            logger.info('stdin polling done')

    def _send_output(self, proc: subprocess.Popen, fp: BinaryIO, protocol: WsProtocol):
        '''
        Polls and sends the output and returncode of a subprocess
        '''
        returncode = None
        logger = LOGGER.getChild('output')
        os.set_blocking(proc.stdout.fileno(), False)

        try:
            logger.info('sending host info')
            info = f'hostname={socket.getfqdn()} pid={proc.pid}'
            line = f'info {info}\n'
            fp.write(line.encode('utf8'))
            fp.flush()

            logger.info('waiting for the first heartbeat')
            t0 = time.monotonic()
            while protocol.last_heartbeat < 0:
                if time.monotonic() - t0 > self.heartbeat_timeout:
                    raise RuntimeError("didn't get an initial heartbeat in time")
                time.sleep(self.pause)

            logger.info('polling the command output')
            while True:
                logger.debug('checking the heartbeats')
                if time.monotonic() - protocol.last_heartbeat > self.heartbeat_timeout:
                    raise RuntimeError('Heartbeat timeout')

                logger.debug('checking for data in stdout')
                ready, _, _ = select.select([proc.stdout], [], [], self.pause)
                if not ready:
                    if proc.poll() is not None:
                        logger.info('Command has finished and stdout is done')
                        break
                    else:
                        logger.debug('no data in stdout yet')
                        continue

                logger.debug('stdout is ready')
                data = proc.stdout.read(self.bufsize)
                logger.debug('read %d bytes from stdout: %r', len(data or b''), data)
                if data is None:
                    logger.debug('got None from stdout')
                    continue
                elif not data:
                    logger.info('Command stdout is done')
                    break

                chunk = b64encode(data).decode('utf8')
                line = f'stdout {chunk}\n'
                fp.write(line.encode('utf8'))
                fp.flush()

            logger.info('Output polling done, waiting for returncode')
            returncode = proc.wait()

            logger.info('command finished with returncode %s', returncode)
            if returncode is None:
                returncode = 1
            fp.write(f'returncode {returncode}\n'.encode('utf8'))
            fp.flush()

            logger.info('waiting for the ack')
            protocol.acked.wait(timeout=self.kill_timeout)
            if not protocol.acked.is_set():
                raise RuntimeError('Not acked in time')

        except Exception:
            logger.exception('Command output polling failed')
            chunk = b64encode(traceback.format_exc().encode('utf8')).decode('utf8')
            line = f'stdout: {chunk}\n'
            fp.write(line.encode('utf8'))
            fp.flush()
            proc.kill()
        finally:
            logger.info('output polling is finished')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--ws-url')
    parser.add_argument('--ws-port', type=int, required=True)
    parser.add_argument('--ws-bin')
    parser.add_argument('--ws-arg', nargs='*')
    parser.add_argument('-e', '--env', nargs='*')
    parser.add_argument('--cwd')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING'], default='INFO')
    parser.add_argument('--bufsize', type=int, default=1024)
    parser.add_argument('--pause', type=float, default=0.1)
    parser.add_argument('--timeout', type=float, default=5.0)
    parser.add_argument('command')
    parser.add_argument('args', nargs='*')
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    env = {}
    for pair in args.env or []:
        key, sep, value = pair.partition('=')
        if not sep:
            value = os.environ[key]
        env[key] = value

    worker = WsWorker(
        ws_url=args.ws_url,
        ws_port=args.ws_port,
        command=args.command,
        args=args.args,
        env=env,
        cwd=args.cwd,
        ws_bin=args.ws_bin,
        ws_args=args.ws_arg,
        bufsize=args.bufsize,
        pause=args.pause,
        kill_timeout=args.timeout,
    )
    worker.run()

if __name__ == '__main__' and not os.getenv(ENV_DISABLE_MAIN):
    main()
