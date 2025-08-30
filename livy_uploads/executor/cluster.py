#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
This file contains all the necessary code for the cluster executor.

Make sure not to import any non-standard libraries here: the contents of this file will be
sent as is to the cluster.
'''

__all__ = ('WsWorker', 'get_free_port', 'ENV_DISABLE_MAIN')

import argparse
from abc import ABC, abstractmethod
from base64 import b64encode, b64decode
from calendar import c
from contextlib import closing
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
from typing import BinaryIO, ClassVar, Dict, List, Mapping, Optional, Tuple, Type, TypeVar, NamedTuple, Union, Set, TYPE_CHECKING

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

class Message(ABC):
    TYPE: ClassVar[str]

    @classmethod
    @abstractmethod
    def parse(cls: Type[T], payload: bytes) -> T:
        '''
        Parses a message payload.

        Raises:
            ValueError: If the message is invalid.
        '''
        raise NotImplementedError

    def process(self, proc: subprocess.Popen, protocol: 'WsProtocol'):
        '''
        Processes this input message sent by the client.

        Meant to be run in the worker.

        Raises:
            NotImplementedError: If the message can't be processed by a worker.
        '''
        raise NotImplementedError

    def handle(self, protocol: 'WsProtocol'):
        '''
        Handles this output message sent by the worker.

        Meant to be run in the client.

        Raises:
            NotImplementedError: If the message can't be handled by a client.
        '''
        raise NotImplementedError

    @abstractmethod
    def encode(self) -> str:
        '''
        Encodes the payload of the message.
        '''
        raise NotImplementedError


class StdinMessage(Message):
    TYPE: ClassVar[str] = 'stdin'

    def __init__(self, data: bytes):
        self.data = data

    @property
    def eof(self) -> bool:
        return not self.data

    @classmethod
    def parse(cls, payload: bytes) -> 'StdinMessage':
        return cls(data=b64decode(payload.encode('utf8')))

    def process(self, proc: subprocess.Popen, protocol: 'WsProtocol'):
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

    def encode(self) -> str:
        return b64encode(self.data).decode("utf8")


class AckMessage(Message):
    TYPE: ClassVar[str] = 'ack'

    def __init__(self):
        pass

    @classmethod
    def parse(cls, payload: bytes) -> 'AckMessage':
        if payload:
            raise ValueError('Ack message does not expect any data')
        return cls()

    def process(self, proc: subprocess.Popen, protocol: 'WsProtocol'):
        if proc.poll() is None:
            INPUT_LOGGER.warning('Ignoring ack before the command has finished')
            return

        INPUT_LOGGER.info('Received ack')
        protocol.acked.set()

    def encode(self) -> str:
        return ''


class SignalMessage(Message):
    TYPE: ClassVar[str] = 'signal'

    def __init__(self, signum: int):
        self.signum = signum

    @property
    def heartbeat(self) -> bool:
        '''
        Whether this is a heartbeat signal.
        '''
        return self.signum == 0

    @classmethod
    def parse(cls, payload: bytes) -> 'SignalMessage':
        signum = int(payload)
        return cls(signum=signum)

    def process(self, proc: subprocess.Popen, protocol: 'WsProtocol'):
        '''
        Sends the requested signal to the command or sends a heartbeat.
        '''
        if self.heartbeat:
            INPUT_LOGGER.info('Heartbeat received')
            protocol.last_heartbeat = time.monotonic()
        elif proc.poll() is not None:
            INPUT_LOGGER.warning("can't send signal %s because the command has finished already", self.signum)
        else:
            INPUT_LOGGER.info('Sending signal %s', self.signum)
            proc.send_signal(self.signum)

    def encode(self) -> str:
        return str(self.signum)


class StdoutMessage(Message):
    TYPE: ClassVar[str] = 'stdout'

    def __init__(self, data: bytes):
        self.data = data

    @property
    def eof(self) -> bool:
        return not self.data

    @classmethod
    def parse(cls, payload: bytes) -> 'StdoutMessage':
        return cls(data=b64decode(payload.encode('utf8')))

    def encode(self) -> str:
        return b64encode(self.data).decode("utf8")

    @classmethod
    def generate(cls, proc: subprocess.Popen, protocol: 'WsProtocol') -> Optional['StdoutMessage']:
        '''
        Tries to generate a message from the stdout of the command.

        Returns:
            The message, or None if there is no data available to send.
        '''
        OUTPUT_LOGGER.debug('checking for data in stdout')
        ready, _, _ = select.select([proc.stdout], [], [], protocol.pause)
        if not ready:
            if proc.poll() is not None:
                OUTPUT_LOGGER.info('Command has finished and stdout is done')
            else:
                OUTPUT_LOGGER.debug('no data in stdout yet')
            return None

        OUTPUT_LOGGER.debug('stdout is ready, now reading %d bytes', protocol.bufsize)
        data = proc.stdout.read(protocol.bufsize)
        OUTPUT_LOGGER.debug('read %d bytes from stdout: %r', len(data or b''), data)
        if data is None:
            OUTPUT_LOGGER.debug('no data in stdout yet')
            return None
        elif not data:
            OUTPUT_LOGGER.info('Command stdout is done')

        return cls(data=data)


_info_pattern = re.compile(r'hostname=([^\s]+) pid=(\d+)')


class InfoMessage(Message):
    TYPE: ClassVar[str] = 'info'

    def __init__(self, hostname: str, pid: int):
        self.hostname = hostname
        self.pid = pid

    @classmethod
    def parse(cls, payload: bytes) -> 'InfoMessage':
        match = _info_pattern.match(payload.decode('utf8'))
        if not match:
            raise ValueError('Invalid info message')
        return cls(hostname=match.group(1), pid=int(match.group(2)))

    def encode(self) -> str:
        return f'hostname={self.hostname} pid={self.pid}'

    @classmethod
    def generate(cls, proc: subprocess.Popen) -> 'InfoMessage':
        '''
        Generates a message with the hostname and pid of the command.
        '''
        return cls(hostname=socket.getfqdn(), pid=proc.pid)


class ReturncodeMessage(Message):
    TYPE: ClassVar[str] = 'returncode'

    def __init__(self, returncode: int):
        self.returncode = returncode

    @classmethod
    def parse(cls, payload: bytes) -> 'ReturncodeMessage':
        return cls(returncode=int(payload.decode('utf8')))

    def encode(self) -> str:
        return str(self.returncode)

    @classmethod
    def generate(cls, proc: subprocess.Popen) -> 'ReturncodeMessage':
        '''
        Waits for the command to finish to generate a message with the returncode.
        '''
        returncode = proc.poll()
        if returncode is None:
            raise RuntimeError('Returncode not set yet')

        OUTPUT_LOGGER.info('command has finished with returncode %s', returncode)
        return cls(returncode=returncode)


class WsProtocol:
    TYPES: ClassVar[Dict[str, Type[Message]]] = {
        StdinMessage.TYPE: StdinMessage,
        SignalMessage.TYPE: SignalMessage,
        AckMessage.TYPE: AckMessage,
        InfoMessage.TYPE: InfoMessage,
        StdoutMessage.TYPE: StdoutMessage,
        ReturncodeMessage.TYPE: ReturncodeMessage,
    }

    def __init__(self, bufsize: int = 1024, pause: float = 1.0):
        self.bufsize = bufsize
        self.pause = pause
        self.last_heartbeat: float = -1.0
        self.acked = threading.Event()
        self.stdin_closed = threading.Event()
        self.returncode: Optional[int] = None

    def parse(self, line: Union[bytes, str]) -> Message:
        if isinstance(line, bytes):
            line = line.decode('utf8')

        prefix, _, payload = line.rstrip('\r\n').partition(' ')
        try:
            cls: Type[Message] = self.TYPES[prefix]
        except KeyError:
            raise ValueError('Unknown prefix: %r', prefix)

        return cls.parse(payload)

    def process(self, line: Union[bytes, str], proc: subprocess.Popen):
        '''
        Parses and processes an input line sent by the client.

        Meant to be run in the worker.
        '''
        try:
            message = self.parse(line)
        except ValueError:
            INPUT_LOGGER.warning('bad input data', exc_info=True)
            return

        try:
            message.process(proc, self)
        except NotImplementedError:
            INPUT_LOGGER.warning('Command %s not supported for workers', message.__class__.__name__)

    def handle(self, line: Union[bytes, str]):
        '''
        Parses and processes an output line sent by worker.

        Meant to be run in the client.
        '''
        try:
            message = self.parse(line)
        except ValueError:
            OUTPUT_LOGGER.warning('bad output data', exc_info=True)
            return

        try:
            message.handle(self)
        except NotImplementedError:
            OUTPUT_LOGGER.warning('Command %s not supported for clients', message.__class__.__name__)

    def send(self, wfile: BinaryIO, message: Message):
        '''
        Encodes and sends a message through the file channel.
        '''
        line = message.TYPE
        payload = message.encode()
        if payload:
            line += ' ' + payload

        wfile.write(line.encode('utf8'))
        wfile.write(b'\n')
        wfile.flush()


class WsWorker:
    def __init__(
        self,
        master_port: int,
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
            master_port: The port on the master to connect stdin and stdout to.
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
        self.master_port = master_port
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
        return url.hostname, self.master_port

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
            [ws_exe, '--log-lvl', ws_log_level, 'client', '-L', f'stdio://localhost:{self.master_port}', *self.ws_args, self.ws_url],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
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
                rfile=ws_proc.stdout,
                proc=cmd_proc,
                protocol=protocol,
            ),
        )
        thread.start()

        try:
            self._send_output(
                proc=cmd_proc,
                wfile=ws_proc.stdin,
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

    def _receive_input(self, rfile: BinaryIO, proc: subprocess.Popen, protocol: WsProtocol):
        '''
        Receives input from the master and writes it to the subprocess stdin
        '''
        logger = LOGGER.getChild('input')
        logger.info('polling the command input')

        try:
            while not protocol.acked.is_set():
                logger.debug('waiting next input line')
                data = rfile.readline()

                if not data:
                    logger.info('input channel is closed, quitting input loop')
                    break

                protocol.process(data, proc)
        except Exception:
            logger.exception('Command input polling failed')
            if proc.poll() is not None:
                proc.terminate()
        finally:
            logger.info('stdin polling done')

    def _send_output(self, proc: subprocess.Popen, wfile: BinaryIO, protocol: WsProtocol):
        '''
        Polls and sends the output and returncode of a subprocess
        '''
        logger = LOGGER.getChild('output')
        os.set_blocking(proc.stdout.fileno(), False)

        try:
            logger.info('sending host info')
            protocol.send(wfile, InfoMessage.generate(proc))

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

                stdout_message = StdoutMessage.generate(proc, protocol)
                if stdout_message is not None:
                    if stdout_message.eof:
                        logger.info('command stdout is done')
                        break
                    protocol.send(wfile, stdout_message)
                elif proc.poll() is not None:
                    logger.info('command has finished and there is no more output to send')
                    break

            logger.info('Output polling done, waiting for returncode')
            protocol.send(wfile, ReturncodeMessage.generate(proc))

            logger.info('waiting for the ack')
            protocol.acked.wait(timeout=self.kill_timeout)
            if not protocol.acked.is_set():
                raise RuntimeError('Not acked in time')

        except Exception:
            logger.exception('command output loop failed')
            proc.kill()
            protocol.send(wfile, ReturncodeMessage(1))
        finally:
            logger.info('output polling is finished')


class WsClient:
    def __init__(
        self,
        worker_address: Tuple[str, int],
    ):
        pass


    def run(self):
        OUTPUT_LOGGER.info('connecting to the worker at %r', self.worker_address)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        protocol = WsProtocol()
        sock.connect(self.worker_address)
        with closing(sock):
            wfile = sock.makefile(mode='wb')

            OUTPUT_LOGGER.info('sending initial heartbeat')
            protocol.send(wfile, SignalMessage(0))

    def _receive_output(self, rfile: BinaryIO, protocol: WsProtocol):
        OUTPUT_LOGGER.info('receiving output from the worker')

        try:
            while True:
                OUTPUT_LOGGER.debug('waiting for output data')
                ready, _, _ = select.select([rfile], [], [], protocol.pause)
                if not ready:
                    OUTPUT_LOGGER.debug('no output data yet')
                    continue

                line = rfile.readline()
                if not line:
                    OUTPUT_LOGGER.info('output channel is closed, quitting output loop')
                    break
                protocol.process(line, protocol)
            OUTPUT_LOGGER.info('sending initial heartbeat')
            self._send_heartbeat(wfile, protocol)
            while not protocol.acked.is_set():
                logger.debug('waiting next input line')
                data = rfile.readline()

                if not data:
                    logger.info('input channel is closed, quitting input loop')
                    break

                protocol.process(data, proc)
        except Exception:
            logger.exception('Command input polling failed')
            if proc.poll() is not None:
                proc.terminate()
        finally:
            logger.info('stdin polling done')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING'], default='INFO')

    subparsers = parser.add_subparsers(dest='command')

    worker_parser = subparsers.add_parser('worker')
    worker_parser.add_argument('--ws-url')
    worker_parser.add_argument('--master-port', type=int, required=True)
    worker_parser.add_argument('--ws-bin')
    worker_parser.add_argument('--ws-arg', nargs='*')
    worker_parser.add_argument('-e', '--env', nargs='*')
    worker_parser.add_argument('--cwd')
    worker_parser.add_argument('--bufsize', type=int, default=1024)
    worker_parser.add_argument('--pause', type=float, default=0.1)
    worker_parser.add_argument('--timeout', type=float, default=5.0)
    worker_parser.add_argument('command')
    worker_parser.add_argument('args', nargs='*')

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    if args.command == 'worker':
        env = {}
        for pair in args.env or []:
            key, sep, value = pair.partition('=')
            if not sep:
                value = os.environ[key]
            env[key] = value

        worker = WsWorker(
            ws_url=args.ws_url,
            master_port=args.master_port,
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
