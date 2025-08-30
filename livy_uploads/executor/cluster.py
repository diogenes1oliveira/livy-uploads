#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
This file contains all the necessary code for the cluster executor.

Make sure not to import any non-standard libraries here: the contents of this file will be
sent as is to the cluster.
'''

__all__ = ('WsWorker', 'get_free_port', 'ENV_DISABLE_MAIN')

import argparse
from base64 import b64encode, b64decode
import os
import socket
import logging
import subprocess
import shlex
import shutil
import select
import traceback
import time
import threading
import queue
from typing import BinaryIO, List, Mapping, Optional, TextIO, Tuple
from urllib.parse import urlparse, urlunparse


# name it explicitly because it will change when submitting directly to Livy
LOGGER = logging.getLogger('livy_uploads.executor.worker')

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

        heartbeats = queue.Queue()
        acked = threading.Event()
        done = threading.Event()
        thread = threading.Thread(
            daemon=True,
            target=self._receive_input,
            kwargs=dict(
                fp=ws_proc.stdout,
                proc=cmd_proc,
                heartbeats=heartbeats,
                acked=acked,
                done=done,
            ),
        )
        thread.start()

        try:
            self._send_output(
                proc=cmd_proc,
                fp=ws_proc.stdin,
                heartbeats=heartbeats,
                acked=acked,
            )
        finally:
            done.set()
            thread.join(timeout=self.kill_timeout)
            ws_proc.terminate()

        if thread.is_alive():
            raise RuntimeError('Command input polling thread did not finish')

        if cmd_proc.poll() is None:
            raise RuntimeError('Command did not finish yet')

        return cmd_proc.returncode

    def _receive_input(self, fp: BinaryIO, proc: subprocess.Popen, heartbeats: queue.Queue, acked: threading.Event, done: threading.Event):
        '''
        Receives input from the master and writes it to the subprocess stdin
        '''
        logger = LOGGER.getChild('input')
        logger.info('polling the command input')
        try:
            # os.set_blocking(fp.fileno(), False)
            while not done.is_set():
                logger.debug('waiting for input data to be available')
                ready, _, _ = select.select([fp], [], [], self.pause)
                if not ready:
                    logger.debug('no data in input channel yet')
                    continue

                logger.debug('input data is available')
                data = fp.readline()
                if data is None:
                    logger.debug('no data in input channel yet')
                    continue
                elif not data:
                    logger.info('input channel is closed, quitting input loop')
                    break
                
                try:
                    line = data.rstrip(b'\r\n').decode('utf8')
                    prefix, _, chunk = line.partition(' ')
                    if prefix == 'stdin' or prefix == 'info':
                        data = b64decode(chunk.encode('utf8'))
                    elif prefix == 'signal':
                        signum = int(chunk)
                    elif prefix == 'ack':
                        pass
                    else:
                        logger.warning('Received unknown prefix: %r', line)
                        continue
                except ValueError:
                    logger.warning('Bad data for prefix %r', prefix, exc_info=True)
                    continue

                if prefix == 'stdin':
                    logger.debug('got %d bytes of stdin data: %r', len(data), data)
                    if proc.poll() is not None:
                        logger.warning("can't write to stdin because the command has finished already")
                        continue
                    if not data:
                        logger.info('Closing stdin')
                        proc.stdin.close()
                    else:
                        proc.stdin.write(data)
                        proc.stdin.flush()
                elif prefix == 'signal':
                    if signum == 0:
                        logger.info('Heartbeat received')
                        heartbeats.put(time.monotonic())
                    elif proc.poll() is not None:
                        logger.warning("can't send signal %s because the command has finished already", signum)
                        continue
                    else:
                        logger.info('Sending signal %s', signum)
                        proc.send_signal(signum)
                elif prefix == 'ack':
                    logger.info('Received ack')
                    acked.set()
                    continue
                elif prefix == 'info':
                    logger.info('Received info: %r', chunk)
                    continue
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

    def _send_output(self, proc: subprocess.Popen, fp: BinaryIO, heartbeats: queue.Queue, acked: threading.Event):
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
            try:
                last_heartbeat = heartbeats.get(timeout=self.heartbeat_timeout)
            except queue.Empty:
                raise RuntimeError(f'No heartbeat received in {self.heartbeat_timeout} seconds')

            logger.info('polling the command output')
            while True:

                logger.debug('checking the heartbeats')
                while True:
                    try:
                        last_heartbeat = heartbeats.get_nowait()
                    except queue.Empty:
                        break

                if time.monotonic() - last_heartbeat > self.heartbeat_timeout:
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
            acked.wait(timeout=self.kill_timeout)
            if not acked.is_set():
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
