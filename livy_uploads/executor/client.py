#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Client for the Livy executor.
"""

__all__ = ('LivyExecutorClient',)

from contextlib import ExitStack
import logging
import os
import time
import signal
import threading
from pathlib import Path
from typing import Any, Optional, List, Mapping, Tuple, BinaryIO, Union

import requests

from livy_uploads.executor.cluster import WorkerClient, HttpBaseClient, HttpRequestsClient, ClientCert
from livy_uploads.executor.commands import (
    LivyPrepareMaster,
    LivyStartProcess,
    LivySignCertificate,
)
from livy_uploads.session import LivySession
from livy_uploads.utils import assert_type
from livy_uploads.retry_policy import TimeoutRetryPolicy
from livy_uploads.executor.console import Console, LineConsole, RawConsole, TTYConsole
from livy_uploads.executor.signals import SignalMonitor, signame
from livy_uploads.executor.cluster import CertManager

LOGGER = logging.getLogger(__name__)


class LivyExecutorClient:
    """
    Client for the Livy executor.
    """

    def __init__(
        self,
        session: LivySession,
        callback_port: Optional[int] = 0,
        callback_hostname: Optional[str] = None,
        bind_address: Optional[str] = '0.0.0.0',
        pause: Optional[float] = None,
        bufsize: Optional[int] = None,
        log_dir: Optional[str] = 'var/log',
        ready_timeout: Optional[float] = None,
        stop_timeout: Optional[float] = None,
        kill_timeout: Optional[float] = None,
        request_timeout: Optional[float] = None,
        proxy: Optional[str] = None,
        stdin_poll_pause: Optional[float] = None,
        basedir: Optional[Union[str, Path]] = None,
    ):
        '''
        Args:
            callback_port: The port to listen on for the callback server. If 0, a free port will be chosen.
            callback_hostname: The advertised master hostname. If not provided, the FQDN will be used.
            bind_address: The address to bind to. If not provided, defaults to `0.0.0.0`.
            pause: The pause time to wait for data in the command output.
            log_dir: The directory to write the logs to. If not provided, uses a `var/log` directory.
            ready_timeout: The timeout to wait for the session to be ready.
            stop_timeout: The timeout to wait for the worker to stop.
            kill_timeout: The timeout to wait for the worker to die.
            request_timeout: The timeout to use for HTTP requests.
            bufsize: The buffer size to use for reading the command output.
            proxy: The proxy to use for polling the worker.
            stdin_poll_pause: The pause time to wait for stdin data.
        '''
        self.session = session
        self.callback_port = callback_port or 0
        self.callback_hostname = callback_hostname or None
        self.bind_address = bind_address or '0.0.0.0'
        self.pause = pause or 1.0
        self.log_dir = log_dir or 'var/log'
        self.ready_timeout = ready_timeout or 60.0
        self.stop_timeout = stop_timeout or 10.0
        self.kill_timeout = kill_timeout or 2.0
        self.bufsize = bufsize or 4096
        self.stdin_poll_pause = stdin_poll_pause
        self.request_timeout = request_timeout
        self.proxy = proxy
        self.basedir = Path(basedir or 'var')
        self.cert_manager: Optional[CertManager] = None
        self.cert: Optional[ClientCert] = None

    @classmethod
    def from_config(cls, config: Optional[Mapping[str, Any]]) -> 'LivyExecutorClient':
        if not config:
            raise ValueError('config is required')

        kwargs = assert_type(config['executor'], dict)
        kwargs = dict(
            callback_port=assert_type(kwargs.get('callback_port'), Optional[int]),
            callback_hostname=assert_type(kwargs.get('callback_hostname'), Optional[str]),
            bind_address=assert_type(kwargs.get('bind_address'), Optional[str]),
            pause=assert_type(kwargs.get('pause'), Optional[float]),
            bufsize=assert_type(kwargs.get('bufsize'), Optional[int]),
            log_dir=assert_type(kwargs.get('log_dir'), Optional[str]),
            stop_timeout=assert_type(kwargs.get('stop_timeout'), Optional[float]),
            kill_timeout=assert_type(kwargs.get('kill_timeout'), Optional[float]),
            proxy=assert_type(kwargs.get('proxy'), Optional[str]),
        )
        session = LivySession.from_config(config)
        return cls(
            session=session,
            **kwargs,
        )

    def setup(self):
        LOGGER.info('waiting for session to be ready')
        self.session.wait_ready(TimeoutRetryPolicy(self.ready_timeout, self.pause))
        LOGGER.info('session is ready')

        callback_url = self.session.apply(LivyPrepareMaster())
        LOGGER.info('callback url: %s', callback_url)

        self.cert_manager, self.cert = self.session.apply(LivySignCertificate(basedir=self.basedir))
        LOGGER.info('got signed certificate for client=%r', self.cert.name)

    def start(
        self,
        command: str,
        args: Optional[List[str]] = None,
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
        stdin: Optional[bool] = True,
        tty_size: Optional[Tuple[int, int]] = None,
        worker_port: Optional[int] = 0,
        worker_hostname: Optional[str] = None,
        bind_address: Optional[str] = '0.0.0.0',
        stop_signal: Optional[Union[int, signal.Signals]] = None,
        max_stop_count: Optional[int] = 2,
    ) -> 'WorkerMonitor':
        '''
        Args:
            command: The command to run.
            args: The arguments to pass to the command.
            env: Override environment variables for the command.
            cwd: The working directory to run the command in. If the directory does not exist, it will be created.
            stdin: Whether to enable stdin in the process, defaults to True.
            tty_size: The initial size of a TTY to allocate for the process.
            worker_port: The port the worker server will listen on. If 0, a free port will be chosen.
            worker_hostname: The advertised worker hostname. If not provided, the FQDN will be used.
            bind_address: The address to bind to. If not provided, defaults to `0.0.0.0`.
            stop_signal: The signal to send to the worker to gracefully stop it. If not provided, defaults to `SIGTERM`.
            max_stop_count: The maximum number of stop signals to send to the worker before sending SIGKILL.
        '''
        assert self.cert_manager is not None, 'Certificate manager is not setup'
        assert self.cert is not None, 'Certificate is not setup'

        stdin = True if stdin is None else stdin
        stop_signal = int(stop_signal) if stop_signal is not None else signal.SIGTERM

        if tty_size is not None:
            env = env or {}
            env['TERM'] = env.get('TERM') or os.getenv('TERM') or 'xterm-256color'

        LOGGER.info('my PID is %d', os.getpid())
        info = self.session.apply(LivyStartProcess(
            command=command,
            args=args,
            env=env,
            cwd=cwd,
            port=worker_port,
            bind_address=bind_address,
            hostname=worker_hostname,
            pause=self.pause,
            log_dir=self.log_dir,
            stdin=stdin,
            tty_size=tty_size,
        ))
        LOGGER.info('got worker info: %s', info)

        return WorkerMonitor(
            url=info.url,
            name=info.name,
            bufsize=self.bufsize,
            pause=self.pause,
            stop_timeout=self.stop_timeout,
            kill_timeout=self.kill_timeout,
            stop_signal=stop_signal,
            max_stop_count=max_stop_count,
            stdin_poll_pause=self.stdin_poll_pause,
            cert_manager=self.cert_manager,
            basedir=self.basedir,
            proxy=self.proxy,
        )


class WorkerMonitor:
    """
    Monitor a worker process in the foreground.
    """

    def __init__(
        self,
        url: str,
        name: Optional[str] = None,
        bufsize: Optional[int] = None,
        pause: Optional[float] = None,
        stop_timeout: Optional[float] = None,
        kill_timeout: Optional[float] = None,
        stop_signal: Optional[int] = None,
        max_stop_count: Optional[int] = 2,
        stdin_poll_pause: Optional[float] = None,
        cert_manager: Optional[CertManager] = None,
        basedir: Optional[Union[str, Path]] = None,
        proxy: Optional[str] = None,
    ):
        self.name = name or None
        self.pause = pause or 1.0
        self.stop_timeout = stop_timeout or 10.0
        self.kill_timeout = kill_timeout or 2.0
        self.bufsize = bufsize or 4096
        self.max_stop_count = max_stop_count or 2
        self.stop_signal = int(stop_signal) if stop_signal is not None else int(signal.SIGTERM)
        self.stdin_poll_pause = stdin_poll_pause
        self.cert_manager = cert_manager
        self.basedir = basedir
        self.proxy = proxy
        self.client = WorkerClient(
            url=url,
            name=self.name,
            bufsize=bufsize,
            cert_manager=self.cert_manager,
            basedir=self.basedir,
            proxy=self.proxy,
        )
        self._done = threading.Event()
        self._interrupted_at: Optional[float] = None

    def run(
        self,
        stdin: Optional[Union[BinaryIO, Console]] = None,
        stdout: Optional[BinaryIO] = None,
        tty: Optional[bool] = None,
        bind_signals: Optional[bool] = True,
    ) -> int:
        if stdin is None:
            console = None
            if tty is True:
                raise ValueError('stdin is None and tty is True')
        elif isinstance(stdin, Console):
            console = stdin
        elif stdin.isatty():
            if tty is not False:
                console = TTYConsole(stdin=stdin, max_wait=self.stdin_poll_pause, bufsize=self.bufsize)
                tty = True
            else:
                console = LineConsole(stdin=stdin, max_wait=self.stdin_poll_pause)
        else:
            if tty is not True:
                console = RawConsole(stdin=stdin, bufsize=self.bufsize, max_wait=self.stdin_poll_pause)
                tty = False
            else:
                console = LineConsole(stdin=stdin, max_wait=self.pause)

        bind_signals = True if bind_signals is None else bind_signals
        signals_monitor = SignalMonitor(
            signals=None if bind_signals else [],
            pause=self.pause,
            stop_signal=self.stop_signal,
            max_stop_count=self.max_stop_count,
        )
        signals_monitor.setup()
        signals_thread = threading.Thread(target=self._receive_signals, args=(signals_monitor, console, tty), daemon=True)
        signals_thread.start()

        if console:
            stdin_thread = threading.Thread(target=self._receive_stdin, args=(console,), daemon=True)
            stdin_thread.start()
        else:
            stdin_thread = None

        try:
            with ExitStack() as stack:
                if console:
                    stack.enter_context(console)
                while not self._done.is_set():
                    result = self.client.poll()
                    if result.returncode is not None:
                        LOGGER.debug('worker process exited with code %d', result.returncode)
                        return result.returncode

                    if result.stdout and stdout:
                        stdout.write(result.stdout)
                        stdout.flush()
                    if len(result.stdout) < self.bufsize:
                        time.sleep(self.pause)
        finally:
            self._done.set()
            signals_thread.join(timeout=self.kill_timeout)
            if signals_thread.is_alive():
                raise RuntimeError('failed to kill signals thread')
            signals_monitor.close()
            if stdin_thread:
                stdin_thread.join(timeout=self.kill_timeout)
                if stdin_thread.is_alive():
                    raise RuntimeError('failed to kill stdin thread')

    def _receive_signals(self, signals_monitor: SignalMonitor, console: Optional[Console], tty: bool) -> None:
        stopped_at: Optional[float] = None

        try:
            if tty and not console:
                raise ValueError('console is None and tty is True')

            while not self._done.is_set():
                if stopped_at is not None and time.monotonic() - stopped_at > self.stop_timeout:
                    LOGGER.warning('stop timeout reached, sending SIGKILL')
                    self.client.send_signal(signal.SIGKILL, None)
                    break

                try:
                    sig = signals_monitor.next_signal()
                except EOFError:
                    LOGGER.info('signals monitor closed')
                    break
                except TimeoutError:
                    continue

                if sig == self.stop_signal and stopped_at is None:
                    stopped_at = time.monotonic()

                if sig == signal.SIGWINCH and tty and console:
                    try:
                        tty_size = console.tty_size
                    except NotImplementedError:
                        tty_size = None
                else:
                    tty_size = None

                LOGGER.debug('sending signal %r (tty_size: %s)', signame(sig), tty_size)
                self.client.send_signal(int(sig), tty_size)
        except Exception:
            LOGGER.exception('failed to receive signals')
            self._done.set()
        finally:
            LOGGER.debug('signals thread done')

    def _receive_stdin(self, console: Console) -> None:
        try:
            while not self._done.is_set():
                try:
                    data = console.read()
                except EOFError:
                    LOGGER.debug('sending EOF to worker')
                    self.client.write_stdin(b'')
                    break
                else:
                    if data:
                        self.client.write_stdin(data)
        except Exception:
            LOGGER.exception('failed to receive stdin')
            self._done.set()
        finally:
            LOGGER.debug('stdin thread done')

