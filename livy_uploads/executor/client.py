#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Client for the Livy executor.
"""

__all__ = ('LivyExecutorClient',)

import logging
import os
import time
import threading
from typing import Any, Optional, List, Mapping, Tuple, BinaryIO, Union

import requests

from livy_uploads.executor.cluster import WorkerClient, get_winsize, BaseHttpClient
from livy_uploads.executor.commands import (
    LivyPrepareMaster,
    LivyStartProcess,
)
from livy_uploads.session import LivySession
from livy_uploads.utils import assert_type
from livy_uploads.retry_policy import TimeoutRetryPolicy
from livy_uploads.executor.console import Console, LineConsole, RawConsole


LOGGER = logging.getLogger(__name__)


class RequestsHttpClient(BaseHttpClient):
    """
    A simple client for HTTP requests using the `requests` library.
    """

    def __init__(self, request_timeout: Optional[float] = None, proxy: Optional[str] = None):
        self.request_timeout = request_timeout or 3.0
        self.proxies = {'http': proxy, 'https': proxy} if proxy else {}
    
    def get(self, url: str) -> Tuple[int, Optional[bytes]]:
        response = requests.get(url, timeout=self.request_timeout, proxies=self.proxies)
        response.raise_for_status()
        return response.status_code, response.content

    def post(self, url: str, data: Optional[bytes] = None) -> Tuple[int, Optional[bytes]]:
        response = requests.post(url, data=data, timeout=self.request_timeout, proxies=self.proxies)
        response.raise_for_status()
        return response.status_code, response.content


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
        self.http_client = RequestsHttpClient(request_timeout=request_timeout, proxy=proxy)

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
        '''
        stdin = True if stdin is None else stdin

        if tty_size is not None:
            env = env or {}
            env['TERM'] = env.get('TERM') or os.getenv('TERM') or 'xterm-256color'

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
            bufsize=self.bufsize,
            pause=self.pause,
            http_client=self.http_client,
            stop_timeout=self.stop_timeout,
            kill_timeout=self.kill_timeout,
        )


class WorkerMonitor:
    """
    Monitor a worker process in the foreground.
    """

    def __init__(
        self,
        url: str,
        bufsize: Optional[int] = None,
        pause: Optional[float] = None,
        http_client: Optional[BaseHttpClient] = None,
        stop_timeout: Optional[float] = None,
        kill_timeout: Optional[float] = None,
    ):
        self.http_client = http_client or RequestsHttpClient()
        self.pause = pause or 1.0
        self.stop_timeout = stop_timeout or 10.0
        self.kill_timeout = kill_timeout or 2.0
        self.bufsize = bufsize or 4096
        self.client = WorkerClient(
            url=url,
            bufsize=bufsize,
            http_client=self.http_client,
        )
        self._done = threading.Event()
        self._interrupted_at: Optional[float] = None

    def run(
        self,
        stdin: Optional[Union[BinaryIO, Console]] = None,
        stdout: Optional[BinaryIO] = None,
        tty: Optional[bool] = None,
    ) -> int:
        if stdin is None:
            console = None
        elif isinstance(stdin, Console):
            console = stdin
        elif stdin.isatty():
            if tty is not False:
                console = LineConsole(stdin=stdin, max_wait=self.pause)
            else:
                console = RawConsole(stdin=stdin, bufsize=self.bufsize, max_wait=self.pause)
        else:
            console = RawConsole(stdin=stdin, bufsize=self.bufsize, max_wait=self.pause)

        # signals_thread = threading.Thread(target=self._receive_signals, args=(console,), daemon=True)
        # signals_thread.start()

        if console:
            stdin_thread = threading.Thread(target=self._receive_stdin, args=(console,), daemon=True)
            stdin_thread.start()
        else:
            stdin_thread = None

        try:
            with console:
                while not self._done.is_set():
                    result = self.client.poll()
                    if result.returncode is not None:
                        return result.returncode

                    if result.stdout and stdout:
                        stdout.write(result.stdout)
                        stdout.flush()
                    if len(result.stdout) < self.bufsize:
                        time.sleep(self.pause)
        finally:
            self._done.set()
            # signals_thread.join(timeout=self.kill_timeout)
            # if signals_thread.is_alive():
            #     raise RuntimeError('failed to kill signals thread')
            if stdin_thread:
                stdin_thread.join(timeout=self.kill_timeout)
                if stdin_thread.is_alive():
                    raise RuntimeError('failed to kill stdin thread')

    def _receive_signals(self, console: Console) -> None:
        try:
            while not self._done.is_set():
                try:
                    sig = console.get_signal()
                except EOFError:
                    break

                if sig is None:
                    continue

                self.client.send_signal(sig.signum, sig.tty_size)
        except KeyboardInterrupt:
            pass

    def _receive_stdin(self, console: Console) -> None:
        try:
            while not self._done.is_set():
                try:
                    data = console.read()
                except EOFError:
                    self.client.write_stdin(b'')
                else:
                    if data:
                        self.client.write_stdin(data)
        except KeyboardInterrupt:
            pass
