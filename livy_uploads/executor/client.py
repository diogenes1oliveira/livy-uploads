#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Client for the Livy executor.
"""

__all__ = ('LivyExecutorClient',)

import logging
from typing import Any, Optional, List, Mapping

from livy_uploads.executor.cluster import WorkerClient
from livy_uploads.executor.commands import (
    LivyPrepareMaster,
    LivyStartProcess,
)
from livy_uploads.session import LivySession
from livy_uploads.utils import assert_type
from livy_uploads.retry_policy import TimeoutRetryPolicy


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
        self.bufsize = bufsize or 4096
        self.proxy = proxy or None

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
        tty: Optional[bool] = None,
        worker_port: Optional[int] = 0,
        worker_hostname: Optional[str] = None,
        bind_address: Optional[str] = '0.0.0.0',
    ) -> WorkerClient:
        '''
        Args:
            command: The command to run.
            args: The arguments to pass to the command.
            env: Override environment variables for the command.
            cwd: The working directory to run the command in. If the directory does not exist, it will be created.
            stdin: Whether to enable stdin in the process.
            worker_port: The port the worker server will listen on. If 0, a free port will be chosen.
            worker_hostname: The advertised worker hostname. If not provided, the FQDN will be used.
            bind_address: The address to bind to. If not provided, defaults to `0.0.0.0`.
        '''
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
        ))
        LOGGER.info('got worker info: %s', info)

        return WorkerClient(
            url=info.url,
            pause=self.pause,
            tty=tty,
            stop_timeout=self.stop_timeout,
            bufsize=self.bufsize,
            proxy=self.proxy,
        )
