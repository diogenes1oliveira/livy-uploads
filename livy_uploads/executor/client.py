#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Client for the Livy executor.
"""

__all__ = ('LivyExecutorClient',)

import logging
from typing import Optional, List, Mapping

from livy_uploads.executor.cluster import WorkerClient
from livy_uploads.executor.commands import (
    LivyPrepareMaster,
    LivyStartProcess,
)
from livy_uploads.session import LivySession


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
        stop_timeout: Optional[float] = None,
    ):
        '''
        Args:
            callback_port: The port to listen on for the callback server. If 0, a free port will be chosen.
            callback_hostname: The advertised master hostname. If not provided, the FQDN will be used.
            bind_address: The address to bind to. If not provided, defaults to `0.0.0.0`.
            pause: The pause time to wait for data in the command output.
            log_dir: The directory to write the logs to. If not provided, uses a `var/log` directory.
            stop_timeout: The timeout to wait for the worker to stop.
            bufsize: The buffer size to use for reading the command output.
        '''
        self.session = session
        self.callback_port = callback_port or 0
        self.callback_hostname = callback_hostname or None
        self.bind_address = bind_address or '0.0.0.0'
        self.pause = pause or 1.0
        self.log_dir = log_dir or 'var/log'
        self.stop_timeout = stop_timeout or 10.0
        self.bufsize = bufsize or 4096

    def setup(self):
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
        )
