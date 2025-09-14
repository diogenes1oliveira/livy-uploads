#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Code to receive callbacks from dynamically created workers.
'''

__all__ = ('CallbackServer', 'CallbackHandler', 'CallbackClient')


import json
import logging
import time
from typing import Dict, Optional

from livy_uploads.executor.cluster.http import HttpBaseServer, HttpBaseHandler, HttpBuiltinClient, HttpBaseClient
from livy_uploads.executor.cluster.model import WorkerInfo
from livy_uploads.executor.cluster.utils import assert_type


LOGGER = logging.getLogger(__name__)


class CallbackServer(HttpBaseServer):
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


class CallbackHandler(HttpBaseHandler):
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


class CallbackClient:
    """
    A client for sending the status from a worker to the callback server.
    """

    def __init__(
        self,
        url: str,
        http_client: Optional[HttpBaseClient] = None,
    ):
        self.url = url
        self.http_client = http_client or HttpBuiltinClient()

    def send_info(self, info: WorkerInfo) -> None:
        url = f'{self.url}/info'
        response = self.http_client.post(url, json.dumps(info.asdict()).encode('utf-8'))
        if not response.ok:
            raise IOError(f'Failed to send info to {self.url}: {response.status}')

