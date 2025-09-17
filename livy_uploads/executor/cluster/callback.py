#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Code to receive callbacks from dynamically created workers.
'''

__all__ = ('CallbackServer', 'CallbackHandler', 'CallbackClient')


import json
import logging
import time
import socket
import hmac
import hashlib
import secrets
import base64
from typing import Dict, Optional, Type

from livy_uploads.executor.cluster.http import HttpBaseServer, HttpBaseHandler, HttpBuiltinClient, HttpBaseClient, parse_entity
from livy_uploads.executor.cluster.model import WorkerInfo, HttpCert, ServerCert
from livy_uploads.executor.cluster.utils import assert_type
from livy_uploads.executor.cluster.certs import CertManager


LOGGER = logging.getLogger(__name__)


class CallbackServer(HttpBaseServer):
    """
    A server to receive callbacks from dynamically created workers.
    """

    WELL_KNOWN = b'well-known/callback'

    def __init__(
        self,
        port: Optional[int] = 0,
        bind_address: Optional[str] = '0.0.0.0',
        hostname: Optional[str] = None,
        domain: Optional[str] = None,
        pause: Optional[float] = None,
        timeout: Optional[float] = None,
        cert_manager: Optional[CertManager] = None,
    ):
        '''
        Args:
            port: The port to listen on. If 0, a free port will be chosen.
            bind_address: The address to bind to. If not provided, defaults to `0.0.0.0`.
            hostname: The advertised hostname. If not provided, the FQDN will be used.
            domain: The domain to use for the server. If not provided, the FQDN will be used.
            pause: The pause time to wait between polling the worker info.
            timeout: The timeout to wait for the worker to be ready.
            cert_manager: The cert manager to use to create the certificates for the workers and for this server.
        '''
        super().__init__(
            RequestHandlerClass=CallbackHandler,
            port=port,
            bind_address=bind_address,
            hostname=hostname,
            domain=domain,
        )
        self.infos: Dict[str, WorkerInfo] = {}
        self.pause = pause or 0.3
        self.timeout = timeout or 20.0
        self.cert_manager = cert_manager
        self._secret = secrets.token_bytes(64)

    def setup(self) -> None:
        super().setup()

        if self.cert_manager:
            self.cert_manager.setup()
            self.cert = self.cert_manager.make_server_cert(self.hostname, mtls=False)

    def close(self) -> None:
        if self.cert_manager:
            self.cert_manager.close()
        super().close()

    def get_worker_secret(self, name: str) -> bytes:
        """
        Derives a token for the named worker.
        """
        h = hashlib.sha512(self._secret)
        h.update(self.WELL_KNOWN)
        h.update(name.encode('utf-8'))
        return h.digest()

    @classmethod
    def build_token(cls, worker_secret: bytes, url: str) -> bytes:
        """
        Builds an auth token for the given worker.
        """
        return hmac.new(worker_secret, url.encode('utf-8'), hashlib.sha512).digest()

    def verify_token(self, auth_token: bytes, name: str, url: str):
        """
        Verifies an auth token for the given worker.
        """
        worker_secret = self.get_worker_secret(name)
        expected_token = self.build_token(worker_secret, url)
        if not hmac.compare_digest(auth_token, expected_token):
            raise PermissionError('Invalid token')

    def handle_info(self, info: WorkerInfo, auth_token: bytes) -> WorkerInfo:
        self.verify_token(auth_token, info.name, info.url)
        if info.name in self.infos:
            raise PermissionError('already registered')

        LOGGER.info('received callback info from %s: %s', info.name, info)
        info = info._replace(master_url=self.url)
        if info.cert:
            if not self.cert_manager:
                raise IOError('cert manager not set')
            cert_data = self.cert_manager.sign_request(
                csr=info.cert.csr,
                conf=info.cert.conf,
            )
            worker_cert = info.cert._replace(cert=cert_data)
            info = info._replace(cert=worker_cert)

        self.infos[info.name] = info
        return info

    def get_info(self, name: str) -> Optional[WorkerInfo]:
        t0 = time.time()
        while True:
            if time.time() - t0 > self.timeout:
                return None
            info = self.infos.get(name)
            if info:
                time.sleep(2.0)
                return info
            time.sleep(self.pause)


class CallbackHandler(HttpBaseHandler):
    """
    Handles HTTP requests for a callback server.

    Routes:
    - `POST /info`: Receives the info from the worker.
    - `GET /ping`: Gets a 200 OK pong response.
    """

    server: CallbackServer

    def do_GET(self) -> None:
        if self.url.path == '/ping':
            self.send_entity('pong')
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        if self.url.path == '/info':
            auth_header = self.headers.get('Authorization')
            auth_token_b64 = auth_header and auth_header.startswith('Bearer ') and auth_header[len('Bearer '):]
            if auth_token_b64:
                auth_token = base64.b64decode(auth_token_b64)
            else:
                auth_token = None
            if not auth_token:
                self.send_error(401)
                return

            data = self.rfile.read(int(self.headers['Content-Length']))
            body = assert_type(json.loads(data), dict)
            info = WorkerInfo.fromdict(body)
            try:
                info = self.server.handle_info(info, auth_token)
                self.send_entity(info.asdict())
            except PermissionError as e:
                self.send_entity(str(e), status=403)
                return
            self.send_entity(None)
        else:
            self.send_error(404)


class CallbackClient:
    """
    A client for sending the status from a worker to the callback server.
    """

    def __init__(
        self,
        server_url: str,
        worker_name: str,
        worker_secret: bytes,
        timeout: Optional[float] = None,
        proxy: Optional[str] = None,
        http_client_class: Type[HttpBaseClient] = HttpBuiltinClient,
        cert_manager: Optional[CertManager] = None,
        client_name: str = 'worker@localhost',
    ):
        self.server_url = server_url
        self.worker_name = worker_name
        self.worker_secret = worker_secret
        if cert_manager:
            cert_manager.setup()
            cert = cert_manager.make_client_cert(client_name, mtls=False)
        else:
            cert = None
        self.http_client = http_client_class(
            timeout=timeout or 10.0,
            proxy=proxy or None,
            cert=cert,
        )

    def send_info(self, info: WorkerInfo) -> WorkerInfo:
        url = f'{self.server_url}/info'
        auth_token = CallbackServer.build_token(self.worker_secret, info.url)
        auth_token_b64 = base64.b64encode(auth_token).decode('utf-8')
        headers = {'Authorization': f'Bearer {auth_token_b64}'}
        body = json.dumps(info.asdict()).encode('utf-8')
        response = self.http_client.post(url, body, headers=headers)
        if not response.ok:
            if response.status == 403:
                raise PermissionError(f'Failed to send info to {self.server_url}: {response.status}')  
            raise IOError(f'Failed to send info to {self.server_url}: {response.status}')

        kwargs = parse_entity(response.data, dict)
        return WorkerInfo.fromdict(kwargs)
