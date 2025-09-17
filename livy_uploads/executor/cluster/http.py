#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HTTP utilities for the executor cluster.
"""

__all__ = ('HttpBaseServer', 'HttpBaseHandler', 'HttpBaseClient', 'HttpBuiltinClient', 'HttpRequestsClient', 'parse_entity')


from abc import ABC, abstractmethod
import collections.abc
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
from pathlib import Path
import ssl
import socket
from socketserver import ThreadingMixIn
import threading
import time
from typing import Optional, Mapping, Type, Union, TypeVar, NamedTuple, TYPE_CHECKING
from urllib.parse import ParseResult, urlparse, parse_qsl
from urllib.request import Request, build_opener, ProxyHandler, HTTPHandler, HTTPSHandler
import urllib.error

if TYPE_CHECKING:
    import requests

from livy_uploads.executor.cluster.model import ServerCert, ClientCert

LOGGER = logging.getLogger(__name__)

T = TypeVar('T')
B = TypeVar('B', str, bytes, dict)


CERT_PREFIX = '-----BEGIN CERTIFICATE-----'
KEY_PREFIX = '-----BEGIN PRIVATE KEY-----'


class HttpResponse(NamedTuple):
    """
    Result of an HTTP request.
    """
    status: int
    "HTTP status code"
    data: Optional[bytes]
    "Response body"

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def parse(self, type: Type[B]) -> Optional[B]:
        if not self.ok:
            raise IOError(f'Bad status code: {self.status}')

        return parse_entity(self.data, type)


class HttpBaseServer(ThreadingMixIn, HTTPServer):
    """
    Base class for HTTP servers with some extra features.
    """

    allow_reuse_address = True

    def __init__(
        self,
        RequestHandlerClass: Type[BaseHTTPRequestHandler],
        port: Optional[int] = 0,
        hostname: Optional[str] = None,
        bind_address: Optional[str] = '0.0.0.0',
        domain: Optional[str] = None,
        cert: Optional[ServerCert] = None,
    ):
        super().__init__(
            server_address=(bind_address or '0.0.0.0', port or 0),
            RequestHandlerClass=RequestHandlerClass,
            bind_and_activate=False,
        )
        self._cert = cert
        self._hostname = hostname or None
        self._domain = domain or None
        self._serve_thread: Optional[threading.Thread] = None

    @property
    def cert(self) -> Optional[ServerCert]:
        """
        The certificate for the server.
        """
        return self._cert

    @cert.setter
    def cert(self, cert: ServerCert) -> None:
        """
        Sets the certificate for the server.
        """
        if self._serve_thread:
            raise RuntimeError('Cannot change the certificate after the server has started')
        self._cert = cert

    @property
    def hostname(self) -> str:
        """
        The hostname advertised by the server.

        Defaults to the FQDN of the machine.
        """
        h = self._hostname or socket.getfqdn()
        if self._domain and '.' not in h:
            h += f'.{self._domain}'
        return h

    @property
    def tls(self) -> bool:
        """
        Whether the server is using TLS.
        """
        return self._cert is not None

    @property
    def url(self) -> str:
        """
        The advertised URL of the server, constructed from the hostname and port.
        """
        if not self.server_address:
            raise RuntimeError('Server port is not set yet')

        port = self.server_address[1]
        scheme = 'https' if self.tls else 'http'
        return f'{scheme}://{self.hostname}:{port}'

    def start(self) -> None:
        """
        Binds the server and starts it.
        """
        self.setup()

        if self.cert:
            LOGGER.info('enabling TLS')
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.load_cert_chain(str(self.cert.cert_path), str(self.cert.key_path))

            if self.cert.mtls:
                LOGGER.info('enabling client authentication')
                context.load_verify_locations(cadata=self.cert.ca_data)
                context.verify_mode = ssl.CERT_REQUIRED
            else:
                context.verify_mode = ssl.CERT_OPTIONAL

            self.socket = context.wrap_socket(self.socket, server_side=True)

        LOGGER.info('serving on %s', self.url)
        thread = threading.Thread(daemon=True, target=self.serve_forever)
        thread.start()
        self._serve_thread = thread
        time.sleep(1.0)

    def setup(self) -> None:
        """
        Binds the server and activates it.
        """
        # from the original constructor
        LOGGER.info('binding server')
        try:
            self.server_bind()
            self.server_activate()
        except:
            self.server_close()
            raise

    def close(self) -> None:
        """
        Shuts down the server if it's running.
        """
        if self._serve_thread:
            LOGGER.info('shutting down the server')
            self.shutdown()
            self._serve_thread.join(timeout=2.0)
            if self._serve_thread.is_alive():
                raise RuntimeError('Server thread did not shut down')
            self._serve_thread = None


class HttpBaseHandler(BaseHTTPRequestHandler):
    """
    Base class for HTTP handlers.
    """

    def send_entity(self, result: Optional[Union[str, bytes, Mapping]], status: Optional[int] = None) -> None:
        """
        Sends an arbitrary entity to the client.

        Args:
            result: The entity to send.

            - `None` will cause a 204 No Content response.
            - A string will be encoded to bytes and sent as `text/plain; charset=utf-8`.
            - A bytes will be sent as `application/octet-stream`.
            - A mapping will be encoded to JSON and sent as `application/json`.

            status: The status code to send. Defaults to 200 for non-`None` results.

        Raises:
            TypeError: non-supported type for result.
        """
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

    def read_entity(self, type: Type[B]) -> Optional[B]:
        """
        Reads and decodes the entity from the request body.
        """
        if 'Content-Length' not in self.headers:
            body = None
        else:
            length = int(self.headers['Content-Length'])
            body = self.rfile.read(length)

        return parse_entity(body, type)

    @property
    def url(self) -> ParseResult:
        """
        The parsed URL of the request.
        """
        try:
            return self._parsed_url
        except AttributeError:
            self._parsed_url = urlparse(self.path)
            return self._parsed_url

    @property
    def params(self) -> Mapping[str, str]:
        """
        The query parameters of the request.
        """
        try:
            return self._query_params
        except AttributeError:
            self._query_params = dict(parse_qsl(self.url.query or ''))
            return self._query_params


class HttpBaseClient(ABC):
    """
    Base class for HTTP clients.
    """

    @abstractmethod
    def __init__(self, *, timeout: Optional[float] = None, proxy: Optional[str] = None, cert: Optional[ClientCert] = None):
        """
        Keyword-only required constructor.

        Args:
            timeout: The total timeout for each request.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, url: str, headers: Optional[Mapping[str, str]] = None) -> HttpResponse:
        """
        Executes a GET request.
        """
        raise NotImplementedError

    @abstractmethod
    def post(self, url: str, data: Optional[bytes] = None, headers: Optional[Mapping[str, str]] = None) -> HttpResponse:
        """
        Executes a POST request.
        """
        raise NotImplementedError


def _create_client_context(cert: Optional[ClientCert]) -> Optional[ssl.SSLContext]:
    if not cert:
        return None

    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.load_verify_locations(cadata=cert.ca_data)
    context.verify_mode = ssl.CERT_REQUIRED
    if cert.mtls:
        context.load_cert_chain(str(cert.cert_path), str(cert.key_path))

    return context


class HttpBuiltinClient(HttpBaseClient):
    """
    An HTTP client that uses the built-in `urllib.request` module.
    """

    def __init__(self, *, timeout: Optional[float] = None, proxy: Optional[str] = None, cert: Optional[ClientCert] = None):
        self.timeout = timeout or 3.0
        self.proxy = proxy
        self._cert = cert

    @property
    def opener(self):
        if not hasattr(self, '_opener'):
            context = _create_client_context(self._cert)
            self._opener = build_opener(
                ProxyHandler({'http': self.proxy, 'https': self.proxy} if self.proxy else {}),
                HTTPHandler(debuglevel=1),
                HTTPSHandler(debuglevel=1, context=context),
            )
        return self._opener

    def get(self, url: str, headers: Optional[Mapping[str, str]] = None) -> HttpResponse:
        request = Request(url=url, method='GET', headers=headers)
        with self.opener.open(request, timeout=self.timeout) as response:
            return self._parse_response(response)

    def post(self, url: str, data: Optional[bytes] = None, headers: Optional[Mapping[str, str]] = None) -> HttpResponse:
        request = Request(url=url, data=data or None, method='POST', headers=headers)
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                return self._parse_response(response)
        except urllib.error.HTTPError as e:
            return HttpResponse(status=e.code, data=e.read())

    def _parse_response(self, response) -> HttpResponse:
        status = int(response.status)
        data: bytes = response.read()
        if status == 204 and not data:
            data = None
        return HttpResponse(status=status, data=data)


class HttpRequestsClient(HttpBaseClient):
    """
    An HTTP client that uses the `requests` library.
    """
    def __init__(
        self,
        *,
        timeout: Optional[float] = None,
        proxy: Optional[str] = None,
        cert: Optional[ClientCert] = None,
        basedir: Optional[Union[str, Path]] = None,
    ):
        import requests

        self.session = requests.Session()
        self.session.proxies = {'http': proxy, 'https': proxy} if proxy else {}
        self.timeout = timeout or 3.0

        if cert:
            basedir = Path(basedir or 'var') / 'certs'
            basedir.mkdir(parents=True, exist_ok=True)

            ca_path = basedir / 'ca.pem'
            ca_path.write_text(cert.ca_data)

            if CERT_PREFIX not in cert.ca_data:
                raise ValueError(f'CA file {cert.ca_path!r} is not a valid certificate')

            self.session.verify = str(ca_path)
            if cert.mtls:
                if CERT_PREFIX not in cert.cert_path.read_text():
                    raise ValueError(f'Certificate file {cert.cert_path!r} is not a valid certificate')
                if KEY_PREFIX not in cert.key_path.read_text():
                    raise ValueError(f'Key file {cert.key_path!r} is not a valid key')
                self.session.cert = (str(cert.cert_path), str(cert.key_path))

    def get(self, url: str, headers: Optional[Mapping[str, str]] = None) -> HttpResponse:
        response = self.session.get(url, timeout=self.timeout, headers=headers)
        return self._parse_response(response)

    def post(self, url: str, data: Optional[bytes] = None, headers: Optional[Mapping[str, str]] = None) -> HttpResponse:
        response = self.session.post(url, data=data, timeout=self.timeout, headers=headers)
        return self._parse_response(response)

    def _parse_response(self, response) -> HttpResponse:
        status = int(response.status_code)
        data = bytes(response.content or b'')
        if status == 204 and not data:
            data = None
        return HttpResponse(status=status, data=data)


def parse_entity(body: Optional[bytes], type: Type[B]) -> Optional[B]:
    """
    Reads and decodes the entity from the request body.
    """
    if type not in (str, bytes, dict):
        raise TypeError(f'Invalid type: {type}')

    if body is None:
        return None

    if not body:
        if type is str:
            return ''
        elif type is bytes:
            return b''
        elif type is dict:
            return None

    if type is str:
        return body.decode('utf-8')
    elif type is bytes:
        return body
    elif type is dict:
        text = body.decode('utf-8').strip()
        if not text:
            return None
        return json.loads(text)
