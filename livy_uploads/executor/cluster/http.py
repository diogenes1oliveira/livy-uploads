#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HTTP utilities for the executor cluster.
"""

__all__ = ('HttpBaseServer', 'HttpBaseHandler', 'HttpBaseClient', 'HttpBuiltinClient', 'parse_entity')


from abc import ABC, abstractmethod
import collections.abc
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import socket
from socketserver import ThreadingMixIn
import threading
import time
from typing import Optional, Mapping, Type, Union, TypeVar
from urllib.parse import ParseResult, urlparse, parse_qsl
from urllib.request import Request, build_opener, ProxyHandler

try:
    import requests
except ImportError:
    requests = None

from livy_uploads.executor.cluster.model import HttpResponse


LOGGER = logging.getLogger(__name__)

T = TypeVar('T')
B = TypeVar('B', str, bytes, dict)


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
    ):
        super().__init__(
            server_address=(bind_address or '0.0.0.0', port or 0),
            RequestHandlerClass=RequestHandlerClass,
            bind_and_activate=False,
        )
        self._hostname = hostname or None
        self._serve_thread: Optional[threading.Thread] = None

    @property
    def hostname(self) -> str:
        """
        The hostname advertised by the server.

        Defaults to the FQDN of the machine.
        """
        return self._hostname or socket.getfqdn()

    @property
    def url(self) -> str:
        """
        The advertised URL of the server, constructed from the hostname and port.
        """
        if not self.server_address:
            raise RuntimeError('Server port is not set yet')

        port = self.server_address[1]
        return f'http://{self.hostname}:{port}'

    def start(self) -> None:
        """
        Binds the server and starts it.
        """
        # from the original constructor
        LOGGER.info('binding server')
        try:
            self.server_bind()
            self.server_activate()
        except:
            self.server_close()
            raise

        LOGGER.info('serving on %s', self.url)
        thread = threading.Thread(daemon=True, target=self.serve_forever)
        thread.start()
        self._serve_thread = thread
        time.sleep(1.0)

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
    def __init__(self, *, timeout: Optional[float] = None):
        """
        Keyword-only required constructor.

        Args:
            timeout: The total timeout for each request.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, url: str) -> HttpResponse:
        """
        Executes a GET request.
        """
        raise NotImplementedError

    @abstractmethod
    def post(self, url: str, data: Optional[bytes] = None) -> HttpResponse:
        """
        Executes a POST request.
        """
        raise NotImplementedError


class HttpBuiltinClient(HttpBaseClient):
    """
    An HTTP client that uses the built-in `urllib.request` module.
    """

    def __init__(self, *, timeout: Optional[float] = None, proxy: Optional[str] = None):
        self.timeout = timeout or 3.0
        self.proxy = proxy
        self._opener = build_opener(ProxyHandler({'http': self.proxy, 'https': self.proxy} if self.proxy else {}))

    def get(self, url: str) -> HttpResponse:
        with self._opener.open(url, timeout=self.timeout) as response:
            return HttpResponse(status=response.status, data=response.read())

    def post(self, url: str, data: Optional[bytes] = None) -> HttpResponse:
        request = Request(url=url, data=data or None, method='POST')
        with self._opener.open(request, timeout=self.timeout) as response:
            return HttpResponse(status=response.status, data=response.read())



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
