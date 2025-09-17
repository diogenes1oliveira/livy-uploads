#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Data models for cluster operations.
'''

__all__ = ('WorkerInfo', 'PollResult', 'ServerCert', 'ClientCert', 'WorkerCert')


from pathlib import Path
from typing import NamedTuple, Optional, Mapping, TypeVar
from urllib.parse import urlparse

from livy_uploads.executor.cluster.utils import assert_type


T = TypeVar('T')



class ServerCert(NamedTuple):
    """
    A certificate for an HTTP server.
    """
    name: str
    'The common name of the server'
    cert_path: Path
    'The TLS certificate file'
    key_path: Path
    'The TLS key file'
    ca_data: Optional[str] = None
    'The CA certificate contents to authenticate clients'

    @property
    def mtls(self) -> bool:
        """
        Whether the certificate is for mutual TLS.

        This implies ca_data to be set.
        """
        return self.ca_data is not None


class ClientCert(NamedTuple):
    """
    A certificate for an HTTP client.
    """
    name: str
    'The common name of the client'
    ca_data: str
    'The CA certificate contents to verify the server'
    cert_path: Optional[Path] = None
    'The TLS certificate file to use for the client'
    key_path: Optional[Path] = None
    'The TLS key file to use for the client'

    @property
    def mtls(self) -> bool:
        """
        Whether the certificate is for mutual TLS.

        This implies cert_path and key_path to be set.
        """
        if self.cert_path:
            if not self.key_path:
                raise ValueError('Client certificate path is set but key path is not set')
            return True
        return False


class HttpCert(NamedTuple):
    """
    A client certificate.
    """
    name: str
    "Main name"
    cert: str
    "Client certificate"
    key: str
    "Client key"
    ca: str
    "CA certificate"
    csr: str
    "CSR contents"
    conf: str
    "Configuration file contents"

    @classmethod
    def fromdict(cls, kwargs: Mapping) -> 'HttpCert':
        name = assert_type(kwargs['name'], str)
        cert = assert_type(kwargs['cert'], str)
        key = assert_type(kwargs['key'], str)
        ca = assert_type(kwargs['ca'], str)
        csr = assert_type(kwargs.get('csr'), Optional[str]) or ''
        conf = assert_type(kwargs.get('conf'), Optional[str]) or ''
        return cls(name=name, cert=cert, key=key, ca=ca, csr=csr, conf=conf)

    def asdict(self) -> dict:
        return dict(self._asdict())


class WorkerCert(NamedTuple):
    """
    Worker certificate.
    """
    name: str
    "Worker main process name"
    csr: str
    "Contents of the CSR"
    conf: str
    "Contents of the OpenSSL configuration file"
    cert: Optional[str] = None
    "Contents of the certificate"

    @classmethod
    def fromdict(cls, kwargs: Mapping) -> 'WorkerCert':
        return cls(
            name=assert_type(kwargs['name'], str),
            csr=assert_type(kwargs['csr'], str),
            conf=assert_type(kwargs['conf'], str),
            cert=assert_type(kwargs.get('cert') or None, Optional[str]) or None,
        )

    def asdict(self) -> dict:
        return dict(self._asdict())


class WorkerInfo(NamedTuple):
    """
    Worker process information.
    """
    name: str
    "Worker main process name"
    pid: int
    "OS PID of the worker process"
    url: str
    "Advertised URL of the worker server"
    master_url: Optional[str] = None
    "URL of the master server"
    cert: Optional[WorkerCert] = None
    "Worker certificate"

    @property
    def hostname(self) -> str:
        return urlparse(self.url).hostname

    @classmethod
    def fromdict(cls, kwargs: Mapping) -> 'WorkerInfo':
        cert_kwargs = assert_type(kwargs.get('cert'), Optional[dict]) or None
        return cls(
            name=assert_type(kwargs['name'], str),
            pid=assert_type(kwargs['pid'], int),
            url=assert_type(kwargs['url'], str),
            cert=WorkerCert.fromdict(cert_kwargs) if cert_kwargs else None,
            master_url=assert_type(kwargs.get('master_url'), Optional[str]) or None,
        )

    def asdict(self) -> dict:
        d = dict(self._asdict())
        if self.cert:
            d['cert'] = self.cert.asdict()
        return d


class PollResult(NamedTuple):
    """
    Result of a worker process poll request.
    """
    stdout: bytes
    "Stdout content"
    returncode: Optional[int]
    "Return code of the worker process"

