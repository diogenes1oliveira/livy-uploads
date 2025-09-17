from contextlib import ExitStack
from pathlib import Path
import socket
from uuid import uuid4

import pytest
import requests

from livy_uploads.executor.cluster.model import HttpCert, ServerCert, ClientCert
from livy_uploads.executor.cluster.certs import CertManager
from livy_uploads.executor.cluster.http import HttpBaseServer, HttpBaseHandler, HttpBuiltinClient, HttpRequestsClient


class TestCertManager:

    def setup_method(self):
        self.stack = ExitStack()

    def teardown_method(self):
        self.stack.close()

    @pytest.mark.parametrize('hostname', ['localhost', socket.getfqdn()])
    def test_server_no_mtls(self, hostname: str, client1mgr: CertManager, server1mgr: CertManager, server2mgr: CertManager, tmp_path: Path):
        server1 = self._start_server(server1mgr, hostname, mtls=False)
        server2 = self._start_server(server2mgr, hostname, mtls=False)

        cert_no_mtls = client1mgr.get_cert('test@example.com')
        client_no_mtls = HttpRequestsClient(cert=cert_no_mtls, basedir=tmp_path/'cert-no-mtls')

        csr_path, _, conf_path, _ = client1mgr.make_request('test@example.com')
        cert_data = server1mgr.sign_request(csr=csr_path.read_text(), conf=conf_path.read_text())
        cert_mtls = client1mgr.get_mtls_cert(ClientCert, 'test@example.com', cert=cert_data)
        client_mtls = HttpRequestsClient(cert=cert_mtls, basedir=tmp_path/'cert-mtls')

        client_no_ca = HttpRequestsClient(cert=None, basedir=tmp_path/'cert-no-ca')

        # should work without client certificate
        response = client_no_mtls.get(server1.url + '/ping')
        assert response == (200, b'/ping')

        # should not work with other server
        with pytest.raises(requests.exceptions.SSLError):
            client_mtls.get(server2.url + '/ping')

        # should not work without CA
        with pytest.raises(requests.exceptions.SSLError):
            client_no_ca.get(server1.url + '/ping')

    @pytest.mark.parametrize('hostname', ['localhost', socket.getfqdn()])
    def test_server_mtls(self, hostname: str, client1mgr: CertManager, server1mgr: CertManager, server2mgr: CertManager, tmp_path: Path):
        server1 = self._start_server(server1mgr, hostname, mtls=True)
        server2 = self._start_server(server2mgr, hostname, mtls=True)

        cert_no_mtls = client1mgr.get_cert('test@example.com')
        client_no_mtls = HttpRequestsClient(cert=cert_no_mtls, basedir=tmp_path/'cert-no-mtls')

        csr_path, _, conf_path, _ = client1mgr.make_request('test@example.com')
        cert_data = server1mgr.sign_request(csr=csr_path.read_text(), conf=conf_path.read_text())
        cert_mtls = client1mgr.get_mtls_cert(ClientCert, 'test@example.com', cert=cert_data)
        client_mtls = HttpRequestsClient(cert=cert_mtls, basedir=tmp_path/'cert-mtls')

        client_no_ca = HttpRequestsClient(cert=None, basedir=tmp_path/'cert-no-cert')

        # should work with client certificate
        response = client_mtls.get(server1.url + '/ping')
        assert response == (200, b'/ping')

        # should not work with other server
        with pytest.raises(requests.exceptions.SSLError):
            client_mtls.get(server2.url + '/ping')

        # should not work without client certificate
        with pytest.raises(requests.exceptions.SSLError):
            client_no_mtls.get(server1.url + '/ping')

        # should not work without CA
        with pytest.raises(requests.exceptions.SSLError):
            client_no_ca.get(server1.url + '/ping')

    def _start_server(self, cert_manager: CertManager, hostname: str, mtls: bool):
        cert = cert_manager.make_server_cert(hostname, mtls=mtls)
        server = DummyServer(hostname=hostname, cert=cert)
        server.start()
        self.stack.callback(server.close)
        return server

    @pytest.fixture()
    def server1mgr(self, tmp_path: Path):
        manager = CertManager(basedir=str(tmp_path / 'server1'))
        manager.setup()
        try:
            yield manager
        finally:
            manager.close()

    @pytest.fixture()
    def server2mgr(self, tmp_path: Path):
        manager = CertManager(basedir=str(tmp_path / 'server2'))
        manager.setup()
        try:
            yield manager
        finally:
            manager.close()

    @pytest.fixture()
    def client1mgr(self, tmp_path: Path, server1mgr: CertManager):
        manager = CertManager(basedir=str(tmp_path / 'client1'), ca_data=server1mgr.ca_data)
        manager.setup()
        try:
            yield manager
        finally:
            manager.close()

    @pytest.fixture()
    def client2mgr(self, tmp_path: Path, server2mgr: CertManager):
        manager = CertManager(basedir=str(tmp_path / 'client2'), ca_data=server2mgr.ca_data)
        manager.setup()
        try:
            yield manager
        finally:
            manager.close()


class DummyServer(HttpBaseServer):
    def __init__(self, hostname: str, cert: HttpCert):

        class DummyHandler(HttpBaseHandler):
            def do_GET(self):
                self.send_entity(self.url.path)

        super().__init__(
            RequestHandlerClass=DummyHandler,
            cert=cert,
            hostname=hostname,
        )

