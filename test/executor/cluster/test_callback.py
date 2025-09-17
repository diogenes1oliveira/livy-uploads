import logging
import json
import threading
import time
from urllib.request import urlopen, Request
from pathlib import Path

import pytest

from livy_uploads.executor.cluster import WorkerServer, CallbackServer, WorkerInfo, WorkerCert
from livy_uploads.executor.cluster.callback import CallbackClient
from livy_uploads.executor.cluster.certs import CertManager


LOGGER = logging.getLogger(__name__)


class TestCallbackServer:
    def test_token(self, server_http: CallbackServer):
        worker_name = 'test1'
        worker_url = 'http://example.com:1234'
        worker_secret = server_http.get_worker_secret(name=worker_name)
        auth_token = CallbackServer.build_token(worker_secret, url=worker_url)
        assert auth_token is not None

        assert server_http.verify_token(auth_token, worker_name, worker_url) is None
        with pytest.raises(PermissionError):
            server_http.verify_token(auth_token, worker_name, 'http://example.com:1235')

    def test_happy_register(self, server_http: CallbackServer):
        t0 = time.monotonic()
        assert server_http.get_info('test') is None
        assert time.monotonic() - t0 >= 2.0

        worker_name = 'test1'
        worker_secret = server_http.get_worker_secret(name=worker_name)

        client = CallbackClient(
            server_url=server_http.url,
            worker_name=worker_name,
            worker_secret=worker_secret,
        )

        worker_url = 'http://example.com:1234'
        info = WorkerInfo(
            name=worker_name,
            pid=1234,
            url=worker_url,
        )

        new_info = client.send_info(info)
        assert new_info is not None
        assert new_info.name == worker_name
        assert new_info.pid == 1234
        assert new_info.url == worker_url
        assert new_info.master_url == server_http.url

        with pytest.raises(PermissionError):
            client.send_info(info)

    def test_https_register(self, server_https: CallbackServer):
        worker_name = 'test1'
        worker_secret = server_https.get_worker_secret(name=worker_name)

        cert_manager = server_https.cert_manager
        client = CallbackClient(
            server_url=server_https.url,
            worker_name=worker_name,
            worker_secret=worker_secret,
            cert_manager=cert_manager,
        )

        csr_path, _, conf_path, _ = cert_manager.make_request(worker_name)

        worker_url = 'http://example.com:1234'
        info = WorkerInfo(
            name=worker_name,
            pid=1234,
            url=worker_url,
            cert=WorkerCert(
                name=worker_name,
                csr=csr_path.read_text(),
                conf=conf_path.read_text(),
            )
        )

        new_info = client.send_info(info)
        assert new_info is not None
        assert new_info.name == worker_name
        assert new_info.pid == 1234
        assert new_info.url == worker_url
        assert new_info.master_url == server_https.url
        assert new_info.cert.cert

        with pytest.raises(PermissionError):
            client.send_info(info)

    @pytest.fixture
    def server_http(self):
        server = CallbackServer(
            hostname='localhost',
            port=0,
            pause=0.1,
            timeout=2.0,
        )

        server.start()
        try:
            yield server
        finally:
            server.close()

    @pytest.fixture
    def cert_manager(self, tmp_path: Path):
        return CertManager(basedir=tmp_path)

    @pytest.fixture
    def server_https(self, cert_manager: CertManager):
        server = CallbackServer(
            hostname='localhost',
            port=0,
            pause=0.1,
            timeout=2.0,
            cert_manager=cert_manager,
        )

        server.start()
        try:
            yield server
        finally:
            server.close()
