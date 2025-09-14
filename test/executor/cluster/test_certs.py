from pathlib import Path

import pytest

from livy_uploads.executor.cluster.certs import CertManager


class TestCertManager:
    @pytest.fixture()
    def manager(self, tmp_path: Path):
        manager = CertManager(basedir=str(tmp_path))
        manager.setup()
        try:
            yield manager
        finally:
            manager.close()

    def test_client_certificate(self, manager: CertManager):
        csr_path, key_path = manager.make_request('test@localhost')
        cert_path = manager.sign_request(csr_path)
