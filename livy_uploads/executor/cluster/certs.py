#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Code to manage the certificates for the server and workers.
'''

__all__ = ('CertManager',)

import logging
from pathlib import Path
import re
import ssl
import subprocess
import tempfile
import textwrap
from typing import Optional, List, Tuple, NamedTuple, Type, TypeVar

from livy_uploads.executor.cluster.model import ServerCert, ClientCert


HOSTNAME_PATTERN = re.compile(r'^[a-zA-Z0-9.-]+$')

LOGGER = logging.getLogger(__name__)


C = TypeVar('C', ServerCert, ClientCert)


class CertManager:
    """
    Manages the certificates for the server and workers.
    """

    def __init__(
        self,
        basedir: Optional[str] = None,
        ca_data: Optional[str] = None,
        key_size: int = 2048,
    ):
        self.basedir = Path(basedir or 'var') / 'certs'
        self.key_size = key_size
        self._client_ca_data = ca_data or None
        if self._client_ca_data and '-----BEGIN CERTIFICATE-----' not in self._client_ca_data:
            raise ValueError('Invalid CA data: not a base64-encoded certificate')
        self._ca_setup = False

    def setup(self) -> None:
        self.basedir = self.basedir.absolute()
        self.basedir.mkdir(parents=True, exist_ok=True)

        if not self._ca_setup:
            if not self._client_ca_data:
                self._make_ca()
            else:
                self.ca_path.write_text(self._client_ca_data)

            self._ca_setup = True

    @property
    def tmp_prefix(self) -> str:
        """
        Prefix for temporary files.
        """
        return str(self.basedir / 'tmp-cert-')

    def close(self) -> None:
        pass

    @property
    def client_mode(self) -> bool:
        """
        Whether the CA is in client mode.
        """
        return self._client_ca_data is not None

    @property
    def ca_path(self) -> Path:
        """
        Path to the CA certificate.
        """
        return self.basedir / 'ca.pem'

    @property
    def ca_data(self) -> str:
        """
        The CA data.
        """
        return self.ca_path.read_text()

    @property
    def key_path(self) -> Path:
        """
        Path to the CA key.
        """
        if self.client_mode:
            raise ValueError('CA key is not available in client mode')
        return self.basedir / 'ca.key'

    def make_key(self, name: str) -> Path:
        """
        Creates a new client RSA key.
        """
        basename = self._get_basename(name)
        key_path = self.basedir / f'cert-{basename}.key'

        LOGGER.info('creating new key at %s', key_path)
        subprocess.run([
            'openssl', 'genrsa', '-out', str(key_path), str(self.key_size),
        ], check=True, stdout=subprocess.DEVNULL)

        return key_path

    def make_request(self, name: str, hostnames: Optional[List[str]] = None, ips: Optional[List[str]] = None) -> Tuple[Path, Path, Path, Path]:
        """
        Creates a new certificate request.

        Args:
            name: The name of the certificate.
            hostnames: The hostnames of the certificate.
            ips: The IPs of the certificate.

        Returns:
            A tuple of the (certificate request path, key path, configuration path, certificate path)
        """
        basename = self._get_basename(name)

        hostnames = hostnames or []
        ips = ips or []
        if '@' not in name and name not in hostnames:
            hostnames = [name] + hostnames

        key_path = self.make_key(basename)
        conf_path = self.basedir / f'cert-{basename}.cnf'
        csr_path = self.basedir / f'cert-{basename}.csr'
        cert_path = self.basedir / f'cert-{basename}.pem'

        LOGGER.info('creating certificate configuration at %s', conf_path)

        # Build alt names section
        alt_names = []
        for i, hostname in enumerate(hostnames):
            alt_names.append(f'DNS.{i+1} = {hostname}')
        for i, ip in enumerate(ips):
            alt_names.append(f'IP.{i+1} = {ip}')

        # Only include subjectAltName if we have alt names
        v3_req_section = textwrap.dedent('''
            [v3_req]
            basicConstraints = CA:FALSE
            keyUsage = nonRepudiation, digitalSignature, keyEncipherment
        ''')
        
        if alt_names:
            LOGGER.info('adding alt names for %r: %s', name, alt_names)
            v3_req_section += 'subjectAltName = @alt_names\n'
        else:
            LOGGER.info('no alt names for %r', name)
        
        conf = textwrap.dedent(f'''
            [req]
            default_bits = 2048
            default_md = sha256
            prompt = no
            distinguished_name = req_dn
            x509_extensions = v3_req

            [req_dn]
            C = US
            ST = California
            L = San Francisco
            O = Livy
            OU = Livy
            CN = {name}

            {v3_req_section}
        ''')
        
        if alt_names:
            conf += '\n[alt_names]\n'
            conf += '\n'.join(alt_names) + '\n'

        conf_path.write_text(conf)

        LOGGER.info('creating new CSR at %s', csr_path)
        subprocess.run([
            'openssl', 'req', '-new', '-key', str(key_path), '-out', str(csr_path), '-config', str(conf_path),
        ], check=True, stdout=subprocess.DEVNULL)

        return csr_path, key_path, conf_path, cert_path

    def sign_request(self, csr: str, conf: str) -> str:
        """
        Signs a certificate request.

        Args:
            csr: The contents of the certificate request.
            conf: The contents of the configuration file.

        Returns:
            The contents of the signed certificate.
        """
        if '-----BEGIN CERTIFICATE REQUEST-----' not in csr:
            raise ValueError('Invalid CSR: not a base64-encoded CSR')
        if '[req]' not in conf:
            raise ValueError('Invalid configuration: not a valid OpenSSL configuration')

        if not self._ca_setup:
            raise RuntimeError('CA is not setup')
        if self.client_mode:
            raise RuntimeError('signing certificates with the CA is not available in client mode')

        with tempfile.TemporaryDirectory(prefix=self.tmp_prefix) as tmpdir:
            csr_path = Path(tmpdir) / 'csr.pem'
            conf_path = Path(tmpdir) / 'conf.pem'
            cert_path = Path(tmpdir) / 'cert.pem'

            csr_path.write_text(csr)
            conf_path.write_text(conf)

            LOGGER.info('signing request at %s and generating certificate at %s', csr_path, cert_path)
            subprocess.run([
                'openssl', 'x509', '-req',
                '-in', str(csr_path), '-out', str(cert_path),
                '-CA', str(self.ca_path), '-CAkey', str(self.key_path), '-CAcreateserial',
                '-days', '365', '-extensions', 'v3_req',
                '-extfile', str(conf_path),
            ], check=True, stdout=subprocess.DEVNULL)

            return cert_path.read_text()

    def get_cert(self, name: str) -> ClientCert:
        """
        Gets a client certificate for non-mTLS contexts.
        """
        return ClientCert(
            name=name,
            ca_data=self.ca_data,
            cert_path=None,
            key_path=None,
        )

    def get_mtls_cert(self, t: Type[C], name: str, cert: str) -> C:
        """
        Gets a client certificate for mTLS contexts.

        The key must have been generated beforehand.
        """
        basename = self._get_basename(name)
        key_path = self.basedir / f'cert-{basename}.key'
        if not key_path.exists():
            raise ValueError(f'Key file not found at {key_path!r}')

        cert_path = self.basedir / f'cert-{basename}.pem'
        cert_path.touch(0o600)
        cert_path.chmod(0o600)
        cert_path.write_text(cert)
        return t(
            name=name,
            ca_data=self.ca_data,
            cert_path=cert_path,
            key_path=key_path,
        )

    def make_cert(self, name: str, hostnames: Optional[List[str]] = None, ips: Optional[List[str]] = None) -> Tuple[Path, Path]:
        """
        Creates a new key, CSR and signed certificate.

        Args:
            name: The name of the certificate.
            hostnames: The hostnames of the certificate.
            ips: The IPs of the certificate.

        Returns:
            A tuple of the certificate path and the key path.
        """
        csr_path, key_path, conf_path, cert_path = self.make_request(name, hostnames, ips)

        cert = self.sign_request(csr_path.read_text(), conf_path.read_text())
        cert_path.write_text(cert)

        return cert_path, key_path

    def make_client_cert(self, name: str, mtls: bool) -> ClientCert:
        """
        Creates a new client certificate.

        Args:
            name: The name of the certificate.

        Returns:
            A new client certificate.
        """
        if not mtls:
            return ClientCert(name=name, cert_path=None, key_path=None, ca_data=self.ca_data)

        cert_path, key_path = self.make_cert(name=name)
        return ClientCert(name=name, cert_path=cert_path, key_path=key_path, ca_data=self.ca_data)

    def make_server_cert(self, hostname: str, mtls: bool, hostnames: Optional[List[str]] = None, ips: Optional[List[str]] = None) -> ServerCert:
        """
        Creates a new server certificate.

        Args:
            hostname: The hostname of the certificate.
            hostnames: The hostnames of the certificate.
            ips: The IPs of the certificate.

        Returns:
            A tuple of the certificate path and the key path.
        """
        hostnames = list({hostname, 'localhost', '*.localhost', *(hostnames or [])})
        ips = list({*(ips or []), '127.0.0.1'})
        cert_path, key_path = self.make_cert(name=hostname, hostnames=hostnames, ips=ips)
        if mtls:
            return ServerCert(name=hostname, cert_path=cert_path, key_path=key_path, ca_data=self.ca_data)
        else:
            return ServerCert(name=hostname, cert_path=cert_path, key_path=key_path, ca_data=None)

    def _get_basename(self, name: str) -> str:
        basename = name.replace('@', '-')
        if not HOSTNAME_PATTERN.match(basename):
            raise ValueError(f'Invalid name: {name!r}')

        return basename

    def _make_ca(self):
        ca_conf_path = self.basedir / 'ca.cnf'
        ca_conf_path.write_text(textwrap.dedent('''
            [req]
            default_bits = 2048
            default_md = sha256
            prompt = no
            distinguished_name = req_dn
            x509_extensions = v3_req

            [req_dn]
            C = US
            ST = California
            L = San Francisco
            O = Livy
            OU = Livy
            CN = livy.ai

            [v3_req]
            basicConstraints = CA:TRUE
            keyUsage = keyCertSign, cRLSign
        '''))

        subprocess.run([
            'openssl', 'req', '-x509', '-new', '-nodes', '-keyout', str(self.key_path), '-out', str(self.ca_path), '-days', '365', '-config', str(ca_conf_path),
        ], check=True, stdout=subprocess.DEVNULL)
